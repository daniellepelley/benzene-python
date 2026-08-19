"""The multi-transport worker host — N long-lived legs, one event loop, one coordinated shutdown.

Covers the contract WorkerHost replaces the k8s_orders example's hand-rolled asyncio.gather with:
whichever leg finishes first winds the others down (cleanly or by crashing), every leg is actually
awaited to completion before run() returns, a crash still propagates for a non-zero exit, and a
misconfigured host (no workers, duplicate names) fails at start-up rather than at message time.

Also covers the per-transport worker factories (benzene.aws.sqs_consumer_worker,
benzene.kafka.kafka_consumer_worker, benzene.http.asgi_server_worker/uvicorn_worker) — each is a
closure over the public loop function it wraps, so the tests drive them against the same duck-typed
fakes those loops are tested with, with no queue, broker or ASGI server anywhere.
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass
from typing import Any

import pytest
from benzene.aws import SqsConsumerApp, sqs_consumer_worker
from benzene.aws.testing import RecordingSqsClient, SqsMessageBuilder
from benzene.core import (
    BenzeneMessageApplication,
    DuplicateWorkerError,
    MiddlewarePipeline,
    NoWorkersError,
    Registry,
    StopSignal,
    WorkerHost,
    background_worker,
)
from benzene.http import asgi_server_worker, uvicorn_worker
from benzene.kafka import KafkaConsumerApp, kafka_consumer_worker
from benzene.kafka.testing import KafkaMessageBuilder, RecordingKafkaConsumer
from benzene.results import Result


@dataclass
class PlaceOrder:
    sku: str = ""


def _application() -> BenzeneMessageApplication:
    async def place(request: PlaceOrder) -> Result:
        return Result.created({"sku": request.sku})

    registry = Registry().register("orders:place", place, request_type=PlaceOrder)
    return BenzeneMessageApplication(registry, MiddlewarePipeline())


def _polling_worker(name: str, log: list[str], *, poll_seconds: float = 0.01):
    """A leg that behaves like a consumer loop: polls until the host's stop signal says otherwise."""

    async def worker(stop: StopSignal) -> None:
        while stop.should_continue():
            await asyncio.sleep(poll_seconds)
        log.append(f"{name}:stopped")

    return worker


# --- coordinated shutdown -------------------------------------------------------------------------


def test_the_first_leg_to_finish_cleanly_winds_every_other_leg_down() -> None:
    log: list[str] = []

    async def short_lived(stop: StopSignal) -> None:
        await asyncio.sleep(0.01)
        log.append("http:returned")  # e.g. uvicorn returning after SIGTERM

    host = (
        WorkerHost()
        .add("http", short_lived)
        .add("sqs", _polling_worker("sqs", log))
        .add("kafka", _polling_worker("kafka", log))
    )
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert log[0] == "http:returned"
    assert sorted(log[1:]) == ["kafka:stopped", "sqs:stopped"]  # both siblings wound down


def test_a_crashing_leg_winds_the_others_down_and_still_propagates() -> None:
    # Kubernetes restarts the pod only if the process exits non-zero, so the crash must survive the
    # orderly shutdown of its siblings rather than being swallowed by it.
    log: list[str] = []

    async def crashing(stop: StopSignal) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("kafka broker went away")

    host = (
        WorkerHost()
        .add("sqs", _polling_worker("sqs", log))
        .add("kafka", crashing)
        .add("http", _polling_worker("http", log))
    )

    with pytest.raises(RuntimeError, match="kafka broker went away"):
        asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert sorted(log) == ["http:stopped", "sqs:stopped"]  # both siblings still wound down first


def test_run_waits_for_every_leg_to_finish_not_just_the_first() -> None:
    finished: list[str] = []

    async def quick(stop: StopSignal) -> None:
        finished.append("quick")

    async def slow_to_notice(stop: StopSignal) -> None:
        await stop.wait()
        await asyncio.sleep(0.05)  # a poll still in flight when the stop signal arrives
        finished.append("slow")

    host = WorkerHost().add("quick", quick).add("slow", slow_to_notice)
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert finished == ["quick", "slow"]  # run() returned only once the slow leg was really done


def test_a_leg_that_ignores_the_stop_signal_is_cancelled_at_the_shutdown_timeout() -> None:
    cancelled: list[str] = []

    async def quick(stop: StopSignal) -> None:
        return None

    async def deaf(stop: StopSignal) -> None:
        try:
            await asyncio.sleep(60)  # never checks stop
        except asyncio.CancelledError:
            cancelled.append("deaf")
            raise

    host = WorkerHost(shutdown_timeout=0.05).add("quick", quick).add("deaf", deaf)
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert cancelled == ["deaf"]  # bounded, not a hung process


def test_stopping_the_host_from_outside_winds_every_leg_down() -> None:
    log: list[str] = []
    host = (
        WorkerHost().add("sqs", _polling_worker("sqs", log)).add("http", _polling_worker("http", log))
    )

    async def scenario() -> None:
        running = asyncio.create_task(host.run())
        await asyncio.sleep(0.02)
        host.stop.set()  # the public escape hatch: anything can wind the whole host down
        await asyncio.wait_for(running, timeout=5)

    asyncio.run(scenario())
    assert sorted(log) == ["http:stopped", "sqs:stopped"]


# --- start-up checks ------------------------------------------------------------------------------


def test_a_host_with_no_workers_fails_at_start_up_naming_the_fix() -> None:
    # The alternative is a process that boots, looks healthy, and handles nothing.
    with pytest.raises(NoWorkersError) as error:
        asyncio.run(WorkerHost().run())

    message = str(error.value)
    assert "no workers" in message
    assert "host.add(" in message  # names what to add, not just that something is missing


def test_two_workers_with_the_same_name_fail_at_wiring_time() -> None:
    host = WorkerHost().add("sqs", _polling_worker("sqs", []))
    with pytest.raises(DuplicateWorkerError, match="already has a worker named 'sqs'"):
        host.add("sqs", _polling_worker("sqs", []))


def test_the_host_reports_its_workers_in_registration_order() -> None:
    host = WorkerHost().add("http", _polling_worker("http", [])).add("sqs", _polling_worker("sqs", []))
    assert host.names == ("http", "sqs")


# --- the stop signal itself -----------------------------------------------------------------------


def test_stop_signal_should_continue_is_the_inverse_of_is_set() -> None:
    stop = StopSignal()
    assert stop.should_continue() and not stop.is_set()
    stop.set()
    stop.set()  # idempotent
    assert stop.is_set() and not stop.should_continue()


# --- the SQS leg ----------------------------------------------------------------------------------


def test_sqs_worker_polls_until_the_host_stops_it() -> None:
    app = SqsConsumerApp(_application())
    message = (
        SqsMessageBuilder("orders:place").with_body({"sku": "A"}).with_receipt_handle("r1").build()
    )
    client = RecordingSqsClient(messages=[message])

    async def stop_once_drained(stop: StopSignal) -> None:
        while client.messages:
            await asyncio.sleep(0.01)

    host = (
        WorkerHost()
        .add("sqs", sqs_consumer_worker(app, client, "https://sqs.example/q", wait_time_seconds=0))
        .add("drain-watcher", stop_once_drained)
    )
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert client.deleted == ["r1"]  # the message was handled through the real loop


def test_sqs_worker_refuses_should_continue_rather_than_ignoring_it() -> None:
    app = SqsConsumerApp(_application())
    with pytest.raises(TypeError, match="run_sqs_consumer_loop"):
        sqs_consumer_worker(
            app, RecordingSqsClient(messages=[]), "https://sqs.example/q", should_continue=bool
        )


# --- the Kafka leg --------------------------------------------------------------------------------


def test_kafka_worker_consumes_and_closes_the_consumer_when_the_host_winds_down() -> None:
    app = KafkaConsumerApp(_application())
    record = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    consumer = RecordingKafkaConsumer(records=[record])

    async def stop_once_drained(stop: StopSignal) -> None:
        while consumer.records:
            await asyncio.sleep(0.01)

    host = (
        WorkerHost()
        .add("kafka", kafka_consumer_worker(app, consumer, poll_timeout=0))
        .add("drain-watcher", stop_once_drained)
    )
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert consumer.committed == [record]
    assert consumer.closed  # the finally the example used to hand-write


def test_kafka_worker_leaves_the_consumer_open_when_asked() -> None:
    app = KafkaConsumerApp(_application())
    consumer = RecordingKafkaConsumer(records=[])

    async def stop_immediately(stop: StopSignal) -> None:
        return None

    host = (
        WorkerHost()
        .add("kafka", kafka_consumer_worker(app, consumer, close=False, poll_timeout=0))
        .add("trigger", stop_immediately)
    )
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))
    assert not consumer.closed


def test_kafka_worker_refuses_should_continue_rather_than_ignoring_it() -> None:
    app = KafkaConsumerApp(_application())
    with pytest.raises(TypeError, match="run_consumer_loop"):
        kafka_consumer_worker(app, RecordingKafkaConsumer(records=[]), should_continue=bool)


# --- the HTTP leg ---------------------------------------------------------------------------------


class FakeAsgiServer:
    """Duck-typed against uvicorn.Server: serve() runs until should_exit flips."""

    def __init__(self) -> None:
        self.should_exit = False
        self.served = False

    async def serve(self) -> None:
        self.served = True
        while not self.should_exit:
            await asyncio.sleep(0.01)


def test_asgi_server_worker_tells_the_server_to_exit_when_a_sibling_stops() -> None:
    server = FakeAsgiServer()

    async def short_lived(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)

    host = WorkerHost().add("http", asgi_server_worker(server)).add("sqs", short_lived)
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert server.served and server.should_exit


def test_asgi_server_returning_on_its_own_signal_winds_the_siblings_down() -> None:
    # uvicorn owns SIGINT/SIGTERM on the main thread; serve() returning is how the host hears it.
    server = FakeAsgiServer()
    log: list[str] = []

    async def signal_after_a_moment(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)
        server.should_exit = True  # stands in for uvicorn's own signal handler
        await stop.wait()
        log.append("sqs:stopped")

    host = WorkerHost().add("http", asgi_server_worker(server)).add("sqs", signal_after_a_moment)
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert log == ["sqs:stopped"]


def test_uvicorn_worker_builds_the_server_from_config_and_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # uvicorn is an optional extra, so this drives the shorthand against a stand-in module: it must
    # build uvicorn.Server(uvicorn.Config(...)) and then be exactly asgi_server_worker(server).
    built: dict[str, Any] = {}

    class Config:
        def __init__(self, app: Any, **kwargs: Any) -> None:
            built["app"] = app
            built["kwargs"] = kwargs

    class Server(FakeAsgiServer):
        def __init__(self, config: Config) -> None:
            super().__init__()
            built["server"] = self

    fake = types.ModuleType("uvicorn")
    fake.Config = Config  # type: ignore[attr-defined]
    fake.Server = Server  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "uvicorn", fake)

    app = object()
    worker = uvicorn_worker(app, port=9001, log_level="warning")

    assert built["app"] is app
    assert built["kwargs"] == {"host": "0.0.0.0", "port": 9001, "log_level": "warning"}

    async def short_lived(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)

    host = WorkerHost().add("http", worker).add("sqs", short_lived)
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))
    assert built["server"].served and built["server"].should_exit


def test_uvicorn_worker_builds_the_server_when_wired_not_on_the_first_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A missing optional server must surface while the host is being built, naming the install and
    # the level below - never as an ImportError on the message path.
    monkeypatch.setitem(sys.modules, "uvicorn", None)
    with pytest.raises(ImportError) as error:
        uvicorn_worker(object())

    message = str(error.value)
    assert 'pip install "benzene-http[uvicorn]"' in message
    assert "asgi_server_worker" in message  # names the rung below


# --- background legs (the cancel-me shape) --------------------------------------------------------


def test_background_worker_is_cancelled_when_the_host_winds_down() -> None:
    # The shape every mesh entry point used to hand-roll: create_task(...) / finally: task.cancel().
    ticks: list[int] = []

    async def run_forever() -> None:
        while True:
            ticks.append(1)
            await asyncio.sleep(0.01)

    async def short_lived(stop: StopSignal) -> None:
        await asyncio.sleep(0.03)

    host = WorkerHost().add("http", short_lived).add("poller", background_worker(run_forever))
    asyncio.run(asyncio.wait_for(host.run(), timeout=5))

    assert ticks  # it really ran, and run() returned rather than hanging on a never-ending loop


def test_background_worker_does_not_schedule_anything_until_the_host_runs() -> None:
    started: list[str] = []

    async def run_forever() -> None:
        started.append("go")

    background_worker(run_forever)  # taking a callable, not a coroutine, means nothing starts here
    assert started == []


def test_a_background_leg_that_crashes_winds_the_others_down_and_propagates() -> None:
    log: list[str] = []

    async def failing_poller() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("discovery API refused the connection")

    host = (
        WorkerHost()
        .add("poller", background_worker(failing_poller))
        .add("http", _polling_worker("http", log))
    )

    with pytest.raises(RuntimeError, match="discovery API refused"):
        asyncio.run(asyncio.wait_for(host.run(), timeout=5))
    assert log == ["http:stopped"]
