"""The Kafka binding — inbound decode + consumer loop + outbound producer (no broker, no SDK).

Drives the real binding logic against duck-typed fakes: a record decodes to the Benzene envelope with
the topic lifted from the `topic` header, the consumer loop dispatches one record per scope and keeps
at-least-once honest (it commits a successful offset but seeks back to a failure rather than
committing past it), and the outbound sender forwards headers onto Kafka headers, maps a delivery
failure to service-unavailable and an unacknowledged flush to a timeout.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import types
from dataclasses import dataclass
from typing import Any

import benzene.kafka.consumer as consumer_module
import pytest
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.kafka import (
    DLT_ORIGINAL_OFFSET_HEADER,
    DLT_ORIGINAL_PARTITION_HEADER,
    DLT_ORIGINAL_TOPIC_HEADER,
    DLT_REASON_HEADER,
    TOPIC_HEADER,
    DeadLetterOptions,
    KafkaConsumerApp,
    KafkaMessageSender,
    build_kafka_consumer,
    decode_kafka_message,
    run_consumer_loop,
)
from benzene.kafka.testing import (
    FakeKafkaMessage,
    KafkaMessageBuilder,
    RecordingKafkaConsumer,
    RecordingKafkaProducer,
)
from benzene.results import Result, Status


@dataclass
class PlaceOrder:
    sku: str = ""


def _app(handler=None) -> BenzeneMessageApplication:
    async def default(request: PlaceOrder) -> Result:
        return Result.created({"sku": request.sku})

    registry = Registry().register("orders:place", handler or default, request_type=PlaceOrder)
    return BenzeneMessageApplication(registry, MiddlewarePipeline())


async def _flaky(request: PlaceOrder) -> Result:
    """Succeeds for the ``ok`` sku, fails for anything else (drives the loop's commit decisions)."""
    return Result.created({}) if request.sku == "ok" else Result.failure(Status.SERVICE_UNAVAILABLE)


def _n_polls(n: int) -> Any:
    """A ``should_continue`` that stops the loop after ``n`` polls."""
    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= n

    return should_continue


# --- inbound decode ----------------------------------------------------------------------------


def test_decode_lifts_topic_from_the_header_and_keeps_the_rest() -> None:
    message = (
        KafkaMessageBuilder("orders:place")
        .with_header("x-correlation-id", "c1")
        .with_body({"sku": "A"})
        .build()
    )
    envelope = decode_kafka_message(message)
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"] == {"x-correlation-id": "c1"}  # topic removed, rest preserved
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_tolerates_absent_headers_and_value() -> None:
    envelope = decode_kafka_message(FakeKafkaMessage(_headers=[], _value=b""))
    assert envelope == {"topic": "", "headers": {}, "body": ""}


# --- consumer dispatch + loop ------------------------------------------------------------------


def test_handle_message_runs_the_pipeline_and_maps_the_result() -> None:
    app = KafkaConsumerApp(_app())
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))
    assert result.status == Status.CREATED


def test_a_poison_record_never_raises() -> None:
    async def boom(_request: PlaceOrder) -> Result:
        raise RuntimeError("handler blew up")

    app = KafkaConsumerApp(_app(boom))
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))  # must not raise
    assert not result.is_successful  # the fault is a failure result, not a crash


def test_loop_commits_only_successful_offsets() -> None:
    async def flaky(request: PlaceOrder) -> Result:
        return (
            Result.created({})
            if request.sku == "ok"
            else Result.failure(Status.SERVICE_UNAVAILABLE)
        )

    app = KafkaConsumerApp(_app(flaky))
    good = KafkaMessageBuilder("orders:place").with_body({"sku": "ok"}).build()
    bad = KafkaMessageBuilder("orders:place").with_body({"sku": "bad"}).build()
    consumer = RecordingKafkaConsumer(records=[good, bad])

    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= 3  # two records + one empty poll

    asyncio.run(run_consumer_loop(app, consumer, should_continue=should_continue))
    # at-least-once: the good record's offset is committed, the failed one is left for redelivery.
    assert consumer.committed == [good]


def test_loop_never_commits_past_an_uncommitted_failure() -> None:
    # Kafka commits are watermarks, not per-message acks: committing the *later* record's offset
    # would also mark the earlier failed one consumed, silently dropping it. The loop must instead
    # seek back to the failure and never commit an offset beyond it.
    app = KafkaConsumerApp(_app(_flaky))
    bad = (
        KafkaMessageBuilder("orders:place", partition=3, offset=5).with_body({"sku": "bad"}).build()
    )
    good = (
        KafkaMessageBuilder("orders:place", partition=3, offset=6).with_body({"sku": "ok"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[bad, good])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(2)))

    assert consumer.committed == []  # committing record 6 would have buried the failure at 5
    assert [(t.topic, t.partition, t.offset) for t in consumer.seeks] == [("benzene", 3, 5)]


def test_loop_commits_the_failed_record_and_its_successor_after_redelivery() -> None:
    attempts = {"n": 0}

    async def fails_once(_request: PlaceOrder) -> Result:
        attempts["n"] += 1
        return Result.failure(Status.SERVICE_UNAVAILABLE) if attempts["n"] == 1 else Result.ok()

    app = KafkaConsumerApp(_app(fails_once))
    first = (
        KafkaMessageBuilder("orders:place", partition=0, offset=5).with_body({"sku": "x"}).build()
    )
    redelivered = (
        KafkaMessageBuilder("orders:place", partition=0, offset=5).with_body({"sku": "x"}).build()
    )
    later = (
        KafkaMessageBuilder("orders:place", partition=0, offset=6).with_body({"sku": "y"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[first, redelivered, later])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(3)))

    # The seek re-served offset 5; once it succeeded the partition unblocked and both commits landed.
    assert consumer.committed == [redelivered, later]
    assert [t.offset for t in consumer.seeks] == [5]


def test_a_failure_on_one_partition_does_not_block_another() -> None:
    app = KafkaConsumerApp(_app(_flaky))
    bad = (
        KafkaMessageBuilder("orders:place", partition=1, offset=9).with_body({"sku": "bad"}).build()
    )
    good = (
        KafkaMessageBuilder("orders:place", partition=2, offset=4).with_body({"sku": "ok"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[bad, good])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(2)))

    # Offsets are per-partition watermarks: partition 2's success cannot bury partition 1's failure.
    assert consumer.committed == [good]
    assert [t.partition for t in consumer.seeks] == [1]


def test_loop_with_commit_disabled_lets_the_caller_control_commits() -> None:
    async def failing(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE)

    app = KafkaConsumerApp(_app(failing))
    bad = KafkaMessageBuilder("orders:place", offset=11).with_body({"sku": "x"}).build()
    consumer = RecordingKafkaConsumer(records=[bad])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=lambda: bool(consumer.records),
            commit=False,
            on_result=lambda m, r: seen.append(r),
        )
    )
    assert len(seen) == 1 and not seen[0].is_successful  # dispatched, but the loop owns no offsets
    assert consumer.committed == []
    assert consumer.seeks == []


def test_loop_warns_when_a_record_is_left_for_redelivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = KafkaConsumerApp(_app(_flaky))
    bad = KafkaMessageBuilder("orders:place", offset=2).with_body({"sku": "bad"}).build()
    consumer = RecordingKafkaConsumer(records=[bad])

    with caplog.at_level(logging.WARNING, logger="benzene.kafka.consumer"):
        asyncio.run(
            run_consumer_loop(app, consumer, should_continue=lambda: bool(consumer.records))
        )

    # A poison record must not loop invisibly: the failure is logged even with no on_result wired.
    assert len(caplog.records) == 1
    logged = caplog.records[0].getMessage()
    assert "orders:place" in logged and "service-unavailable" in logged


def test_loop_runs_the_blocking_kafka_calls_via_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # poll/commit are synchronous confluent-kafka calls: run directly on the event loop, an idle
    # topic would spin without a single await point and starve every coroutine sharing the loop.
    routed: list[str] = []
    real_to_thread = asyncio.to_thread

    async def spy(func: Any, *args: Any, **kwargs: Any) -> Any:
        routed.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(consumer_module.asyncio, "to_thread", spy)

    app = KafkaConsumerApp(_app())
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    consumer = RecordingKafkaConsumer(records=[message])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=lambda: bool(consumer.records)))

    assert routed == ["poll", "commit"]
    assert consumer.committed == [message]


def test_seek_target_uses_the_real_topic_partition_when_the_sdk_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # confluent's Consumer.seek only accepts a genuine TopicPartition; without the SDK the loop
    # falls back to a same-shaped record so the binding still runs (and tests) with plain fakes.
    class _TopicPartition:
        def __init__(self, topic: str, partition: int, offset: int) -> None:
            self.topic, self.partition, self.offset = topic, partition, offset

    stub = types.ModuleType("confluent_kafka")
    stub.TopicPartition = _TopicPartition  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", stub)

    message = KafkaMessageBuilder(
        "orders:place", kafka_topic="orders-events", partition=2, offset=17
    ).build()
    target = consumer_module._seek_target(message)
    assert isinstance(target, _TopicPartition)
    assert (target.topic, target.partition, target.offset) == ("orders-events", 2, 17)


def test_loop_skips_records_carrying_a_broker_error() -> None:
    app = KafkaConsumerApp(_app())
    errored = FakeKafkaMessage(
        _headers=[(TOPIC_HEADER, b"orders:place")], _value=b"{}", _error="EOF"
    )
    consumer = RecordingKafkaConsumer(records=[errored])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=lambda: bool(consumer.records),
            on_result=lambda m, r: seen.append(r),
        )
    )
    assert seen == []  # the error record was skipped, never dispatched
    assert consumer.committed == []


# --- dead-letter bound (a poison record must not wedge the partition) ---------------------------


async def _always_unavailable(_request: PlaceOrder) -> Result:
    """A *transient*-looking failure that never clears — the poison record the bound exists for."""
    return Result.failure(Status.SERVICE_UNAVAILABLE)


async def _always_bad_request(_request: PlaceOrder) -> Result:
    """A *final* failure: deterministic, so a redelivery cannot possibly change the outcome."""
    return Result.failure(Status.BAD_REQUEST)


async def _bad_request_unless_ok(request: PlaceOrder) -> Result:
    """A final failure for the poison sku, a success for ``ok`` (so a successor can commit)."""
    return Result.created({}) if request.sku == "ok" else Result.failure(Status.BAD_REQUEST)


def _record(offset: int, *, partition: int = 0, sku: str = "x") -> Any:
    return (
        KafkaMessageBuilder("orders:place", partition=partition, offset=offset)
        .with_key(b"k1")
        .with_body({"sku": sku})
        .build()
    )


def _dlt_headers(produced: dict[str, Any]) -> dict[str, bytes]:
    return dict(produced["headers"])


def _offsets(messages: list[Any]) -> list[int]:
    return [m.offset() for m in messages]


def test_a_transient_failure_is_dead_lettered_only_after_the_attempt_bound() -> None:
    # The watermark fix seeks back to a failed record; unbounded, that re-serves a poison record
    # forever and the partition never advances. The bound ends it: after max_attempts deliveries of
    # the same (topic, partition, offset), the record is routed to the dead-letter seam and the
    # partition is unblocked.
    app = KafkaConsumerApp(_app(_flaky))  # transient for the poison sku, success for "ok"
    producer = RecordingKafkaProducer()
    poison = [_record(5), _record(5), _record(5)]  # the seek-backs the broker would re-serve
    successor = _record(6, sku="ok")
    consumer = RecordingKafkaConsumer(records=[*poison, successor])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(4),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=3),
        )
    )

    assert [t.offset for t in consumer.seeks] == [5, 5]  # attempts 1 and 2 seek back as before
    assert len(producer.produced) == 1  # the third failure routes it instead of seeking again
    # The partition advances: the dead-lettered record's own offset is committed, and its successor
    # is no longer buried behind a block that can never clear.
    assert _offsets(consumer.committed) == [5, 6]


def test_the_dead_letter_carries_the_original_bytes_and_the_diagnostic_headers() -> None:
    # Replay is the point of a dead-letter topic, so the record must round-trip unmodified: the
    # original key, value and headers, plus the four x-dlt-* diagnostics.
    app = KafkaConsumerApp(_app(_always_bad_request))
    producer = RecordingKafkaProducer()
    poison = (
        KafkaMessageBuilder("orders:place", kafka_topic="orders", partition=7, offset=42)
        .with_header("x-correlation-id", "c1")
        .with_key(b"k1")
        .with_body({"sku": "A"})
        .build()
    )
    consumer = RecordingKafkaConsumer(records=[poison])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(1),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=1),
        )
    )

    (produced,) = producer.produced
    assert produced["topic"] == "orders.DLT"
    assert produced["key"] == b"k1"
    assert json.loads(produced["value"]) == {"sku": "A"}  # the wire body, verbatim
    headers = _dlt_headers(produced)
    assert headers["x-correlation-id"] == b"c1"  # the original headers ride along
    assert headers[TOPIC_HEADER] == b"orders:place"
    assert headers[DLT_REASON_HEADER] == b"bad-request"  # the status, never an exception message
    assert headers[DLT_ORIGINAL_TOPIC_HEADER] == b"orders"
    assert headers[DLT_ORIGINAL_PARTITION_HEADER] == b"7"
    assert headers[DLT_ORIGINAL_OFFSET_HEADER] == b"42"
    assert producer.flushed == [10.0]  # produce is fire-and-buffer: only a flush proves delivery


def test_a_final_status_is_dead_lettered_on_the_first_failure() -> None:
    # Consistency with the RabbitMQ consumer: a status outside DEFAULT_RETRYABLE is deterministic,
    # so spending the retry budget on it just delays the partition for no possible gain.
    app = KafkaConsumerApp(_app(_bad_request_unless_ok))
    producer = RecordingKafkaProducer()
    consumer = RecordingKafkaConsumer(records=[_record(5), _record(6, sku="ok")])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(2),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=3),
        )
    )

    assert consumer.seeks == []  # never re-served: a bad-request cannot become good
    assert len(producer.produced) == 1
    assert _offsets(consumer.committed) == [5, 6]


def test_the_attempt_count_is_per_offset_and_is_released_with_the_block() -> None:
    # The counter lives on the block entry itself, keyed by (topic, partition, offset): a
    # dead-lettered record takes its count with it, so the next record starts from a full budget
    # rather than inheriting a spent one (and nothing accumulates per record).
    app = KafkaConsumerApp(_app(_always_unavailable))
    producer = RecordingKafkaProducer()
    consumer = RecordingKafkaConsumer(records=[_record(5), _record(5), _record(6), _record(6)])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(4),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=2),
        )
    )

    assert _offsets(consumer.committed) == [5, 6]
    assert [t.offset for t in consumer.seeks] == [5, 6]  # one seek each, then each is routed
    assert len(producer.produced) == 2


def test_a_failed_dead_letter_produce_stops_the_loop_without_committing() -> None:
    # No-loss over availability (.NET's trade): if the record cannot be routed anywhere, committing
    # past it would drop it silently. Leave the offset uncommitted and stop, so a restart redelivers.
    app = KafkaConsumerApp(_app(_always_unavailable))
    producer = RecordingKafkaProducer(fail=True)
    consumer = RecordingKafkaConsumer(records=[_record(5), _record(6, sku="ok")])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(2),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=1),
        )
    )

    assert consumer.committed == []  # nothing may advance past a record that went nowhere
    assert len(consumer.records) == 1  # the loop returned; the successor was never polled


def test_an_unacknowledged_dead_letter_produce_is_a_failure_too() -> None:
    # produce() only buffers locally; a flush that leaves messages in flight means the broker never
    # acknowledged, so treating it as routed would lose the record exactly as a raise would.
    app = KafkaConsumerApp(_app(_always_unavailable))
    producer = RecordingKafkaProducer(remaining=1)
    consumer = RecordingKafkaConsumer(records=[_record(5)])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(1),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=1),
        )
    )
    assert consumer.committed == []


def test_a_delivery_error_on_the_dead_letter_is_a_failure_too() -> None:
    app = KafkaConsumerApp(_app(_always_unavailable))
    producer = RecordingKafkaProducer(delivery_error="broker refused")
    consumer = RecordingKafkaConsumer(records=[_record(5)])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(1),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=1),
        )
    )
    assert consumer.committed == []


def test_without_dead_letter_options_a_poison_record_still_blocks_its_partition() -> None:
    # The bound is opt-in: with no seam configured there is nowhere to route the record, and
    # dropping it would be the silent data loss the watermark fix exists to prevent.
    app = KafkaConsumerApp(_app(_always_unavailable))
    consumer = RecordingKafkaConsumer(records=[_record(5), _record(5), _record(5), _record(5)])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(4)))

    assert consumer.committed == []
    assert [t.offset for t in consumer.seeks] == [5, 5, 5, 5]


def test_the_dead_letter_produce_runs_via_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # produce/flush are blocking librdkafka calls like poll/commit — never on the event loop.
    routed: list[str] = []
    real_to_thread = asyncio.to_thread

    async def spy(func: Any, *args: Any, **kwargs: Any) -> Any:
        routed.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(consumer_module.asyncio, "to_thread", spy)

    app = KafkaConsumerApp(_app(_always_bad_request))
    producer = RecordingKafkaProducer()
    consumer = RecordingKafkaConsumer(records=[_record(5)])

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=_n_polls(1),
            dead_letter=DeadLetterOptions(topic="orders.DLT", producer=producer, max_attempts=1),
        )
    )
    # No seek at all: the record was routed, then the partition committed past it.
    assert routed == ["poll", "_publish_dead_letter", "commit"]


def test_the_dead_letter_is_logged_at_error_with_the_destination(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = KafkaConsumerApp(_app(_always_bad_request))
    producer = RecordingKafkaProducer()
    consumer = RecordingKafkaConsumer(records=[_record(5)])

    with caplog.at_level(logging.ERROR, logger="benzene.kafka.consumer"):
        asyncio.run(
            run_consumer_loop(
                app,
                consumer,
                should_continue=_n_polls(1),
                dead_letter=DeadLetterOptions(
                    topic="orders.DLT", producer=producer, max_attempts=1
                ),
            )
        )

    logged = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(logged) == 1
    assert "orders.DLT" in logged[0] and "bad-request" in logged[0]


# --- outbound producer -------------------------------------------------------------------------


class _FakeProducer:
    """A duck-typed ``confluent_kafka.Producer``: ``produce`` + ``flush`` → messages still in flight."""

    def __init__(self, *, fail: bool = False, remaining: int = 0, deliver: bool = True) -> None:
        self.produced: list[dict[str, Any]] = []
        self.flushed: list[float] = []
        self._fail = fail
        self._remaining = remaining
        self._deliver = deliver

    def produce(self, topic, value, headers, on_delivery) -> None:
        self.produced.append({"topic": topic, "value": value, "headers": headers})
        if self._deliver:  # a broker that never acks leaves the callback unfired
            on_delivery("boom" if self._fail else None, None)

    def flush(self, timeout) -> int:
        self.flushed.append(timeout)
        return self._remaining


def test_sender_forwards_headers_and_tags_the_topic() -> None:
    producer = _FakeProducer()
    sender = KafkaMessageSender("orders-events", producer=producer)
    result = asyncio.run(
        sender.send_message("orders:created", {"sku": "A"}, headers={"x-correlation-id": "c1"})
    )
    assert result.is_successful
    record = producer.produced[0]
    assert record["topic"] == "orders-events"  # the physical Kafka topic
    header_map = {k: v.decode() for k, v in record["headers"]}
    assert header_map[TOPIC_HEADER] == "orders:created"  # Benzene topic carried in the header
    assert header_map["x-correlation-id"] == "c1"
    assert json.loads(record["value"].decode()) == {"sku": "A"}


def test_sender_maps_a_delivery_failure_to_service_unavailable() -> None:
    sender = KafkaMessageSender("orders-events", producer=_FakeProducer(fail=True))
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.SERVICE_UNAVAILABLE


def test_sender_maps_an_unacknowledged_flush_to_a_timeout() -> None:
    # Broker unreachable: produce() only buffers locally, flush times out with the message still in
    # flight and no delivery callback ever fires — reporting ok here would lose the message silently.
    producer = _FakeProducer(remaining=1, deliver=False)
    sender = KafkaMessageSender("orders-events", producer=producer, flush_timeout=0.25)
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.TIMEOUT
    assert producer.flushed == [0.25]  # the configured bound was actually passed to flush


def test_sender_is_successful_when_flush_drains_and_the_callback_fires() -> None:
    sender = KafkaMessageSender("orders-events", producer=_FakeProducer(remaining=0))
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.OK


def test_sender_without_the_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A missing optional dependency is a deployment error, not a per-message Result: it must escape
    # send_message's failure mapper so it can't be retried or tripped over by a circuit breaker.
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    sender = KafkaMessageSender("orders-events", bootstrap_servers="localhost:9092")
    with pytest.raises(ImportError, match=r"benzene-kafka\[kafka\]"):
        asyncio.run(sender.send_message("orders:created", {"sku": "A"}))


# --- the consumer builder --------------------------------------------------------------------------


def _stub_confluent(monkeypatch: Any, recorded: dict[str, Any]) -> None:
    """Stand in for confluent_kafka so the builder's config is assertable without the SDK."""

    class Consumer:
        def __init__(self, config: dict[str, Any]) -> None:
            recorded["config"] = config

        def subscribe(self, topics: list[str]) -> None:
            recorded["topics"] = topics

    module = types.ModuleType("confluent_kafka")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)


def test_build_kafka_consumer_disables_auto_commit_so_at_least_once_stays_true(
    monkeypatch: Any,
) -> None:
    # run_consumer_loop commits only successful offsets; auto-commit on would defeat redelivery,
    # and getting that wrong is silent - so the builder, not the caller, owns the setting.
    recorded: dict[str, Any] = {}
    _stub_confluent(monkeypatch, recorded)

    build_kafka_consumer(bootstrap_servers="broker:9092", group_id="orders", topics=["orders-in"])

    assert recorded["config"]["enable.auto.commit"] is False
    assert recorded["config"]["bootstrap.servers"] == "broker:9092"
    assert recorded["config"]["group.id"] == "orders"
    assert recorded["config"]["auto.offset.reset"] == "earliest"
    assert recorded["topics"] == ["orders-in"]


def test_build_kafka_consumer_lets_the_caller_win_on_any_setting(monkeypatch: Any) -> None:
    recorded: dict[str, Any] = {}
    _stub_confluent(monkeypatch, recorded)

    build_kafka_consumer(
        bootstrap_servers="broker:9092",
        group_id="orders",
        topics=["orders-in"],
        auto_offset_reset="latest",
        **{"enable.auto.commit": True, "session.timeout.ms": 45000},
    )

    assert recorded["config"]["auto.offset.reset"] == "latest"
    assert recorded["config"]["enable.auto.commit"] is True  # overridable, as every default must be
    assert recorded["config"]["session.timeout.ms"] == 45000
