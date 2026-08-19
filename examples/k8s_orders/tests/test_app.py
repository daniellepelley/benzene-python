"""The multi-transport entry point actually builds — three named legs, no cloud, no broker, no server.

The one thing this example claims is that HTTP + SQS + Kafka run in one process off one composition
root. That claim is only worth anything if the entry point is executed, so this builds the real
``WorkerHost`` with the SDKs stubbed out (``boto3``/``confluent_kafka``/``uvicorn`` are container
dependencies, not test dependencies) and then runs it to completion to prove the legs really do wind
each other down.
"""

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any

import pytest
from benzene.core import StopSignal, WorkerHost


@pytest.fixture
def stubbed_sdks(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stand in for the three container-only SDKs, recording what the example asked each one for."""
    recorded: dict[str, Any] = {}

    class SqsClient:
        """An always-empty queue: the loop long-polls, gets nothing, and checks its stop signal."""

        def receive_message(self, **kwargs: Any) -> dict[str, Any]:
            recorded["sqs_queue_url"] = kwargs["QueueUrl"]
            return {}

        def delete_message(self, **kwargs: Any) -> None: ...

    boto3 = types.ModuleType("boto3")
    boto3.client = lambda service, **kwargs: recorded.setdefault(  # type: ignore[attr-defined]
        "sqs_client", SqsClient()
    )

    class Consumer:
        """An always-idle topic: poll returns nothing, so the loop is driven by its stop signal."""

        def __init__(self, config: dict[str, Any]) -> None:
            recorded["kafka_config"] = config

        def subscribe(self, topics: list[str]) -> None:
            recorded["kafka_topics"] = topics

        def poll(self, timeout: float) -> None:
            return None

        def commit(self, message: Any = None) -> None: ...

        def close(self) -> None:
            recorded["kafka_closed"] = True

    confluent = types.ModuleType("confluent_kafka")
    confluent.Consumer = Consumer  # type: ignore[attr-defined]

    class Config:
        def __init__(self, app: Any, **kwargs: Any) -> None:
            recorded["uvicorn_kwargs"] = kwargs

    class Server:
        def __init__(self, config: Config) -> None:
            self.should_exit = False
            recorded["server"] = self

        async def serve(self) -> None:
            while not self.should_exit:
                await asyncio.sleep(0.01)

    uvicorn = types.ModuleType("uvicorn")
    uvicorn.Config = Config  # type: ignore[attr-defined]
    uvicorn.Server = Server  # type: ignore[attr-defined]

    for name, module in (("boto3", boto3), ("confluent_kafka", confluent), ("uvicorn", uvicorn)):
        monkeypatch.setitem(sys.modules, name, module)

    monkeypatch.setenv("PORT", "8081")
    monkeypatch.setenv("BENZENE_ORDERS_EVENTS_URL", "http://downstream.example")
    monkeypatch.setenv("BENZENE_SQS_EVENTS_QUEUE_URL", "https://sqs.example/events")
    monkeypatch.setenv("BENZENE_SQS_CONSUME_QUEUE_URL", "https://sqs.example/in")
    monkeypatch.setenv("BENZENE_KAFKA_TOPIC", "orders-events")
    monkeypatch.setenv("BENZENE_KAFKA_CONSUME_TOPIC", "orders-in")
    return recorded


def test_the_entry_point_builds_all_three_transports_off_one_composition_root(
    stubbed_sdks: dict[str, Any],
) -> None:
    from k8s_orders.app import build_orders_worker_host

    host = build_orders_worker_host()

    assert isinstance(host, WorkerHost)
    assert host.names == ("http", "sqs", "kafka")
    assert stubbed_sdks["uvicorn_kwargs"]["port"] == 8081
    assert stubbed_sdks["kafka_topics"] == ["orders-in"]
    # The consumer builder, not the example, is what keeps at-least-once honest.
    assert stubbed_sdks["kafka_config"]["enable.auto.commit"] is False


def test_one_leg_stopping_winds_the_whole_process_down(stubbed_sdks: dict[str, Any]) -> None:
    from k8s_orders.app import build_orders_worker_host

    host = build_orders_worker_host()

    async def sigterm_after_a_moment(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)
        stubbed_sdks["server"].should_exit = True  # stands in for uvicorn's SIGTERM handling

    host.add("fake-sigterm", sigterm_after_a_moment)
    asyncio.run(asyncio.wait_for(host.run(), timeout=10))  # returns => every leg wound down
    assert stubbed_sdks["kafka_closed"]  # the Kafka worker released its partition assignment
