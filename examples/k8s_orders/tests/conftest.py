"""The three container-only SDKs, stubbed — shared by this example's two test modules.

``boto3``/``confluent_kafka``/``uvicorn`` are deployment dependencies of this example, not test
dependencies: the point of the example is the *wiring*, and the wiring is only worth anything if it
is actually executed. Standing the three SDKs up as recording fakes lets both modules here build and
run the real :class:`~benzene.core.WorkerHost` off the real composition root, with no cloud, no
broker and no socket.
"""

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any

import pytest


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
