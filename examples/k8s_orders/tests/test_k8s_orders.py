"""Dogfooded, in-memory tests for the one-process, three-transport example.

Two halves, matching the two things this example actually claims:

1. **The shared-domain claim** — "all three transports dispatch into the same ``orders_domain``".
   Booted the same way every other example's suite boots: ``create_test_host(OrdersStartUp)``, fake
   only the outbound edge, and specialize with the single ``build_*()`` call — here three times over,
   which is exactly what ``app.py`` does in production with three real hosts.
2. **The crash claim** — "a consumer dying takes the whole process down, loudly". Driven through the
   *real* :func:`~k8s_orders.app.build_orders_worker_host` with the SDKs stubbed (``conftest.py``),
   so it is this example's three legs that are proven to wind down, not a hand-rolled stand-in.
   ``WorkerHost``'s supervision semantics in the abstract belong to ``tests/test_worker_host.py``;
   what is example-specific is that ``app.py``'s legs are wired into it correctly.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from benzene.core import MessageSender, StopSignal
from benzene.results import Status
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import (
    ORDER_CREATED_TOPIC,
    PLACE_ORDER_TOPIC,
    OrderEventLog,
    OrderService,
    OrdersStartUp,
)


def make_host(build: str):
    """Boot the real app from ``OrdersStartUp``, fake only the edge, specialize in ONE call.

    Byte-for-byte the setup of every other example suite — ``build`` is the only moving part, which
    is the whole point of this example: one composition root, three front doors.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)  # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    builder = create_test_host(OrdersStartUp).with_services(overrides)
    return getattr(builder, build)(), service, sender, seen


def test_all_three_apps_boot_from_the_shared_domain() -> None:
    """One order through each of the three front doors ``app.py`` opens; identical egress each time.

    HTTP is a request/response host; the SQS and Kafka legs are consumer loops (awaitable
    ``send_*``). Nothing in ``orders_domain`` knows which one called it — the assertion below is the
    same three lines for all three.
    """
    # 1. HTTP — the uvicorn leg.
    http_host, http_service, http_sender, _ = make_host("build_http")
    response = http_host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})
    assert response.status_code == 201
    order = json.loads(response.body)
    assert http_sender.last_topic == ORDER_CREATED_TOPIC
    assert http_sender.last_message.sku == "ABC"
    assert order["id"] in http_service.orders

    # 2. SQS — the self-hosted poller leg.
    sqs_host, sqs_service, sqs_sender, _ = make_host("build_sqs_consumer")
    result = asyncio.run(sqs_host.send_sqs_consumer(PLACE_ORDER_TOPIC, {"sku": "ABC", "quantity": 2}))
    assert result.status == Status.CREATED
    assert sqs_sender.last_topic == ORDER_CREATED_TOPIC
    assert sqs_sender.last_message.sku == "ABC"
    assert sqs_sender.last_message.id in sqs_service.orders

    # 3. Kafka — the consumer-loop leg.
    kafka_host, kafka_service, kafka_sender, _ = make_host("build_kafka")
    result = asyncio.run(kafka_host.send_kafka(PLACE_ORDER_TOPIC, {"sku": "ABC", "quantity": 2}))
    assert result.status == Status.CREATED
    assert kafka_sender.last_topic == ORDER_CREATED_TOPIC
    assert kafka_sender.last_message.sku == "ABC"
    assert kafka_sender.last_message.id in kafka_service.orders


def test_the_order_created_subscriber_is_reachable_from_both_consumer_legs() -> None:
    """The domain's own subscriber (``orders:created``) answers on both message legs.

    (Not the HTTP leg: ``OrdersStartUp`` mounts no route for that topic — it is a subscriber, so the
    queue and the broker are its front doors.)
    """
    sqs_host, _, _, sqs_seen = make_host("build_sqs_consumer")
    asyncio.run(sqs_host.send_sqs_consumer(ORDER_CREATED_TOPIC, {"id": "o-sqs", "sku": "ABC"}))

    kafka_host, _, _, kafka_seen = make_host("build_kafka")
    asyncio.run(kafka_host.send_kafka(ORDER_CREATED_TOPIC, {"id": "o-kafka", "sku": "ABC"}))

    assert (sqs_seen, kafka_seen) == (["o-sqs"], ["o-kafka"])


def test_a_crashing_leg_winds_this_example_down_and_still_exits_non_zero(
    stubbed_sdks: dict[str, Any],
) -> None:
    """A consumer dying stops the real HTTP/SQS/Kafka legs *and* propagates out of ``run()``.

    This is the claim ``app.py``'s docstring makes to a Kubernetes operator: no half-dead pod that
    keeps serving HTTP while consuming nothing. Asserted against the real host, so it also pins that
    the Kafka leg's ``finally`` still runs (its consumer is closed) when a sibling is what died.
    """
    from k8s_orders.app import build_orders_worker_host

    host = build_orders_worker_host()

    async def crashing_consumer(stop: StopSignal) -> None:
        await asyncio.sleep(0.02)
        raise RuntimeError("kafka consumer died")

    host.add("fake-crash", crashing_consumer)

    with pytest.raises(RuntimeError, match="kafka consumer died"):
        asyncio.run(asyncio.wait_for(host.run(), timeout=10))

    assert host.stop.is_set()  # every sibling was told to wind down
    assert stubbed_sdks["server"].should_exit is True  # ...including uvicorn
    assert stubbed_sdks["kafka_closed"]  # the Kafka leg released its partition assignment
