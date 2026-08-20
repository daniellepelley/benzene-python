"""Dogfooded, in-memory tests for the one-process, three-transport example.

Two halves, matching the two things this example actually claims:

1. **The concurrency claim** — "whichever leg finishes first winds the others down, and a crashed
   consumer takes uvicorn with it and exits non-zero". :func:`~k8s_orders.app.run_legs` is driven
   directly with duck-typed legs and a stand-in server, so the claim is enforced with no socket, no
   broker and no queue (and no ``uvicorn``/``confluent-kafka``/``boto3`` installed).
2. **The shared-domain claim** — "all three transports dispatch into the same ``orders_domain``".
   Booted the same way every other example's suite boots: ``create_test_host(OrdersStartUp)``, fake
   only the outbound edge, and specialize with the single ``build_*()`` call — here three times over,
   which is exactly what ``app.py`` does in production with three real hosts.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from benzene.core import MessageSender
from benzene.results import Status
from benzene.testing import FakeMessageSender, create_test_host
from k8s_orders.app import run_legs
from orders_domain import (
    ORDER_CREATED_TOPIC,
    PLACE_ORDER_TOPIC,
    OrderEventLog,
    OrderService,
    OrdersStartUp,
)


@dataclass
class FakeServer:
    """Stands in for ``uvicorn.Server`` — ``run_legs`` only ever touches ``should_exit``."""

    should_exit: bool = False


def test_one_leg_crashing_winds_down_the_others() -> None:
    """A consumer loop dying sets the shared stop, tells uvicorn to exit, and propagates non-zero.

    The two surviving legs park exactly where the real ones do — on the shared ``stop`` their
    ``should_continue`` predicate reads — so this pins the whole wind-down chain: crash → ``stop``
    set → ``server.should_exit`` → siblings leave their loops → the error still escapes ``main()``.
    """

    async def scenario() -> None:
        server = FakeServer()
        stop = asyncio.Event()
        wound_down: list[str] = []

        async def parked(name: str) -> None:
            # Stands in for a consumer loop between polls: it leaves on the next ``stop`` check.
            await stop.wait()
            wound_down.append(name)

        async def crashing_consumer() -> None:
            raise RuntimeError("kafka consumer died")

        http_leg = asyncio.create_task(parked("http"))
        sqs_leg = asyncio.create_task(parked("sqs"))

        with pytest.raises(RuntimeError, match="kafka consumer died"):
            await run_legs(server, stop, [http_leg, sqs_leg, crashing_consumer()])

        # The crash propagates (Kubernetes sees a non-zero exit) *and* winds the process down.
        assert stop.is_set()
        assert server.should_exit is True

        await asyncio.gather(http_leg, sqs_leg)  # both siblings left on the shared stop
        assert wound_down == ["http", "sqs"]

    asyncio.run(scenario())


def test_a_clean_leg_exit_also_winds_the_others_down() -> None:
    """The signal path: uvicorn returning from ``serve()`` stops the consumers just the same."""

    async def scenario() -> None:
        server = FakeServer()
        stop = asyncio.Event()

        async def serve() -> None:
            return None  # uvicorn returning after SIGTERM

        async def parked() -> None:
            await stop.wait()

        await run_legs(server, stop, [serve(), parked()])

        assert stop.is_set()
        assert server.should_exit is True

    asyncio.run(scenario())


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
