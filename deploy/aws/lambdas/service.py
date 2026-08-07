"""Build one mesh domain service as an AWS Lambda handler (the AWS lift of ``deploy/mesh/services.py``).

Each of ``orders`` / ``payments`` / ``shipping`` becomes an :class:`~benzene.aws.AwsLambdaApp` fronted by
an HTTP API Gateway — the same domain the in-process and localhost stacks drive
(:mod:`mesh_fleet.domain`), now hosted on Lambda. A service:

- serves its routes + the Cloud Service Profile surfaces (``/benzene/invoke|health|spec``) through the
  API Gateway binding, so the Mesh Host's aggregator can poll its ``/benzene/spec`` + ``/benzene/health``
  once discovery has found it;
- traces every invocation (:func:`~benzene.mesh.trace_middleware`) into a buffer, and **pushes its mesh
  feeds to the host over HTTP** — ``register`` + ``heartbeat`` on cold start, a ``traces`` batch after
  every invocation — via a :class:`~benzene.mesh.MeshFeedSender` carrying the optional shared secret;
- calls its peers over another ``/benzene/invoke`` leg, wrapped in
  :class:`~benzene.mesh.TracePropagatingMessageSender` so the caller's span rides along.

**Everything is read from the environment** (the Terraform stack sets it), so the same image runs all
three functions — the Lambda's ``image_config.command`` selects which handler module is the entrypoint:

===============================  ==============================================================
Env var                          Meaning
===============================  ==============================================================
``BENZENE_MESH_HOST_URL``        the Mesh Host base URL; feeds POST to ``{url}/benzene/invoke``
``BENZENE_PEER_PAYMENTS_URL``    payments' API base URL (orders calls it)
``BENZENE_PEER_SHIPPING_URL``    shipping's API base URL (orders + payments call it)
``BENZENE_MESH_KEY``             the optional shared secret attached to every feed (else open)
``BENZENE_INSTANCE_ID``          this instance's id (defaults to ``AWS_LAMBDA_FUNCTION_NAME``)
===============================  ==============================================================

The feeds are fire-and-report (they never raise), and every feed call is additionally guarded here, so a
missing/unset host URL degrades to "no feeds" without ever failing a domain request — the mesh is
optional and additive, exactly as :mod:`benzene.mesh` promises.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from benzene.aws import AwsLambdaApp
from benzene.core import (
    BenzeneMessageApplication,
    HealthCheckResult,
    HealthChecks,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
)
from benzene.http import HttpRouter, InvokeMessageSender, StandardPaths
from benzene.mesh import (
    Heartbeat,
    MeshFeedSender,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    TracePropagatingMessageSender,
    trace_middleware,
)
from mesh_fleet.domain import (
    PAYMENT_CAPTURE_TOPIC,
    SHIPPING_BOOK_TOPIC,
    BookShipment,
    CapturePayment,
    CreateOrder,
    Order,
    OrderStore,
    make_book_shipment,
    make_capture_payment,
    make_create_order,
    make_get_all_orders,
)

ORDERS_CREATE_TOPIC = "orders:create"
ORDERS_GET_ALL_TOPIC = "orders:get-all"

# Which peer owns each outbound domain topic (used to resolve orders'/payments' peer invoke URLs).
_TOPIC_OWNER = {PAYMENT_CAPTURE_TOPIC: "payments", SHIPPING_BOOK_TOPIC: "shipping"}


@dataclass
class LambdaService:
    """A built Lambda service: its app + the feed machinery a wrapped handler drives."""

    name: str
    instance_id: str
    app: AwsLambdaApp
    descriptor: ServiceDescriptor
    exporter: QueueTraceExporter
    feeds: MeshFeedSender | None
    is_healthy: bool
    health_checks: dict[str, Any]

    async def announce(self) -> None:
        if self.feeds is None:
            return
        sent_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        await self.feeds.register(self.descriptor)
        await self.feeds.publish_heartbeat(
            Heartbeat(
                service=self.name,
                sent_at=sent_at,
                instance_id=self.instance_id,
                descriptor_hash=self.descriptor.descriptor_hash(),
                is_healthy=self.is_healthy,
                health_checks=self.health_checks,
            )
        )

    async def flush_traces(self) -> None:
        if self.feeds is None:
            return
        events = self.exporter.drain()
        if events:
            await self.feeds.publish_traces(events)


# --- environment helpers ------------------------------------------------------------------------
def _instance_id(default: str) -> str:
    return os.environ.get("BENZENE_INSTANCE_ID") or os.environ.get(
        "AWS_LAMBDA_FUNCTION_NAME", default
    )


def _host_invoke_url() -> str | None:
    base = os.environ.get("BENZENE_MESH_HOST_URL", "").strip().rstrip("/")
    return f"{base}/benzene/invoke" if base else None


def _peer_invoke_url_for(topic: str) -> str:
    owner = _TOPIC_OWNER[topic]
    base = os.environ.get(f"BENZENE_PEER_{owner.upper()}_URL", "").strip().rstrip("/")
    if not base:
        raise RuntimeError(f"No peer URL configured for {owner!r} (set BENZENE_PEER_{owner.upper()}_URL)")
    return f"{base}/benzene/invoke"


def _feed_sender() -> MeshFeedSender | None:
    url = _host_invoke_url()
    if url is None:
        return None  # no host configured → the service still serves traffic, just reports no feeds
    key = os.environ.get("BENZENE_MESH_KEY") or None
    return MeshFeedSender(InvokeMessageSender(lambda _topic: url), key=key)


def _peer_sender() -> MessageSender:
    return TracePropagatingMessageSender(InvokeMessageSender(_peer_invoke_url_for))


def _health(*checks: tuple[str, HealthCheckResult]) -> HealthChecks:
    registry = HealthChecks()
    for name, result in checks:
        registry.add(name, (lambda r: (lambda: r))(result))
    return registry


def _lambda_app(router: HttpRouter, service: str, instance_id: str, exporter: QueueTraceExporter,
                health: HealthChecks) -> AwsLambdaApp:
    registry = Registry.from_definitions(router)
    pipeline = MiddlewarePipeline(
        [trace_middleware(exporter=exporter, service=service, instance_id=instance_id)]
    )
    application = BenzeneMessageApplication(registry, pipeline)
    spec = ServiceSpec.derive(registry, service=service)
    standard = StandardPaths(invoke=True, health=health, spec=spec)
    return AwsLambdaApp(router, application=application, standard_paths=standard)


# --- the three services -------------------------------------------------------------------------
def build_orders() -> LambdaService:
    exporter = QueueTraceExporter()
    store = OrderStore()
    sender = _peer_sender()
    instance_id = _instance_id("orders-1")
    router = HttpRouter()
    router.register("POST", "/orders", ORDERS_CREATE_TOPIC, make_create_order(store, sender),
                    request_type=CreateOrder, response_type=Order)
    router.register("GET", "/orders", ORDERS_GET_ALL_TOPIC, make_get_all_orders(store),
                    response_type=list[Order])
    health = _health(
        ("PostgresDatabase", HealthCheckResult.healthy("latency 4ms")),
        ("RedisCache", HealthCheckResult.healthy("hit rate 0.97")),
    )
    registry = Registry.from_definitions(router)
    return LambdaService(
        name="orders", instance_id=instance_id,
        app=_lambda_app(router, "orders", instance_id, exporter, health),
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("orders", instance_id=instance_id)),
        exporter=exporter, feeds=_feed_sender(), is_healthy=True,
        health_checks={"PostgresDatabase": {"isHealthy": True}, "RedisCache": {"isHealthy": True}},
    )


def build_payments() -> LambdaService:
    exporter = QueueTraceExporter()
    sender = _peer_sender()
    instance_id = _instance_id("payments-1")
    router = HttpRouter()
    router.register("POST", "/payments/capture", PAYMENT_CAPTURE_TOPIC,
                    make_capture_payment(sender), request_type=CapturePayment)
    # payments is unhealthy: its gateway check fails, so /benzene/health reports 503.
    health = _health(
        ("PaymentsGateway", HealthCheckResult.unhealthy("gateway timeout")),
        ("PostgresDatabase", HealthCheckResult.healthy("latency 6ms")),
    )
    registry = Registry.from_definitions(router)
    return LambdaService(
        name="payments", instance_id=instance_id,
        app=_lambda_app(router, "payments", instance_id, exporter, health),
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("payments", instance_id=instance_id)),
        exporter=exporter, feeds=_feed_sender(), is_healthy=False,
        health_checks={"PaymentsGateway": {"isHealthy": False}, "PostgresDatabase": {"isHealthy": True}},
    )


def build_shipping() -> LambdaService:
    exporter = QueueTraceExporter()
    instance_id = _instance_id("shipping-1")
    router = HttpRouter()
    router.register("POST", "/shipping/book", SHIPPING_BOOK_TOPIC, make_book_shipment(),
                    request_type=BookShipment)
    health = _health(("ParcelApi", HealthCheckResult.healthy("ok")))
    registry = Registry.from_definitions(router)
    return LambdaService(
        name="shipping", instance_id=instance_id,
        app=_lambda_app(router, "shipping", instance_id, exporter, health),
        descriptor=ServiceDescriptor.derive(registry, ServiceInfo("shipping", instance_id=instance_id)),
        exporter=exporter, feeds=_feed_sender(), is_healthy=True,
        health_checks={"ParcelApi": {"isHealthy": True}},
    )


_BUILDERS: dict[str, Callable[[], LambdaService]] = {
    "orders": build_orders,
    "payments": build_payments,
    "shipping": build_shipping,
}


def make_handler(service_name: str) -> Callable[[dict[str, Any], Any], Any]:
    """Build a service once (at import/cold-start) and return the ``handler(event, context)`` Lambda calls.

    The returned handler: announces the service to the host on the first invocation (register +
    heartbeat, best-effort), runs the API Gateway request through the :class:`~benzene.aws.AwsLambdaApp`,
    then flushes the invocation's trace batch to the host — so the collector plane sees this Lambda's
    liveness and edges while the aggregator polls its spec/health. Feed failures are swallowed (the mesh
    is optional): a domain request never fails because the host is unreachable.
    """
    service = _BUILDERS[service_name]()
    announced = {"done": False}

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        if not announced["done"]:
            announced["done"] = True
            _run_best_effort(service.announce())
        response = service.app.handle(event, context)
        _run_best_effort(service.flush_traces())
        return response

    return handler


def _run_best_effort(coro: Any) -> None:
    with contextlib.suppress(Exception):  # a feed must never break a domain request
        asyncio.run(coro)
