"""A configurable Benzene fleet service, deployed as one env-driven AWS Lambda.

One Lambda image serves any node in the demo fleet (``orders`` / ``inventory`` / ``notifications``);
the environment decides its identity, the topic it serves, the sibling it calls next, and where it
reports traces. Each service:

- exposes ``/benzene/spec`` + ``/benzene/health`` (so the mesh collector can **poll** it — identity,
  topics, health only; ``ServiceSpec`` has no ``consumes`` field),
- runs ``trace_middleware`` and, when ``NEXT_URL`` is set, calls the next service through a
  ``with_trace_propagation``-wrapped HTTP sender (so the ``traceparent`` crosses the hop), and
- when ``COLLECTOR_URL`` is set, **pushes** its ``ServiceDescriptor`` — ``consumes=[NEXT_TOPIC]`` when
  there's a next hop — once at startup, so the collector's consumer edge exists before a single request
  is served (mesh.md §4), then pushes each trace batch after every invocation to feed that edge's
  invocation/error stats (best-effort — a collector hiccup never fails the request).

Env: ``SERVICE_NAME``, ``SERVICE_TOPIC``, ``SERVICE_ROUTE`` (default ``/<name>``), ``NEXT_URL`` +
``NEXT_TOPIC`` (optional), ``COLLECTOR_URL`` (optional).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from benzene.aws import AwsLambdaApp, to_lambda_handler
from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    MessageSender,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
)
from benzene.http import HttpMessageSender, HttpRouter, StandardPaths
from benzene.mesh import (
    REGISTER_TOPIC,
    TRACES_TOPIC,
    MeshFeedSender,
    OutboundRegistry,
    QueueTraceExporter,
    ServiceDescriptor,
    ServiceInfo,
    trace_middleware,
    with_trace_propagation,
)
from benzene.results import Result


@dataclass
class FleetRequest:
    """The demo request payload — typed so ``/benzene/spec`` carries a real request schema."""

    sku: str = ""
    quantity: int = 1


@dataclass
class FleetReply:
    """The demo response payload — typed so ``/benzene/spec`` carries a real response schema."""

    service: str
    handled: bool = True


@dataclass
class ServiceConfig:
    name: str
    topic: str
    route: str
    next_url: str | None = None
    next_topic: str | None = None
    collector_url: str | None = None


def config_from_env(env: Mapping[str, str] | None = None) -> ServiceConfig:
    env = os.environ if env is None else env
    name = env["SERVICE_NAME"]
    return ServiceConfig(
        name=name,
        topic=env["SERVICE_TOPIC"],
        route=env.get("SERVICE_ROUTE", f"/{name}"),
        next_url=env.get("NEXT_URL") or None,
        next_topic=env.get("NEXT_TOPIC") or None,
        collector_url=env.get("COLLECTOR_URL") or None,
    )


@dataclass
class FleetService:
    """A wired fleet node: the Lambda app, its trace exporter, and an optional collector feed."""

    app: AwsLambdaApp
    exporter: QueueTraceExporter
    feeds: MeshFeedSender | None


def build_service(
    config: ServiceConfig,
    *,
    next_sender: MessageSender | None = None,
    feeds: MeshFeedSender | None = None,
) -> FleetService:
    """Build the service. ``next_sender``/``feeds`` are injectable for tests; production derives both
    from the env (``NEXT_URL``/``NEXT_TOPIC`` and ``COLLECTOR_URL`` respectively)."""
    exporter = QueueTraceExporter()

    if next_sender is None and config.next_url and config.next_topic:
        # NEXT_URL is the callee's exact route URL; map the topic straight to it so the sender POSTs
        # there (the callee's inbound route resolves that path back to the topic).
        next_sender = with_trace_propagation(
            HttpMessageSender({config.next_topic: config.next_url})
        )

    async def handle(request: FleetRequest) -> Result:
        if next_sender is not None and config.next_topic:
            await next_sender.send_message(
                config.next_topic, FleetRequest(sku=request.sku, quantity=request.quantity)
            )
        return Result.ok(FleetReply(service=config.name))

    router = HttpRouter().register(
        "POST",
        config.route,
        config.topic,
        handle,
        request_type=FleetRequest,
        response_type=FleetReply,
    )
    registry = Registry.from_definitions(router)
    pipeline = MiddlewarePipeline().use(
        trace_middleware(exporter, service=config.name, instance_id=config.name)
    )
    standard = StandardPaths(
        # Two named checks so the mesh-ui per-service page shows real per-check health detail.
        health=HealthChecks().add("core", lambda: True).add("trace-exporter", lambda: True),
        spec=ServiceSpec.derive(registry, service=config.name),
    )
    app = AwsLambdaApp(
        http_router=router,
        application=BenzeneMessageApplication(registry, pipeline),
        standard_paths=standard,
    )

    if feeds is None and config.collector_url:
        # Push register (once, below) to /mesh/register and each trace batch to /mesh/traces — one
        # sender, both routes (MeshFeedSender picks the route by topic).
        base = config.collector_url.rstrip("/")
        feeds = MeshFeedSender(
            HttpMessageSender(
                {REGISTER_TOPIC: f"{base}/mesh/register", TRACES_TOPIC: f"{base}/mesh/traces"}
            )
        )
    if feeds is not None:
        # What this service consumes (mesh.md §2.3) — the collector's consumer edge exists from this
        # one call, with zero traffic; it does not wait on (or come from) a trace.
        outbound = OutboundRegistry()
        if config.next_topic:
            outbound.register(config.next_topic)
        descriptor = ServiceDescriptor.derive(registry, ServiceInfo(service=config.name), outbound)
        with contextlib.suppress(Exception):  # register is best-effort, never fails startup
            asyncio.run(feeds.register(descriptor))
    return FleetService(app=app, exporter=exporter, feeds=feeds)


def lambda_handler_for(service: FleetService):
    """Return the ``handler(event, context)`` Lambda invokes, pushing traces after each invocation."""
    base = to_lambda_handler(service.app)

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        result = base(event, context)
        if service.feeds is not None:
            events = service.exporter.drain()
            if events:
                with contextlib.suppress(
                    Exception
                ):  # trace push is best-effort, never fails the request
                    asyncio.run(service.feeds.publish_traces(events))
        return result

    return handler


# The module-level entrypoint Lambda targets: `service.handler`.
handler = (
    lambda_handler_for(build_service(config_from_env())) if os.environ.get("SERVICE_NAME") else None
)
