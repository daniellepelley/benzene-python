"""A configurable Benzene fleet service, deployed as one env-driven AWS Lambda.

One Lambda image serves any node in the demo fleet (``orders`` / ``inventory`` / ``notifications``);
the environment decides its identity, the topic it serves, the sibling it calls next, and where it
reports traces. Each service:

- exposes ``/benzene/spec`` + ``/benzene/health`` (so the mesh collector can **poll** it),
- runs ``trace_middleware`` and, when ``NEXT_URL`` is set, calls the next service through a
  ``with_trace_propagation``-wrapped HTTP sender (so the ``traceparent`` crosses the hop), and
- when ``COLLECTOR_URL`` is set, **pushes** its trace batch to the collector after each invocation, so
  the collector derives the consumer edges (best-effort — a collector hiccup never fails the request).

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
    TRACES_TOPIC,
    MeshFeedSender,
    QueueTraceExporter,
    trace_middleware,
    with_trace_propagation,
)
from benzene.results import Result


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


def build_service(config: ServiceConfig, *, next_sender: MessageSender | None = None) -> FleetService:
    """Build the service. ``next_sender`` is injectable for tests; production derives it from the env."""
    exporter = QueueTraceExporter()

    if next_sender is None and config.next_url and config.next_topic:
        # NEXT_URL is the callee's exact route URL; map the topic straight to it so the sender POSTs
        # there (the callee's inbound route resolves that path back to the topic).
        next_sender = with_trace_propagation(
            HttpMessageSender({config.next_topic: config.next_url})
        )

    async def handle(_request: dict) -> Result:
        if next_sender is not None and config.next_topic:
            await next_sender.send_message(config.next_topic, {"from": config.name})
        return Result.ok({"service": config.name})

    router = HttpRouter().register("POST", config.route, config.topic, handle)
    registry = Registry.from_definitions(router)
    pipeline = MiddlewarePipeline().use(
        trace_middleware(exporter, service=config.name, instance_id=config.name)
    )
    standard = StandardPaths(
        health=HealthChecks().add("core", lambda: True),
        spec=ServiceSpec.derive(registry, service=config.name),
    )
    app = AwsLambdaApp(
        http_router=router,
        application=BenzeneMessageApplication(registry, pipeline),
        standard_paths=standard,
    )

    feeds: MeshFeedSender | None = None
    if config.collector_url:
        # Push traces to the collector's /mesh/traces route (MeshFeedSender sends to TRACES_TOPIC).
        feeds = MeshFeedSender(
            HttpMessageSender({TRACES_TOPIC: f"{config.collector_url.rstrip('/')}/mesh/traces"})
        )
    return FleetService(app=app, exporter=exporter, feeds=feeds)


def lambda_handler_for(service: FleetService):
    """Return the ``handler(event, context)`` Lambda invokes, pushing traces after each invocation."""
    base = to_lambda_handler(service.app)

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        result = base(event, context)
        if service.feeds is not None:
            events = service.exporter.drain()
            if events:
                with contextlib.suppress(Exception):  # trace push is best-effort, never fails the request
                    asyncio.run(service.feeds.publish_traces(events))
        return result

    return handler


# The module-level entrypoint Lambda targets: `service.handler`.
handler = lambda_handler_for(build_service(config_from_env())) if os.environ.get("SERVICE_NAME") else None
