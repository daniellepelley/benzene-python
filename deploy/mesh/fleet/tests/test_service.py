"""The env-driven fleet Lambda service — config, cross-service call, spec surface, trace push.

Exercises the deployable service in-memory (no AWS): the Lambda handler dispatches an API Gateway
event, calls its configured sibling with trace propagation, exposes /benzene/spec, and pushes its
trace batch to the (fake) collector after the invocation.
"""

from __future__ import annotations

import json

from benzene.aws.testing import ApiGatewayRequestBuilder
from benzene.results import Result

from fleet.service import (
    FleetService,
    ServiceConfig,
    build_service,
    config_from_env,
    lambda_handler_for,
)


class _RecordingSender:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def send_message(self, topic, message, headers=None) -> Result:
        self.calls.append((topic, message))
        return Result.ok()


def test_config_from_env_reads_the_node_identity() -> None:
    config = config_from_env(
        {
            "SERVICE_NAME": "orders",
            "SERVICE_TOPIC": "orders:place",
            "NEXT_URL": "https://inventory.example",
            "NEXT_TOPIC": "inventory:reserve",
            "COLLECTOR_URL": "http://mesh.example",
        }
    )
    assert config.name == "orders"
    assert config.route == "/orders"  # defaulted from the name
    assert config.next_topic == "inventory:reserve"


def test_invocation_calls_the_next_service() -> None:
    sender = _RecordingSender()
    service = build_service(
        ServiceConfig("orders", "orders:place", "/orders", next_topic="inventory:reserve"),
        next_sender=sender,
    )
    handler = lambda_handler_for(service)

    event = ApiGatewayRequestBuilder("POST", "/orders").with_body({"sku": "A"}).build()
    response = handler(event)

    assert response["statusCode"] == 200
    assert sender.calls == [("inventory:reserve", {"from": "orders"})]  # the hop happened


def test_spec_surface_is_served_through_the_lambda() -> None:
    service = build_service(ServiceConfig("orders", "orders:place", "/orders"))
    handler = lambda_handler_for(service)
    response = handler(ApiGatewayRequestBuilder("GET", "/benzene/spec").build())
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["service"] == "orders"


def test_traces_are_pushed_to_the_collector_after_an_invocation() -> None:
    pushed: list[list] = []

    class _Feeds:
        async def publish_traces(self, events):
            pushed.append(list(events))
            return Result.ok()

    service = build_service(ServiceConfig("orders", "orders:place", "/orders"))
    # swap in a fake collector feed (build_service only wires a real one when COLLECTOR_URL is set)
    service = FleetService(app=service.app, exporter=service.exporter, feeds=_Feeds())  # type: ignore[arg-type]
    handler = lambda_handler_for(service)

    handler(ApiGatewayRequestBuilder("POST", "/orders").with_body({}).build())
    assert len(pushed) == 1 and len(pushed[0]) == 1  # one invocation -> one trace event pushed


def test_no_collector_configured_means_no_push() -> None:
    service = build_service(ServiceConfig("orders", "orders:place", "/orders"))
    assert service.feeds is None  # COLLECTOR_URL absent -> nothing to push to, no network
    handler = lambda_handler_for(service)
    assert handler(ApiGatewayRequestBuilder("POST", "/orders").with_body({}).build())["statusCode"] == 200
