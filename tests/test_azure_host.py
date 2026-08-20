"""The Azure Functions host's entry points and trigger-cardinality edges.

Parallels ``test_aws_host`` / ``test_gcp_host``: covers the branches the batch-oriented in-memory
``send_*`` helpers don't reach — a message-only app answering HTTP with 501, and the Event Hub
single-event cardinality ('one', a bare event rather than a batch list).
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("benzene.azure")

from benzene.azure import AzureFunctionsApp, event_hub_function
from benzene.azure.testing import AzureFunctionsTestHost, event_hub_event
from benzene.core import Registry
from benzene.results import Result


async def _echo(request: dict) -> Result:
    return Result.ok(request)


def test_message_only_app_answers_http_with_501() -> None:
    # A message-only app (no HTTP router) still answers HTTP with a clean 501, not a crash.
    app = AzureFunctionsApp(registry=Registry().register("t", _echo))
    response = app.handle_http(method="GET", path="/x")
    assert response.status_code == 501
    assert response.body == '{"status": "not-implemented"}'


def test_event_hub_single_event_cardinality_reaches_the_handler() -> None:
    seen: list[dict] = []

    async def record(request: dict) -> Result:
        seen.append(request)
        return Result.ok()

    app = AzureFunctionsApp(registry=Registry().register("orders:created", record))
    # A bare event (cardinality 'one'), not a batch list — exercises the single-event wrap.
    app.handle_event_hub(event_hub_event("orders:created", {"id": "e1"}))
    assert len(seen) == 1
    assert seen[0]["id"] == "e1"


def test_event_hub_entry_point_accepts_a_single_event() -> None:
    seen: list[dict] = []

    async def record(request: dict) -> Result:
        seen.append(request)
        return Result.ok()

    entry = event_hub_function(
        AzureFunctionsApp(registry=Registry().register("orders:created", record))
    )
    entry(event_hub_event("orders:created", {"id": "e2"}))  # the platform callable, single event
    assert seen[0]["id"] == "e2"


# --- the sync test host, called from an async test (D10) -------------------------------------------


def test_a_sync_send_inside_a_running_loop_teaches_how_to_drive_the_app() -> None:
    host = AzureFunctionsTestHost(AzureFunctionsApp(registry=Registry().register("t", _echo)))

    async def an_async_test() -> None:
        host.send_service_bus("t", {})

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(an_async_test())
    detail = str(excinfo.value)
    assert "send_service_bus() is synchronous" in detail
    assert "plain 'def' test" in detail
    assert "await host._app.handle(...)" in detail
