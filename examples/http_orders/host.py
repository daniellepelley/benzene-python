"""HTTP host wiring: boot the shared ``OrdersStartUp`` and serve it on a standalone HTTP server.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``HttpMessageSender`` to the next Benzene service in
production, a fake in tests (via ``create_test_host``). Only this file is HTTP-specific.

This is the Python analog of the .NET ``Asp`` example: the same order handlers on ``benzene-http``'s
ASGI binding (:class:`~benzene.http.BenzeneHttpApp`), no cloud runtime.
"""

from __future__ import annotations

import os

from benzene.core import Container, MessageSender, build_application
from benzene.http import BenzeneHttpApp, HttpMessageSender
from orders_domain import OrdersStartUp


def build_http_orders_app() -> BenzeneHttpApp:
    """Boot ``OrdersStartUp`` onto the standalone ASGI binding, publishing ``orders:created`` over HTTP.

    The default publishes to the base URL of a downstream Benzene service named by
    ``BENZENE_ORDERS_EVENTS_URL`` (the topic is appended as a path segment, matching the inbound HTTP
    route convention).
    """
    events_url = os.environ.get("BENZENE_ORDERS_EVENTS_URL")
    if not events_url:
        raise RuntimeError(
            "Set BENZENE_ORDERS_EVENTS_URL (the downstream Benzene service's base URL) to run the "
            "HTTP host (tests use create_test_host instead)."
        )

    def use_http_sender(services: Container) -> None:
        services.add_instance(MessageSender, HttpMessageSender(events_url))

    definition, _ = build_application(OrdersStartUp, overrides=[use_http_sender])
    return BenzeneHttpApp.from_definition(definition)
