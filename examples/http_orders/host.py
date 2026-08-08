"""HTTP host wiring: boot the shared ``OrdersStartUp`` and serve it on a standalone HTTP server.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``HttpMessageSender`` to the next Benzene service in
production, a fake in tests. Only this file is HTTP-specific.

This is the Python analog of the .NET ``Asp`` example and the natural sibling of ``aws_orders`` /
``gcp_orders``: the same order handlers, hosted directly on ``benzene-http``'s ASGI binding
(:class:`~benzene.http.BenzeneHttpApp`) rather than behind a cloud's Lambda/Functions runtime.
"""

from __future__ import annotations

from benzene.core import Container, MessageSender, application_from, build_application
from benzene.http import BenzeneHttpApp, HttpMessageSender
from orders_domain import ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def build_http_orders_app(
    service: OrderService | None = None,
    sender: MessageSender | None = None,
    seen: list[str] | None = None,
) -> BenzeneHttpApp:
    """Build the standalone-HTTP app for the order domain, booting from ``OrdersStartUp``.

    Pass a store / outbound client / event log to override the defaults (tests do this via the test
    harness instead); in production the default publishes ``orders:created`` over HTTP to the base
    URL of a downstream Benzene service named by ``BENZENE_ORDERS_EVENTS_URL`` (the topic is appended
    as a path segment, matching the inbound HTTP binding's route convention).
    """

    def overrides(services: Container) -> None:
        if service is not None:
            services.add_instance(OrderService, service)
        if seen is not None:
            services.add_instance(ORDER_EVENTS_KEY, seen)
        if sender is not None:
            services.add_instance(MessageSender, sender)
        else:
            import os

            events_url = os.environ.get("BENZENE_ORDERS_EVENTS_URL")
            if not events_url:
                raise RuntimeError(
                    "Set BENZENE_ORDERS_EVENTS_URL (the base URL of the downstream Benzene service) "
                    "to run the HTTP host with a real outbound client, or pass sender=... in tests."
                )
            services.add_instance(MessageSender, HttpMessageSender(events_url))

    definition, _ = build_application(OrdersStartUp, overrides=[overrides])
    return BenzeneHttpApp(
        definition.router,
        application=application_from(definition),
        standard_paths=definition.standard_paths,
    )
