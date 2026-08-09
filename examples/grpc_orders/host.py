"""gRPC host wiring: boot the shared ``OrdersStartUp`` and serve it over gRPC.

Deployment and tests build the app from the *same* composition root (``OrdersStartUp``); only the
outbound ``MessageSender`` differs — a real ``GrpcMessageSender`` to the next hop in production, a
fake in tests. Only this file is gRPC-specific.

The gRPC binding is a generic handler: the method name *is* the topic, so the whole domain registry
is served by one ``add_benzene_handler`` call — no route table to mirror.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from benzene.core import Container, MessageSender, application_from, build_application
from benzene.grpc import add_benzene_handler
from orders_domain import OrdersStartUp


def build_grpc_orders_server(
    bind: str = "[::]:0",
    *,
    sender: MessageSender,
    max_workers: int = 4,
) -> tuple[Any, int]:
    """Build a ``grpc.Server`` serving the order domain, booting from ``OrdersStartUp``.

    Returns ``(server, port)`` — the caller starts it (``server.start()``) and stops it. ``bind``
    defaults to an ephemeral port (``[::]:0``); the resolved port is returned so a test can dial it.
    ``sender`` is the outbound client (a fake in tests, a real ``GrpcMessageSender`` to the next
    service in production).
    """
    import grpc

    def use_sender(services: Container) -> None:
        services.add_instance(MessageSender, sender)

    definition, _ = build_application(OrdersStartUp, overrides=[use_sender])
    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    add_benzene_handler(server, application_from(definition))
    port = server.add_insecure_port(bind)
    return server, port
