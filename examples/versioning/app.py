"""Wire the two ``order:create`` handlers and declare which one is the topic's default.

Both handlers are registered against the same topic under different versions; the message router
resolves the incoming ``benzene-version`` header to exactly one of them (an unknown version is
``not-found``). V1 is **declared the default** (``default=True``), so a producer that predates
versioning — one that sends no version at all — keeps reaching the handler it always reached. This is
the whole of Mechanism A: version is metadata, never in the body.
"""

from __future__ import annotations

from benzene.core import BenzeneMessageApplication, Registry

from .contracts import ORDER_CREATE_TOPIC, V1, V2
from .handlers import ProcessedLog, make_create_order_v1, make_create_order_v2


def build_versioning_app(log: ProcessedLog | None = None) -> BenzeneMessageApplication:
    """Build the versioning demo app, booting the two versioned handlers.

    Pass a :class:`~versioning.handlers.ProcessedLog` to observe which handler ran (a test does this);
    otherwise a fresh one is created. Returns the transport-neutral
    :class:`~benzene.core.BenzeneMessageApplication` — drive it with an ``InMemoryBenzeneHost`` or
    mount it on any transport.
    """
    log = log if log is not None else ProcessedLog()

    # Same topic, two versions; request_type is inferred from each handler's annotation. The oldest
    # version the topic still accepts is the default, so unversioned producers keep working.
    registry = (
        Registry()
        .register(ORDER_CREATE_TOPIC, make_create_order_v1(log), version=V1, default=True)
        .register(ORDER_CREATE_TOPIC, make_create_order_v2(log), version=V2)
    )

    return BenzeneMessageApplication(registry)
