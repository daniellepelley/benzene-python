"""Wire the two ``order:create`` handlers and pick the version-selection policy.

Both handlers are registered against the same topic under different versions; the message router
resolves the incoming ``benzene-version`` metadata to one of them. The app opts into the
``highest_version`` selector so that a message carrying **no** version falls back to the highest
registered handler (``v2``) — exact matches still win, so a pinned ``v1`` always routes to V1. This
is the whole of Mechanism A: version is metadata, never in the body.
"""

from __future__ import annotations

from benzene.core import BenzeneMessageApplication, Registry, highest_version

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

    # Same topic, two versions; request_type is inferred from each handler's annotation.
    registry = (
        Registry()
        .register(ORDER_CREATE_TOPIC, make_create_order_v1(log), version=V1)
        .register(ORDER_CREATE_TOPIC, make_create_order_v2(log), version=V2)
    )

    # highest_version: exact (topic, version) match wins; a versionless message falls back to v2.
    return BenzeneMessageApplication(registry, version_selector=highest_version)
