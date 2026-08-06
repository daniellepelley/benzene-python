"""The reserved ``benzene:mesh`` topic interceptor (mesh.md §1).

A mesh-enabled service intercepts the reserved topic id ``benzene:mesh`` (plus any app-chosen
aliases) exactly the way health-check interception works (core-concepts §5): **by topic id alone,
ignoring version**. The response is status ``ok`` with the :class:`ServiceDescriptor` as payload.

This is an ordinary Benzene middleware — install it *before* the message router (which is the
terminal middleware) and it short-circuits the pipeline for the reserved topic, leaving every
other message to route normally. Provisioning it is a deployment choice: don't install it and the
endpoint simply doesn't exist, while every other mesh feed keeps working.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from benzene.core import Context, Middleware, Next
from benzene.results import Result

from .descriptor import MESH_TOPIC, ServiceDescriptor

#: A descriptor, or a zero-arg callable returning one (re-derived per request, e.g. for degradation).
DescriptorSource = ServiceDescriptor | Callable[[], ServiceDescriptor]


def mesh_interception(
    descriptor: DescriptorSource, *, aliases: Iterable[str] = ()
) -> Middleware:
    """Middleware that answers ``benzene:mesh`` (and any ``aliases``) with the ServiceDescriptor.

    ``descriptor`` may be a :class:`ServiceDescriptor` or a callable returning one (so a service can
    recompute it — e.g. to reflect a degraded subsystem). Interception is by topic id, version
    ignored.
    """
    topics = {MESH_TOPIC, *aliases}

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if context.topic in topics:
            resolved = descriptor() if callable(descriptor) else descriptor
            context.result = Result.ok(resolved.to_payload())
            return  # short-circuit: the reserved topic never reaches the router
        await next()

    return middleware
