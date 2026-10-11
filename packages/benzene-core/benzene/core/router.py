"""The message-router middleware (core-concepts.md section 4, terminal middleware).

Resolves the context's topic to a handler, maps the request, runs the handler, and records the
result. Registered last in the pipeline. Empty topic → ``validation-error``; no handler →
``not-found`` naming the topic and the version asked for; an uncaught handler exception →
``service-unavailable`` (never crashes the adapter).

The router also answers the reserved reachability probe ``benzene:ping`` (wire-contracts §7) with
``ok`` and no payload, on every transport and without registration: health checks send it into a
queue or a function to prove the path works, and on a one-way binding an unanswered probe would be a
``not-found`` failure the transport redelivers until it dead-letters it.
"""

from __future__ import annotations

import warnings

from benzene.results import Result

from .context import Context
from .mapping import to_request
from .pipeline import Middleware, Next
from .registry import NoDefaultHandlerWarning, Registry, VersionSelector, exact_version

#: The reserved reachability probe (wire-contracts §7): answered ``ok``, no payload, version ignored.
PING_TOPIC = "benzene:ping"


def message_router(
    registry: Registry, version_selector: VersionSelector | None = None
) -> Middleware:
    """Terminal middleware resolving topic → handler. ``version_selector`` chooses the
    version-selection policy — same keyword as :class:`BenzeneMessageApplication` (default:
    :func:`~benzene.core.exact_version`, the spec's rule; pass ``highest_version`` to opt in to a
    latest-wins fallback).

    Building the router is the start-up moment, so under the default selector it warns
    (:class:`~benzene.core.NoDefaultHandlerWarning`) about every topic whose handlers are all
    versioned with none declared default — a message without a version to one of them fails.
    """
    selector = version_selector or exact_version
    if selector is exact_version:
        _warn_about_topics_without_default(registry)

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if not context.topic:
            context.result = Result.validation_error("Topic is required")
            return
        if context.topic == PING_TOPIC:
            context.result = Result.ok()
            return

        definition = selector(registry, context.topic, context.version)
        if definition is None:
            if context.ignore_unmatched_topic and not registry.versions_of(context.topic):
                # A shared-bus subscriber's option (transport-bindings §1, obligation 8): another
                # service's topic is acknowledged as ignored rather than redelivered as a failure.
                context.result = Result.ignored()
                return
            context.result = Result.not_found(_no_handler(registry, context.topic, context.version))
            return

        # Mapping the payload onto the handler's declared type can fail (e.g. a required field is
        # missing) — that is the caller's error, so classify it as bad-request rather than letting
        # the TypeError crash the adapter (a distinct failure mode from a handler exception below).
        try:
            request = to_request(definition.request_type, context.request)
        except Exception as ex:
            name = getattr(definition.request_type, "__name__", "the request type")
            context.result = Result.bad_request(f"Could not map request to {name}: {ex}")
            return

        try:
            context.result = await definition.handler(request)
        except Exception as ex:  # domain code must not crash the transport adapter
            context.result = Result.service_unavailable(str(ex))
        # Terminal middleware: deliberately does not call next().

    return middleware


def _no_handler(registry: Registry, topic: str, version: str) -> str:
    """The ``not-found`` detail: which topic, which version, and what the topic does serve."""
    served = [d.version for d in registry.versions_of(topic)]
    if not served:
        return f"No handler found for topic {topic!r}"
    listed = ", ".join(repr(v) if v else "unversioned" for v in served)
    if version:
        return f"No handler found for topic {topic!r} at version {version!r} (it serves {listed})"
    return (
        f"No default handler for topic {topic!r}: the message carries no version and every "
        f"handler is versioned ({listed}), none declared default"
    )


def _warn_about_topics_without_default(registry: Registry) -> None:
    for topic in registry.topics_without_default():
        versions = ", ".join(repr(d.version) for d in registry.versions_of(topic))
        warnings.warn(
            f"Topic {topic!r} has only versioned handlers ({versions}) and none is declared its "
            "default, so a message to it without a benzene-version header answers not-found. "
            f"Declare one default — @message({topic!r}, version=..., default=True) or "
            "registry.register(..., default=True) — or register an unversioned handler.",
            NoDefaultHandlerWarning,
            stacklevel=4,  # _warn → message_router → BenzeneMessageApplication → the caller
        )
