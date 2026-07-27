"""The message-router middleware (core-concepts.md section 4, terminal middleware).

Resolves the context's topic to a handler, maps the request, runs the handler, and records the
result. Registered last in the pipeline. Empty topic → ``validation-error``; no handler →
``not-found``; an uncaught handler exception → ``service-unavailable`` (never crashes the adapter).
"""

from __future__ import annotations

from benzene.results import Result

from .context import Context
from .discovery import suggest_topic
from .mapping import to_request
from .pipeline import Middleware, Next
from .registry import Registry


def _unresolved_topic_message(registry: Registry, topic: str, version: str) -> str:
    """Say *why* the topic did not resolve, not merely that it did not.

    Three different bugs arrive here looking identical, and they have nothing in common to fix:
    nothing was ever registered (wiring never ran), the topic is registered but not at the version
    asked for, or the topic is genuinely unknown (usually a typo). Naming which one turns "why
    can't I find my handler?" into a one-line answer.

    What this deliberately does *not* do is list every registered topic: the message travels to the
    caller on the wire, and a full topic inventory is more than a failed request has earned. A
    count, and a single close match to a topic the caller already named, are enough to act on.
    """
    if len(registry) == 0:
        return (
            f"No handler found for topic {topic!r}: the handler registry is empty. "
            "No handlers were registered — check the application's startup wiring."
        )

    registered_versions = registry.versions_for(topic)
    if registered_versions:
        known = ", ".join(repr(v) if v else "unversioned" for v in registered_versions)
        return (
            f"No handler found for topic {topic!r} at version {version!r}: the topic is "
            f"registered, but only for {known}."
        )

    suggestion = suggest_topic(topic, registry.topics())
    hint = f" Did you mean {suggestion!r}?" if suggestion else ""
    return (
        f"No handler found for topic {topic!r}. "
        f"{len(registry)} handler(s) are registered.{hint}"
    )


def message_router(registry: Registry) -> Middleware:
    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if not context.topic:
            context.result = Result.validation_error("Topic is required")
            return

        definition = registry.find(context.topic, context.version)
        if definition is None:
            context.result = Result.not_found(
                _unresolved_topic_message(registry, context.topic, context.version)
            )
            return

        request = to_request(definition.request_type, context.request)
        try:
            context.result = await definition.handler(request)
        except Exception as ex:  # domain code must not crash the transport adapter
            context.result = Result.service_unavailable(str(ex))
        # Terminal middleware: deliberately does not call next().

    return middleware
