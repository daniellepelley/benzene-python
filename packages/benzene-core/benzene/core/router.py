"""The message-router middleware (core-concepts.md section 4, terminal middleware).

Resolves the context's topic to a handler, maps the request, runs the handler, and records the
result. Registered last in the pipeline. Empty topic → ``validation-error``; no handler →
``not-found``; an uncaught handler exception → ``service-unavailable`` (never crashes the adapter).
"""

from __future__ import annotations

from benzene.results import Result

from .context import Context
from .mapping import to_request
from .pipeline import Middleware, Next
from .registry import Registry, VersionSelector, exact_version


def message_router(
    registry: Registry, version_selector: VersionSelector | None = None
) -> Middleware:
    """Terminal middleware resolving topic → handler. ``version_selector`` chooses the
    version-selection policy — same keyword as :class:`BenzeneMessageApplication` (default: exact
    ``(topic, version)`` match; pass ``highest_version`` for latest-wins fallback)."""
    selector = version_selector or exact_version

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if not context.topic:
            context.result = Result.validation_error("Topic is required")
            return

        definition = selector(registry, context.topic, context.version)
        if definition is None:
            context.result = Result.not_found(f"No handler found for topic {context.topic}")
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
