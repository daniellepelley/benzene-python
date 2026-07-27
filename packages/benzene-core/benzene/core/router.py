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
from .registry import Registry


def message_router(registry: Registry) -> Middleware:
    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if not context.topic:
            context.result = Result.validation_error("Topic is required")
            return

        definition = registry.find(context.topic, context.version)
        if definition is None:
            context.result = Result.not_found(
                f"No handler found for topic {context.topic}"
            )
            return

        request = to_request(definition.request_type, context.request)
        try:
            context.result = await definition.handler(request)
        except Exception as ex:  # domain code must not crash the transport adapter
            context.result = Result.service_unavailable(str(ex))
        # Terminal middleware: deliberately does not call next().

    return middleware
