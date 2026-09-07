"""The middleware pipeline (core-concepts.md section 4).

Middleware is ``async def mw(context, next) -> None``. Middleware runs in registration order
(first registered is outermost); a middleware that does not ``await next()`` short-circuits the
pipeline (the health-check interceptor relies on this). The pipeline runs exactly once per
invocation. The message router (topic → handler) is an ordinary middleware, registered last.

**The pipeline boundary contains exceptions.** The router already turns a handler exception into
``service-unavailable`` so domain code cannot crash a transport adapter, but it is the *last*
middleware: anything raised by one of the middleware in front of it — auth, tracing, mesh
interception, rate limiting, a user-written middleware — used to propagate out of :meth:`handle`
and into whichever adapter was hosting the pipeline, each of which handles it differently or not
at all. The promise is that request content never crashes the host, so :meth:`handle` maps an
escaping exception the same way the router does. See :meth:`handle` for the cancellation rule.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from benzene.results import Result

from .context import Context

Next = Callable[[], Awaitable[None]]
Middleware = Callable[[Context, Next], Awaitable[None]]


class MiddlewarePipeline:
    """An ordered chain of middleware, run exactly once per invocation.

    Register middleware with :meth:`use` (first registered is outermost); drive one context through
    the chain with :meth:`handle`.
    """

    def __init__(self, middleware: list[Middleware] | None = None) -> None:
        self._middleware: list[Middleware] = list(middleware or [])

    def use(self, middleware: Middleware) -> MiddlewarePipeline:
        self._middleware.append(middleware)
        return self

    async def handle(self, context: Context) -> None:
        """Drive ``context`` through the chain, containing anything it raises.

        An exception that escapes the middleware becomes ``service-unavailable`` on the context —
        the identical mapping :func:`~benzene.core.message_router` applies to a handler exception,
        so a fault has the same shape on the wire wherever in the pipeline it came from, and the
        structured error carries the exception's message.

        Two deliberate exclusions:

        * ``asyncio.CancelledError`` propagates untouched. It derives from ``BaseException``, so
          ``except Exception`` misses it already; the clause is written out because the intent is
          load-bearing. Cancellation is the host cooperatively shutting the invocation down, not a
          request fault — swallowing it would settle a fabricated failure (and, on a queue
          transport, ack a message that was never processed) instead of letting the transport
          redeliver. The circuit breaker draws the same line for the same reason.
        * A result already on the context wins. A middleware that produced a result and then failed
          while unwinding has already answered the caller; overwriting that with the unwind's
          exception would lose the real outcome.
        """
        try:
            await self._invoke(0, context)
        except asyncio.CancelledError:
            raise  # cooperative cancellation, not a request fault
        except Exception as ex:  # a middleware must not crash the transport adapter
            if context.result is None:
                context.result = Result.service_unavailable(str(ex))

    async def _invoke(self, index: int, context: Context) -> None:
        if index >= len(self._middleware):
            return
        middleware = self._middleware[index]

        async def _next() -> None:
            await self._invoke(index + 1, context)

        await middleware(context, _next)
