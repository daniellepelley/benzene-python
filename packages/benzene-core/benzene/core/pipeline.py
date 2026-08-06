"""The middleware pipeline (core-concepts.md section 4).

Middleware is ``async def mw(context, next) -> None``. Middleware runs in registration order
(first registered is outermost); a middleware that does not ``await next()`` short-circuits the
pipeline (the health-check interceptor relies on this). The pipeline runs exactly once per
invocation. The message router (topic → handler) is an ordinary middleware, registered last.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

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
        await self._invoke(0, context)

    async def _invoke(self, index: int, context: Context) -> None:
        if index >= len(self._middleware):
            return
        middleware = self._middleware[index]

        async def _next() -> None:
            await self._invoke(index + 1, context)

        await middleware(context, _next)
