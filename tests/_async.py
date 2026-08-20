"""Awaiting an injection seam from a synchronous test."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


def run(awaitable: Awaitable[T]) -> T:
    """``asyncio.run`` for an ``Awaitable`` rather than a ``Coroutine``.

    ``asyncio.run`` requires a coroutine specifically, so awaiting the result of an injection seam -
    ``HttpTransport``, ``HttpGet`` - does not type-check without this.

    Those seams are annotated ``Awaitable`` **on purpose** and should stay that way: an injection
    point wants the most permissive thing it can accept from a caller, so that an ``async def``, a
    ``functools.partial`` returning a Future, and a mock returning a completed Future all satisfy it.
    Narrowing them to ``Coroutine`` would make the framework harder to inject into in order to make a
    test tidier, which is backwards. The wrapper belongs on the test side, so it lives here.

    (``Handler`` is the opposite case and *was* narrowed: the framework documents a handler as
    ``async def handle(request) -> Result``, and calling one directly is the most natural way to unit
    test it, so ``asyncio.run(handler(request))`` should simply work.)
    """

    async def _await() -> T:
        return await awaitable

    return asyncio.run(_await())
