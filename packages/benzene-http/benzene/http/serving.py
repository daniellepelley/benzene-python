"""Serving a :class:`~benzene.http.BenzeneHttpApp` as one leg of a multi-transport process.

:class:`BenzeneHttpApp` is a plain ASGI application, so the ordinary way to run it is to point an
ASGI server at it and let that server be the process::

    uvicorn my_service.main:app --port 8080

That stays true and is still the right answer for an HTTP-only service — nothing here replaces it.
This module exists for the *other* shape: a process that serves HTTP **and** polls a queue, where the
server has to run as a coroutine among siblings under a :class:`~benzene.core.WorkerHost` and be
told to stand down when one of them stops.

Two rungs, both public:

* :func:`asgi_server_worker` — adapts an already-built, uvicorn-shaped server object (anything with
  ``await serve()`` and a settable ``should_exit``) into a :data:`~benzene.core.Worker`. Use it when
  you construct and configure the server yourself, or run something other than uvicorn.
* :func:`uvicorn_worker` — the shorthand: builds ``uvicorn.Server(uvicorn.Config(...))`` for you and
  hands it to :func:`asgi_server_worker`. Drop one level by building the server yourself and calling
  :func:`asgi_server_worker` — that is the whole of what this function does.

``uvicorn`` is imported lazily inside :func:`uvicorn_worker` and is an optional extra
(``pip install "benzene-http[uvicorn]"``); importing :mod:`benzene.http` never requires it.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from benzene.core import StopSignal, Worker

__all__ = [
    "SupportsAsgiServing",
    "asgi_server_worker",
    "uvicorn_worker",
]


class SupportsAsgiServing(Protocol):
    """The two things a server must expose to be supervised: run until told, and be tellable.

    Modelled on (and satisfied by) ``uvicorn.Server``; duck-typed, so a fake in a test or a
    different server exposing the same pair works without inheriting anything.
    """

    should_exit: bool

    async def serve(self) -> None: ...


def asgi_server_worker(server: SupportsAsgiServing) -> Worker:
    """Supervise an already-built ASGI server as one leg of a :class:`~benzene.core.WorkerHost`.

    **The explicit form this composes**, which is also all it does, is a task that watches the stop
    signal and flips the server's own shutdown flag::

        async def worker(stop):
            async def stand_down():
                await stop.wait()
                server.should_exit = True     # uvicorn's own documented shutdown flag

            watcher = asyncio.create_task(stand_down())
            try:
                await server.serve()          # returns on SIGINT/SIGTERM, or when should_exit is set
            finally:
                watcher.cancel()

    ``server.serve()`` is awaited on the calling thread, so uvicorn keeps its native SIGINT/SIGTERM
    handling: a signal ends ``serve()``, the worker returns, and the host winds the other legs down.
    The reverse direction is the watcher above — a sibling stopping sets the signal, which sets
    ``should_exit``.
    """

    async def worker(stop: StopSignal) -> None:
        async def stand_down() -> None:
            await stop.wait()
            server.should_exit = True

        watcher = asyncio.create_task(stand_down())
        try:
            await server.serve()
        finally:
            watcher.cancel()

    return worker


def uvicorn_worker(
    app: Any,
    *,
    host: str = "0.0.0.0",  # noqa: S104 - a container listens on all interfaces by design
    port: int = 8080,
    **uvicorn_config: Any,
) -> Worker:
    """Serve an ASGI app under uvicorn as one leg of a :class:`~benzene.core.WorkerHost`.

    **The explicit form this composes** is two lines, and they remain the way to configure anything
    this signature does not surface::

        server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080))
        worker = asgi_server_worker(server)

    ``**uvicorn_config`` is forwarded verbatim to ``uvicorn.Config`` (``log_level``, ``access_log``,
    ``lifespan``, ``ssl_certfile``, ...), so the shorthand does not cap what you can set. The
    defaults are container defaults: all interfaces, port 8080.

    Requires the optional extra: ``pip install "benzene-http[uvicorn]"``.
    """
    try:
        import uvicorn
    except ImportError as error:  # pragma: no cover - exercised only without the optional server
        raise ImportError(
            "uvicorn_worker() needs uvicorn, which is an optional extra of benzene-http. Install it "
            'with: pip install "benzene-http[uvicorn]". To use a different ASGI server, build its '
            "server object yourself and pass it to benzene.http.asgi_server_worker(server)."
        ) from error

    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, **uvicorn_config))
    return asgi_server_worker(server)
