"""Running several long-lived legs — an ASGI server, an SQS poll loop, a Kafka consumer — in one
process, on one event loop, with one coordinated shutdown.

A service that speaks more than one transport has to solve the same problem every time: start N
things that never return on their own, make whichever one finishes first wind the others down, and
still exit loudly if one of them crashed so the orchestrator restarts the pod. That is framework
work, not service work, so :class:`WorkerHost` does it.

**The explicit form this composes** (still fully supported, and what to write when you want
different semantics) is a hand-rolled :func:`asyncio.gather` over the transports' own loop functions,
with a shared :class:`asyncio.Event` threaded through their ``should_continue`` parameters::

    stop = asyncio.Event()

    async def supervised(leg):
        try:
            await leg
        finally:
            stop.set()
            server.should_exit = True

    await asyncio.gather(
        supervised(server.serve()),
        supervised(run_sqs_consumer_loop(sqs_app, client, url,
                                         should_continue=lambda: not stop.is_set())),
        supervised(run_consumer_loop(kafka_app, consumer,
                                     should_continue=lambda: not stop.is_set())),
    )

:class:`WorkerHost` is that block and nothing more: it owns the ``stop`` event (as a
:class:`StopSignal`), the per-leg ``finally``, and the wait-for-everyone-then-re-raise. Each leg is a
:data:`Worker` — a callable taking the :class:`StopSignal` and returning a coroutine — and the
transport packages ship one-line factories for theirs
(:func:`benzene.aws.sqs_consumer_worker`, :func:`benzene.kafka.kafka_consumer_worker`,
:func:`benzene.http.uvicorn_worker`), each of which is a closure over the public loop function above.
Nothing here is privileged: a worker is an ordinary ``async def`` you can write yourself.

Two things this host deliberately does **not** do, because they belong to the legs:

* **It starts no threads and installs no signal handlers.** ``uvicorn.Server.serve()`` installs its
  own SIGINT/SIGTERM handling and only works on the main thread, so :meth:`WorkerHost.run` is called
  from the main thread and leaves signals to it. On a signal uvicorn returns, its worker's
  ``finally`` sets the stop signal, and the consumer loops see it on their next iteration.
* **It does not make blocking SDK calls safe.** Sharing one event loop is only sound because the
  consumer loops route their ``boto3``/``confluent-kafka`` calls through :func:`asyncio.to_thread`
  themselves. A worker that blocks the loop starves its siblings, and no supervisor can fix that.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

__all__ = [
    "DuplicateWorkerError",
    "background_worker",
    "NoWorkersError",
    "StopSignal",
    "Worker",
    "WorkerHost",
]


class StopSignal:
    """The one shared "wind down now" flag a :class:`WorkerHost` passes to each of its workers.

    A thin, dependency-free wrapper over :class:`asyncio.Event` — the explicit form this replaces is
    an ``asyncio.Event`` you create and close over yourself. It adds exactly one thing: a bound
    :meth:`should_continue` that drops straight into the transports' loop signatures, so a worker
    reads ``should_continue=stop.should_continue`` instead of ``lambda: not stop.is_set()``.

    Setting it is idempotent and safe from any worker; every worker sees the same instance.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def set(self) -> None:
        """Ask every worker in the host to wind down. Idempotent."""
        self._event.set()

    def is_set(self) -> bool:
        """True once shutdown has been requested."""
        return self._event.is_set()

    def should_continue(self) -> bool:
        """``not is_set()`` — pass this straight as a consumer loop's ``should_continue``."""
        return not self._event.is_set()

    async def wait(self) -> None:
        """Block until shutdown is requested (for a worker that must be *told*, not polled)."""
        await self._event.wait()


#: One leg of a :class:`WorkerHost`: a callable handed the shared :class:`StopSignal`, returning a
#: coroutine that runs until it is asked to stop (or decides to stop, or fails). Any ``async def
#: worker(stop: StopSignal) -> None`` satisfies this — there is no base class to inherit.
Worker = Callable[[StopSignal], Awaitable[None]]


class NoWorkersError(RuntimeError):
    """:meth:`WorkerHost.run` was called with nothing to run — raised at start-up, not at message time."""


class DuplicateWorkerError(ValueError):
    """Two workers were added under the same name, which would make a shutdown report ambiguous."""


class WorkerHost:
    """Run N long-lived workers on one event loop, and wind them all down together.

    The whole contract, and all of it observable:

    * every worker is started concurrently and handed the same :class:`StopSignal`;
    * whichever worker returns or raises **first** sets that signal, so the others wind down —
      a clean signal-triggered exit and a crash behave identically here;
    * :meth:`run` waits for every worker to actually finish (bounded by ``shutdown_timeout``,
      after which stragglers are cancelled), so shutdown is orderly rather than abandoned;
    * if any worker raised, the first such exception is re-raised once everyone is down — a
      crash still leaves the process with a non-zero exit for an orchestrator to restart.

    Composed from public API only: this is :func:`asyncio.gather` over the transports' own loop
    functions plus a shared :class:`asyncio.Event`, which is exactly the explicit form documented in
    :mod:`benzene.core.worker`. To drop one level down, write that ``gather`` yourself; the worker
    factories (:func:`benzene.aws.sqs_consumer_worker` and friends) stay usable either way, and the
    loop functions they wrap (:func:`~benzene.aws.run_sqs_consumer_loop`,
    :func:`~benzene.kafka.run_consumer_loop`) remain public and unchanged.

    Call :meth:`run` from the main thread: it starts no threads of its own so that a worker hosting
    an ASGI server keeps its native SIGINT/SIGTERM handling.

        host = WorkerHost()
        host.add("http", uvicorn_worker(http_app, port=8080))
        host.add("sqs", sqs_consumer_worker(sqs_app, client, queue_url))
        await host.run()
    """

    def __init__(self, *, shutdown_timeout: float | None = 30.0) -> None:
        """``shutdown_timeout`` bounds how long a *sibling* gets to notice the stop signal.

        The default of 30 seconds is chosen to clear an SQS long-poll (20s at most) so a polling
        worker returns on its own rather than being cancelled mid-message. Pass ``None`` to wait
        indefinitely for well-behaved workers, or a smaller number for a tighter shutdown budget.
        """
        self._workers: list[tuple[str, Worker]] = []
        self._shutdown_timeout = shutdown_timeout
        self._stop = StopSignal()

    @property
    def stop(self) -> StopSignal:
        """The signal shared by every worker — set it to wind the whole host down from outside."""
        return self._stop

    @property
    def names(self) -> tuple[str, ...]:
        """The registered worker names, in the order they were added."""
        return tuple(name for name, _ in self._workers)

    def add(self, name: str, worker: Worker) -> WorkerHost:
        """Register one worker under a name used in errors and shutdown reporting. Returns ``self``.

        The name must be unique; a duplicate raises :class:`DuplicateWorkerError` here, at wiring
        time, rather than producing an ambiguous report at shutdown.
        """
        if name in self.names:
            raise DuplicateWorkerError(
                f"WorkerHost already has a worker named {name!r} (workers: "
                f"{', '.join(self.names)}). Give this one a different name, e.g. {name!r} and "
                f"{name + '-2'!r}, so shutdown and error reporting can tell them apart."
            )
        self._workers.append((name, worker))
        return self

    async def run(self) -> None:
        """Start every worker, wind them all down together, and re-raise the first failure.

        Raises :class:`NoWorkersError` immediately if nothing was added — a start-up failure naming
        the fix, never a process that looks healthy and handles nothing.
        """
        if not self._workers:
            raise NoWorkersError(
                "WorkerHost.run() was called with no workers, so this process would start up and "
                "handle nothing. Add at least one before running, e.g. "
                'host.add("http", uvicorn_worker(app, port=8080)) or '
                'host.add("sqs", sqs_consumer_worker(app, client, queue_url)).'
            )

        tasks = [
            asyncio.create_task(self._supervise(worker), name=f"benzene-worker:{name}")
            for name, worker in self._workers
        ]
        try:
            results = await self._gather_then_wind_down(tasks)
        except asyncio.CancelledError:
            self._stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for outcome in results:
            if isinstance(outcome, BaseException) and not isinstance(
                outcome, asyncio.CancelledError
            ):
                # A crash propagates so the process exits non-zero and the orchestrator restarts it.
                # Every other worker has already been wound down by the wait above; the first
                # failure in registration order is the one raised.
                raise outcome
        return None

    async def _supervise(self, worker: Worker) -> None:
        """One leg: run it, and whatever happens, wind its siblings down on the way out."""
        try:
            await worker(self._stop)
        finally:
            self._stop.set()

    async def _gather_then_wind_down(self, tasks: list[asyncio.Task[None]]) -> list[object]:
        """Wait for the first worker to finish, then give the rest ``shutdown_timeout`` to follow."""
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        pending = [task for task in tasks if not task.done()]
        if pending:
            _, still_running = await asyncio.wait(pending, timeout=self._shutdown_timeout)
            for task in still_running:
                # It never noticed the stop signal inside the budget; take it down rather than hang.
                task.cancel()
            if still_running:
                await asyncio.wait(still_running)
        return [
            task.exception() if not task.cancelled() else asyncio.CancelledError() for task in tasks
        ]


def background_worker(start: Callable[[], Coroutine[Any, Any, None]]) -> Worker:
    """Run a never-ending background coroutine as one leg — the shape that must be *cancelled*.

    Consumer loops take a ``should_continue`` and stop politely. Plenty of long-lived loops do not:
    a mesh poller, a reporter, a refresh timer written as ``while True: ... await sleep(n)``. Their
    shutdown is cancellation, and the code for that is always the same three lines::

        task = asyncio.create_task(host.reporter.run_forever())
        try:
            await server.serve()
        finally:
            task.cancel()

    **The explicit form this composes** is exactly that, generalised to the host's stop signal: start
    the coroutine, wait for either it or the stop signal, and cancel it if the stop signal won.
    Write it yourself whenever the loop wants a gentler wind-down than cancellation — and if it takes
    a ``should_continue``-style parameter, prefer passing ``stop.should_continue`` and skip this
    entirely, since a leg that stops on its own never has to be cancelled mid-work.

    ``start`` is a **callable returning the coroutine**, not the coroutine itself, so nothing is
    scheduled until the host actually runs. If the background loop ends or raises on its own, that
    outcome is the leg's outcome: it propagates, and the host winds the other legs down.

        WorkerHost().add("http", uvicorn_worker(app)).add(
            "reporter", background_worker(host.reporter.run_forever)
        )
    """

    async def worker(stop: StopSignal) -> None:
        task: asyncio.Task[None] = asyncio.create_task(start())
        waiter = asyncio.create_task(stop.wait())
        try:
            await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
            if task.done():
                await task  # it finished or failed on its own - that outcome is this leg's outcome
                return
        finally:
            waiter.cancel()
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    return worker
