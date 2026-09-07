"""The poll loop that runs the dispatcher, in the shape every other Benzene consumer loop takes.

Same idiom as :func:`benzene.aws.run_consumer_loop` and :func:`benzene.kafka.run_consumer_loop`: a
module-level ``run_*_loop`` bounded by ``should_continue``, plus a ``*_worker`` factory that adapts
it to :class:`benzene.core.WorkerHost`, so an outbox relay is just another leg of a multi-transport
process and shuts down with the rest of them on SIGTERM.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from benzene.core import StopSignal, Worker

logger = logging.getLogger("benzene.outbox")


class DispatchesOutbox(Protocol):
    """The one method the loop drives — :class:`~benzene.outbox.OutboxDispatcher` satisfies it."""

    async def run_once(self) -> Any: ...


async def run_outbox_dispatcher_loop(
    dispatcher: DispatchesOutbox,
    *,
    should_continue: Callable[[], bool] = lambda: True,
    poll_interval: float = 5.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Call ``run_once`` every ``poll_interval`` seconds until ``should_continue`` says stop.

    **A run that raises is logged and survived**, never fatal: the outbox table being briefly
    unreachable must not kill the relay — the envelopes are durable, so the next poll simply picks
    up where this one stopped. ``sleep`` is injectable so a test paces the loop without waiting.

    This loop is for a process that has one: a container, a VM, a Kubernetes pod. On Lambda there is
    no background thread, so the pattern is a change-stream relay calling
    :meth:`~benzene.outbox.OutboxDispatcher.dispatch_one` per inserted envelope **plus** a
    low-frequency scheduled sweep calling ``run_once`` — a stream fires once per insert and cannot
    drive retries, parking or retention on its own.
    """
    while should_continue():
        try:
            await dispatcher.run_once()
        except asyncio.CancelledError:
            raise  # cooperative shutdown, not a dispatch failure
        except Exception:
            logger.warning("outbox dispatch run failed; retrying at the next poll", exc_info=True)
        await sleep(poll_interval)


def outbox_dispatcher_worker(dispatcher: DispatchesOutbox, **loop_options: Any) -> Worker:
    """A :data:`benzene.core.Worker` running :func:`run_outbox_dispatcher_loop` under a host.

    ``host.add("outbox", outbox_dispatcher_worker(dispatcher))`` runs the relay beside the service's
    own transports on one event loop, with one coordinated shutdown.
    """
    if "should_continue" in loop_options:
        raise TypeError(
            "outbox_dispatcher_worker() does not take should_continue - the WorkerHost supplies it, "
            "so one leg finishing winds the others down. To bound the loop yourself, call "
            "run_outbox_dispatcher_loop(dispatcher, should_continue=...) directly instead."
        )

    async def worker(stop: StopSignal) -> None:
        await run_outbox_dispatcher_loop(
            dispatcher, should_continue=stop.should_continue, **loop_options
        )

    return worker
