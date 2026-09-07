"""The staging seam — "in the same transaction as my state write", without abstracting your database.

Python has no ambient transaction, and this package does not invent one. A stage is simply
*somewhere to put an envelope that is not the store*: the honest durable shape is
:class:`~benzene.outbox.SqlOutboxStage`, which holds **the caller's own connection** and issues one
``INSERT`` on it — it never opens a transaction, never commits, never rolls back, and knows nothing
about the rest of the schema. Your transaction stays yours; the envelope simply rides along in it.

:class:`BufferedOutboxStage` is the other shape: a list, drained by whatever commit you supply. It is
what :func:`outbox_transaction` and :func:`~benzene.outbox.outbox_interception` bind when you have not
handed them a stage of your own.

The ambient binding is a :class:`contextvars.ContextVar`, which is per-``asyncio``-task, so two
concurrent handler invocations never see each other's staged envelopes.
"""

from __future__ import annotations

import contextvars
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Any, Protocol, runtime_checkable

from .envelope import OutboxEnvelope

#: Called with the drained envelopes when a buffered :func:`outbox_transaction` block exits cleanly.
#: May be sync or async — a plain ``store.add`` works either way.
OutboxCommit = Callable[[Sequence[OutboxEnvelope]], Any]


@runtime_checkable
class OutboxStage(Protocol):
    """Somewhere to put a captured envelope that is not (yet) the store."""

    async def stage(self, envelope: OutboxEnvelope) -> None: ...


class OutboxNotStagedError(LookupError):
    """Raised when a transactional capture happens with no stage in scope.

    Deliberately loud. .NET's buffered stage discards silently-with-a-warning in this case; here the
    send would vanish with nothing written and nothing said, which is the exact failure the outbox
    exists to remove. A misconfiguration should not look like a smaller guarantee.
    """


_ambient: contextvars.ContextVar[OutboxStage | None] = contextvars.ContextVar(
    "benzene_outbox_stage", default=None
)


def current_stage() -> OutboxStage | None:
    """The stage bound to this task, if any — what a transactional capture writes through."""
    return _ambient.get()


def bind_stage(stage: OutboxStage) -> contextvars.Token[OutboxStage | None]:
    """Bind ``stage`` for this task and return the token that unbinds it.

    The seam :func:`outbox_transaction` and :func:`~benzene.outbox.outbox_interception` are built
    from; reach for it only when you are scoping a stage to something neither of those covers, and
    always unbind in a ``finally``.
    """
    return _ambient.set(stage)


def unbind_stage(token: contextvars.Token[OutboxStage | None]) -> None:
    """Undo a :func:`bind_stage`, restoring whatever stage (or none) was bound before it."""
    _ambient.reset(token)


class BufferedOutboxStage:
    """Buffers staged envelopes in a list until something drains them.

    Persisting nothing is the point: the buffer is handed to *your* commit, so the envelopes reach
    storage in whatever operation you were already performing.
    """

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._staged: list[OutboxEnvelope] = []
        self._logger = logger or logging.getLogger("benzene.outbox")

    @property
    def staged(self) -> tuple[OutboxEnvelope, ...]:
        """What has been staged and not yet drained."""
        return tuple(self._staged)

    async def stage(self, envelope: OutboxEnvelope) -> None:
        self._staged.append(envelope)

    def drain(self) -> list[OutboxEnvelope]:
        """Return everything staged so far and clear the buffer."""
        drained, self._staged = self._staged, []
        return drained

    def close(self) -> None:
        """Finish with this stage, warning loudly if anything staged was never drained.

        Expected when a handler failed before committing — the envelopes are correctly discarded,
        because no state was committed either. Unexpected otherwise, and worth a loud signal rather
        than a silent hole where a send used to be.
        """
        if self._staged:
            self._logger.warning(
                "%d outbox envelope(s) were staged but never drained/committed, and are discarded. "
                "Expected if the handler failed before its own commit (nothing was written either); "
                "otherwise the commit that should persist them is missing.",
                len(self._staged),
            )
            self._staged = []


@asynccontextmanager
async def outbox_transaction(
    *,
    stage: OutboxStage | None = None,
    commit: OutboxCommit | None = None,
) -> AsyncIterator[OutboxStage]:
    """Bind a stage for the duration of a block, so transactional captures inside it are staged.

    Two shapes, and which one you want depends on where the atomicity comes from:

    * ``outbox_transaction(stage=SqlOutboxStage(connection))`` — the envelope is inserted on **your**
      connection as it is captured. Your own ``commit()`` (or ``rollback()``) then settles the state
      write and the envelope together, because they are literally the same transaction. Benzene
      neither commits nor rolls back anything.
    * ``outbox_transaction(commit=...)`` — envelopes are buffered, and on a clean exit ``commit`` is
      called once with all of them (``commit`` may be sync or async, e.g. ``store.add``). An
      exception leaving the block discards the buffer instead: consistent by construction, since
      whatever the block was doing did not finish either.

    Passing both is an error — the buffer would have nothing to do.
    """
    if stage is not None and commit is not None:
        raise ValueError(
            "outbox_transaction takes either stage= (your own connection commits the envelope) or "
            "commit= (a buffer drained on clean exit), not both."
        )
    buffered = BufferedOutboxStage() if stage is None else None
    active: OutboxStage = buffered if buffered is not None else stage  # type: ignore[assignment]
    token = _ambient.set(active)
    try:
        yield active
    except BaseException:
        if buffered is not None:
            buffered.drain()  # discard: nothing this block did was committed
        raise
    else:
        if buffered is not None and commit is not None:
            drained = buffered.drain()
            if drained:
                outcome = commit(drained)
                if isawaitable(outcome):
                    await outcome
    finally:
        _ambient.reset(token)
        if buffered is not None:
            buffered.close()
