"""Middleware that scopes staging to one handler invocation.

.NET's ``OutboxMiddleware`` is an *outbound-route* middleware because that is where its capture
happens; here capture is :class:`~benzene.outbox.OutboxMessageSender`, so what is left for inbound
middleware is the other half of the same job: deciding **when the staged envelopes are committed**.
This middleware opens a stage for the invocation and commits what was staged only if the invocation
succeeded — the unit-of-work boundary, expressed where the pipeline already has one.
"""

from __future__ import annotations

from collections.abc import Callable
from inspect import isawaitable

from benzene.core import Context, Middleware, Next
from benzene.results import Result

from .stage import BufferedOutboxStage, OutboxCommit, bind_stage, unbind_stage
from .store import OutboxStore


def outbox_interception(
    store: OutboxStore | None = None,
    *,
    commit: OutboxCommit | None = None,
    commit_when: Callable[[Result | None], bool] | None = None,
) -> Middleware:
    """Stage the sends a handler makes, and commit them when the handler succeeds.

    Install it ahead of the message router, and give the capturing sender
    ``OutboxOptions(write_mode="transactional")``. Every send the handler makes is then buffered for
    that invocation; when the handler settles successfully the buffer is committed in one call
    (``store.add`` by default, or your own ``commit``); when it fails, or raises, the buffer is
    discarded — nothing was sent, and if your state write shared the same transaction, nothing was
    written either.

    Be clear about what this alone buys. With the default ``commit=store.add`` it closes the "the
    handler decided to send, then failed" window and defers every send to the dispatcher — but the
    envelope write is still a *second* write, not atomic with your state write. For the atomic
    version, pass a ``commit`` that performs your own transaction's commit, or stage straight onto
    your connection with :class:`~benzene.outbox.SqlOutboxStage` and
    :func:`~benzene.outbox.outbox_transaction`.

    ``commit_when`` overrides the default rule (commit when the invocation's result is successful).
    """
    if commit is not None:
        committer: OutboxCommit = commit
    elif store is not None:
        committer = store.add
    else:
        raise ValueError(
            "outbox_interception needs somewhere to commit staged envelopes: pass a store "
            "(outbox_interception(store)) or your own commit= callable."
        )
    should_commit = commit_when or (lambda result: result is not None and result.is_successful)

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        stage = BufferedOutboxStage()
        token = bind_stage(stage)
        try:
            await next()
            if should_commit(context.result):
                drained = stage.drain()
                if drained:
                    outcome = committer(drained)
                    if isawaitable(outcome):
                        await outcome
        finally:
            unbind_stage(token)
            stage.close()  # warns if the handler staged sends that no commit ever took

    return middleware
