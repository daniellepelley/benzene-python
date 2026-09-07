"""The capture point: a decorator over ``MessageSender`` that records a send instead of doing it.

.NET captures with an outbound-route middleware because its outbound side *is* a middleware chain.
Python's outbound side is a decorator over the one :class:`~benzene.core.MessageSender` seam
(``with_retry``, ``with_correlation_id``, ``with_rate_limit``), so the outbox belongs in that idiom:
``with_outbox(sender, store)`` is a ``MessageSender``, the call site never changes, and the handler
still writes ``await sender.send_message(topic, message)``.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any

from benzene.core import MessageSender, encode_body
from benzene.results import Result

from .envelope import OutboxEnvelope
from .options import IDEMPOTENCY_KEY_HEADER, OutboxOptions
from .stage import OutboxNotStagedError, OutboxStage, current_stage
from .store import OutboxStore


class OutboxMessageSender:
    """Captures a send into the outbox instead of performing it, and answers ``accepted``.

    **Capture is terminal: ``inner`` is never called here.** It is held because it is the sender the
    *dispatcher* will use to perform the deferred send — keeping it on the capture object is what
    lets one line of wiring name the transport once (``captured = with_outbox(sqs, store)``;
    ``OutboxDispatcher(store, captured.inner)``).

    **Composition order is load-bearing.** Header-stamping decorators go **outside** the outbox, so
    what they stamp is captured in the envelope and replayed at business time::

        sender = with_correlation_id(with_outbox(sqs_sender, store))   # correct
        sender = with_outbox(with_correlation_id(sqs_sender), store)   # wrong: never runs

    Transport-guarding decorators (retry, circuit breaker) belong on the sender the **dispatcher**
    holds, not on the capture chain: capture writes to a database, and the dispatcher already retries
    with durable, cross-process backoff::

        dispatcher = OutboxDispatcher(store, with_retry(sqs_sender))

    **An outboxed send is fire-and-forget.** The caller is told ``accepted`` — "recorded, and it will
    go" — never the downstream's real answer, which does not exist yet. Request/response topics must
    not be outboxed.

    A store or stage failure **propagates**: the caller learns the send was not recorded, exactly as
    they would learn a transport refused it. Never a silent loss.
    """

    def __init__(
        self,
        inner: MessageSender,
        store: OutboxStore,
        *,
        options: OutboxOptions | None = None,
        stage: OutboxStage | None = None,
        new_id: Callable[[], str] = lambda: uuid.uuid4().hex,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._inner = inner
        self._store = store
        self._options = options or OutboxOptions()
        self._stage = stage
        self._new_id = new_id
        self._clock = clock

    @property
    def inner(self) -> MessageSender:
        """The transport the deferred send will eventually use — hand this to the dispatcher."""
        return self._inner

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        envelope = self.capture(topic, message, headers)
        if self._options.write_mode == "immediate":
            await self._store.add([envelope])
        else:
            await self._resolve_stage().stage(envelope)
        return Result.accepted()

    def capture(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> OutboxEnvelope:
        """Build the envelope for a send without storing it (the seam a custom capture reuses)."""
        envelope_id = self._new_id()
        captured = {key.lower(): value for key, value in (headers or {}).items()}
        if self._options.stamp_idempotency_key and IDEMPOTENCY_KEY_HEADER not in captured:
            # The one line that makes an outbox producer and an idempotent consumer click together
            # with no configuration: every redelivery of this envelope carries this same key.
            captured[IDEMPOTENCY_KEY_HEADER] = envelope_id
        return OutboxEnvelope(
            id=envelope_id,
            topic=topic,
            # encode_body is the wire-naming entry point every outbound client uses, so the stored
            # body is byte-identical to what an inline send would have produced.
            payload=encode_body(message),
            headers=captured,
            created_at=self._clock(),
        )

    def _resolve_stage(self) -> OutboxStage:
        stage = self._stage or current_stage()
        if stage is None:
            raise OutboxNotStagedError(
                "write_mode='transactional' captures into a stage, and none is in scope. Open one "
                "with `async with outbox_transaction(stage=SqlOutboxStage(connection)):` (the "
                "envelope then commits with your own transaction), or with "
                "`outbox_transaction(commit=store.add)`, or install outbox_interception(store) in "
                "the pipeline. Use write_mode='immediate' if you want capture to write to the store "
                "directly."
            )
        return stage


def with_outbox(inner: MessageSender, store: OutboxStore, **options: Any) -> OutboxMessageSender:
    """Sugar for :class:`OutboxMessageSender` — ``with_outbox(sqs_sender, store)``."""
    return OutboxMessageSender(inner, store, **options)
