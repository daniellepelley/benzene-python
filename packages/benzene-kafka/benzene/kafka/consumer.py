"""Kafka inbound binding — a self-hosted consumer (transport-bindings §"Kafka").

A Kafka record carries the Benzene **topic** in its ``topic`` header (the cross-port convention shared
with SQS/SNS/Pub/Sub/Service Bus, wire-contracts §2); the remaining Kafka headers are the Benzene
headers (UTF-8 decoded), and the record's value is the JSON body. There is **no response channel** —
per the spec, result mapping is *acknowledge/log only*: one record is one pipeline invocation and one
DI scope, and the handler's result governs whether the loop commits the offset (success) or seeks
back so the record is redelivered, never a reply.

Everything here is duck-typed against the ``confluent-kafka`` shapes (``message.headers()`` /
``message.value()`` / ``message.error()`` / ``message.topic()`` / ``message.partition()`` /
``message.offset()``; ``consumer.poll()`` / ``consumer.commit()`` / ``consumer.seek()``), so the
binding — decode, per-record dispatch, and the consumer loop — is exercised in memory with fakes and
needs neither a broker nor the SDK. Only the real client is an optional dependency.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    StopSignal,
    Worker,
    application_from,
    read_message_metadata,
)
from benzene.results import Result

logger = logging.getLogger(__name__)


class KafkaMessage(Protocol):
    """The subset of a ``confluent_kafka.Message`` this binding reads (duck-typed)."""

    def headers(self) -> list[tuple[str, bytes]] | None: ...
    def value(self) -> bytes | None: ...


def _decode_headers(raw: list[tuple[str, Any]] | None) -> dict[str, str]:
    """Kafka headers (a list of ``(str, bytes)``) → the flat string metadata dictionary (UTF-8)."""
    attributes: dict[str, str] = {}
    for key, value in raw or []:
        if isinstance(value, (bytes, bytearray)):
            attributes[key] = bytes(value).decode("utf-8", "replace")
        else:
            attributes[key] = "" if value is None else str(value)
    return attributes


def decode_kafka_message(message: KafkaMessage) -> dict[str, Any]:
    """Decode a Kafka record into a Benzene envelope ``{topic, headers, body}``.

    The Benzene topic is resolved out of the ``topic`` header (the reserved metadata key); every other
    header becomes a Benzene header. The record value (bytes) is decoded as the UTF-8 JSON body.
    """
    topic, headers = read_message_metadata(_decode_headers(message.headers()))
    raw = message.value()
    if raw is None:
        body = ""
    elif isinstance(raw, (bytes, bytearray)):
        body = bytes(raw).decode("utf-8")
    else:
        body = str(raw)
    return {"topic": topic, "headers": headers, "body": body}


class KafkaConsumerApp:
    """Runs one Kafka record through the Benzene pipeline (one record → one invocation → one scope).

    :meth:`handle_message` never raises — the entry point always returns a response envelope (a
    malformed body is a ``bad-request``, a handler fault a ``service-unavailable``), so a poison record
    can never crash the consumer loop (the binding's failure rule). Since Kafka has no response
    channel, the mapped :class:`~benzene.results.Result` is returned for the loop to *act on* (commit /
    log), not sent back to a caller.
    """

    def __init__(self, application: BenzeneMessageApplication) -> None:
        self._application = application

    @classmethod
    def from_definition(cls, definition: AppDefinition) -> KafkaConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition))

    async def handle_message(self, message: KafkaMessage) -> Result:
        envelope = decode_kafka_message(message)
        response = await self._application.handle(envelope)
        return Result(response["statusCode"])


def _message_error(message: Any) -> Any:
    """A confluent record signals broker/EOF conditions via ``.error()``; absent on plain fakes."""
    error = getattr(message, "error", None)
    return error() if callable(error) else None


def _accessor(message: Any, name: str, default: Any) -> Any:
    """Read a confluent accessor (``topic()``/``partition()``/``offset()``), tolerating leaner fakes."""
    accessor = getattr(message, name, None)
    if not callable(accessor):
        return default
    value = accessor()
    return default if value is None else value


@dataclass(frozen=True)
class _TopicOffset:
    """A ``TopicPartition``-shaped seek target for consumers that are not the real SDK client.

    ``confluent_kafka.Consumer.seek`` requires a genuine ``TopicPartition``, so :func:`_seek_target`
    builds one when the SDK is installed. Without it the consumer can only be a duck-typed fake (the
    binding's standing promise: no broker, no SDK needed to run or test), and this carries the same
    three fields under the same names.
    """

    topic: str
    partition: int
    offset: int


def _seek_target(message: Any) -> Any:
    """The ``(topic, partition, offset)`` of ``message`` as a seek target the consumer will accept."""
    topic = _accessor(message, "topic", "")
    partition = _accessor(message, "partition", 0)
    offset = _accessor(message, "offset", 0)
    try:
        from confluent_kafka import TopicPartition  # lazy: optional dependency
    except ImportError:
        return _TopicOffset(topic, partition, offset)
    return TopicPartition(topic, partition, offset)


async def run_consumer_loop(
    app: KafkaConsumerApp,
    consumer: Any,
    *,
    poll_timeout: float = 1.0,
    should_continue: Callable[[], bool] = lambda: True,
    commit: bool = True,
    on_result: Callable[[Any, Result], None] | None = None,
) -> None:
    """Drive a self-hosted consumer: poll, dispatch one record at a time, commit the offset on success.

    ``consumer`` is duck-typed (``poll(timeout)`` → a record or ``None``; ``commit(message=...)``;
    ``seek(topic_partition)``). A record carrying a broker error (``message.error()``) is skipped.

    With ``commit=True`` (the default, at-least-once) the offset is committed only after a
    **successful** result. A Kafka commit is a *watermark*, not a per-message ack — committing a
    later record's offset also marks every earlier record on that partition consumed — so on a
    failure the loop instead ``seek``s back to that record's ``(topic, partition, offset)`` and
    never commits past it: the partition stays blocked until the failed record itself succeeds, and
    a success on any *other* partition still commits normally (offsets are per-partition).

    The consequence is that a poison record is re-served rather than silently dropped: the loop will
    keep re-delivering it until it succeeds. Callers cap that with the ``on_result`` /
    ``should_continue`` seams — count attempts, dead-letter the record and stop blocking, or break
    out of the loop. Every failure is logged at warning level, so a poison record is never invisible.
    A caller wanting different semantics passes ``commit=False``, in which case the loop touches
    neither ``commit`` nor ``seek`` and offset management is entirely the caller's (via
    ``on_result``). ``should_continue`` bounds the loop (a real worker loops forever; a test stops
    after N polls).

    ``consumer.poll``/``consumer.commit``/``consumer.seek`` are plain synchronous ``confluent-kafka``
    calls, run via :func:`asyncio.to_thread` rather than called directly on the event loop - called
    directly, an idle topic means ``poll`` returning ``None`` in a tight loop with **no** ``await``
    point at all, which would starve every other coroutine on the loop *permanently* (worse than the
    SQS consumer's periodic long-poll block above), e.g. an HTTP server hosted alongside this
    consumer. See ``docs/getting-started-kubernetes.md`` for the multi-transport-in-one-process story
    this makes possible.
    """
    # (topic, partition) → the offset of the oldest record that failed and has not yet succeeded.
    # While an entry stands, nothing on that partition may be committed: the commit would be a
    # watermark past the failure, marking it consumed.
    blocked: dict[tuple[str, int], int] = {}
    while should_continue():
        message = await asyncio.to_thread(consumer.poll, poll_timeout)
        if message is None:
            continue
        if _message_error(message) is not None:
            continue
        result = await app.handle_message(message)
        if on_result is not None:
            on_result(message, result)
        key = (_accessor(message, "topic", ""), _accessor(message, "partition", 0))
        offset = _accessor(message, "offset", 0)
        if not result.is_successful:
            # A failing record must never loop invisibly, even with no ``on_result`` wired.
            logger.warning(
                "record on topic %r (partition %s, offset %s) failed with status %s; %s",
                read_message_metadata(_decode_headers(message.headers()))[0],
                key[1],
                offset,
                result.status,
                "seeking back to redeliver it" if commit else "offsets left to the caller",
            )
            if commit:
                # At-least-once: stop advancing past the failure and re-fetch it on the next poll.
                blocked.setdefault(key, offset)
                await asyncio.to_thread(consumer.seek, _seek_target(message))
            continue
        if not commit:
            continue  # the caller owns offsets entirely: touch neither commit nor seek
        if blocked.get(key) == offset:
            del blocked[key]  # the failed record itself succeeded on redelivery — unblock
        if key in blocked:
            continue  # a later record succeeded first; committing it would bury the failure
        await asyncio.to_thread(consumer.commit, message=message)


def build_kafka_consumer(
    *,
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
    **config: Any,
) -> Any:
    """Build and subscribe a ``confluent_kafka.Consumer`` configured to match this binding's loop.

    The one setting worth calling out is ``enable.auto.commit=False``. It is not a preference: it is
    what makes :func:`run_consumer_loop`'s at-least-once rule true, because the loop commits the
    offset itself and only after a **successful** result. Auto-commit on, and a record that failed
    would have its offset committed anyway and never be redelivered. Getting that wrong is silent,
    so this builder gets it right by default.

    **The explicit form this composes** — still the thing to write when you want different broker
    settings, and equally supported — is the ``confluent-kafka`` constructor directly::

        consumer = Consumer({
            "bootstrap.servers": ..., "group.id": ...,
            "enable.auto.commit": False, "auto.offset.reset": "earliest",
        })
        consumer.subscribe(topics)

    Any extra ``**config`` is merged in, using ``confluent-kafka``'s own dotted key names, and wins
    over the defaults above — including ``enable.auto.commit``, if you really mean it.
    """
    try:
        from confluent_kafka import Consumer
    except ImportError as error:  # pragma: no cover - exercised only without the optional SDK
        raise ImportError(
            "build_kafka_consumer() needs the confluent-kafka SDK, which is an optional extra of "
            "benzene-kafka. Install it with: pip install 'benzene-kafka[kafka]'."
        ) from error

    settings: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        # The loop commits on success (at-least-once); auto-commit would defeat redelivery.
        "enable.auto.commit": False,
        "auto.offset.reset": auto_offset_reset,
    }
    settings.update(config)
    consumer = Consumer(settings)
    consumer.subscribe(topics)
    return consumer


def kafka_consumer_worker(
    app: KafkaConsumerApp,
    consumer: Any,
    *,
    close: bool = True,
    **loop_options: Any,
) -> Worker:
    """This consumer as one leg of a :class:`~benzene.core.WorkerHost` — for Kafka *alongside* HTTP.

    Reach for this only when the process runs more than one transport; a Kafka-only worker just
    awaits :func:`run_consumer_loop` directly and needs nothing from here.

    **The explicit form this composes** is a handful of lines you can write yourself, and it is the
    whole implementation — there is no privileged path::

        async def worker(stop):
            try:
                await run_consumer_loop(app, consumer, should_continue=stop.should_continue)
            finally:
                consumer.close()      # leave the group promptly rather than waiting for the timeout

    ``close`` owns that ``finally``: on by default because a consumer left open holds its partition
    assignment until the session times out, which delays every rebalance. Pass ``close=False`` when
    the consumer outlives the worker and you close it yourself.

    ``**loop_options`` are passed straight through to :func:`run_consumer_loop` (``poll_timeout``,
    ``commit``, ``on_result``, ...). Passing ``should_continue`` is refused: the host owns that, and
    silently ignoring your callback would be worse than saying so.
    """
    if "should_continue" in loop_options:
        raise TypeError(
            "kafka_consumer_worker() does not take should_continue - the WorkerHost supplies it, so "
            "one leg finishing winds the others down. To bound the loop yourself, call "
            "run_consumer_loop(app, consumer, should_continue=...) directly instead."
        )

    async def worker(stop: StopSignal) -> None:
        try:
            await run_consumer_loop(
                app, consumer, should_continue=stop.should_continue, **loop_options
            )
        finally:
            if close:
                consumer.close()

    return worker
