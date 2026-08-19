"""Kafka inbound binding — a self-hosted consumer (transport-bindings §"Kafka").

A Kafka record carries the Benzene **topic** in its ``topic`` header (the cross-port convention shared
with SQS/SNS/Pub/Sub/Service Bus, wire-contracts §2); the remaining Kafka headers are the Benzene
headers (UTF-8 decoded), and the record's value is the JSON body. There is **no response channel** —
per the spec, result mapping is *acknowledge/log only*: one record is one pipeline invocation and one
DI scope, and the handler's result governs whether the loop commits the offset (success) or leaves it
for redelivery, never a reply.

Everything here is duck-typed against the ``confluent-kafka`` shapes (``message.headers()`` /
``message.value()`` / ``message.error()``; ``consumer.poll()`` / ``consumer.commit()``), so the
binding — decode, per-record dispatch, and the consumer loop — is exercised in memory with fakes and
needs neither a broker nor the SDK. Only the real client is an optional dependency.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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

    ``consumer`` is duck-typed (``poll(timeout)`` → a record or ``None``; ``commit(message=...)``).
    A record carrying a broker error (``message.error()``) is skipped. With ``commit=True`` (the
    default, at-least-once) the offset is committed only after a **successful** result, so a failed
    record is redelivered rather than silently dropped; a caller wanting different semantics passes
    ``commit=False`` and commits from ``on_result``. ``should_continue`` bounds the loop (a real
    worker loops forever; a test stops after N polls).

    ``consumer.poll``/``consumer.commit`` are plain synchronous ``confluent-kafka`` calls, run via
    :func:`asyncio.to_thread` rather than called directly on the event loop - called directly, an idle
    topic means ``poll`` returning ``None`` in a tight loop with **no** ``await`` point at all, which
    would starve every other coroutine on the loop *permanently* (worse than the SQS consumer's
    periodic long-poll block above), e.g. an HTTP server hosted alongside this consumer. See
    ``docs/getting-started-kubernetes.md`` for the multi-transport-in-one-process story this makes
    possible.
    """
    while should_continue():
        message = await asyncio.to_thread(consumer.poll, poll_timeout)
        if message is None:
            continue
        if _message_error(message) is not None:
            continue
        result = await app.handle_message(message)
        if on_result is not None:
            on_result(message, result)
        # At-least-once: commit only a successful outcome, so a failed record is redelivered, not lost.
        if commit and result.is_successful:
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
