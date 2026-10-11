"""Kafka inbound binding — a self-hosted consumer (transport-bindings §"Kafka").

A Kafka record carries the Benzene **topic** in its ``topic`` header (the cross-port convention shared
with SQS/SNS/Pub/Sub/Service Bus, wire-contracts §2) — or, when no such header is present, the Kafka
topic it was consumed from, so a producer that writes no header still routes; the remaining Kafka headers are the Benzene
headers (UTF-8 decoded), and the record's value is the JSON body. There is **no response channel** —
one record is one pipeline invocation and one DI scope, and the outcome **settles** it
(transport-bindings §1, obligation 8).

**Settlement: skip-and-continue by default.** Kafka is a partition-ordered stream, so a record the loop
declines to acknowledge blocks every record behind it. By default the loop commits a record's offset
only when its invocation ended ``isSuccessful`` (``ignored`` included) and otherwise **moves on**: the
failed record — a failure, a missing result, an unmatched topic — is not committed itself, but the
next commit on its partition passes it, so it is redelivered only if the consumer restarts or
rebalances before another record on that partition succeeds. That is skip-and-continue, the default
a stream binding may choose (and what .NET's Kafka worker defaults to); the unit of settlement is the
record offset, committed cumulatively per partition. ``run_consumer_loop(..., stop_on_failure=True)``
is the **at-least-once** mode: the loop raises :class:`~benzene.core.MessageHandlingError` at the first
unacknowledged record without committing it, so the worker stops and a restart resumes at that record.
``KafkaConsumerApp(..., settlement=Settlement(at_most_once=True))`` commits whatever the outcome;
``Settlement(ignore_unmatched_topics=True)`` commits a topic no handler serves as ``ignored``.

Everything here is duck-typed against the ``confluent-kafka`` shapes (``message.headers()`` /
``message.value()`` / ``message.error()``; ``consumer.poll()`` / ``consumer.commit()``), so the
binding — decode, per-record dispatch, and the consumer loop — is exercised in memory with fakes and
needs neither a broker nor the SDK. Only the real client is an optional dependency.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, ClassVar, Protocol

from benzene.core import (
    DEFAULT_METADATA_KEYS,
    SKIP_AND_CONTINUE,
    STREAM,
    AppDefinition,
    BenzeneMessageApplication,
    MessageHandlingError,
    MetadataKeys,
    Settlement,
    SettlementDefaults,
    StopSignal,
    Worker,
    application_from,
    dispatch_one_way,
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


def decode_kafka_message(
    message: KafkaMessage, *, keys: MetadataKeys = DEFAULT_METADATA_KEYS
) -> dict[str, Any]:
    """Decode a Kafka record into a Benzene envelope ``{topic, headers, body}``.

    The Benzene topic is resolved out of the ``topic`` header (the reserved metadata key,
    ``keys.topic``), falling back to the Kafka topic the record was consumed from
    (``message.topic()``) when the header is absent, so a producer that writes no header still
    routes (``topicSources`` in ``transport-metadata-cases.json``); the header wins whenever present.
    Every other header becomes a Benzene header. The record value (bytes) is decoded as the UTF-8 JSON
    body.
    """
    topic, headers = read_message_metadata(_decode_headers(message.headers()), keys)
    if not topic:
        topic = _consumed_topic(message)
    raw = message.value()
    if raw is None:
        body = ""
    elif isinstance(raw, (bytes, bytearray)):
        body = bytes(raw).decode("utf-8")
    else:
        body = str(raw)
    return {"topic": topic, "headers": headers, "body": body}


def _consumed_topic(message: Any) -> str:
    """The Kafka topic the record was consumed from (``confluent_kafka.Message.topic()``), or ``""``."""
    topic = getattr(message, "topic", None)
    if callable(topic):
        topic = topic()
    if isinstance(topic, (bytes, bytearray)):
        return bytes(topic).decode("utf-8", "replace")
    return "" if topic is None else str(topic)


class KafkaConsumerApp:
    """Runs one Kafka record through the Benzene pipeline (one record → one invocation → one scope).

    :meth:`handle_message` returns the invocation's :class:`~benzene.results.Result` — decoded from
    the response envelope with its ``isSuccessful`` authoritative, never re-derived from the status
    text — for the loop to *settle* (commit / pass over / stop), not to send back to a caller. A
    malformed body is a ``bad-request`` and a handler fault a ``service-unavailable``, so content never
    crashes the loop.
    """

    #: Skip-and-continue by default, a stream's documented choice (see the module docstring).
    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (
        SettlementDefaults("kafka", STREAM, SKIP_AND_CONTINUE, SKIP_AND_CONTINUE),
    )

    def __init__(
        self, application: BenzeneMessageApplication, *, settlement: Settlement = Settlement()
    ) -> None:
        self._application = application
        self._settlement = settlement

    @classmethod
    def from_definition(
        cls, definition: AppDefinition, *, settlement: Settlement = Settlement()
    ) -> KafkaConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition), settlement=settlement)

    @property
    def settlement(self) -> Settlement:
        """How the loop settles each record: committed iff ``settlement.acknowledges(result)``."""
        return self._settlement

    async def handle_message(self, message: KafkaMessage) -> Result:
        outcome = await dispatch_one_way(
            self._application,
            self._settlement,
            decode_kafka_message,
            message,
            keys=self._application.metadata_keys,
        )
        return outcome.result


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
    stop_on_failure: bool = False,
    on_result: Callable[[Any, Result], None] | None = None,
) -> None:
    """Drive a self-hosted consumer: poll, dispatch one record at a time, settle each.

    ``consumer`` is duck-typed (``poll(timeout)`` → a record or ``None``; ``commit(message=...)``).
    A record carrying a broker error (``message.error()``) is skipped. With ``commit=True`` (the
    default) a record's offset is committed only when ``app.settlement`` acknowledges its result — by
    default a **successful** one; an unacknowledged record is **passed over** (skip-and-continue: the
    next commit on its partition passes it). ``stop_on_failure=True`` is the at-least-once mode:
    the first unacknowledged record raises :class:`~benzene.core.MessageHandlingError` without being
    committed, so the worker stops and a restart resumes at it. A caller committing by hand passes
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
        if app.settlement.acknowledges(result):
            if commit:
                await asyncio.to_thread(consumer.commit, message=message)
        elif stop_on_failure:
            # At-least-once: stop before anything commits past this record, so a restart resumes here.
            topic = decode_kafka_message(message)["topic"]
            raise MessageHandlingError(topic, result.status, ", ".join(result.messages))
        # Otherwise skip-and-continue: not committed, and the loop moves on.


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
    what makes :func:`run_consumer_loop`'s settlement true, because the loop commits the offset
    itself and only for an acknowledged result. Auto-commit on, and a record that failed would have
    its offset committed before the loop even saw the outcome — no ``stop_on_failure`` could hold it
    back. Getting that wrong is silent, so this builder gets it right by default.

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
        # The loop commits what it acknowledges; auto-commit would commit before the outcome is known.
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
