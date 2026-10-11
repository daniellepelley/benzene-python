"""Settlement — whether a one-way binding lets its transport forget a message.

A one-way binding (a queue, a topic, a stream, an event trigger) has no caller to answer, so the only
thing it does with a result is **settle** the message: acknowledge it (delete, ack, commit, return
normally) or decline to, so the transport redelivers it (transport-bindings §1, obligation 8):

- A message whose invocation ended ``isSuccessful: false``, with **no result** (a middleware ended the
  pipeline without setting one), or with an uncaught error is **not acknowledged**. A success —
  ``ignored`` included — is. Delivery is therefore at-least-once, and handlers on one-way bindings
  must be idempotent.
- A topic no handler serves is a ``not-found`` failure, so it is redelivered too.
  :attr:`Settlement.ignore_unmatched_topics` treats it as ``ignored`` instead, for a subscriber on a
  shared bus that sees other services' topics.
- :attr:`Settlement.at_most_once` acknowledges whatever the outcome — a failure is logged and dropped.
- Dead-lettering is the transport's own redrive configuration (a maximum receive count, a dead-letter
  queue or topic): Benzene declines the acknowledgement, the transport decides when to give up.

Every one-way host takes the same ``settlement=Settlement(...)`` keyword — ``AwsLambdaApp``,
``AzureFunctionsApp``, ``GcpFunctionsApp``, and the self-hosted SQS, Kafka and RabbitMQ consumer apps —
and records its shape and defaults as ``SETTLEMENT_DEFAULTS`` (:class:`SettlementDefaults`), which the
conformance runner checks against ``transport-metadata-cases.json``'s ``settlementDefaults``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from benzene.results import Result

from .envelope import BenzeneMessageApplication, decode_response

_log = logging.getLogger("benzene.settlement")

#: A binding's settlement **shape** (``settlementDefaults.shapes`` in transport-metadata-cases.json).
QUEUE = "queue"
#: A single delivery with no per-message ack — an asynchronous function invocation, an event trigger.
ONE_SHOT = "one-shot"
#: A partition-ordered stream whose retention blocks the partition behind a failed message.
STREAM = "stream"
#: A binding with a caller to answer: it settles by responding, so no default applies.
REQUEST_RESPONSE = "request-response"

#: The failed message is not acknowledged, so the transport redelivers it.
REDELIVER = "redeliver"
#: A stream's alternative: the failed message is passed over and the partition moves on.
SKIP_AND_CONTINUE = "skip-and-continue"
#: No settlement to default (a request/response binding).
NOT_APPLICABLE = "n/a"


@dataclass(frozen=True)
class Settlement:
    """How a one-way binding settles each message — the override knobs, both off by default.

    The default is the spec's at-least-once rule above. ``at_most_once=True`` acknowledges every
    message whatever its outcome (a failure, a missing result, an uncaught error are logged at
    WARNING on ``benzene.settlement`` and the message is gone). ``ignore_unmatched_topics=True`` answers
    a topic no handler serves — at any version — ``ignored``, which is acknowledged; a known topic at an
    unknown version is still a ``not-found`` failure.
    """

    at_most_once: bool = False
    ignore_unmatched_topics: bool = False

    def acknowledges(self, result: Result[Any]) -> bool:
        """Whether a message whose invocation produced ``result`` is acknowledged."""
        return self.at_most_once or result.is_successful


#: The spec's default: acknowledge successes only.
AT_LEAST_ONCE = Settlement()


@dataclass(frozen=True)
class SettlementDefaults:
    """A binding's settlement constants (transport-bindings §1, obligation 8).

    ``binding`` is the binding's id in ``transport-metadata-cases.json``'s ``settlementDefaults``,
    ``shape`` one of :data:`QUEUE` / :data:`ONE_SHOT` / :data:`STREAM` / :data:`REQUEST_RESPONSE`, and
    ``on_failure`` / ``unmatched_topic`` what the binding does by default with a failed (or
    result-less, or erroring) invocation and with a topic no handler serves.
    """

    binding: str
    shape: str
    on_failure: str
    unmatched_topic: str


def redelivers(binding: str, shape: str) -> SettlementDefaults:
    """The :class:`SettlementDefaults` of a binding that redelivers (or, request/response, answers)."""
    if shape == REQUEST_RESPONSE:
        return SettlementDefaults(binding, shape, NOT_APPLICABLE, NOT_APPLICABLE)
    return SettlementDefaults(binding, shape, REDELIVER, REDELIVER)


@dataclass(frozen=True)
class OneWayOutcome:
    """One message's settlement: the decoded result, the response envelope, and whether to ack.

    ``response`` is ``None`` only when the invocation raised and :attr:`Settlement.at_most_once`
    swallowed it (``error`` then holds the exception); otherwise ``result`` is the response envelope
    decoded with its ``isSuccessful`` authoritative (wire-contracts §1.2) — never re-derived from the
    status text.
    """

    topic: str
    result: Result[Any]
    response: Mapping[str, Any] | None
    acknowledge: bool
    error: Exception | None = None

    @property
    def detail(self) -> str:
        """What to say about a failure in a log line or a :class:`MessageHandlingError`."""
        if self.response is not None:
            return str(self.response.get("body") or "")
        return repr(self.error) if self.error is not None else ""


async def dispatch_one_way(
    application: BenzeneMessageApplication,
    settlement: Settlement,
    decode: Callable[..., Mapping[str, Any]],
    /,
    *args: Any,
    **kwargs: Any,
) -> OneWayOutcome:
    """Decode one native message, run it, and decide whether to acknowledge it.

    ``decode(*args, **kwargs)`` turns the native message into an envelope — passed rather than
    called by the binding, the way :func:`asyncio.to_thread` takes a function and its arguments, so
    that a decode failure is settled by the same rule as a pipeline failure. Without
    ``at_most_once`` an exception propagates (the binding fails the delivery, and the transport
    redelivers it); with it, the exception is logged and the message acknowledged.
    """
    topic = ""
    try:
        envelope = decode(*args, **kwargs)
        topic = str(envelope.get("topic") or "")
        response = await application.handle(
            dict(envelope), ignore_unmatched_topic=settlement.ignore_unmatched_topics
        )
    except Exception as error:
        if not settlement.at_most_once:
            raise
        _log.warning(
            "at-most-once settlement acknowledged a message on topic %r whose invocation raised",
            topic,
            exc_info=error,
        )
        failed = Result.unexpected_error(f"{type(error).__name__}: {error}")
        return OneWayOutcome(topic, failed, None, acknowledge=True, error=error)

    result = decode_response(response)
    acknowledge = settlement.acknowledges(result)
    if acknowledge and not result.is_successful:
        _log.warning(
            "at-most-once settlement acknowledged a failed message on topic %r (%s)",
            topic,
            result.status,
        )
    return OneWayOutcome(topic, result, response, acknowledge)
