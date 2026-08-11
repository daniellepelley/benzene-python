"""Response-as-event — publish each invocation's result to an event sink (mirrors ``Benzene.ResponseEvents``).

Where :mod:`benzene.otel.exporter` exports *traces* (how long, where in the trace), this pattern
exports *outcomes*: after a handler runs, the invocation's result — its topic, semantic status, and
optional payload, tagged with the business correlation id — is emitted as a discrete event to a
pluggable :class:`ResponseEventSink`. That feeds an event stream (an OTel event/log pipeline, an
outbox, an audit topic) with "topic ``x`` produced status ``y``" without the handler knowing it is
observed.

It ships as an ordinary middleware, :func:`response_event_interception`, installed anywhere ahead of
the message router. It emits *after* ``await next()``, once the result exists. Like every observer in
this port it is non-blocking on the request path in spirit and **lossy by contract**: a sink that
raises is swallowed (and logged), because emitting an event must never break the invocation that
produced it — exactly the guarantee :func:`benzene.mesh.trace_middleware` gives for trace export.

Mirrors .NET's ``Benzene.ResponseEvents``: the response, reshaped as an event and handed to a sink.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from benzene.core import Context, Middleware, Next
from benzene.results import Result, Status, is_successful

_logger = logging.getLogger("benzene.otel.response_events")

#: The business correlation id header, as read by :func:`benzene.mesh.trace_middleware`.
CORRELATION_ID_HEADER = "x-correlation-id"


@dataclass(frozen=True)
class ResponseEvent:
    """One invocation's result, reshaped as an event. ``to_payload`` is its camelCase wire form.

    Captures the topic and (optional) version that were handled, the semantic status produced, the
    business ``correlation_id`` (from ``x-correlation-id``), any failure ``errors``, and — unless the
    middleware was told to omit it — the response ``payload``.
    """

    topic: str
    status: str
    version: str | None = None
    correlation_id: str | None = None
    payload: Any | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_successful(self) -> bool:
        return is_successful(self.status)

    def to_payload(self) -> dict[str, Any]:
        """The wire payload — camelCase keys, omitting (never nulling) what isn't known."""
        payload: dict[str, Any] = {"topic": self.topic, "status": self.status}
        optional = {
            "version": self.version,
            "correlationId": self.correlation_id,
            "payload": self.payload,
        }
        payload.update({k: v for k, v in optional.items() if v is not None})
        if self.errors:
            payload["errors"] = list(self.errors)
        return payload


@runtime_checkable
class ResponseEventSink(Protocol):
    """Where response events go — the seam a concrete stream (OTel, outbox, audit topic) implements.

    ``emit`` is async so a network-backed sink is a drop-in; it should not raise, but the middleware
    tolerates it if it does. :class:`RecordingSink` is the in-memory fake for tests.
    """

    async def emit(self, event: ResponseEvent) -> None: ...


class RecordingSink(list):
    """A ``list`` of emitted :class:`ResponseEvent`s (``emit`` appends) — the fake for tests and dogfooding.

    Unbounded and in-memory; perfect for asserting what a pipeline published, not for production.
    """

    async def emit(self, event: ResponseEvent) -> None:
        self.append(event)


def response_event_interception(
    sink: ResponseEventSink,
    *,
    when: Callable[[Result], bool] | None = None,
    include_payload: bool = True,
) -> Middleware:
    """Middleware that emits each invocation's result to ``sink`` after the handler runs.

    Install it ahead of the message router. After ``await next()`` it reads ``context.result`` (a
    missing result is treated as ``unexpected-error``, so a swallowed crash still surfaces an event),
    builds a :class:`ResponseEvent`, and awaits ``sink.emit`` for it.

    ``when`` filters which results are emitted; the default is **every** result (successes *and*
    failures — the response stream is a complete audit of outcomes). Pass
    ``when=lambda r: r.is_successful`` for successes only. ``include_payload=False`` drops the payload
    from the event (topic/status/correlation only) for a lighter or less sensitive stream.

    A sink that raises is logged and swallowed — emitting an event never breaks the invocation.
    """
    should_emit = when or (lambda _result: True)

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        await next()
        result = (
            context.result
            if context.result is not None
            else Result.failure(Status.UNEXPECTED_ERROR, "handler produced no result")
        )
        if not should_emit(result):
            return
        event = ResponseEvent(
            topic=context.topic,
            status=result.status,
            version=context.version or None,
            correlation_id=context.headers.get(CORRELATION_ID_HEADER),
            payload=result.payload if include_payload else None,
            errors=result.errors,
        )
        # Lossy by contract: a failing sink must never break the invocation it observes.
        try:
            await sink.emit(event)
        except Exception:
            _logger.warning("response-event sink failed for topic %r", context.topic, exc_info=True)

    return middleware
