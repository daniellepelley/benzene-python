"""An in-process :class:`MessageSender`: dispatches straight to a handler pipeline built in the
same runtime, without going over any wire (no SQS/SNS/HTTP/socket — not even loopback). It exists
for the case where functionality that used to live in a different service has been moved into the
caller's own service, and the topic that used to be sent over a real transport now has no reason
to leave the process.

See the cross-language modular monolith pattern for the shape this is written toward:
https://github.com/daniellepelley/Benzene/blob/main/docs/patterns/modular-monolith.md

PORT DIVERGENCE from .NET/TypeScript: those ports build one named in-process pipeline per module
inside a single registration call and route to it from a shared, container-wide outbound routing
table (``OutboundRoutingBuilder``/``addOutboundRouting``) via a name-only ``useInProcess(name)``.
This port has neither of those things to slot into — for reasons specific to this port's own
architecture, not a limitation introduced here:

* There is no outbound routing table in this port at all — ``MessageSender`` is a single interface
  an application constructs and uses directly (e.g. ``SnsMessageSender(topic_arn)``). An in-process
  sender fits the exact same shape: construct it, use it directly, no route table to hook into.
* :class:`~benzene.core.Registry` is per-instance, not a process-wide singleton the way .NET's
  ``MessageHandlerDefinitionIndex`` or TypeScript's decorator-driven finder are — each named
  pipeline in a :class:`Pipelines` set has its own independent
  :class:`~benzene.core.BenzeneMessageApplication` (its own ``Registry``, ``Container``), so two
  different named pipelines CAN legitimately register a handler for the literal same topic with
  zero collision — unlike .NET/TypeScript, :class:`InProcessFanOutSender` needs no per-target-topic
  workaround (see those ports' ``DuplicateInProcessFanOutTargetException`` for why they needed one).
* :meth:`Pipelines.add` raises for a duplicate name, and both sender constructors raise for a name
  that isn't registered — checked at construction, which in this port's explicit-wiring style
  already happens once, at startup. There is no separate boot-time-validation mechanism to add on
  top (.NET's ``IStartUpCheck`` has no analogue in this port), because there is nothing lazy to
  validate: a sender that failed to construct simply doesn't exist to be misused later.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from benzene.results import Result

from .envelope import BenzeneMessageApplication, decode_response
from .mapping import encode_body

logger = logging.getLogger(__name__)


class DuplicatePipelineError(Exception):
    """Raised by :meth:`Pipelines.add` when a name is already registered."""


class PipelineNotFoundError(Exception):
    """Raised by :class:`InProcessMessageSender`/:class:`InProcessFanOutSender` construction when
    a requested pipeline name isn't registered in the :class:`Pipelines` set."""


class Pipelines:
    """Accumulates one named in-process pipeline per module.

    Each name maps to its own independent :class:`~benzene.core.BenzeneMessageApplication` — its
    own ``Registry``/``Container`` — so different modules never share a handler namespace.
    """

    def __init__(self) -> None:
        self._by_name: dict[str, BenzeneMessageApplication] = {}

    def add(self, name: str, application: BenzeneMessageApplication) -> Pipelines:
        """Registers ``application`` under ``name``.

        Raises :class:`DuplicatePipelineError` if ``name`` is already registered.
        """
        if name in self._by_name:
            raise DuplicatePipelineError(
                f"Pipeline {name!r} was already added to this Pipelines set."
            )
        self._by_name[name] = application
        return self

    def names(self) -> list[str]:
        """Every registered pipeline name, for diagnostics (error messages)."""
        return list(self._by_name)

    def get(self, name: str) -> BenzeneMessageApplication:
        """Resolves the application registered under ``name``.

        Raises :class:`PipelineNotFoundError` if ``name`` isn't registered.
        """
        application = self._by_name.get(name)
        if application is None:
            raise PipelineNotFoundError(
                f"No pipeline named {name!r} is registered in this Pipelines set "
                f"(registered: {self.names()!r})."
            )
        return application


class InProcessMessageSender:
    """A :class:`MessageSender` that dispatches straight to one named pipeline in a
    :class:`Pipelines` set, without leaving the process.

    The request body is still serialized to JSON on the way in and the response payload still
    parsed from JSON on the way out (via :class:`~benzene.core.BenzeneMessageApplication`'s own
    envelope encode/decode) — this transport removes the network hop, not the serialize/deserialize
    step, so a handler behind it sees exactly the same shape of request a real transport would hand
    it, and switching a topic between this and a real transport later is a one-line change.
    """

    def __init__(self, pipelines: Pipelines, name: str) -> None:
        # Resolved eagerly (not deferred to first send) so a typo'd or forgotten name is caught at
        # construction time — see the module docstring for why no separate boot-time check exists.
        self._application = pipelines.get(name)
        self._name = name

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        return await _dispatch(self._application, topic, message, headers)


class InProcessFanOutSender:
    """A :class:`MessageSender` that dispatches one send to several named pipelines in a
    :class:`Pipelines` set concurrently — the in-monolith equivalent of one SNS topic fanning out
    to several subscribers.

    Unlike the .NET and TypeScript ports' fan-out, no per-target topic is needed: each named
    pipeline has its own independent :class:`~benzene.core.Registry` (not shared process-wide), so
    two targets CAN legitimately both handle the literal same topic with zero collision.

    Each target's failure (a raised exception or an unsuccessful status) is isolated — logged, but
    does not affect the other targets or this sender's own (always successful) return value,
    matching what a real SNS publish does (accepted once published, no visibility into subscriber
    outcomes). There is no in-process DLQ: a target's failure is genuinely lost unless its own
    handler retries internally.
    """

    def __init__(self, pipelines: Pipelines, *names: str) -> None:
        if not names:
            raise ValueError("InProcessFanOutSender requires at least one pipeline name.")
        # Resolved eagerly, for the same reason InProcessMessageSender does - see the module
        # docstring.
        self._applications = [(name, pipelines.get(name)) for name in names]

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        await asyncio.gather(
            *(
                self._dispatch_one(name, application, topic, message, headers)
                for name, application in self._applications
            )
        )
        # Unconditional success once accepted - matching what a real SNS publish returns (no
        # visibility into subscriber outcomes).
        return Result.ok()

    async def _dispatch_one(
        self,
        name: str,
        application: BenzeneMessageApplication,
        topic: str,
        message: Any,
        headers: dict[str, str] | None,
    ) -> None:
        try:
            result = await _dispatch(application, topic, message, headers)
        except Exception:
            # Isolated from the other targets and from the caller by design - one failing
            # reaction must not affect delivery to the others or the fan-out's own
            # (always-success) response. There is no in-process DLQ, so this is the only place
            # the failure is visible.
            logger.warning(
                "in-process fan-out consumer %r raised for topic %r", name, topic, exc_info=True
            )
            return
        if not result.is_successful:
            logger.warning(
                "in-process fan-out consumer %r returned unsuccessful status %r for topic %r",
                name,
                result.status,
                topic,
            )


async def _dispatch(
    application: BenzeneMessageApplication,
    topic: str,
    message: Any,
    headers: dict[str, str] | None,
) -> Result:
    """Runs one pipeline invocation against ``application`` and converts its response envelope
    back into a :class:`Result` — the shared mechanics behind both sender classes above.
    """
    response = await application.handle(
        {"topic": topic, "headers": headers or {}, "body": encode_body(message)}
    )
    return decode_response(response)
