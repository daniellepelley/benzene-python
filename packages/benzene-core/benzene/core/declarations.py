"""Outbound declarations — the events a service publishes and the sends it makes (contract-document §3).

What a service puts on the wire is declared, never inferred (contract-document §3.3): an application
hands the framework ``(topic, version, types)`` records as it registers its handlers, exactly as the
inbound :class:`~benzene.core.Registry` takes handler records. Two kinds, split by **who owns the
contract**, not by whether a reply comes back:

* an **event** (:meth:`OutboundRegistry.register_event`) is a topic this service publishes *and
  defines* — anyone may subscribe, and its payload is this service's to change. It is part of what a
  client of this service depends on, so it is in the Contract Document's ``events[]`` and in its
  ``contractHash``.
* a **send** (:meth:`OutboundRegistry.register_send`) is a message to a topic **another party**
  defines — a command onto a queue, an RPC, a publish onto someone else's topic. It is a dependency of
  this service, which a client of it cannot observe: ``sends[]``, hashed only into
  ``registrationHash``. Leave ``response_type`` off for a one-way send.

So for an ``orders`` service, ``order:placed`` is an event and ``payment:take`` (owned by payments) is
a send, though both are one-way. Declaring one ``(topic, version)`` as both is a start-up error, and
there is deliberately no kind-less ``register``: every call site says which it means.

A declaration names a topic, never a queue URL or ARN — that is deployment configuration, and the
document does not change between environments.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class DuplicateOutboundRegistrationError(Exception):
    """A ``(topic, version)`` was declared twice — twice as one kind, or once as each kind."""


@dataclass(frozen=True)
class EventDeclaration:
    """One event this service publishes and defines: ``events[] {topic, version?, message}``."""

    topic: str
    version: str = ""
    message_type: Any = None


@dataclass(frozen=True)
class SendDeclaration:
    """One message this service sends to a topic another party defines: ``sends[]``.

    ``response_type`` is ``None`` for a one-way send (``response`` absent from the document).
    """

    topic: str
    version: str = ""
    request_type: Any = None
    response_type: Any = None


class OutboundRegistry:
    """The service's event and send declarations (contract-document §3.3).

    ::

        outbound = (
            OutboundRegistry()
            .register_event("order:placed", OrderPlaced)
            .register_send("payment:take", TakePayment)                      # one-way
            .register_send("inventory:reserve", Reserve, Reservation, version="v2")
        )

    Each ``(topic, version)`` is declared at most once across both kinds: a repeat raises
    :class:`DuplicateOutboundRegistrationError` at start-up, the same strictness the handler
    registry has.
    """

    def __init__(self) -> None:
        self._events: dict[tuple[str, str], EventDeclaration] = {}
        self._sends: dict[tuple[str, str], SendDeclaration] = {}

    def register_event(
        self, topic: str, message_type: Any = None, *, version: str = ""
    ) -> OutboundRegistry:
        """Declare an event this service publishes and defines (``events[]``)."""
        self._claim(topic, version, "an event")
        self._events[(topic, version)] = EventDeclaration(topic, version, message_type)
        return self

    def register_send(
        self,
        topic: str,
        request_type: Any = None,
        response_type: Any = None,
        *,
        version: str = "",
    ) -> OutboundRegistry:
        """Declare a message this service sends to a topic another party defines (``sends[]``).

        ``response_type`` is the reply's type; leave it ``None`` for a one-way send.
        """
        self._claim(topic, version, "a send")
        self._sends[(topic, version)] = SendDeclaration(topic, version, request_type, response_type)
        return self

    def register(self, *args: Any, **kwargs: Any) -> OutboundRegistry:
        """Removed: an outbound declaration has to say which kind it is."""
        raise TypeError(
            "OutboundRegistry.register() is gone: say which kind of outbound topic this is. "
            "register_event(topic, message_type) declares an event this service publishes and "
            "defines; register_send(topic, request_type, response_type=None) declares a message to "
            "a topic another party defines (contract-document §3)."
        )

    def events(self) -> list[EventDeclaration]:
        """The declared events, in declaration order."""
        return list(self._events.values())

    def sends(self) -> list[SendDeclaration]:
        """The declared sends, in declaration order."""
        return list(self._sends.values())

    def _claim(self, topic: str, version: str, kind: str) -> None:
        key = (topic, version)
        existing = (
            "an event" if key in self._events else "a send" if key in self._sends else None
        )
        if existing is None:
            return
        where = f"topic {topic!r}" + (f" version {version!r}" if version else " (unversioned)")
        if existing == kind:
            raise DuplicateOutboundRegistrationError(
                f"{where} is already declared as {kind}. Each (topic, version) needs exactly one "
                "declaration — remove the duplicate, or give one a distinct version."
            )
        raise DuplicateOutboundRegistrationError(
            f"{where} is declared as both an event and a send. An event is a topic this service "
            "publishes and defines; a send is a message to a topic another party defines. Keep the "
            "one that says who owns the contract (contract-document §3.3)."
        )

