"""Outbound client port (transport-bindings.md, "Outbound clients (the reverse direction)").

Every outbound client implements one interface — ``send_message(topic, message, headers) -> Result``
— over whatever native transport it wraps (Pub/Sub publish, SNS/SQS, Service Bus, an HTTP POST of
the wire envelope, …). Keeping the port in the core (not in a transport package) is what lets a
handler depend on "an outbound client" without coupling to a vendor, and lets tests substitute a
fake — the same shape the .NET port expresses as ``IBenzeneMessageSender``.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from benzene.results import Result


@runtime_checkable
class MessageSender(Protocol):
    """Sends a message on a topic through some transport, returning a Benzene :class:`Result`."""

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result: ...
