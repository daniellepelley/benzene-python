"""Outbound registration — the concept a service uses to declare which topics it *may send*
(mesh.md §2.3), mirroring :class:`benzene.core.Registry`'s inbound handler discovery exactly: an
explicit list of ``(topic, version, request type, response type)`` records, with no handler, since
nothing here receives. This is what makes :attr:`~benzene.mesh.ServiceDescriptor.produces` a
**hard-coded contract** rather than an inference — reliable for the identical reason the inbound
registry already makes ``topics`` reliable. A port MUST NOT attempt to infer this by scanning call
sites or any other form of static analysis; explicit registration is the only conforming path
(mesh.md §2.3).

A service that registers a handler is that topic's *consumer* (``topics``); a service registered
here is that topic's *provider* (``produces``) — standard pub/sub terminology, the receiver is the
"consumer".

A registered outbound record needs no destination address, queue name, or topic ARN — those are
transport/deployment configuration, orthogonal to the *contract* this registers. A service can
declare it produces ``payments:capture`` while its actual SQS queue URL is injected at deploy time;
the descriptor doesn't change between environments, only the wiring does.
"""

from __future__ import annotations

from dataclasses import dataclass


class DuplicateOutboundRegistrationError(Exception):
    """Raised when the same ``(topic, version)`` pair is registered as an outbound topic twice."""


@dataclass(frozen=True)
class OutboundDefinition:
    """One declared outbound topic: no handler, since nothing here receives."""

    topic: str
    version: str = ""
    request_type: type | None = None
    response_type: type | None = None


class OutboundRegistry:
    """Declares which topics a service may send — mesh.md §2.3's outbound registration.

    ``register`` is the explicit path (mirrors :meth:`~benzene.core.Registry.register`, minus a
    handler). Registering the same ``(topic, version)`` pair twice raises
    :class:`DuplicateOutboundRegistrationError` at startup, the same strictness inbound registration
    already has — two declarations for one outbound edge can only ever describe one contract.
    """

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, str], OutboundDefinition] = {}

    def register(
        self,
        topic: str,
        version: str = "",
        request_type: type | None = None,
        response_type: type | None = None,
    ) -> OutboundRegistry:
        key = (topic, version)
        if key in self._by_key:
            where = f"topic {topic!r}" + (f" version {version!r}" if version else " (unversioned)")
            raise DuplicateOutboundRegistrationError(
                f"{where} is already registered as an outbound topic. Each (topic, version) pair "
                "needs exactly one declaration — remove the duplicate, or give one a distinct version."
            )
        self._by_key[key] = OutboundDefinition(topic, version, request_type, response_type)
        return self

    def definitions(self) -> list[OutboundDefinition]:
        return list(self._by_key.values())
