"""The reserved wire names, and the native-metadata split that uses them (wire-contracts.md §2).

Every transport that carries Benzene metadata natively exposes the same shape under a different
name — SQS/SNS message attributes, Pub/Sub attributes, Service Bus/Event Hub application
properties, Kafka/RabbitMQ headers — so the split belongs here once rather than in each binding.

**The names are defaults, not literals.** The spec requires an implementation to expose them as a
single injectable value so a service can align with a convention it does not control, replacing a
name in one place instead of at every binding. :class:`WireNames` is that value; register a
replacement with :func:`use_wire_names` and every binding follows.

Two rules a per-binding reimplementation tends to get wrong, and did:

* **Read case-insensitively.** "All header keys are case-insensitive on read and SHOULD be written
  lower-case." A producer that wrote ``Topic`` must still route.
* **Consume the routing key.** The topic key is metadata the binding used, not an application
  header, so it must not also appear in the header dictionary.

**One override, both directions.** Whatever names a service registers are used by its inbound
bindings *and* its outbound clients. Overriding only one side would send messages the service
cannot itself receive, and the symptom — a message that arrives and never routes — is
indistinguishable from a missing handler. That is why the unresolved-topic error names the
configured key.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

#: The default metadata key carrying the topic on transports without the envelope (wire-contracts
#: §2, tier A). The same spelling as the envelope's own field, so one concept has one name wherever
#: it travels. Overridable per service — see :class:`WireNames`.
TOPIC_KEY = "topic"

#: The default metadata key carrying the payload's schema version (wire-contracts §2, tier C).
VERSION_KEY = "benzene-version"


@dataclass(frozen=True)
class WireNames:
    """The reserved names this service reads and writes on the wire.

    Frozen because these are read on every invocation and must not drift mid-process. Register a
    replacement at startup with :func:`use_wire_names`; resolve it with :func:`wire_names`.
    """

    topic: str = TOPIC_KEY
    version: str = VERSION_KEY


#: The defaults, as a value. Bindings fall back to this when nothing was registered, so a service
#: that never touches the container behaves exactly as the spec's defaults describe.
DEFAULT_WIRE_NAMES = WireNames()


def use_wire_names(services, names: WireNames) -> None:
    """Override the reserved names for this service (the extension the spec requires).

        use_wire_names(services, WireNames(topic="x-my-topic"))

    Call it from ``configure_services``. Registered as an instance, so every binding and outbound
    client in the service resolves the same value.
    """
    services.add_instance(WireNames, names)


def wire_names(scope) -> WireNames:
    """Resolve the service's names, falling back to the defaults when none were registered.

    A missing container, or one with no registration, means *the defaults* — never an error.
    Bindings decode messages in places where a scope is not always to hand, and a service that
    never touches the container must behave exactly as the spec's defaults describe.
    """
    if scope is None:
        return DEFAULT_WIRE_NAMES
    lookup = getattr(scope, "try_get_service", None)
    if not callable(lookup):
        return DEFAULT_WIRE_NAMES
    try:
        resolved = lookup(WireNames)
    except Exception:
        return DEFAULT_WIRE_NAMES
    return resolved if isinstance(resolved, WireNames) else DEFAULT_WIRE_NAMES


def take_topic(
    metadata: Mapping[str, object], names: WireNames | str | None = None
) -> tuple[str, dict[str, str]]:
    """Split native metadata into ``(topic, headers)``.

    The topic key is matched case-insensitively and removed from the headers. Everything else
    becomes a header with its original spelling. Duplicate keys: last value wins (wire-contracts §2).

    ``names`` accepts a :class:`WireNames`, a bare key string (convenient for a binding that already
    resolved one), or ``None`` for the defaults.
    """
    if names is None:
        topic_key = TOPIC_KEY
    elif isinstance(names, str):
        topic_key = names
    else:
        topic_key = names.topic

    wanted = topic_key.lower()
    topic = ""
    headers: dict[str, str] = {}
    for key, value in metadata.items():
        if str(key).lower() == wanted:
            topic = str(value)
            continue
        headers[str(key)] = str(value)
    return topic, headers
