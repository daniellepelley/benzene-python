"""Topic + header resolution from a transport's native metadata (wire-contracts.md §2).

Every message transport that carries Benzene metadata natively exposes a string→string channel under
a different native name — SQS/SNS message attributes, Pub/Sub attributes, Service Bus / Event Hub
application properties, Kafka/RabbitMQ headers. Each binding turns that native channel into a plain
``dict`` and hands it here; :func:`read_message_metadata` resolves the reserved **topic** key out of it
and returns the rest as headers. Version travels as the ``benzene-version`` header and is resolved
downstream by :func:`~benzene.core.resolve_version`, exactly as in the envelope entry point.

The reserved names are **a single injectable value** (:class:`MetadataKeys`), not a literal each
binding hard-codes: the defaults carry interop (two untouched Benzene services must interoperate), and
an override applies to inbound bindings and outbound clients alike. Keys are matched
case-insensitively and returned lower-cased, per the wire-contract header convention.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .envelope import VERSION_HEADER

#: The default reserved metadata key names (wire-contracts §2). The interop baseline.
DEFAULT_TOPIC_KEY = "topic"
DEFAULT_VERSION_KEY = VERSION_HEADER  # "benzene-version"


@dataclass(frozen=True)
class MetadataKeys:
    """The reserved metadata key names — override to opt a service out of the interop defaults."""

    topic: str = DEFAULT_TOPIC_KEY
    version: str = DEFAULT_VERSION_KEY


DEFAULT_METADATA_KEYS = MetadataKeys()


def read_message_metadata(
    metadata: Mapping[str, Any] | None, keys: MetadataKeys = DEFAULT_METADATA_KEYS
) -> tuple[str, dict[str, str]]:
    """Resolve ``(topic, headers)`` from a transport's native metadata dictionary.

    The reserved topic key (``keys.topic``, matched case-insensitively) becomes the topic and is
    removed; every remaining entry becomes a header (keys lower-cased). An absent topic key leaves the
    topic ``""`` (unresolved — the router decides what that means), and a *non-reserved* key never
    routes. ``benzene-version`` stays among the headers for :func:`resolve_version` to read.
    """
    headers = {str(key).lower(): str(value) for key, value in (metadata or {}).items()}
    topic = headers.pop(keys.topic.lower(), "")
    return topic, headers
