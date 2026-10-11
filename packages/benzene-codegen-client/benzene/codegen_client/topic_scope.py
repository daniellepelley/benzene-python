"""Topic-scope projection (contract-document.md §5.2).

Shared by the service-client and topic-client generators (and by the conformance test against
``topicScopeCases``/``parseCases``' fail-loud case) — filtering the document's ``requests[]`` once,
here, is what keeps every downstream site (method emission, ``REQUIRED_TOPICS``, per-topic client
selection) from being able to disagree about what's in scope.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from .document import ContractDocument, RequestResponse
from .schema_closure import reachable_schemas


class UnknownTopicsError(ValueError):
    """The include-list names a topic the document does not have (§5.2, fail loud)."""

    def __init__(self, unknown_topics: list[str], valid_topics: list[str]) -> None:
        self.unknown_topics = sorted(unknown_topics)
        self.valid_topics = sorted(valid_topics)
        super().__init__(
            "--topics names topic(s) not present in the document: "
            f"{', '.join(self.unknown_topics)}. Valid topics: {', '.join(self.valid_topics)}."
        )


@dataclass(frozen=True)
class TopicScopeOptions:
    """§5.2's include-list. ``benzene:`` entries never reach this far: the parser ignores them."""

    topics: tuple[str, ...] | None = None


def apply_topic_scope(document: ContractDocument, options: TopicScopeOptions) -> ContractDocument:
    """Project ``document.requests`` down to the topics in scope per ``options``.

    Everything else (events, sends, components, the document-level fields) is unchanged — §5.2 is
    explicit the include-list scopes ``requests[]`` only. The projection's hashes are recomputed by
    whoever needs them (:mod:`benzene.codegen_client.contract_hash`), never copied.
    """
    requested = options.topics if options.topics else None

    if requested:
        known = {r.topic for r in document.requests}
        unknown = [t for t in requested if t not in known]
        if unknown:
            raise UnknownTopicsError(unknown, sorted(known))
        included = set(requested)
        filtered = tuple(r for r in document.requests if r.topic in included)
    else:
        filtered = document.requests

    return replace(document, requests=filtered, contract_hash=None, registration_hash=None)


def is_in_scope(request: RequestResponse, options: TopicScopeOptions) -> bool:
    if options.topics:
        return request.topic in options.topics
    return True


class TopicNotFoundError(ValueError):
    def __init__(self, topic: str, valid_topics: list[str]) -> None:
        self.topic = topic
        self.valid_topics = sorted(valid_topics)
        super().__init__(
            f"Topic {topic!r} is not present in the document. "
            f"Valid topics: {', '.join(self.valid_topics)}."
        )


def topic_closure(document: ContractDocument, request: RequestResponse) -> dict[str, Any]:
    """The component schemas ``request``'s request and response reach (§5.3)."""
    return reachable_schemas(document.schemas, request.request, request.response)


def topic_projection(document: ContractDocument, topic: str) -> ContractDocument:
    """§5.3's topic-scoped projection: exactly that topic's ``requests[]`` entry, ``events`` and
    ``sends`` empty, ``components.schemas`` the reached set, and no hashes carried over — they are
    recomputed over the projection."""
    request = document.find_request(topic)
    if request is None:
        raise TopicNotFoundError(topic, [r.topic for r in document.requests])
    return replace(
        document,
        requests=(request,),
        events=(),
        sends=(),
        schemas=topic_closure(document, request),
        contract_hash=None,
        registration_hash=None,
    )
