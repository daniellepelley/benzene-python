"""Topic-scope projection (contract-document.md §5.1-§5.2).

Shared by the service-client and topic-client generators (and by the conformance test against
``topicScopeCases``/``parseCases``' fail-loud case) — filtering the document's ``requests[]`` once,
here, is what keeps every downstream site (method emission, ``REQUIRED_TOPICS``, per-topic client
selection) from being able to disagree about what's in scope.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from .document import ContractDocument, RequestResponse


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
    """§5.2's include-list plus the reserved-topic policy."""

    topics: tuple[str, ...] | None = None
    include_reserved: bool = False


def apply_topic_scope(document: ContractDocument, options: TopicScopeOptions) -> ContractDocument:
    """Project ``document.requests`` down to the topics in scope per ``options``.

    Everything else (info, events, components, messageEndpoint, transports) is unchanged — §5.2
    is explicit the include-list scopes ``requests[]`` only.
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
        filtered = tuple(
            r for r in document.requests if options.include_reserved or not r.is_reserved()
        )

    return replace(document, requests=filtered)


def is_in_scope(request: RequestResponse, options: TopicScopeOptions) -> bool:
    if options.topics:
        return request.topic in options.topics
    return options.include_reserved or not request.is_reserved()
