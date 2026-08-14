"""Naming: topic -> method/module name, schema name -> class name, property name -> field name.

Method naming and file layout are explicitly *not* pinned by the spec (contract-document.md §5.5) —
each port picks its own idiom. This module ports the .NET reference's default convention
(``Benzene.CodeGen.Client.TopicReversedMethodName`` / ``TopicMethodName`` / ``CSharpNameFormatter``)
translated to Python's ``snake_case``/``PascalCase`` conventions instead of C#'s ``PascalCase``
methods:

- .NET's ``TopicReversedMethodName`` splits a topic on ``:``, reverses the segments, Pascal-cases
  each and concatenates with no separator — ``payments:capture`` -> ``CaptureAsync``... i.e.
  ``CapturePayments``. This port's equivalent is :func:`default_method_name`, which does the same
  reversal but joins words with ``_`` (Python's word separator): ``payments:capture`` ->
  ``capture_payments``.
- .NET's ``TopicMethodName`` (used to name each atomic/topic client) keeps the segments in their
  original order — ``payments:capture`` -> ``PaymentsCapture``. This port's :func:`topic_identifier`
  is the ``snake_case`` analog: ``payments:capture`` -> ``payments_capture``.
- .NET's ``CSharpNameFormatter`` (component schema name -> class name) ensures a valid identifier,
  strips separators, and Pascal-cases. :func:`class_name` is the direct Python analog, producing
  ``PascalCase`` (Python class-naming convention, PEP 8) rather than ``snake_case``.
"""

from __future__ import annotations

import keyword
import re

_WORD_SPLIT_RE = re.compile(r"[^0-9a-zA-Z]+")
_CAMEL_WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z0-9]+|[A-Z]+|[0-9]+")


def _segment_words(segment: str) -> list[str]:
    """Split a topic segment or schema/property name into lowercase words.

    Handles kebab-case (``get-all``), snake_case, and camelCase/PascalCase boundaries so
    ``OrderId``, ``order_id``, and ``orderId`` all yield ``["order", "id"]``.
    """
    words: list[str] = []
    for chunk in _WORD_SPLIT_RE.split(segment):
        if not chunk:
            continue
        words.extend(m.group(0).lower() for m in _CAMEL_WORD_RE.finditer(chunk))
    return words


def _ensure_identifier(name: str) -> str:
    if not name:
        return "_"
    if name[0].isdigit():
        name = f"_{name}"
    if keyword.iskeyword(name):
        name = f"{name}_"
    return name


def default_method_name(topic: str) -> str:
    """The default per-topic method name: reversed-topic-segments, ``snake_case``.

    ``payments:capture`` -> ``capture_payments`` (ports .NET's ``TopicReversedMethodName``, whose
    reversal exists so the *verb* — the last topic segment — reads first, the way a method name
    normally leads with its verb).
    """
    words: list[str] = []
    for segment in reversed(topic.split(":")):
        words.extend(_segment_words(segment))
    return _ensure_identifier("_".join(words) or "call")


def topic_identifier(topic: str) -> str:
    """A ``snake_case`` identifier for a topic, segments kept in order: ``payments:capture`` -> ``payments_capture``.

    Used to name a topic-scoped client's module/class/factory (ports .NET's ``TopicMethodName``,
    used there to name each atomic client).
    """
    words: list[str] = []
    for segment in topic.split(":"):
        words.extend(_segment_words(segment))
    return _ensure_identifier("_".join(words) or "topic")


def class_name(schema_name: str) -> str:
    """A ``PascalCase`` Python class name for a component schema name (ports ``CSharpNameFormatter``)."""
    words = _segment_words(schema_name)
    pascal = "".join(w[:1].upper() + w[1:] for w in words if w)
    if not pascal:
        pascal = "Schema"
    if pascal[0].isdigit():
        pascal = f"_{pascal}"
    return pascal


def field_name(property_name: str) -> str:
    """A ``snake_case`` dataclass field name for a JSON property name.

    Deliberately loose (case-/separator-insensitive, like ``benzene.core.mapping``'s own matching):
    the exact spelling doesn't need to be preserved because ``to_jsonable``/``to_request`` already
    camelCase-on-write and fold case/separators on-read, so any ``snake_case`` rendering of the same
    words round-trips through the wire correctly without per-field metadata.
    """
    words = _segment_words(property_name)
    name = "_".join(words) if words else property_name
    return _ensure_identifier(name)


def pascal_case(words_source: str) -> str:
    """PascalCase a snake/kebab/camel string (used for e.g. ``{TopicClientClass}``)."""
    return class_name(words_source)
