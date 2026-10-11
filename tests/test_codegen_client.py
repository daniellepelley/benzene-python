"""Unit tests for benzene-codegen-client beyond the language-neutral conformance fixtures:
naming conventions, dataclass/type emission (allOf inheritance, oneOf unions), and the CLI's
fail-loud behaviour on bad input (parity checklist row 12).
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from benzene.codegen_client import (
    UnknownSchemaDialectError,
    UnknownTopicsError,
    generate_service_client,
    generate_topic_client,
    parse_document,
)
from benzene.codegen_client.cli import main as cli_main
from benzene.codegen_client.naming import (
    class_name,
    default_method_name,
    field_name,
    topic_identifier,
)
from benzene.codegen_client.topic_scope import TopicNotFoundError

_CONFORMANCE_DIR = Path(__file__).resolve().parent.parent / "conformance"
_PAYMENTS_SPEC = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "orders_payments_client"
    / "contracts"
    / "payments.spec.json"
)


#: allOf inheritance, a oneOf union alias, and collections of named types - shapes the emitter has
#: to turn into importable Python. A test document of this port's own, not a conformance fixture.
_CLOSURE_DOCUMENT: dict[str, Any] = {
    "schemaDialect": "openapi-3.0",
    "service": "closure",
    "requests": [
        {"topic": "closure:compose", "request": {"$ref": "#/components/schemas/D"}},
        {
            "topic": "closure:collections",
            "request": {"$ref": "#/components/schemas/MapWrapper"},
            "response": {"$ref": "#/components/schemas/ListWrapper"},
        },
        {
            "topic": "closure:nullable",
            "request": {
                "type": "object",
                "nullable": True,
                "allOf": [{"$ref": "#/components/schemas/G"}],
            },
        },
    ],
    "events": [],
    "sends": [],
    "components": {
        "schemas": {
            "D": {"oneOf": [{"$ref": "#/components/schemas/E"}, {"$ref": "#/components/schemas/F"}]},
            "E": {
                "allOf": [
                    {"$ref": "#/components/schemas/G"},
                    {"type": "object", "properties": {"x": {"type": "string"}}},
                ]
            },
            "F": {"type": "object"},
            "G": {"type": "object", "properties": {"y": {"type": "string"}}},
            "MapWrapper": {
                "type": "object",
                "additionalProperties": {"$ref": "#/components/schemas/Item"},
            },
            "Item": {"type": "object", "properties": {"tag": {"type": "string"}}},
            "ListWrapper": {"type": "array", "items": {"$ref": "#/components/schemas/ListItem"}},
            "ListItem": {"type": "object"},
        }
    },
}


def _closure_document():
    return parse_document(_CLOSURE_DOCUMENT)


def _canonical_document():
    data = json.loads((_CONFORMANCE_DIR / "contract-document-cases.json").read_text())
    return parse_document(data["documents"]["canonical"])


def _payments_document():
    return parse_document(json.loads(_PAYMENTS_SPEC.read_text()))


# --- naming ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "topic,expected",
    [
        ("payments:capture", "capture_payments"),
        ("payments:get-all", "get_all_payments"),
        ("order:created", "created_order"),
        ("say:hello", "hello_say"),
    ],
)
def test_default_method_name_reverses_topic_segments(topic: str, expected: str) -> None:
    assert default_method_name(topic) == expected


@pytest.mark.parametrize(
    "topic,expected",
    [
        ("payments:capture", "payments_capture"),
        ("payments:get-all", "payments_get_all"),
    ],
)
def test_topic_identifier_keeps_segment_order(topic: str, expected: str) -> None:
    assert topic_identifier(topic) == expected


def test_class_name_pascal_cases_schema_names() -> None:
    assert class_name("CreateOrder") == "CreateOrder"
    assert class_name("order-created") == "OrderCreated"


def test_field_name_is_snake_case_and_round_trips_camel_case() -> None:
    assert field_name("OrderId") == "order_id"
    assert field_name("orderId") == "order_id"
    assert field_name("order_id") == "order_id"


def test_field_name_avoids_python_keywords() -> None:
    assert field_name("class") == "class_"


# --- type/dataclass emission --------------------------------------------------------------------


def test_topic_client_with_allof_and_oneof_generates_valid_importable_python() -> None:
    document = _closure_document()
    generated = generate_topic_client(document, topic="closure:compose")

    # Valid Python: parses and actually imports/executes without error.
    ast.parse(generated.source)

    # Any, not object: this namespace is populated by exec'ing generated source, so what it holds is
    # exactly what the test is about to assert - classes to issubclass and instantiate.
    namespace: dict[str, Any] = {}
    exec(compile(generated.source, "<generated:closure:compose>", "exec"), namespace)  # noqa: S102

    # D is a bare oneOf (E | F) with no properties of its own -> a Union type alias, not a class.
    assert namespace["D"] == namespace["E"] | namespace["F"] if hasattr(namespace["E"], "__or__") else True
    # E composes allOf[G, {x}] -> a real subclass of G carrying G's own field plus its own.
    assert issubclass(namespace["E"], namespace["G"])
    e_instance = namespace["E"]()
    assert hasattr(e_instance, "y")  # inherited from G
    assert hasattr(e_instance, "x")  # E's own property


def test_topic_client_with_collections_generates_valid_python() -> None:
    document = _closure_document()
    generated = generate_topic_client(document, topic="closure:collections")
    ast.parse(generated.source)
    namespace: dict[str, object] = {}
    exec(compile(generated.source, "<generated:closure:collections>", "exec"), namespace)  # noqa: S102
    assert "MapWrapper" in namespace
    assert "Item" in namespace
    assert "ListItem" in namespace


# --- topic scope / include-list -----------------------------------------------------------------


def test_service_client_include_list_fails_loud_on_unknown_topic() -> None:
    document = _canonical_document()
    with pytest.raises(UnknownTopicsError) as excinfo:
        generate_service_client(document, service_name="Conformance", topics=("not-a-real-topic",))
    assert excinfo.value.unknown_topics == ["not-a-real-topic"]
    assert "conformance:greet" in excinfo.value.valid_topics


def test_a_default_service_client_embeds_the_served_contract_hash() -> None:
    # The drift check compares a client's embedded hash with the served one, so a default client
    # (no include-list) must embed exactly the document's contractHash (contract-document §6.4).
    document = _canonical_document()
    generated = generate_service_client(document, service_name="Conformance")
    assert generated.contract_hash == document.contract_hash


def test_an_absent_response_types_the_method_as_returning_no_payload() -> None:
    generated = generate_topic_client(_closure_document(), topic="closure:compose")
    assert "-> Result[None]:" in generated.source


def test_a_nullable_wrapper_types_as_the_wrapped_class_or_none() -> None:
    generated = generate_topic_client(_closure_document(), topic="closure:nullable")
    assert "request: G | None" in generated.source
    ast.parse(generated.source)


def test_an_unknown_schema_dialect_is_rejected() -> None:
    with pytest.raises(UnknownSchemaDialectError):
        parse_document({**_CLOSURE_DOCUMENT, "schemaDialect": "json-schema-2020-12"})
    legacy = {k: v for k, v in _CLOSURE_DOCUMENT.items() if k != "schemaDialect"}
    with pytest.raises(UnknownSchemaDialectError, match="predates spec-0.3.0"):
        parse_document({**legacy, "openapi": "3.0.1"})


def test_cli_rejects_an_unknown_dialect_and_has_no_include_reserved(tmp_path: Path) -> None:
    spec = tmp_path / "legacy.spec.json"
    spec.write_text(json.dumps({**_CLOSURE_DOCUMENT, "schemaDialect": "openapi-3.1"}))
    with pytest.raises(SystemExit):
        cli_main(["service", "--spec", str(spec), "--service", "X"])
    with pytest.raises(SystemExit):
        cli_main(["service", "--spec", str(_PAYMENTS_SPEC), "--service", "X", "--include-reserved"])


def test_topic_client_unknown_topic_fails_loud() -> None:
    document = _payments_document()
    with pytest.raises(TopicNotFoundError):
        generate_topic_client(document, topic="payments:does-not-exist")


# --- CLI -------------------------------------------------------------------------------------


def test_cli_unknown_topic_exits_nonzero(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli_main(["topic", "--spec", str(_PAYMENTS_SPEC), "--topic", "payments:nope"])
    assert exit_code == 1


def test_cli_unparseable_document_exits_nonzero(tmp_path: Path) -> None:
    bad_spec = tmp_path / "bad.spec.json"
    bad_spec.write_text("{not json")
    with pytest.raises(SystemExit):
        cli_main(["topic", "--spec", str(bad_spec), "--topic", "x"])


def test_cli_missing_file_exits_nonzero() -> None:
    with pytest.raises(SystemExit):
        cli_main(["topic", "--spec", "/no/such/file.spec.json", "--topic", "x"])


def test_cli_unknown_flag_exits_nonzero_as_a_subprocess() -> None:
    # argparse calls sys.exit() directly on a bad flag, bypassing our own SystemExit path — assert
    # the real console-script behaviour end to end, via python -m, as a subprocess.
    result = subprocess.run(
        [sys.executable, "-m", "benzene.codegen_client.cli", "topic", "--not-a-flag", "x"],
        cwd=Path(__file__).resolve().parent.parent,
        env={
            "PYTHONPATH": ":".join(
                [
                    "packages/benzene-results",
                    "packages/benzene-core",
                    "packages/benzene-codegen-client",
                ]
            ),
            "PATH": "/usr/bin:/bin",
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0


def test_cli_service_writes_a_file_with_no_reserved_topics(tmp_path: Path) -> None:
    out = tmp_path / "payments_client.py"
    exit_code = cli_main(
        ["service", "--spec", str(_PAYMENTS_SPEC), "--service", "Payments", "--out", str(out)]
    )
    assert exit_code == 0
    source = out.read_text()
    assert "benzene:" not in source
    ast.parse(source)
