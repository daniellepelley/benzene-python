"""Dependency-free runner for the language-neutral conformance fixtures.

Runs the fixtures in ``conformance/`` against this implementation and returns a list of failures.
Imported by the pytest suite (``test_conformance.py``) and runnable directly:

    python -m tests.conformance_runner
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from benzene.core import BenzeneMessageApplication, Registry
from benzene.http import from_http, to_http
from benzene.results import is_successful

from .canonical_handlers import register_canonical

CONFORMANCE_DIR = Path(__file__).resolve().parent.parent / "conformance"


def _load(name: str) -> Any:
    return json.loads((CONFORMANCE_DIR / name).read_text())


def _is_subset(expected: Any, actual: Any) -> bool:
    """Subset matching: every key in `expected` must be present and equal in `actual`."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        return all(k in actual and _is_subset(v, actual[k]) for k, v in expected.items())
    return expected == actual


def _app() -> BenzeneMessageApplication:
    return BenzeneMessageApplication(register_canonical(Registry()))


def run_status_vocabulary() -> list[str]:
    failures: list[str] = []
    data = _load("status-vocabulary.json")
    for entry in data["statuses"]:
        status, expected = entry["status"], entry["isSuccess"]
        if is_successful(status) != expected:
            failures.append(
                f"status-vocabulary: {status!r} classified {is_successful(status)}, expected {expected}"
            )
    return failures


def run_http_mapping() -> list[str]:
    failures: list[str] = []
    data = _load("http-status-mapping.json")
    # forward: benzene status -> HTTP code (code carried as a string in the fixture)
    for case in data.get("forward", []):
        got, expected = to_http(case["from"]), int(case["to"])
        if got != expected:
            failures.append(f"benzene->http: {case['from']!r} -> {got}, expected {expected}")
    # reverse: HTTP code -> benzene status
    for case in data.get("reverse", []):
        got, expected = from_http(int(case["from"])), case["to"]
        if got != expected:
            failures.append(f"http->benzene: {case['from']} -> {got!r}, expected {expected!r}")
    return failures


def run_envelope_cases() -> list[str]:
    failures: list[str] = []
    app = _app()
    data = _load("envelope-cases.json")
    for case in data["cases"]:
        response = asyncio.run(app.handle_async(case["request"]))
        expected = case["expected"]
        name = case["name"]

        if response["statusCode"] != expected["statusCode"]:
            failures.append(
                f"envelope[{name}]: statusCode {response['statusCode']!r}, expected {expected['statusCode']!r}"
            )
        if "headers" in expected and not _is_subset(expected["headers"], response.get("headers", {})):
            failures.append(f"envelope[{name}]: headers {response.get('headers')} !⊇ {expected['headers']}")
        if "body" in expected:
            actual_body = json.loads(response["body"]) if response["body"] else {}
            if not _is_subset(expected["body"], actual_body):
                failures.append(f"envelope[{name}]: body {actual_body} !⊇ {expected['body']}")
    return failures


# Each metadata-carrying binding this port implements, as an adapter from the fixture's neutral
# string->string metadata dictionary to that transport's native message. The fixture cannot express
# the native shapes (they are per-cloud), so the *runner* owns the adaptation and the fixture owns
# the contract: which key carries the topic, and what the decoded envelope must look like.
def _metadata_adapters() -> dict[str, Any]:
    from benzene.aws.events import TOPIC_ATTRIBUTE as AWS_TOPIC_KEY
    from benzene.aws.events import sns_record_envelope, sqs_record_envelope
    from benzene.azure.events import TOPIC_PROPERTY as AZURE_TOPIC_KEY
    from benzene.azure.events import decode_service_bus
    from benzene.azure.testing import FakeServiceBusMessage
    from benzene.gcp.pubsub import TOPIC_ATTRIBUTE as GCP_TOPIC_KEY
    from benzene.gcp.pubsub import decode_pubsub_message

    def sqs(metadata: dict[str, str], names: Any = None) -> dict[str, Any]:
        attrs = {k: {"stringValue": v} for k, v in metadata.items()}
        return sqs_record_envelope({"messageAttributes": attrs, "body": "{}"}, names)

    def sns(metadata: dict[str, str], names: Any = None) -> dict[str, Any]:
        attrs = {k: {"Type": "String", "Value": v} for k, v in metadata.items()}
        return sns_record_envelope({"Sns": {"MessageAttributes": attrs, "Message": "{}"}}, names)

    def pubsub(metadata: dict[str, str], names: Any = None) -> dict[str, Any]:
        return decode_pubsub_message({"attributes": dict(metadata), "data": "e30="}, names)

    def service_bus(metadata: dict[str, str], names: Any = None) -> dict[str, Any]:
        return decode_service_bus(FakeServiceBusMessage(b"{}", dict(metadata)), names)

    return {
        "sqs": (sqs, AWS_TOPIC_KEY),
        "sns": (sns, AWS_TOPIC_KEY),
        "pubsub": (pubsub, GCP_TOPIC_KEY),
        "servicebus": (service_bus, AZURE_TOPIC_KEY),
    }


def run_transport_metadata_cases() -> list[str]:
    """Assert the reserved metadata key names, and the decode behaviour that depends on them.

    The key names are what no other fixture covers: a port can rename the topic attribute, pass
    every envelope/status/HTTP case, and still be unable to exchange a queue message with another
    port. So this checks the constant *and* runs every case through the real decoder.
    """
    from benzene.core import WireNames

    failures: list[str] = []
    data = _load("transport-metadata-cases.json")
    expected_topic_key = data["defaultMetadataKeys"]["topic"]
    adapters = _metadata_adapters()

    # Default cases run with no override (names=None); each override case supplies its own, which
    # is what proves the names are configurable rather than hard-coded.
    cases = [(c, None) for c in data["metadataCases"]] + [
        (c, WireNames(topic=c["metadataKeys"]["topic"])) for c in data.get("overrideCases", [])
    ]

    for binding, (decode, topic_key) in sorted(adapters.items()):
        if topic_key != expected_topic_key:
            failures.append(
                f"transport-metadata[{binding}]: default topic key {topic_key!r}, "
                f"expected {expected_topic_key!r}"
            )

        for case, names in cases:
            # benzene-version is tier C; this port does not implement payload versioning yet.
            if case.get("requires") == "versioning":
                continue
            name, expected = case["name"], case["expected"]
            envelope = decode(case["metadata"], names)

            if envelope["topic"] != expected["topic"]:
                failures.append(
                    f"transport-metadata[{binding}/{name}]: topic {envelope['topic']!r}, "
                    f"expected {expected['topic']!r}"
                )
            headers = {k.lower(): v for k, v in (envelope.get("headers") or {}).items()}
            if "headers" in expected and not _is_subset(
                {k.lower(): v for k, v in expected["headers"].items()}, headers
            ):
                failures.append(
                    f"transport-metadata[{binding}/{name}]: headers {headers} !⊇ {expected['headers']}"
                )
            for excluded in expected.get("headersExclude", []):
                if excluded.lower() in headers:
                    failures.append(
                        f"transport-metadata[{binding}/{name}]: header {excluded!r} leaked "
                        "(it was consumed as routing metadata)"
                    )
    return failures


def run_all() -> list[str]:
    return (
        run_status_vocabulary()
        + run_http_mapping()
        + run_envelope_cases()
        + run_transport_metadata_cases()
    )


if __name__ == "__main__":
    all_failures = run_all()
    if all_failures:
        print(f"CONFORMANCE FAILED ({len(all_failures)}):")
        for f in all_failures:
            print("  -", f)
        sys.exit(1)
    print(
        "CONFORMANCE PASSED — status vocabulary, HTTP mapping, envelope cases, "
        "and transport metadata all green."
    )
