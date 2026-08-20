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

from benzene.codegen_client import contract_hash as codegen_contract_hash
from benzene.codegen_client.document import parse_document
from benzene.codegen_client.schema_closure import reachable_names
from benzene.codegen_client.topic_scope import (
    TopicScopeOptions,
    UnknownTopicsError,
    apply_topic_scope,
)
from benzene.core import (
    BenzeneMessageApplication,
    MetadataKeys,
    MiddlewarePipeline,
    Registry,
    read_message_metadata,
    resolve_version,
)
from benzene.http import from_http, to_http
from benzene.mesh import (
    InMemoryTraceExporter,
    MeshCollector,
    OutboundRegistry,
    ServiceDescriptor,
    ServiceInfo,
    collector_registry,
    parse_traceparent,
    trace_middleware,
)
from benzene.results import is_successful

from .canonical_handlers import (
    register_canonical,
    register_canonical_outbound,
    register_with_panic,
)

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
        # isSuccessful only distinguishes the two '<unknown>' rows (see the fixture's description);
        # for a known status it is absent and the default failure treatment is irrelevant.
        got, expected = to_http(case["from"], case.get("isSuccessful")), int(case["to"])
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
        response = asyncio.run(app.handle(case["request"]))
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


def _mesh_subset(expected: Any, actual: Any) -> bool:
    """Mesh subset matching (mesh.md conformance): dicts match by subset; arrays by exact length
    with per-element subset; an expected empty array matches an actual empty *or absent* array."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        return all(k in actual and _mesh_subset(v, actual[k]) for k, v in expected.items())
    if isinstance(expected, list):
        if expected == [] and (actual is None or actual == []):
            return True
        if not isinstance(actual, list) or len(actual) != len(expected):
            return False
        return all(_mesh_subset(e, a) for e, a in zip(expected, actual, strict=True))  # lengths checked above
    return expected == actual


def _mesh_subset_absent_ok(expected: dict, actual: dict) -> bool:
    """Like ``_mesh_subset`` for dicts, but an expected empty-array value may be absent in ``actual``."""
    for key, value in expected.items():
        if isinstance(value, list) and value == [] and key not in actual:
            continue
        if key not in actual or not _mesh_subset(value, actual[key]):
            return False
    return True


def _info_from_fixture(service_info: dict, **overrides: Any) -> ServiceInfo:
    fields = {
        "service": service_info["service"],
        "service_version": service_info.get("serviceVersion"),
        "placement": service_info.get("placement"),
    }
    fields.update(overrides)
    return ServiceInfo(**fields)


def _asserted(hash_spec: dict, key: str, failures: list[str]) -> bool:
    """Whether the fixture asks for a hash property, recording a failure if it never mentions it.

    ``hash_spec.get(key)`` reads the same whether the fixture says ``false`` or has never heard of
    the key, and those mean opposite things. The Go and TypeScript runners each spent the whole
    producer/consumer role inversion asking for ``sensitiveToConsumes`` after the fixture had renamed
    it to ``sensitiveToProduces``; the guard read falsey, the check quietly stopped running, and the
    suite stayed green while asserting nothing about produced topics. A key the fixture does not
    carry is drift between runner and fixture - never permission to stop checking.
    """
    if key not in hash_spec:
        failures.append(
            f"mesh-descriptor: fixture hash section has no {key!r} - "
            "the runner and the fixture have drifted"
        )
        return False
    return bool(hash_spec[key])


def run_mesh_descriptor() -> list[str]:
    failures: list[str] = []
    data = _load("mesh-descriptor-cases.json")
    info = _info_from_fixture(data["serviceInfo"])
    descriptor = ServiceDescriptor.derive(
        register_canonical(Registry()), info, register_canonical_outbound(OutboundRegistry())
    )
    payload = descriptor.to_payload()

    if not _mesh_subset_absent_ok(data["expectedDescriptor"], payload):
        failures.append(f"mesh-descriptor: derived {payload} !⊇ {data['expectedDescriptor']}")

    hash_spec = data["hash"]
    h = descriptor.descriptor_hash()
    if not h.startswith(hash_spec["prefix"]):
        failures.append(f"mesh-descriptor: hash {h!r} lacks prefix {hash_spec['prefix']!r}")
    if len(h) - len(hash_spec["prefix"]) != hash_spec["hexLength"]:
        failures.append(f"mesh-descriptor: hash hex length != {hash_spec['hexLength']}")
    if _asserted(hash_spec, "invariantToInstanceId", failures):
        other = ServiceDescriptor.derive(
            register_canonical(Registry()),
            _info_from_fixture(data["serviceInfo"], instance_id="i-xyz"),
            register_canonical_outbound(OutboundRegistry()),
        )
        if other.descriptor_hash() != h:
            failures.append("mesh-descriptor: hash not invariant to instanceId")
    if _asserted(hash_spec, "sensitiveToServiceVersion", failures):
        other = ServiceDescriptor.derive(
            register_canonical(Registry()),
            _info_from_fixture(data["serviceInfo"], service_version="9.9.9"),
            register_canonical_outbound(OutboundRegistry()),
        )
        if other.descriptor_hash() == h:
            failures.append("mesh-descriptor: hash not sensitive to serviceVersion")
    if _asserted(hash_spec, "sensitiveToTopics", failures):
        from .canonical_handlers import greet

        other = ServiceDescriptor.derive(
            Registry().add(greet), info, register_canonical_outbound(OutboundRegistry())
        )
        if other.descriptor_hash() == h:
            failures.append("mesh-descriptor: hash not sensitive to the topic set")
    if _asserted(hash_spec, "sensitiveToProduces", failures):
        other = ServiceDescriptor.derive(register_canonical(Registry()), info)  # no produces at all
        if other.descriptor_hash() == h:
            failures.append("mesh-descriptor: hash not sensitive to the produced-topic set")
    return failures


def run_mesh_trace() -> list[str]:
    failures: list[str] = []
    data = _load("mesh-trace-cases.json")

    for row in data["traceparent"]:
        parsed = parse_traceparent(row["header"])
        if row["valid"]:
            if parsed != (row["traceId"], row["parentSpanId"]):
                failures.append(f"mesh-trace[{row['name']}]: expected join {parsed!r}")
        elif parsed is not None:
            failures.append(f"mesh-trace[{row['name']}]: expected fresh trace, got {parsed!r}")

    for case in data["invocations"]:
        exporter = InMemoryTraceExporter()
        pipeline = MiddlewarePipeline().use(trace_middleware(exporter, service="conformance"))
        app = BenzeneMessageApplication(register_with_panic(Registry()), pipeline)
        asyncio.run(app.handle(case["request"]))
        if len(exporter) != 1:
            failures.append(f"mesh-trace[{case['name']}]: expected exactly one event, got {len(exporter)}")
            continue
        event = exporter[0].to_payload()
        if not _mesh_subset(case["expectedEvent"], event):
            failures.append(f"mesh-trace[{case['name']}]: event {event} !⊇ {case['expectedEvent']}")
    return failures


def run_collector_fixture(filename: str) -> list[str]:
    """Run a collector step-fixture (each case's steps against one fresh collector)."""
    failures: list[str] = []
    data = _load(filename)
    for case in data["cases"]:
        app = BenzeneMessageApplication(collector_registry(MeshCollector()))
        for index, step in enumerate(case["steps"]):
            response = asyncio.run(app.handle(step["request"]))
            expected = step["expected"]
            name = f"{case['name']}[{index}]"
            if response["statusCode"] != expected["statusCode"]:
                failures.append(
                    f"{filename}[{name}]: statusCode {response['statusCode']!r}, "
                    f"expected {expected['statusCode']!r}"
                )
                continue
            if "body" in expected:
                body = json.loads(response["body"]) if response["body"] else {}
                if not _mesh_subset(expected["body"], body):
                    failures.append(f"{filename}[{name}]: body {body} !⊇ {expected['body']}")
    return failures


def run_mesh_collector() -> list[str]:
    return run_collector_fixture("mesh-collector-cases.json") + run_collector_fixture(
        "mesh-issue-cases.json"
    )


def run_transport_metadata() -> list[str]:
    """Topic + header resolution from native metadata (wire-contracts §2)."""
    failures: list[str] = []
    data = _load("transport-metadata-cases.json")

    def check(case: dict, keys: MetadataKeys) -> None:
        topic, headers = read_message_metadata(case["metadata"], keys)
        expected = case["expected"]
        name = case["name"]
        if "topic" in expected and topic != expected["topic"]:
            failures.append(f"metadata[{name}]: topic {topic!r}, expected {expected['topic']!r}")
        for key, value in expected.get("headers", {}).items():
            if headers.get(key) != value:
                failures.append(f"metadata[{name}]: header {key}={headers.get(key)!r}, expected {value!r}")
        for key in expected.get("headersExclude", []):
            if key.lower() in headers:
                failures.append(f"metadata[{name}]: header {key!r} should be excluded")
        if "version" in expected and resolve_version(headers) != expected["version"]:
            failures.append(f"metadata[{name}]: version {resolve_version(headers)!r}, expected {expected['version']!r}")

    for case in data.get("metadataCases", []):
        check(case, MetadataKeys())
    for case in data.get("overrideCases", []):
        check(case, MetadataKeys(topic=case["metadataKeys"]["topic"]))
    return failures


def run_contract_document_cases() -> list[str]:
    """contract-document-cases.json: parseCases, topicScopeCases, schemaClosureCases (§§1-5)."""
    failures: list[str] = []
    data = _load("contract-document-cases.json")
    documents = {doc_id: parse_document(raw) for doc_id, raw in data["documents"].items()}

    for case in data.get("parseCases", []):
        name = case["name"]
        document = documents[case["documentRef"]]

        if "expectedError" in case:
            options = case.get("options", {})
            scope_options = TopicScopeOptions(
                topics=tuple(options["topics"]) if options.get("topics") else None,
                include_reserved=bool(options.get("includeReserved", False)),
            )
            try:
                apply_topic_scope(document, scope_options)
                failures.append(f"parseCases[{name}]: expected UnknownTopicsError, none raised")
            except UnknownTopicsError as exc:
                expected = case["expectedError"]
                if sorted(exc.unknown_topics) != sorted(expected["unknownTopics"]):
                    failures.append(
                        f"parseCases[{name}]: unknownTopics {exc.unknown_topics} != {expected['unknownTopics']}"
                    )
                if sorted(exc.valid_topics) != sorted(expected["validTopics"]):
                    failures.append(
                        f"parseCases[{name}]: validTopics {exc.valid_topics} != {expected['validTopics']}"
                    )
            continue

        expected = case["expected"]
        if "openapi" in expected and document.openapi != expected["openapi"]:
            failures.append(f"parseCases[{name}]: openapi {document.openapi!r} != {expected['openapi']!r}")
        for expected_request in expected.get("requests", []):
            actual = document.find_request(expected_request["topic"])
            if actual is None:
                failures.append(f"parseCases[{name}]: request topic {expected_request['topic']!r} not found")
                continue
            if "versionPresent" in expected_request and actual.version_present != expected_request["versionPresent"]:
                failures.append(f"parseCases[{name}]: {actual.topic} versionPresent mismatch")
            if "version" in expected_request and actual.version != expected_request["version"]:
                failures.append(f"parseCases[{name}]: {actual.topic} version mismatch")
            if "reserved" in expected_request and actual.is_reserved() != expected_request["reserved"]:
                failures.append(f"parseCases[{name}]: {actual.topic} reserved mismatch")
        for expected_event in expected.get("events", []):
            actual_event = next((e for e in document.events if e.topic == expected_event["topic"]), None)
            if actual_event is None:
                failures.append(f"parseCases[{name}]: event topic {expected_event['topic']!r} not found")
                continue
            if "versionPresent" in expected_event and actual_event.version_present != expected_event["versionPresent"]:
                failures.append(f"parseCases[{name}]: event {actual_event.topic} versionPresent mismatch")
            if "version" in expected_event and actual_event.version != expected_event["version"]:
                failures.append(f"parseCases[{name}]: event {actual_event.topic} version mismatch")

    for case in data.get("topicScopeCases", []):
        name = case["name"]
        document = documents[case["documentRef"]]
        options = case.get("options", {})
        scope_options = TopicScopeOptions(
            topics=tuple(options["topics"]) if options.get("topics") else None,
            include_reserved=bool(options.get("includeReserved", False)),
        )
        scoped = apply_topic_scope(document, scope_options)
        actual_topics = set(scoped.topics())
        expected_topics = set(case["expectedTopics"])
        if actual_topics != expected_topics:
            failures.append(f"topicScopeCases[{name}]: {actual_topics} != {expected_topics}")

    for case in data.get("schemaClosureCases", []):
        name = case["name"]
        document = documents[case["documentRef"]]
        request = document.find_request(case["topic"])
        if request is None:
            failures.append(f"schemaClosureCases[{name}]: topic {case['topic']!r} not found")
            continue
        actual_components = reachable_names(document.schemas, request.request, request.response)
        expected_components = set(case["expectedComponents"])
        if actual_components != expected_components:
            failures.append(f"schemaClosureCases[{name}]: {actual_components} != {expected_components}")

    return failures


def run_contract_hash_cases() -> list[str]:
    """contract-hash-cases.json: exact contractHash values (§6)."""
    failures: list[str] = []
    data = _load("contract-hash-cases.json")
    for case in data["cases"]:
        name = case["name"]
        topic_scoped = name == "topic-scoped-projection"
        got = codegen_contract_hash.compute(case["document"], topic_scoped=topic_scoped)
        if got != case["expectedHash"]:
            failures.append(f"contract-hash[{name}]: {got} != {case['expectedHash']}")
    return failures


def run_all() -> list[str]:
    return (
        run_status_vocabulary()
        + run_http_mapping()
        + run_envelope_cases()
        + run_transport_metadata()
        + run_mesh_descriptor()
        + run_mesh_trace()
        + run_mesh_collector()
        + run_contract_document_cases()
        + run_contract_hash_cases()
    )


if __name__ == "__main__":
    all_failures = run_all()
    if all_failures:
        print(f"CONFORMANCE FAILED ({len(all_failures)}):")
        for f in all_failures:
            print("  -", f)
        sys.exit(1)
    print(
        "CONFORMANCE PASSED — status vocabulary, HTTP mapping, envelope, transport metadata, "
        "mesh (descriptor + trace + collector + issues), contract-document, and contract-hash "
        "cases all green."
    )
