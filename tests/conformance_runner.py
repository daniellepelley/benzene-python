"""Dependency-free runner for the language-neutral conformance fixtures.

Runs the fixtures in ``conformance/`` against this implementation and returns a list of failures.
Imported by the pytest suite (``test_conformance.py``) and runnable directly:

    python -m tests.conformance_runner
"""

from __future__ import annotations

import ast
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
from benzene.core.envelope import encode_response
from benzene.grpc.status import BENZENE_STATUS_TRAILER, from_grpc, to_grpc
from benzene.http import from_http, to_http
from benzene.http.app import _to_http_response, http_problem_response
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
from benzene.results import Result, is_successful
from benzene.results.problems import problem_http_status, problem_title, problem_type

from .canonical_handlers import (
    register_canonical,
    register_canonical_outbound,
    register_canonical_with_problem,
    register_with_panic,
)

CONFORMANCE_DIR = Path(__file__).resolve().parent.parent / "conformance"

#: Vendored fixtures that no runner opens, each with the reason it is deliberately unrun.
#:
#: Every other ``conformance/*.json`` must be referenced by something under ``tests/``;
#: :func:`run_fixture_coverage` fails the run otherwise. The drift check
#: (.github/workflows/conformance-drift-check.yml) guards a fixture's *bytes* against canonical and
#: guards that a canonical fixture is not missing from the snapshot - but nothing guarded that a
#: fixture, once vendored, is ever actually opened. Two mesh fixtures sat here green and unrun,
#: which is the same failure mode ``_cases`` catches one level down (a renamed key silently
#: disabling a check), one level up: a whole file silently checking nothing.
#:
#: An entry here is a claim about capability, not a way to make a fixture go quiet. Both entries
#: below are fixtures conformance/README.md itself marks conditional on a capability this port does
#: not implement; adding one to skip a fixture this port *should* be running is exactly the drift
#: this list exists to expose, in writing, where a reviewer sees it.
UNRUN_FIXTURES: dict[str, str] = {
    "mesh-service-version-cases.json": (
        "conditional (conformance/README.md): required only of a collector claiming service-version "
        "identity. MeshCollector keys its catalog by service name alone, not by "
        "(service, serviceVersion), so this port does not claim mesh §2.4 - claim it and run this, "
        "or keep saying so here"
    ),
    "mesh-version-order-cases.json": (
        "conditional (conformance/README.md): required only of a port that ORDERS service versions. "
        "This port ships no version comparator (mesh §2.5), and §2.4 identity without §2.5 ordering "
        "is conformant"
    ),
}


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


def _cases(data: dict, key: str, fixture: str, failures: list[str]) -> list:
    """The fixture's list under ``key``, recording a failure if the fixture has no such key.

    ``data.get(key, [])`` iterates nothing when the key is absent, so a runner that loops over it
    checks nothing and reports no failures - indistinguishable from a clean pass. That is not a
    hypothetical: the Go and TypeScript descriptor runners each spent the whole producer/consumer
    role inversion reading a hash-property key the canonical fixture had renamed, silently asserting
    nothing while CI stayed green. These fixtures are vendored snapshots of a canonical set that
    other people rename, so the runner is the only thing positioned to notice the drift.

    An empty list that the fixture really does carry is fine and iterates as before; it is the
    absent key that is a defect.
    """
    if key not in data:
        failures.append(
            f"{fixture}: fixture has no {key!r} - the runner and the fixture have drifted"
        )
        return []
    return data[key]


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
    for case in _cases(data, "forward", "http-status-mapping", failures):
        # isSuccessful only distinguishes the two '<unknown>' rows (see the fixture's description);
        # for a known status it is absent and the default failure treatment is irrelevant.
        got, expected = to_http(case["from"], case.get("isSuccessful")), int(case["to"])
        if got != expected:
            failures.append(f"benzene->http: {case['from']!r} -> {got}, expected {expected}")
    # reverse: HTTP code -> benzene status. Its own names: `got`/`expected` above are the ints of
    # the forward direction, and reusing them for status strings is what makes the pair untypeable.
    for case in _cases(data, "reverse", "http-status-mapping", failures):
        got_status, expected_status = from_http(int(case["from"])), case["to"]
        if got_status != expected_status:
            failures.append(
                f"http->benzene: {case['from']} -> {got_status!r}, expected {expected_status!r}"
            )
    return failures


def envelope_case_failures(response: dict, expected: dict, name: str) -> list[str]:
    """Check one response against one envelope case's expectations (the envelope case format).

    The single place the envelope case format is implemented. There used to be two - this runner and
    the parametrized pytest test beside it - and they had drifted: both skipped ``isSuccessful`` and
    ``bodyExclude``, so all 17 cases' success-signal assertions and all 11 withdrawn-member
    assertions were passing without being checked, and the port really was omitting ``isSuccessful``
    from every response envelope. Two similar loops is how that happens; there is now one.
    """
    failures: list[str] = []

    if response["statusCode"] != expected["statusCode"]:
        failures.append(
            f"envelope[{name}]: statusCode {response['statusCode']!r}, expected {expected['statusCode']!r}"
        )

    # isSuccessful is required and authoritative (section 1.2), so it is checked exactly against the
    # envelope's own member - never inferred from the status, which is the whole point of it.
    if "isSuccessful" in expected:
        if "isSuccessful" not in response:
            failures.append(
                f"envelope[{name}]: isSuccessful is absent, expected {expected['isSuccessful']} "
                "stated outright (section 1.2 requires it)"
            )
        elif response["isSuccessful"] != expected["isSuccessful"]:
            failures.append(
                f"envelope[{name}]: isSuccessful {response['isSuccessful']}, "
                f"expected {expected['isSuccessful']}"
            )

    if "headers" in expected and not _is_subset(expected["headers"], response.get("headers", {})):
        failures.append(f"envelope[{name}]: headers {response.get('headers')} !⊇ {expected['headers']}")

    if "body" in expected:
        actual_body = json.loads(response["body"]) if response["body"] else {}
        if not _is_subset(expected["body"], actual_body):
            failures.append(f"envelope[{name}]: body {actual_body} !⊇ {expected['body']}")
        # bodyExclude names members that MUST NOT appear. Asserting only that the new members are
        # present would pass just as happily for a writer that also still emits a withdrawn one,
        # which is exactly what this guards (section 1.3's `status` rename).
        for member in expected.get("bodyExclude", []):
            if member in actual_body:
                failures.append(f"envelope[{name}]: body must not contain {member!r}: {actual_body}")

    return failures


def run_envelope_cases() -> list[str]:
    failures: list[str] = []
    app = _app()
    data = _load("envelope-cases.json")
    for case in _cases(data, "cases", "envelope-cases", failures):
        response = asyncio.run(app.handle(case["request"]))
        failures += envelope_case_failures(response, case["expected"], case["name"])
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

    for case in _cases(data, "metadataCases", "transport-metadata", failures):
        check(case, MetadataKeys())
    for case in _cases(data, "overrideCases", "transport-metadata", failures):
        check(case, MetadataKeys(topic=case["metadataKeys"]["topic"]))
    return failures


def run_contract_document_cases() -> list[str]:
    """contract-document-cases.json: parseCases, topicScopeCases, schemaClosureCases (§§1-5)."""
    failures: list[str] = []
    data = _load("contract-document-cases.json")
    documents = {doc_id: parse_document(raw) for doc_id, raw in data["documents"].items()}

    for case in _cases(data, "parseCases", "contract-document", failures):
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

    for case in _cases(data, "topicScopeCases", "contract-document", failures):
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

    for case in _cases(data, "schemaClosureCases", "contract-document", failures):
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


def run_problem_details() -> list[str]:
    """problem-details-cases.json (wire-contracts.md 1.3, 3.1, 4.1) - three independent groups.

    ``registry`` and ``envelopeCases`` are required for the Benzene Core claim; ``httpRules`` is
    required for each HTTP binding a port ships, and this port ships ``benzene.http``, so all three
    run. The fixture was vendored here for some time with nothing reading it.
    """
    failures: list[str] = []
    data = _load("problem-details-cases.json")

    # --- registry: this port's own table against the fixture's rows, no message to build ---------
    registry = data.get("registry", {})
    if "registry" not in data:
        failures.append("problem-details: fixture has no 'registry' - the runner and the fixture have drifted")
    for row in _cases(registry, "rows", "problem-details.registry", failures):
        status = row["benzeneStatus"]
        if problem_type(status) != row["type"]:
            failures.append(
                f"problem-details[registry {status}]: type {problem_type(status)!r}, expected {row['type']!r}"
            )
        if problem_http_status(status) != row["httpStatus"]:
            failures.append(
                f"problem-details[registry {status}]: httpStatus {problem_http_status(status)}, "
                f"expected {row['httpStatus']}"
            )
        # Title wording is never asserted (conformance/README.md); its presence is - a row with no
        # title is a row that has fallen out of the table.
        if not problem_title(status):
            failures.append(f"problem-details[registry {status}]: no registry title")

    # An application-defined failure status has no row at all, and falls to the 4.1 unknown row.
    unknown = registry.get("unknownStatus", {})
    app_defined = "insufficient-funds"
    if problem_type(app_defined) is not None:
        failures.append(
            f"problem-details[unknownStatus]: type {problem_type(app_defined)!r} for an "
            "application-defined status, expected none"
        )
    if problem_title(app_defined) is not None:
        failures.append(
            f"problem-details[unknownStatus]: title {problem_title(app_defined)!r} for an "
            "application-defined status, expected none"
        )
    if "httpStatus" not in unknown:
        failures.append(
            "problem-details: fixture has no 'registry.unknownStatus.httpStatus' - "
            "the runner and the fixture have drifted"
        )
    elif problem_http_status(app_defined) != unknown["httpStatus"]:
        failures.append(
            f"problem-details[unknownStatus]: httpStatus {problem_http_status(app_defined)}, "
            f"expected {unknown['httpStatus']}"
        )

    # --- envelopeCases: exactly the envelope case format, so exactly the same checker ------------
    app = BenzeneMessageApplication(register_canonical_with_problem(Registry()))
    for case in _cases(data, "envelopeCases", "problem-details", failures):
        response = asyncio.run(app.handle(case["request"]))
        failures += envelope_case_failures(response, case["expected"], case["name"])

    # --- httpRules: the response line and the document's status member must agree ----------------
    rules = data.get("httpRules", {})
    if "httpRules" not in data:
        failures.append("problem-details: fixture has no 'httpRules' - the runner and the fixture have drifted")
    for case in _cases(rules, "failureCases", "problem-details.httpRules", failures):
        status, expected_http = case["benzeneStatus"], case["httpStatus"]
        http_response = http_problem_response(Result.failure(status, "boom"))
        name = f"httpRules {status}"

        if http_response.status_code != expected_http:
            failures.append(f"problem-details[{name}]: HTTP {http_response.status_code}, expected {expected_http}")
        content_type = (http_response.headers or {}).get("content-type")
        if content_type != "application/problem+json":
            failures.append(f"problem-details[{name}]: content-type {content_type!r}, expected application/problem+json")

        document = json.loads(http_response.body)
        if document.get("status") != expected_http:
            failures.append(
                f"problem-details[{name}]: document status {document.get('status')!r}, expected {expected_http}"
            )
        # The two MUST come from the same mapping (4.1), so they can never disagree.
        if document.get("status") != http_response.status_code:
            failures.append(
                f"problem-details[{name}]: document status {document.get('status')!r} disagrees with "
                f"the response status {http_response.status_code}"
            )
        if document.get("benzeneStatus") != status:
            failures.append(
                f"problem-details[{name}]: benzeneStatus {document.get('benzeneStatus')!r}, expected {status!r}"
            )

    success = rules.get("successCase")
    if success is None:
        failures.append(
            "problem-details: fixture has no 'httpRules.successCase' - the runner and the fixture have drifted"
        )
    else:
        http_response = _to_http_response(encode_response(Result.ok({"applied": success["benzeneStatus"]})))
        if http_response.status_code != success["httpStatus"]:
            failures.append(
                f"problem-details[httpRules success]: HTTP {http_response.status_code}, expected {success['httpStatus']}"
            )
        content_type = (http_response.headers or {}).get("content-type", "")
        if not content_type.startswith(success["contentType"]):
            failures.append(
                f"problem-details[httpRules success]: content-type {content_type!r}, "
                f"expected {success['contentType']!r}"
            )
        body = json.loads(http_response.body) if http_response.body else {}
        for member in ("type", "title", "status", "benzeneStatus", "errors"):
            if member in body:
                failures.append(
                    f"problem-details[httpRules success]: a success body must carry no problem "
                    f"member, found {member!r}"
                )

    return failures


def run_grpc_status_mapping() -> list[str]:
    """grpc-status-mapping.json: the Benzene ↔ gRPC code mapping (wire-contracts §4.2).

    Dependency-free like the rest of this runner - ``benzene.grpc.status`` maps to and from gRPC
    status-code *names* and imports no ``grpcio``, which is exactly why there was no reason for this
    fixture to be the one the runner skipped.
    """
    failures: list[str] = []
    data = _load("grpc-status-mapping.json")

    for case in _cases(data, "forward", "grpc-status-mapping", failures):
        # "<unknown>" stands for any status outside the vocabulary; its two rows are told apart by
        # isSuccessful (§4.2). A known status maps by its own row, so those rows carry no
        # isSuccessful and it passes through as None.
        status = "some-app-extension" if case["from"] == "<unknown>" else case["from"]
        actual = to_grpc(status, case.get("isSuccessful"))
        if actual != case["to"]:
            failures.append(
                f"grpc-status-mapping[forward {case['from']}]: {actual!r}, expected {case['to']!r}"
            )

    for case in _cases(data, "reverse", "grpc-status-mapping", failures):
        actual = from_grpc(case["from"])
        if actual != case["to"]:
            failures.append(
                f"grpc-status-mapping[reverse {case['from']}]: {actual!r}, expected {case['to']!r}"
            )

    # The fixture carries no trailer table on purpose ("covered by implementation tests, not these
    # tables"), because a trailer winning verbatim is not a mapping row. tests/test_grpc.py holds
    # that rule; the name is pinned here so a rename of it cannot pass unnoticed on this path.
    if BENZENE_STATUS_TRAILER != "benzene-status":
        failures.append(
            f"grpc-status-mapping[trailer]: the §4.2 trailer is {BENZENE_STATUS_TRAILER!r}, "
            "expected 'benzene-status'"
        )

    return failures


def _fixture_names_referenced_by_tests() -> set[str]:
    """Every string literal in the test package — the fixture names something here actually names.

    A string literal, not a substring of the file, so the ``UNRUN_FIXTURES`` table below cannot
    claim its own entries. That table is excluded explicitly for the same reason: listing a fixture
    as deliberately unrun must never read as running it.
    """
    referenced: set[str] = set()
    for path in sorted(Path(__file__).resolve().parent.rglob("*.py")):
        tree = ast.parse(path.read_text())
        excluded: set[int] = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "UNRUN_FIXTURES"
            ):
                excluded.update(id(child) for child in ast.walk(node))
        referenced.update(
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in excluded
        )
    return referenced


def run_fixture_coverage() -> list[str]:
    """Every vendored fixture is opened by something under ``tests/``, or is an explicit opt-out.

    The blind spot this closes: the drift check guards each fixture's bytes against canonical, and
    ``_cases`` guards that a fixture key the runner reads still exists - but a fixture that no
    runner ever opens is invisible to both. It is vendored, byte-identical to canonical, and checks
    nothing, and CI prints PASSED. A claim is a literal reference to the file name in a test module,
    which is what a runner opening it looks like and what a repo-wide grep for it would find.
    """
    failures: list[str] = []
    claimed = _fixture_names_referenced_by_tests()
    present = {path.name for path in CONFORMANCE_DIR.glob("*.json")}

    for name in sorted(present):
        if name in claimed or name in UNRUN_FIXTURES:
            continue
        failures.append(
            f"{name}: vendored but no runner opens it - run it, or add it to UNRUN_FIXTURES with "
            "the reason it is deliberately unrun"
        )

    for name, reason in sorted(UNRUN_FIXTURES.items()):
        if name not in present:
            failures.append(
                f"{name}: listed in UNRUN_FIXTURES ({reason}) but no longer vendored - drop the entry"
            )
        elif name in claimed:
            failures.append(
                f"{name}: listed in UNRUN_FIXTURES but a runner now opens it - drop the entry"
            )

    return failures


def run_all() -> list[str]:
    return (
        run_status_vocabulary()
        + run_http_mapping()
        + run_envelope_cases()
        + run_problem_details()
        + run_transport_metadata()
        + run_grpc_status_mapping()
        + run_mesh_descriptor()
        + run_mesh_trace()
        + run_mesh_collector()
        + run_contract_document_cases()
        + run_contract_hash_cases()
        + run_fixture_coverage()
    )


if __name__ == "__main__":
    all_failures = run_all()
    if all_failures:
        print(f"CONFORMANCE FAILED ({len(all_failures)}):")
        for f in all_failures:
            print("  -", f)
        sys.exit(1)
    print(
        "CONFORMANCE PASSED — status vocabulary, HTTP mapping, problem details, envelope, "
        "transport metadata, gRPC status mapping, mesh (descriptor + trace + collector + issues), "
        "contract-document, and contract-hash cases all green; "
        f"every vendored fixture claimed ({len(UNRUN_FIXTURES)} explicitly unrun)."
    )
