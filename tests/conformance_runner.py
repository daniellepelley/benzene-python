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
    ServiceDescriptor,
    ServiceInfo,
    collector_registry,
    parse_traceparent,
    trace_middleware,
)
from benzene.results import is_successful

from .canonical_handlers import register_canonical, register_with_panic

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
        return all(_mesh_subset(e, a) for e, a in zip(expected, actual))
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


def run_mesh_descriptor() -> list[str]:
    failures: list[str] = []
    data = _load("mesh-descriptor-cases.json")
    info = _info_from_fixture(data["serviceInfo"])
    descriptor = ServiceDescriptor.derive(register_canonical(Registry()), info)
    payload = descriptor.to_payload()

    if not _mesh_subset_absent_ok(data["expectedDescriptor"], payload):
        failures.append(f"mesh-descriptor: derived {payload} !⊇ {data['expectedDescriptor']}")

    hash_spec = data["hash"]
    h = descriptor.descriptor_hash()
    if not h.startswith(hash_spec["prefix"]):
        failures.append(f"mesh-descriptor: hash {h!r} lacks prefix {hash_spec['prefix']!r}")
    if len(h) - len(hash_spec["prefix"]) != hash_spec["hexLength"]:
        failures.append(f"mesh-descriptor: hash hex length != {hash_spec['hexLength']}")
    if hash_spec.get("invariantToInstanceId"):
        other = ServiceDescriptor.derive(
            register_canonical(Registry()), _info_from_fixture(data["serviceInfo"], instance_id="i-xyz")
        )
        if other.descriptor_hash() != h:
            failures.append("mesh-descriptor: hash not invariant to instanceId")
    if hash_spec.get("sensitiveToServiceVersion"):
        other = ServiceDescriptor.derive(
            register_canonical(Registry()), _info_from_fixture(data["serviceInfo"], service_version="9.9.9")
        )
        if other.descriptor_hash() == h:
            failures.append("mesh-descriptor: hash not sensitive to serviceVersion")
    if hash_spec.get("sensitiveToTopics"):
        from .canonical_handlers import greet

        other = ServiceDescriptor.derive(Registry().add(greet), info)
        if other.descriptor_hash() == h:
            failures.append("mesh-descriptor: hash not sensitive to the topic set")
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


def run_all() -> list[str]:
    return (
        run_status_vocabulary()
        + run_http_mapping()
        + run_envelope_cases()
        + run_transport_metadata()
        + run_mesh_descriptor()
        + run_mesh_trace()
        + run_mesh_collector()
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
        "and mesh (descriptor + trace + collector + issues) cases all green."
    )
