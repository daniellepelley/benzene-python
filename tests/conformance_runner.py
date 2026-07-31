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


def run_all() -> list[str]:
    return run_status_vocabulary() + run_http_mapping() + run_envelope_cases()


if __name__ == "__main__":
    all_failures = run_all()
    if all_failures:
        print(f"CONFORMANCE FAILED ({len(all_failures)}):")
        for f in all_failures:
            print("  -", f)
        sys.exit(1)
    print("CONFORMANCE PASSED — status vocabulary, HTTP mapping, and envelope cases all green.")
