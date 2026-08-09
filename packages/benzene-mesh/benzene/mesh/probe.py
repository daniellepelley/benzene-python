"""The Cloud Service Profile **live-probe checker** — audit a deployed service over plain HTTP.

Where :mod:`benzene.mesh.profile` is the service's *own* wiring-time self-check, this is the
outside-in counterpart: point it at a running service's base URL and it reports, per requirement, a
**tri-state** verdict — ``satisfied`` / ``not-satisfied`` / ``inconclusive`` — always with a reason.
It only speaks the language-neutral HTTP surfaces (``/benzene/spec``, ``/benzene/health``,
``/benzene/invoke``), so it audits a Go, Node, or .NET service exactly as it audits a Python one; it
depends on nothing in the rest of the port.

A black-box probe genuinely cannot verify everything a service-side self-check can, so — matching the
profile spec (cloud-service-profile.md §5) — two things stay ``inconclusive`` by design and are never
silently upgraded:

- **R8** (trace-context propagation) needs a *second* service to observe a forwarded ``traceparent``,
  or a collector deriving consumer edges — unobservable from one service.
- **R6**'s register/heartbeat half: only the reserved ``benzene:mesh`` descriptor response is directly
  observable; delivery to a collector is not. A passing descriptor check is reported as the
  *observable half only*, never the whole of R6.

**R7** likewise degrades to ``inconclusive`` the moment the caller points the probe at a non-default
prefix — it then has no way to know the service's own defaults, only that something answers where it
looked.

This complements, not replaces, the live cross-language interop checks (porting-guide §3): those prove
wire compatibility end to end; this audits one deployed service's profile conformance from outside.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

#: The default well-known path prefix (design-principles §5.2).
DEFAULT_PREFIX = "/benzene"

#: An async HTTP call: ``(method, url, body) -> (status, text)``. Injectable so the probe can run
#: against an in-memory app (tests) or a real URL (the default :func:`_stdlib_http`).
HttpCall = Callable[[str, str, "str | None"], Awaitable["tuple[int, str]"]]


class Verdict(str, Enum):
    """A requirement's tri-state audit outcome (a ``str`` enum so it serializes as its value)."""

    SATISFIED = "satisfied"
    NOT_SATISFIED = "not-satisfied"
    INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True)
class RequirementProbe:
    """One requirement's audited verdict and the reason the probe reached it."""

    id: str
    verdict: Verdict
    reason: str


@dataclass(frozen=True)
class CloudServiceProbeReport:
    """The outcome of auditing one service's base URL against the profile (R1–R8)."""

    base_url: str
    probes: tuple[RequirementProbe, ...]

    def verdict(self, requirement_id: str) -> Verdict:
        for probe in self.probes:
            if probe.id == requirement_id:
                return probe.verdict
        raise KeyError(requirement_id)

    @property
    def not_satisfied(self) -> list[str]:
        """Requirement ids the probe positively found *unmet* — the actionable failures."""
        return [p.id for p in self.probes if p.verdict is Verdict.NOT_SATISFIED]

    @property
    def inconclusive(self) -> list[str]:
        """Requirement ids the probe could not verify from outside (never a failure by itself)."""
        return [p.id for p in self.probes if p.verdict is Verdict.INCONCLUSIVE]

    @property
    def is_clean(self) -> bool:
        """True when nothing was positively found unmet (inconclusive results are tolerated)."""
        return not self.not_satisfied

    def to_payload(self) -> dict[str, Any]:
        return {
            "baseUrl": self.base_url,
            "requirements": [
                {"id": p.id, "verdict": p.verdict.value, "reason": p.reason} for p in self.probes
            ],
        }


def _is_reserved(topic: str) -> bool:
    return topic.startswith("benzene:")


async def probe_cloud_service(
    base_url: str,
    *,
    prefix: str = DEFAULT_PREFIX,
    http: HttpCall | None = None,
) -> CloudServiceProbeReport:
    """Audit the service at ``base_url`` against the Cloud Service Profile's R1–R8 over HTTP.

    ``prefix`` is the well-known path prefix to probe (default ``/benzene``); pointing it elsewhere
    makes R7 ``inconclusive`` (the probe can no longer know the service's own defaults). ``http`` is an
    injectable ``(method, url, body) -> (status, text)`` — the default uses ``urllib`` on a worker
    thread; pass a fake that routes to an in-memory app in tests.
    """
    call = http or _stdlib_http()
    root = base_url.rstrip("/")
    at_default_prefix = prefix == DEFAULT_PREFIX

    spec = await _get_json(call, f"{root}{prefix}/spec")
    health = await _get_json(call, f"{root}{prefix}/health", ok_statuses=(200, 503))
    descriptor = await _invoke(call, f"{root}{prefix}/invoke", "benzene:mesh")

    reachable = any(result.reached for result in (spec, health, descriptor))
    # A surface *answering* (a parsed spec/health document, or an invoke envelope) is stronger than a
    # mere HTTP response — a bare service reachable only through 404s answers nothing.
    surfaces_answered = (
        spec.document is not None or health.document is not None or descriptor.envelope
    )
    probes: list[RequirementProbe] = []

    # R1 — a hosted pipeline behind a binding: something answered at the service's HTTP surface.
    probes.append(
        RequirementProbe(
            "R1",
            Verdict.SATISFIED if reachable else Verdict.INCONCLUSIVE,
            "the service answered on its HTTP surface"
            if reachable
            else "no /benzene/* surface was reachable to audit",
        )
    )

    # R2 — application topics: read the non-reserved topics off the derived spec.
    if not spec.reached:
        probes.append(RequirementProbe("R2", Verdict.INCONCLUSIVE, spec.detail))
    elif spec.document is None:
        probes.append(
            RequirementProbe("R2", Verdict.NOT_SATISFIED, "/benzene/spec did not return a document")
        )
    else:
        topic_ids = [str(t.get("id", "")) for t in spec.document.get("topics", [])]
        app_topics = [t for t in topic_ids if t and not _is_reserved(t)]
        probes.append(
            RequirementProbe(
                "R2",
                Verdict.SATISFIED if app_topics else Verdict.NOT_SATISFIED,
                f"spec lists {len(app_topics)} application topic(s)"
                if app_topics
                else "spec lists no non-reserved application topics",
            )
        )

    # R3 — health aggregate at /benzene/health.
    probes.append(
        _surface_verdict(
            "R3",
            health,
            lambda doc: isinstance(doc.get("isHealthy"), bool),
            present="/benzene/health returns the {isHealthy, healthChecks} aggregate",
            malformed="/benzene/health did not return a health aggregate",
        )
    )

    # R4 — wire-envelope invocability: /benzene/invoke accepted an envelope and answered with one.
    probes.append(
        RequirementProbe(
            "R4",
            Verdict.SATISFIED
            if descriptor.reached and descriptor.envelope
            else Verdict.NOT_SATISFIED
            if descriptor.status == 404
            else Verdict.INCONCLUSIVE,
            "/benzene/invoke accepted an envelope and returned one"
            if descriptor.reached and descriptor.envelope
            else descriptor.detail,
        )
    )

    # R5 — derived spec at /benzene/spec.
    probes.append(
        _surface_verdict(
            "R5",
            spec,
            lambda doc: "service" in doc and "topics" in doc,
            present="/benzene/spec returns a derived spec document",
            malformed="/benzene/spec did not return a {service, topics} document",
        )
    )

    # R6 — only the reserved benzene:mesh descriptor is observable; collector delivery is not, so the
    # most a black-box probe can say is "the observable half holds" → inconclusive by design.
    if descriptor.reached and descriptor.document is not None and "topics" in descriptor.document:
        probes.append(
            RequirementProbe(
                "R6",
                Verdict.INCONCLUSIVE,
                "benzene:mesh returns a descriptor (observable half); register/heartbeat delivery to "
                "a collector is not observable from outside",
            )
        )
    else:
        probes.append(
            RequirementProbe(
                "R6",
                Verdict.NOT_SATISFIED if descriptor.reached else Verdict.INCONCLUSIVE,
                "benzene:mesh did not return a descriptor"
                if descriptor.reached
                else descriptor.detail,
            )
        )

    # R7 — default standard paths: satisfiable only when we probed the default prefix and it answered.
    if not at_default_prefix:
        probes.append(
            RequirementProbe(
                "R7",
                Verdict.INCONCLUSIVE,
                f"probed a non-default prefix {prefix!r}; the service's own defaults are unknown",
            )
        )
    elif surfaces_answered:
        probes.append(
            RequirementProbe(
                "R7",
                Verdict.SATISFIED,
                "the well-known surfaces answer under the default /benzene/ prefix",
            )
        )
    elif reachable:
        probes.append(
            RequirementProbe(
                "R7",
                Verdict.NOT_SATISFIED,
                "the service is reachable but no /benzene/ surface answered at the default paths",
            )
        )
    else:
        probes.append(
            RequirementProbe(
                "R7",
                Verdict.INCONCLUSIVE,
                "no /benzene/ surface was reachable to audit the default paths",
            )
        )

    # R8 — trace propagation is structurally unobservable from a single service.
    probes.append(
        RequirementProbe(
            "R8",
            Verdict.INCONCLUSIVE,
            "propagation needs a second service or a collector to observe forwarded traceparent",
        )
    )

    probes.sort(key=lambda p: p.id)
    return CloudServiceProbeReport(base_url=root, probes=tuple(probes))


@dataclass(frozen=True)
class _Fetched:
    """The outcome of one probe request: whether it reached the surface, and the parsed body."""

    reached: bool
    status: int
    document: dict[str, Any] | None
    envelope: bool
    detail: str


async def _get_json(call: HttpCall, url: str, *, ok_statuses: tuple[int, ...] = (200,)) -> _Fetched:
    try:
        status, text = await call("GET", url, None)
    except Exception as exc:  # a connection failure is inconclusive, not a proof of non-conformance
        return _Fetched(False, 0, None, False, f"{url} was unreachable: {exc}")
    if status == 404:
        return _Fetched(True, 404, None, False, f"{url} returned 404 (surface not exposed)")
    if status not in ok_statuses:
        return _Fetched(True, status, None, False, f"{url} returned status {status}")
    document = _parse_object(text)
    return _Fetched(True, status, document, False, f"{url} returned status {status}")


async def _invoke(call: HttpCall, url: str, topic: str) -> _Fetched:
    """POST a ``{topic}`` envelope to /benzene/invoke and unwrap the response envelope's body."""
    body = json.dumps({"topic": topic, "headers": {}, "body": ""})
    try:
        status, text = await call("POST", url, body)
    except Exception as exc:
        return _Fetched(False, 0, None, False, f"{url} was unreachable: {exc}")
    if status == 404:
        return _Fetched(True, 404, None, False, f"{url} returned 404 (endpoint not exposed)")
    envelope = _parse_object(text)
    if envelope is None or "statusCode" not in envelope:
        return _Fetched(True, status, None, False, f"{url} did not return a response envelope")
    inner = _parse_object(envelope.get("body") if isinstance(envelope.get("body"), str) else "")
    return _Fetched(True, status, inner, True, f"{url} returned a response envelope")


def _parse_object(text: str | None) -> dict[str, Any] | None:
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _surface_verdict(
    requirement_id: str,
    fetched: _Fetched,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    present: str,
    malformed: str,
) -> RequirementProbe:
    """Grade a GET surface: unreachable → inconclusive, 404 → not-satisfied, else predicate on body."""
    if not fetched.reached:
        return RequirementProbe(requirement_id, Verdict.INCONCLUSIVE, fetched.detail)
    if fetched.status == 404:
        return RequirementProbe(requirement_id, Verdict.NOT_SATISFIED, fetched.detail)
    if fetched.document is not None and predicate(fetched.document):
        return RequirementProbe(requirement_id, Verdict.SATISFIED, present)
    return RequirementProbe(requirement_id, Verdict.NOT_SATISFIED, malformed)


def _stdlib_http(*, timeout: float = 10.0) -> HttpCall:
    """A zero-dependency :data:`HttpCall` using ``urllib`` on a worker thread."""

    async def call(method: str, url: str, body: str | None) -> tuple[int, str]:
        def _do() -> tuple[int, str]:
            data = body.encode("utf-8") if body is not None else None
            request = urllib.request.Request(  # noqa: S310 - fixed http(s) base URL
                url, data=data, method=method, headers={"content-type": "application/json"}
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                    return response.status, response.read().decode("utf-8")
            except urllib.error.HTTPError as exc:  # a 503/4xx still carries a body worth reading
                return exc.code, exc.read().decode("utf-8")

        return await asyncio.to_thread(_do)

    return call


def _main(argv: list[str] | None = None) -> int:
    """``python -m benzene.mesh.probe <url> [--prefix /benzene]`` — audit a service, print the report."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="benzene.mesh.probe",
        description="Audit a deployed Benzene service against the Cloud Service Profile (R1–R8).",
    )
    parser.add_argument("url", help="the service's base URL, e.g. https://orders.example.com")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="well-known path prefix")
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args(argv)

    report = asyncio.run(probe_cloud_service(args.url, prefix=args.prefix))
    if args.json:
        print(json.dumps(report.to_payload(), indent=2))
    else:
        print(f"Cloud Service Profile audit of {report.base_url}")
        for probe in report.probes:
            print(f"  {probe.id}: {probe.verdict.value:<14} {probe.reason}")
        print(
            f"\n{'CLEAN' if report.is_clean else 'FAILURES: ' + ', '.join(report.not_satisfied)}"
            f" (inconclusive: {', '.join(report.inconclusive) or 'none'})"
        )
    return 1 if not report.is_clean else 0


if __name__ == "__main__":
    raise SystemExit(_main())
