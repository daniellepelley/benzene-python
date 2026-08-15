"""The mesh collector: the language-neutral collector fixture plus focused derivation unit tests."""

from __future__ import annotations

import asyncio
import json

import pytest
from benzene.core import BenzeneMessageApplication
from benzene.mesh import MeshCollector, collector_registry

from .conformance_runner import CONFORMANCE_DIR, _mesh_subset, run_mesh_collector


def _cases(fixture: str) -> list:
    return json.loads((CONFORMANCE_DIR / fixture).read_text())["cases"]


def test_mesh_collector_conforms() -> None:
    assert run_mesh_collector() == []  # collector + issue fixtures


@pytest.mark.parametrize(
    "case",
    _cases("mesh-collector-cases.json") + _cases("mesh-issue-cases.json"),
    ids=lambda c: c["name"],
)
def test_collector_case(case: dict) -> None:
    app = BenzeneMessageApplication(collector_registry(MeshCollector()))
    for step in case["steps"]:
        response = asyncio.run(app.handle(step["request"]))
        expected = step["expected"]
        assert response["statusCode"] == expected["statusCode"]
        if "body" in expected:
            body = json.loads(response["body"]) if response["body"] else {}
            assert _mesh_subset(expected["body"], body)


# --- focused unit tests of the derivation rules (against MeshCollector directly) ------------------


def _register(
    c: MeshCollector,
    service: str,
    topics: list[str],
    descriptor_hash: str | None = None,
    produces: list[str] | None = None,
) -> None:
    body: dict = {"service": service, "topics": [{"id": t} for t in topics]}
    if produces is not None:
        body["produces"] = [{"id": t} for t in produces]
    if descriptor_hash:
        body["descriptorHash"] = descriptor_hash
    c.ingest_register(body)


def test_reregistration_replaces_consumer_edges() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])
    _register(c, "orders", ["order:cancel"])  # replaces wholesale
    # order:create is still a known topic, but orders no longer handles (consumes) it
    assert c.query_topic({"topic": "order:create"})["consumers"] == []
    assert c.query_topic({"topic": "order:cancel"})["consumers"] == ["orders"]


def test_provider_edges_are_declared_not_trace_derived() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], produces=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providers"] == ["orders"]  # declared, not derived from any trace
    assert topic["consumers"] == ["payments"]
    assert topic["invocations"] == 0

    # Re-registering without `produces` drops the provider edge wholesale, like `topics` does.
    _register(c, "orders", ["order:create"])
    assert c.query_topic({"topic": "payments:capture"})["providers"] == []


def test_trace_parentage_does_not_admit_a_provider_edge() -> None:
    c = MeshCollector()
    _register(c, "greeter", ["greet"])
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t1",
                    "spanId": "s-front",
                    "service": "frontdoor",
                    "topic": "welcome",
                    "status": "ok",
                },
                {
                    "traceId": "t1",
                    "spanId": "s-greet",
                    "parentSpanId": "s-front",
                    "service": "greeter",
                    "topic": "greet",
                    "status": "ok",
                },
            ]
        }
    )
    topic = c.query_topic({"topic": "greet"})
    assert topic["providers"] == []  # trace parentage feeds stats, not graph membership
    assert topic["consumers"] == ["greeter"]
    assert topic["invocations"] == 1


# --- §4.2 declared vs. observed: liveness + drift --------------------------------------------------


def test_a_declared_edge_with_no_trace_is_unobserved_not_removed() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], produces=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providers"] == ["orders"]  # still declared — unobserved is a candidate, not a fact
    assert topic["providerActivity"] == {"orders": {}}  # no lastObservedAt: never traced
    assert topic["consumerActivity"] == {"payments": {}}


def test_a_declared_edge_is_observed_once_a_matching_trace_arrives() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], produces=["payments:capture"])
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t1",
                    "spanId": "s1",
                    "service": "orders",
                    "topic": "order:create",
                    "status": "ok",
                },
                {
                    "traceId": "t1",
                    "spanId": "s2",
                    "parentSpanId": "s1",
                    "service": "payments",
                    "topic": "payments:capture",
                    "status": "ok",
                    "startedAt": "2026-08-15T09:00:00Z",
                },
            ]
        }
    )
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providerActivity"] == {"orders": {"lastObservedAt": "2026-08-15T09:00:00Z"}}
    assert topic["consumerActivity"] == {"payments": {"lastObservedAt": "2026-08-15T09:00:00Z"}}


def test_an_undeclared_consumer_edge_files_a_contract_drift_issue() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])  # does NOT handle (consume) payments:refund
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t1",
                    "spanId": "s1",
                    "service": "payments",
                    "topic": "payments:refund",
                    "status": "ok",
                    "startedAt": "2026-08-15T09:00:00Z",
                }
            ]
        }
    )
    issues = c.query_fleet({})["issues"]
    assert len(issues) == 1
    assert issues[0]["classification"] == "contract-drift"
    assert issues[0]["service"] == "payments"
    assert issues[0]["topic"] == "payments:refund"


def test_an_undeclared_provider_edge_files_a_contract_drift_issue() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"])  # does NOT declare produces=[payments:capture]
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t1",
                    "spanId": "s1",
                    "service": "orders",
                    "topic": "order:create",
                    "status": "ok",
                },
                {
                    "traceId": "t1",
                    "spanId": "s2",
                    "parentSpanId": "s1",
                    "service": "payments",
                    "topic": "payments:capture",
                    "status": "ok",
                },
            ]
        }
    )
    issues = c.query_fleet({})["issues"]
    drift = [i for i in issues if i["classification"] == "contract-drift"]
    assert len(drift) == 1
    assert drift[0]["service"] == "orders"
    assert drift[0]["topic"] == "payments:capture"


def test_an_anonymous_service_is_never_flagged_for_drift() -> None:
    # A service that never registered has no declared contract to diverge from — it's an existing,
    # separate signal (missingFeeds: descriptor), not contract-drift.
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t1",
                    "spanId": "s1",
                    "service": "frontdoor",
                    "topic": "welcome",
                    "status": "ok",
                }
            ]
        }
    )
    assert c.query_fleet({})["issues"] == []


def test_repeated_undeclared_calls_merge_into_one_drift_issue_by_fingerprint() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    for trace_id in ("t1", "t2", "t3"):
        c.ingest_traces(
            {
                "events": [
                    {
                        "traceId": trace_id,
                        "spanId": f"s-{trace_id}",
                        "service": "payments",
                        "topic": "payments:refund",
                        "status": "ok",
                    }
                ]
            }
        )
    issues = c.query_fleet({})["issues"]
    assert len(issues) == 1
    assert issues[0]["count"] == 3  # merged by fingerprint, not one issue per occurrence


def test_heartbeats_drive_health_and_hash_mismatch() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"], descriptor_hash="sha256:aaaa")
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i1",
            "descriptorHash": "sha256:aaaa",
            "health": {"isHealthy": True, "healthChecks": {}},
        }
    )
    assert c.query_service({"service": "orders"})["health"] == "healthy"
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i2",
            "descriptorHash": "sha256:bbbb",
            "health": {"isHealthy": False, "healthChecks": {}},
        }
    )
    service = c.query_service({"service": "orders"})
    assert service["health"] == "degraded"  # mixed instances
    i2 = next(i for i in service["instances"] if i["instanceId"] == "i2")
    assert i2["healthy"] is False and i2["hashMatches"] is False


def test_missing_feeds_and_anonymous_service() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])  # descriptor only
    fleet = c.query_fleet({})
    orders = next(s for s in fleet["services"] if s["service"] == "orders")
    assert orders["health"] == "unknown"
    assert orders["missingFeeds"] == ["health", "traces"]
    # a trace-only service is anonymous (no descriptor, no health)
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t",
                    "spanId": "s",
                    "service": "frontdoor",
                    "topic": "welcome",
                    "status": "ok",
                }
            ]
        }
    )
    front = c.query_service({"service": "frontdoor"})
    assert front["invocations"] == 1
    assert front["missingFeeds"] == ["descriptor", "health"]


def test_issues_merge_by_fingerprint_with_delta_counts() -> None:
    c = MeshCollector()
    fp = "aaaa1111aaaa1111aaaa1111aaaa1111"
    c.ingest_issues(
        {
            "service": "orders",
            "issues": [
                {
                    "fingerprint": fp,
                    "topic": "order:create",
                    "status": "service-unavailable",
                    "count": 2,
                    "exemplarTraceIds": ["trace-1"],
                }
            ],
        }
    )
    c.ingest_issues(
        {
            "service": "orders",
            "issues": [
                {
                    "fingerprint": fp,
                    "topic": "order:create",
                    "status": "service-unavailable",
                    "count": 3,
                    "exemplarTraceIds": ["trace-2"],
                }
            ],
        }
    )
    issue = next(i for i in c.query_fleet({})["issues"] if i["fingerprint"] == fp)
    assert issue["count"] == 5  # deltas merge: 2 + 3
    assert issue["exemplarTraceIds"] == ["trace-1", "trace-2"]


def test_issue_merge_keeps_newest_three_exemplars_and_latest_fields() -> None:
    c = MeshCollector()
    fp = "cccc3333cccc3333cccc3333cccc3333"
    for i in range(5):  # five batches, each a new exemplar
        c.ingest_issues(
            {
                "service": "orders",
                "issues": [
                    {
                        "fingerprint": fp,
                        "topic": "t",
                        "status": "service-unavailable",
                        "count": 1,
                        "transport": f"transport-{i}",
                        "exemplarTraceIds": [f"trace-{i}"],
                    }
                ],
            }
        )
    issue = next(i for i in c.query_fleet({})["issues"] if i["fingerprint"] == fp)
    assert issue["count"] == 5
    assert issue["exemplarTraceIds"] == [
        "trace-2",
        "trace-3",
        "trace-4",
    ]  # newest ≤3 (mesh.md §4.1)
    assert issue["transport"] == "transport-4"  # other fields latest-wins


def test_invalid_issue_entries_are_skipped_not_rejected() -> None:
    c = MeshCollector()
    accepted = c.ingest_issues(
        {
            "service": "orders",
            "issues": [
                {
                    "fingerprint": "",
                    "topic": "order:create",
                    "status": "bad-request",
                    "count": 1,
                },  # skipped
                {
                    "fingerprint": "bbbb",
                    "topic": "order:create",
                    "status": "bad-request",
                    "count": 1,
                },
            ],
        }
    )
    assert accepted == {"accepted": 1}  # one valid, one skipped, batch accepted


def test_null_aggregated_fields_do_not_crash_the_batch() -> None:
    # A null count/firstSeen/lastSeen (e.g. from a hand-rolled cross-language POST) must be tolerated,
    # not crash the merge and drop valid entries in the same batch (mesh.md §4 skip-don't-reject).
    c = MeshCollector()
    # New-issue path: a null count coerces to 0, and a sibling valid entry still lands.
    c.ingest_issues(
        {
            "service": "o",
            "issues": [{"fingerprint": "g", "count": 1}, {"fingerprint": "f", "count": None}],
        }
    )
    issues = {i["fingerprint"]: i for i in c.query_fleet({})["issues"]}
    assert issues["g"]["count"] == 1
    assert issues["f"]["count"] == 0
    # Merge path: a null firstSeen/lastSeen on a second beat must not raise.
    c.ingest_issues(
        {
            "service": "o",
            "issues": [
                {
                    "fingerprint": "f",
                    "count": 2,
                    "firstSeen": "2026-01-02",
                    "lastSeen": "2026-01-02",
                }
            ],
        }
    )
    c.ingest_issues(
        {
            "service": "o",
            "issues": [{"fingerprint": "f", "count": None, "firstSeen": None, "lastSeen": None}],
        }
    )
    merged = {i["fingerprint"]: i for i in c.query_fleet({})["issues"]}["f"]
    assert merged["count"] == 2  # 0 + 2 + 0
    assert merged["firstSeen"] == "2026-01-02"  # a null incoming span is ignored, not compared


def test_issues_feed_absence_flagged_only_when_a_failure_needs_explaining() -> None:
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                {
                    "traceId": "t",
                    "spanId": "s",
                    "service": "orders",
                    "topic": "order:create",
                    "status": "service-unavailable",
                }
            ]
        }
    )
    orders = next(s for s in c.query_fleet({})["services"] if s["service"] == "orders")
    assert orders["missingFeeds"] == ["descriptor", "health", "issues"]  # unexplained failure
    c.ingest_issues({"service": "orders", "issues": []})  # a liveness beat clears the flag
    orders = next(s for s in c.query_fleet({})["services"] if s["service"] == "orders")
    assert orders["missingFeeds"] == ["descriptor", "health"]
