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
    requests: list[str],
    registration_hash: str | None = None,
    sends: list[str] | None = None,
    events: list[str] | None = None,
) -> None:
    """Register a Contract Document (mesh.md §2) that asserts all three members."""
    body: dict = {
        "service": service,
        "requests": [{"topic": t, "request": {}} for t in requests],
        "events": [{"topic": t, "message": {}} for t in events or []],
        "sends": [{"topic": t, "request": {}} for t in sends or []],
    }
    if registration_hash:
        body["registrationHash"] = registration_hash
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
    _register(c, "orders", ["order:create"], sends=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providers"] == ["orders"]  # declared, not derived from any trace
    assert topic["consumers"] == ["payments"]
    assert topic["invocations"] == 0

    # Re-registering without the send drops the provider edge wholesale, as `requests` does.
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
    _register(c, "orders", ["order:create"], sends=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providers"] == ["orders"]  # still declared — unobserved is a candidate, not a fact
    assert topic["providerActivity"] == {"orders": {}}  # no lastObservedAt: never traced
    assert topic["consumerActivity"] == {"payments": {}}


def test_a_declared_edge_is_observed_once_a_matching_trace_arrives() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], sends=["payments:capture"])
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
    _register(c, "orders", ["order:create"])  # does NOT declare sends=[payments:capture]
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
    # separate signal (missingFeeds: descriptor, the read models' name for the registration), not
    # contract-drift.
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
    _register(c, "orders", ["order:create"], registration_hash="sha256:aaaa")
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i1",
            "registrationHash": "sha256:aaaa",
            "health": {"isHealthy": True, "healthChecks": {}},
        }
    )
    assert c.query_service({"service": "orders"})["health"] == "healthy"
    c.ingest_heartbeat(
        {
            "service": "orders",
            "instanceId": "i2",
            "registrationHash": "sha256:bbbb",
            "health": {"isHealthy": False, "healthChecks": {}},
        }
    )
    service = c.query_service({"service": "orders"})
    assert service["health"] == "degraded"  # mixed instances
    i2 = next(i for i in service["instances"] if i["instanceId"] == "i2")
    assert i2["healthy"] is False and i2["hashMatches"] is False


def test_missing_feeds_and_anonymous_service() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])  # registration only
    fleet = c.query_fleet({})
    orders = next(s for s in fleet["services"] if s["service"] == "orders")
    assert orders["health"] == "unknown"
    assert orders["missingFeeds"] == ["health", "traces"]
    # a trace-only service is anonymous (no registration, no health)
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


def _issue_with_errors(errors: object, count: int = 1) -> dict:
    return {
        "service": "orders",
        "issues": [
            {
                "fingerprint": "e",
                "classification": "validation",
                "service": "orders",
                "topic": "order:create",
                "status": "validation-error",
                "count": count,
                "errors": errors,
            }
        ],
    }


def test_invalid_error_detail_entries_are_skipped_and_the_issue_kept() -> None:
    # mesh.md §4.1: an entry that is not an object, has neither member, or whose count is not a
    # positive integer is skipped - the issue and the batch are still accepted. In Python a JSON
    # `true` decodes to an int, so it is rejected explicitly; so is a member that is not a string.
    c = MeshCollector()
    accepted = c.ingest_issues(
        _issue_with_errors(
            [
                "required",
                ["/a", "required", 1],
                {"count": 1},
                {"field": None, "code": None, "count": 1},
                {"field": "/a", "count": 0},
                {"field": "/a", "count": -2},
                {"field": "/a", "count": 1.5},
                {"field": "/a", "count": 2.0},
                {"field": "/a", "count": True},
                {"field": "/a", "count": "3"},
                {"field": "/a", "code": "required"},
                {"field": 7, "count": 1},
                {"field": "/a", "code": ["required"], "count": 1},
                {"field": "", "code": "", "count": 5},  # both empty: neither member
                {"field": "/b", "code": "required", "count": 2, "message": "kept out"},
                {"field": None, "code": "TotalsDisagree", "count": 1},
                {"field": "", "code": "TotalsDisagree", "count": 1},  # "" is absent: same identity
            ]
        )
    )
    assert accepted == {"accepted": 1}
    issue = c.query_fleet({})["issues"][0]
    assert issue["count"] == 1
    assert issue["errors"] == [
        {"code": "TotalsDisagree", "count": 2},  # absent field sorts first among equal counts
        {"field": "/b", "code": "required", "count": 2},  # the message is never kept
    ]


def test_an_errors_member_that_is_not_an_array_is_no_detail() -> None:
    c = MeshCollector()
    assert c.ingest_issues(_issue_with_errors("required")) == {"accepted": 1}
    assert c.ingest_issues(_issue_with_errors(None)) == {"accepted": 1}
    issue = c.query_fleet({})["issues"][0]
    assert issue["count"] == 2
    assert "errors" not in issue  # reported only when the issue holds any


def test_error_detail_merges_by_identity_and_keeps_the_bound() -> None:
    c = MeshCollector()
    c.ingest_issues(_issue_with_errors([{"field": f"/f{i:02d}", "count": 1} for i in range(10)]))
    c.ingest_issues(
        _issue_with_errors(
            [{"field": "/f09", "count": 2}, {"field": "/new", "count": 1}, {"code": "c", "count": 1}]
        )
    )
    # A later batch without detail keeps what the issue already holds.
    c.ingest_issues(_issue_with_errors([]))
    errors = c.query_fleet({})["issues"][0]["errors"]
    assert len(errors) == 10
    assert errors[0] == {"field": "/f09", "count": 3}
    # Ties order absent field first, then by field: `{code: c}` leads the count-1 entries, and the
    # bound drops the tail (/f08, /new) rather than the earliest arrivals.
    assert errors[1] == {"code": "c", "count": 1}
    assert [e["field"] for e in errors[2:]] == [f"/f{i:02d}" for i in range(8)]


def test_error_detail_survives_a_snapshot_round_trip() -> None:
    source = MeshCollector()
    source.ingest_issues(_issue_with_errors([{"field": "/a", "code": "required", "count": 2}]))
    restored = MeshCollector()
    restored.restore(source.snapshot())
    restored.ingest_issues(_issue_with_errors([{"field": "/a", "code": "required", "count": 1}]))
    assert restored.query_fleet({})["issues"][0]["errors"] == [
        {"field": "/a", "code": "required", "count": 3}
    ]


def _span(
    trace: str,
    span: str,
    status: str,
    started_at: str | None,
    service: str = "svc",
    duration_ms: object = None,
) -> dict:
    event: dict = {"traceId": trace, "spanId": span, "service": service, "topic": "t", "status": status}
    if started_at is not None:
        event["startedAt"] = started_at
    if duration_ms is not None:
        event["durationMs"] = duration_ms
    return event


def test_a_failed_flow_names_its_earliest_failure_by_time_not_text() -> None:
    # `...09:00:00.005Z` sorts before `...09:00:00Z` as text; as instants it is later. The failing
    # events arrive latest-first, so receipt order would also pick the wrong one.
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("t1", "s3", "service-unavailable", "2026-07-16T09:00:00.010Z"),
                _span("t1", "s2", "validation-error", "2026-07-16T09:00:00.005Z"),
                _span("t1", "s1", "ok", "2026-07-16T09:00:00Z"),
            ]
        }
    )
    (flow,) = c.query_fleet({})["traces"]
    assert flow["failed"] is True
    assert flow["status"] == "validation-error"
    assert flow["events"] == 3
    assert flow["startedAt"] == "2026-07-16T09:00:00Z"


def test_a_successful_flow_carries_its_earliest_events_status() -> None:
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("t1", "s2", "created", "2026-07-16T09:00:00.0050000Z"),  # seven digits (.NET)
                _span("t1", "s1", "ok", "2026-07-16T09:00:00+00:00"),
            ]
        }
    )
    (flow,) = c.query_fleet({})["traces"]
    assert flow["failed"] is False
    assert flow["status"] == "ok"


def test_equal_start_times_keep_receipt_order_and_an_untimed_event_sorts_last() -> None:
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("tie", "a", "timeout", "2026-07-16T09:00:00Z"),
                _span("tie", "b", "not-found", "2026-07-16T09:00:00Z"),
                _span("late", "c", "conflict", None),
                _span("late", "d", "accepted", "2026-07-16T08:00:00Z"),
            ]
        }
    )
    flows = {flow["traceId"]: flow for flow in c.query_fleet({})["traces"]}
    assert flows["tie"]["status"] == "timeout"  # the first received of two equal start times
    # The untimed failing event cannot claim to be the earliest; it is still the only failure.
    assert flows["late"]["status"] == "conflict"
    assert flows["late"]["failed"] is True


def test_fleet_traces_are_newest_first_and_omit_an_unknown_status() -> None:
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("old", "a", "ok", "2026-07-16T08:00:00Z"),
                _span("new", "b", "ok", "2026-07-16T09:00:00Z"),
                {"traceId": "blank", "spanId": "c", "service": "svc", "topic": "t"},
            ]
        }
    )
    flows = c.query_fleet({})["traces"]
    assert [flow["traceId"] for flow in flows] == ["new", "old", "blank"]
    blank = flows[2]
    assert "status" not in blank
    assert blank["failed"] is True  # an empty status is not a success (wire-contracts §3)


def test_a_flow_lasts_from_its_earliest_start_to_its_latest_end() -> None:
    # meshd's rule: (latest event end) - (earliest start). The root's 30ms outlasts its children.
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("t1", "s2", "ok", "2026-07-16T09:00:00.005Z", duration_ms=10),
                _span("t1", "s3", "ok", "2026-07-16T09:00:00.020Z", duration_ms=25.5),
                _span("t1", "s1", "ok", "2026-07-16T09:00:00Z", duration_ms=30),
            ]
        }
    )
    (flow,) = c.query_fleet({})["traces"]
    assert flow["startedAt"] == "2026-07-16T09:00:00Z"
    assert flow["durationMs"] == 45.5  # .020 + 25.5ms, measured from .000


def test_every_flow_summary_carries_started_at_and_duration_ms() -> None:
    # The mesh UI formats both unconditionally. An event with no readable start cannot be placed on
    # the timeline, so it moves neither end; its own duration is still a lower bound on the flow's.
    # A flow with no readable start at all says so with an empty startedAt, never a made-up time.
    c = MeshCollector()
    c.ingest_traces(
        {
            "events": [
                _span("mixed", "a", "ok", "2026-07-16T09:00:00Z", duration_ms=5),
                _span("mixed", "b", "ok", "not a time", duration_ms=40),
                _span("untimed", "c", "ok", None, duration_ms=12),
                _span("untimed", "d", "ok", None, duration_ms=True),  # not a duration: 0
                _span("bare", "e", "ok", None),
                _span("huge", "f", "ok", "9999-12-31T23:59:59Z", duration_ms=1e300),
            ]
        }
    )
    flows = {flow["traceId"]: flow for flow in c.query_fleet({})["traces"]}
    for flow in flows.values():
        assert isinstance(flow["startedAt"], str)
        assert isinstance(flow["durationMs"], float)
    assert flows["mixed"]["startedAt"] == "2026-07-16T09:00:00Z"
    assert flows["mixed"]["durationMs"] == 40.0
    assert flows["untimed"] == {**flows["untimed"], "startedAt": "", "durationMs": 12.0}
    assert flows["bare"]["durationMs"] == 0.0
    assert flows["huge"]["durationMs"] == 1e300  # past datetime's range: its own length still counts


def test_event_durations_survive_a_snapshot_round_trip() -> None:
    source = MeshCollector()
    source.ingest_traces({"events": [_span("t1", "s1", "ok", "2026-07-16T09:00:00Z", duration_ms=7)]})
    restored = MeshCollector()
    restored.restore(source.snapshot())
    assert restored.query_fleet({})["traces"][0]["durationMs"] == 7.0


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


# --- the register body is the Contract Document (mesh.md §2, §4) ---------------------------------


def _trace(c: MeshCollector, service: str, topic: str, span: str, parent: str | None = None) -> None:
    event = {"traceId": "t", "spanId": span, "service": service, "topic": topic, "status": "ok"}
    if parent:
        event["parentSpanId"] = parent
    c.ingest_traces({"events": [event]})


def test_a_published_event_makes_its_publisher_a_provider() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"], events=["order:placed"])
    _register(c, "shipping", ["order:placed"])
    topic = c.query_topic({"topic": "order:placed"})
    assert topic["providers"] == ["orders"]
    assert topic["consumers"] == ["shipping"]


def test_benzene_entries_are_ignored_on_every_member() -> None:
    c = MeshCollector()
    _register(
        c,
        "orders",
        ["order:create", "benzene:spec"],
        events=["benzene:mesh:heartbeat"],
        sends=["benzene:mesh:traces"],
    )
    assert [t["topic"] for t in c.query_fleet({})["topics"]] == ["order:create"]
    assert c.query_fleet({})["services"][0]["topics"] == 1


def test_a_body_without_a_dialect_or_hashes_is_accepted() -> None:
    # A collector requires only `service` (mesh.md §2): no schemaDialect check, no hash check.
    c = MeshCollector()
    assert c.ingest_register({"service": "orders", "requests": [{"topic": "order:create"}]}) == {
        "accepted": 1
    }
    assert c.query_topic({"topic": "order:create"})["consumers"] == ["orders"]


def test_a_carried_registration_hash_is_trusted_not_recomputed() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"], registration_hash="sha256:aaaa")  # a fake value
    c.ingest_heartbeat({"service": "orders", "instanceId": "i1", "registrationHash": "sha256:aaaa"})
    assert c.query_service({"service": "orders"})["instances"][0]["hashMatches"] is True


def test_a_service_only_registration_asserts_nothing_and_flags_no_drift() -> None:
    c = MeshCollector()
    c.ingest_register({"service": "orders"})
    _trace(c, "orders", "order:create", "s1")  # orders handles a topic its body never listed
    assert c.query_fleet({})["issues"] == []  # an unknown member cannot be diverged from
    assert c.query_topic({"topic": "order:create"})["consumers"] == []  # and adds no edge


def test_a_degraded_member_is_unknown() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    c.ingest_register(
        {
            "service": "orders",
            "requests": [{"topic": "order:create", "request": {}}],
            "events": [],
            "sends": [],
            "degraded": ["sends"],
        }
    )
    _trace(c, "orders", "order:create", "s-orders")
    _trace(c, "payments", "payments:capture", "s-payments", parent="s-orders")
    # orders could not determine its sends, so its call to payments:capture is not undeclared...
    assert c.query_fleet({})["issues"] == []
    # ...but its requests are known, so an unlisted handled topic still is.
    _trace(c, "orders", "order:cancel", "s-cancel")
    drift = c.query_fleet({})["issues"]
    assert [(i["service"], i["topic"]) for i in drift] == [("orders", "order:cancel")]


def test_request_schemas_are_kept_dereferenced_and_the_document_retained() -> None:
    c = MeshCollector()
    body = {
        "schemaDialect": "openapi-3.0",
        "service": "orders",
        "requests": [
            {
                "topic": "order:create",
                "version": "2",
                "request": {"$ref": "#/components/schemas/CreateOrder"},
                "response": {},
            }
        ],
        "events": [],
        "sends": [],
        "components": {
            "schemas": {"CreateOrder": {"type": "object", "properties": {"sku": {"type": "string"}}}}
        },
    }
    c.ingest_register(body)
    entry = c.snapshot()["services"][0]
    assert entry["topicSpecs"]["order:create"] == {
        "version": "2",
        "requestSchema": {"type": "object", "properties": {"sku": {"type": "string"}}},
    }
    assert entry["document"] == body


def test_a_traced_reserved_topic_is_never_undeclared_drift() -> None:
    # benzene: topics are never listed in a document, so a trace of one (a GET /benzene/spec, a
    # health probe) is not a contract the registration failed to declare.
    c = MeshCollector()
    _register(c, "orders", ["order:create"])
    _trace(c, "orders", "benzene:spec", "s1")
    _trace(c, "orders", "benzene:healthcheck", "s2")
    assert c.query_fleet({})["issues"] == []
