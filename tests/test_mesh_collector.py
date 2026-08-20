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
    consumes: list[str] | None = None,
) -> None:
    body: dict = {"service": service, "topics": [{"id": t} for t in topics]}
    if consumes is not None:
        body["consumes"] = [{"id": t} for t in consumes]
    if descriptor_hash:
        body["descriptorHash"] = descriptor_hash
    c.ingest_register(body)


def test_reregistration_replaces_provider_edges() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])
    _register(c, "orders", ["order:cancel"])  # replaces wholesale
    # order:create is still a known topic, but orders no longer provides it
    assert c.query_topic({"topic": "order:create"})["providers"] == []
    assert c.query_topic({"topic": "order:cancel"})["providers"] == ["orders"]


def test_consumer_edges_are_declared_not_trace_derived() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], consumes=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["providers"] == ["payments"]
    assert topic["consumers"] == ["orders"]  # declared, not derived from any trace
    assert topic["invocations"] == 0

    # Re-registering without `consumes` drops the consumer edge wholesale, like `topics` does.
    _register(c, "orders", ["order:create"])
    assert c.query_topic({"topic": "payments:capture"})["consumers"] == []


def test_trace_parentage_does_not_admit_a_consumer_edge() -> None:
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
    assert topic["providers"] == ["greeter"]
    assert topic["consumers"] == []  # trace parentage feeds stats, not graph membership
    assert topic["invocations"] == 1


# --- §4.2 declared vs. observed: liveness + drift --------------------------------------------------


def test_a_declared_edge_with_no_trace_is_unobserved_not_removed() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], consumes=["payments:capture"])
    topic = c.query_topic({"topic": "payments:capture"})
    assert topic["consumers"] == ["orders"]  # still declared — unobserved is a candidate, not a fact
    assert topic["consumerActivity"] == {"orders": {}}  # no lastObservedAt: never traced
    assert topic["providerActivity"] == {"payments": {}}


def test_a_declared_edge_is_observed_once_a_matching_trace_arrives() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], consumes=["payments:capture"])
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
    assert topic["consumerActivity"] == {"orders": {"lastObservedAt": "2026-08-15T09:00:00Z"}}
    assert topic["providerActivity"] == {"payments": {"lastObservedAt": "2026-08-15T09:00:00Z"}}


def test_an_undeclared_provider_edge_files_a_contract_drift_issue() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])  # does NOT provide payments:refund
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


def test_an_undeclared_consumer_edge_files_a_contract_drift_issue() -> None:
    c = MeshCollector()
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"])  # does NOT declare consumes=[payments:capture]
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


# --- retention + off-loop persistence (the event log is bounded; the store save is threaded) ------


def _trace_event(n: int, service: str = "orders", topic: str = "order:create") -> dict:
    return {
        "traceId": f"t{n}",
        "spanId": f"s{n}",
        "service": service,
        "topic": topic,
        "status": "ok",
        "startedAt": f"2026-08-15T09:00:0{n}Z",
    }


def test_the_event_log_is_capped_at_max_events() -> None:
    c = MeshCollector(max_events=3)
    _register(c, "orders", ["order:create"])
    for n in range(5):
        c.ingest_traces({"events": [_trace_event(n)]})

    # Only the newest `max_events` are retained: memory, snapshot size, and every full-scan query
    # stop growing with uptime instead of climbing forever.
    snapshot = c.snapshot()
    assert [event["spanId"] for event in snapshot["events"]] == ["s2", "s3", "s4"]
    # Within the retained window every query still answers exactly as before.
    topic = c.query_topic({"topic": "order:create"})
    assert topic["invocations"] == 3
    assert topic["providers"] == ["orders"]
    assert topic["providerActivity"] == {"orders": {"lastObservedAt": "2026-08-15T09:00:04Z"}}
    assert c.query_trace({"traceId": "t4"})["events"] == [{"spanId": "s4", "service": "orders"}]


def test_the_event_log_defaults_to_a_generous_cap() -> None:
    # The default is far above any conformance fixture or ordinary burst — it bounds a long-lived
    # collector without changing what a test or a fixture sees.
    c = MeshCollector()
    assert c.snapshot()["events"] == []
    assert MeshCollector(max_events=10)._events.maxlen == 10
    assert MeshCollector()._events.maxlen == 10_000


def test_a_restored_collector_rebuilds_span_ownership_from_the_retained_window() -> None:
    c = MeshCollector(max_events=4)
    _register(c, "payments", ["payments:capture"])
    _register(c, "orders", ["order:create"], consumes=["payments:capture"])
    c.ingest_traces(
        {
            "events": [
                _trace_event(1, "orders", "order:create"),
                {
                    "traceId": "t1",
                    "spanId": "s2",
                    "parentSpanId": "s1",
                    "service": "payments",
                    "topic": "payments:capture",
                    "status": "ok",
                    "startedAt": "2026-08-15T09:00:02Z",
                },
            ]
        }
    )

    restored = MeshCollector(max_events=4)
    restored.restore(c.snapshot())

    # The span-owner index is not persisted — it is rebuilt from the retained events, so the §4.2
    # observed-consumer signal survives a restart exactly as it did before the cap existed.
    assert restored.query_topic({"topic": "payments:capture"}) == c.query_topic(
        {"topic": "payments:capture"}
    )
    assert restored.query_topic({"topic": "payments:capture"})["consumerActivity"] == {
        "orders": {"lastObservedAt": "2026-08-15T09:00:02Z"}
    }


def test_restore_honours_the_cap_and_keeps_the_newest_events() -> None:
    source = MeshCollector()
    _register(source, "orders", ["order:create"])
    for n in range(5):
        source.ingest_traces({"events": [_trace_event(n)]})

    capped = MeshCollector(max_events=2)
    capped.restore(source.snapshot())  # a snapshot written by a bigger (or uncapped) collector

    assert [event["spanId"] for event in capped.snapshot()["events"]] == ["s3", "s4"]


class _SpyToThread:
    """A drop-in for ``asyncio.to_thread`` that records each dispatched callable, then really runs it."""

    def __init__(self, real) -> None:
        self._real = real  # the genuine asyncio.to_thread, captured before patching
        self.dispatched: list = []

    async def __call__(self, func, /, *args, **kwargs):
        self.dispatched.append(func)
        return await self._real(func, *args, **kwargs)


class _RecordingStore:
    """A duck-typed :class:`CollectorStore` that records the snapshots it was asked to save."""

    def __init__(self) -> None:
        self.saved: list[dict] = []

    def load(self) -> dict | None:
        return None

    def save(self, snapshot: dict) -> None:
        self.saved.append(snapshot)


def test_the_ingest_handlers_persist_off_the_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _RecordingStore()
    collector = MeshCollector(store=store)
    app = BenzeneMessageApplication(collector_registry(collector))
    spy = _SpyToThread(asyncio.to_thread)
    monkeypatch.setattr(asyncio, "to_thread", spy)

    response = asyncio.run(
        app.handle(
            {
                "topic": "benzene:mesh:register",
                "headers": {},
                "body": json.dumps({"service": "orders", "topics": [{"id": "order:create"}]}),
            }
        )
    )

    assert response["statusCode"] == "ok"
    # The store's file/S3 I/O is blocking: re-serializing the catalog on the event loop stalls every
    # other invocation the collector is serving, so the save must be dispatched to a worker thread.
    assert store.saved and store.saved[-1]["services"][0]["name"] == "orders"
    assert store.save in spy.dispatched


def test_a_direct_sync_ingest_still_persists_immediately() -> None:
    # The collector is also driven synchronously (a poller sweep, a batch aggregation pass), where
    # there is no loop to protect and no await to hang the save on — durability stays immediate.
    store = _RecordingStore()
    c = MeshCollector(store=store)
    c.ingest_register({"service": "orders", "topics": [{"id": "order:create"}]})
    assert len(store.saved) == 1


def _register_message(service: str) -> dict:
    return {
        "topic": "benzene:mesh:register",
        "headers": {},
        "body": json.dumps({"service": service, "topics": [{"id": f"{service}:do"}]}),
    }


def test_a_collector_is_reusable_across_event_loops() -> None:
    store = _RecordingStore()
    collector = MeshCollector(store=store)
    app = BenzeneMessageApplication(collector_registry(collector))

    async def two_concurrent_ingests() -> None:
        await asyncio.gather(
            app.handle(_register_message("orders")), app.handle(_register_message("payments"))
        )

    # A collector outlives a single loop — a warm Lambda container reuses the instance across
    # invocations, each with its own `asyncio.run`. Nothing in the save path may bind to the first.
    asyncio.run(two_concurrent_ingests())
    asyncio.run(two_concurrent_ingests())

    assert len(store.saved) == 4
    assert {s["name"] for s in store.saved[-1]["services"]} == {"orders", "payments"}
