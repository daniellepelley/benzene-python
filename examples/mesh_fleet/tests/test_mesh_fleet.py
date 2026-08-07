"""The mesh fleet end to end: three services, cross-service traces, a derived topology.

Drives one order through orders → inventory → notifications and asserts the shared collector shows the
whole fleet and the **consumer edges** it derived purely from trace parentage — the core of what a
deployed mesh renders.
"""

from __future__ import annotations

import asyncio

from mesh_fleet import build_fleet


def test_the_collector_sees_the_whole_fleet() -> None:
    fleet = build_fleet()
    result = asyncio.run(fleet.place_order("ABC", 2))
    assert result.status == "created"

    services = {s["service"] for s in fleet.collector.query_fleet({})["services"]}
    assert services == {"orders", "inventory", "notifications"}


def test_consumer_edges_are_derived_from_cross_service_traces() -> None:
    fleet = build_fleet()
    asyncio.run(fleet.place_order("ABC"))

    # orders called inventory:reserve; inventory called notify:send — the collector derives each edge
    # from the span parentage that propagated across the hops.
    reserve = fleet.collector.query_topic({"topic": "inventory:reserve"})
    assert reserve["providers"] == ["inventory"]
    assert reserve["consumers"] == ["orders"]

    notify = fleet.collector.query_topic({"topic": "notify:send"})
    assert notify["providers"] == ["notifications"]
    assert notify["consumers"] == ["inventory"]


def test_all_hops_share_one_trace() -> None:
    # The consumer edges above can only exist if the three hops shared one trace (propagation joined
    # them). Assert that directly: capture the trace id from orders' span, then read it back.
    fleet = build_fleet()
    asyncio.run(
        fleet.orders.handle({"topic": "orders:place", "headers": {}, "body": '{"sku": "ABC"}'})
    )
    trace_id = next(e for e in fleet.exporters["orders"].drain() if e.topic == "orders:place").trace_id
    for name in ("inventory", "notifications"):
        fleet.collector.ingest_traces({"events": [e.to_payload() for e in fleet.exporters[name].drain()]})

    # orders' own span wasn't reported above (drained for the id), but inventory+notifications carry
    # the same trace id — proof the trace propagated across every hop.
    trace = fleet.collector.query_trace({"traceId": trace_id})
    assert {e["service"] for e in trace["events"]} == {"inventory", "notifications"}
