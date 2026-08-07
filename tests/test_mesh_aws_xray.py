"""Unit tests for the AWS **X-Ray topology source** — its service-graph → edge mapping, with a fake client.

Drives :class:`~benzene.mesh.aws.XRayTopologySource` off a hand-written fake
:class:`~benzene.mesh.aws.XRayServiceGraphClient` (no ``boto3``, no ``moto``), asserting the mapping of
X-Ray's ``GetServiceGraph`` statistics to the pinned ``topology.json`` edge shape: request rate from
``TotalCount`` ÷ window, error rate from error+fault counts, latency percentiles from the response-time
histogram (seconds → ms), and the ``source: "xray"`` tag.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from benzene.mesh.aws import XRayTopologySource
from benzene.mesh.aws.xray import _percentile_ms

_AT = datetime(2026, 7, 16, 9, 15, 0, tzinfo=timezone.utc)


class _FakeXRay:
    """A fake X-Ray client returning canned service-graph pages, optionally across a ``NextToken`` split."""

    def __init__(self, *pages: dict) -> None:
        self._pages = list(pages)
        self.calls: list[str | None] = []

    def get_service_graph(self, *, start_time, end_time, next_token=None):
        self.calls.append(next_token)
        index = 0 if next_token is None else int(next_token)
        page = self._pages[index]
        result = dict(page.get("body", page))
        if index + 1 < len(self._pages):
            result["NextToken"] = str(index + 1)
        return result


def _graph() -> dict:
    """orders-api → payments-api (5184 calls/hr, 933 failures) and orders-api → shipping-api (1446 calls)."""
    return {
        "Services": [
            {
                "ReferenceId": 0,
                "Name": "orders-api",
                "Edges": [
                    {
                        "ReferenceId": 1,
                        "SummaryStatistics": {
                            "TotalCount": 5184,
                            "OkCount": 4251,
                            "ErrorStatistics": {"TotalCount": 900},
                            "FaultStatistics": {"TotalCount": 33},
                        },
                        "ResponseTimeHistogram": [
                            {"Value": 0.045, "Count": 500},
                            {"Value": 0.42, "Count": 40},
                            {"Value": 0.89, "Count": 10},
                        ],
                    },
                    {
                        "ReferenceId": 2,
                        "SummaryStatistics": {
                            "TotalCount": 1446,
                            "ErrorStatistics": {"TotalCount": 0},
                            "FaultStatistics": {"TotalCount": 0},
                        },
                        "ResponseTimeHistogram": [{"Value": 0.012, "Count": 1446}],
                    },
                ],
            },
            {"ReferenceId": 1, "Name": "payments-api", "Edges": []},
            {"ReferenceId": 2, "Name": "shipping-api", "Edges": []},
        ]
    }


def _source(*pages: dict, window=timedelta(hours=1)) -> XRayTopologySource:
    return XRayTopologySource(_FakeXRay(*pages), window=window, clock=lambda: _AT)


def test_maps_service_graph_edges_to_topology_edges() -> None:
    edges = _source(_graph()).topology()
    assert [(e.client, e.server) for e in edges] == [
        ("orders-api", "payments-api"),
        ("orders-api", "shipping-api"),
    ]
    for e in edges:
        assert e.source == "xray"


def test_request_rate_is_total_count_over_window_minutes() -> None:
    payments = next(
        e for e in _source(_graph()).topology() if e.server == "payments-api"
    )
    # 5184 calls over a 60-minute window → 86.4 req/min.
    assert payments.requests_per_minute == 86.4


def test_error_rate_sums_error_and_fault_counts() -> None:
    payments = next(
        e for e in _source(_graph()).topology() if e.server == "payments-api"
    )
    # (900 errors + 33 faults) / 5184 total.
    assert payments.error_rate == (900 + 33) / 5184


def test_latency_percentiles_read_the_histogram_in_ms() -> None:
    payments = next(
        e for e in _source(_graph()).topology() if e.server == "payments-api"
    )
    # Histogram values are seconds; the edge carries milliseconds.
    assert payments.p50_latency_ms == 45.0
    assert payments.p95_latency_ms == 420.0
    assert payments.p99_latency_ms == 890.0


def test_to_payload_matches_the_pinned_edge_shape() -> None:
    shipping = next(
        e for e in _source(_graph()).topology() if e.server == "shipping-api"
    ).to_payload()
    assert set(shipping) == {
        "client", "server", "source", "requestsPerMinute", "errorRate",
        "p50LatencyMs", "p95LatencyMs", "p99LatencyMs",
    }
    assert shipping["source"] == "xray"
    assert shipping["errorRate"] == 0.0  # zero failures → an honest 0.0, not null
    assert shipping["requestsPerMinute"] == 1446 / 60


def test_self_edges_and_unknown_targets_are_dropped() -> None:
    graph = {
        "Services": [
            {
                "ReferenceId": 0,
                "Name": "a",
                "Edges": [
                    {"ReferenceId": 0, "SummaryStatistics": {"TotalCount": 5}},  # self-edge
                    {"ReferenceId": 9, "SummaryStatistics": {"TotalCount": 5}},  # unknown ref
                ],
            }
        ]
    }
    assert _source(graph).topology() == []


def test_pages_are_followed_via_next_token() -> None:
    page1 = {"Services": [{"ReferenceId": 0, "Name": "a", "Edges": [
        {"ReferenceId": 1, "SummaryStatistics": {"TotalCount": 60}}]}]}
    page2 = {"Services": [{"ReferenceId": 1, "Name": "b", "Edges": []}]}
    source = _source(page1, page2)
    edges = source.topology()
    # The b→ name only resolves once page 2 is fetched, so the a→b edge needs both pages.
    assert [(e.client, e.server) for e in edges] == [("a", "b")]


def test_percentile_of_empty_histogram_is_none() -> None:
    assert _percentile_ms([], 0.5) is None
    assert _percentile_ms([{"Value": 0.1, "Count": 0}], 0.5) is None
