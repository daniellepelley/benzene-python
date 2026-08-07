"""The AWS **X-Ray topology source** — real ``client → server`` edges from X-Ray's service graph.

Ports .NET's ``Benzene.Mesh.Fleet.Aws.XRay`` / ``Benzene.Mesh.Tracing.Tempo`` idea onto AWS X-Ray's
``GetServiceGraph`` API: X-Ray already aggregates observed traces into a service graph whose edges carry
per-edge call statistics (``SummaryStatistics``) and a response-time histogram. This turns that graph into
the mesh's :class:`~benzene.mesh.TopologyEdge` shape — one edge per observed ``client → server`` call —
so the mesh UI's topology plane shows real request rates, error rates, and latency percentiles without a
push collector, the AWS sibling of the Tempo service-graph topology builder.

The field mapping X-Ray → :class:`~benzene.mesh.TopologyEdge` (per edge, over the queried window):

======================  ==============================================================================
Edge field              X-Ray service-graph derivation
======================  ==============================================================================
``client``              the enclosing service's ``Name``
``server``              the service the edge points at (resolved via the edge's ``ReferenceId``)
``requestsPerMinute``   ``SummaryStatistics.TotalCount`` ÷ window minutes
``errorRate``           (``ErrorStatistics.TotalCount`` + ``FaultStatistics.TotalCount``) ÷ ``TotalCount``
``p50/p95/p99Ms``       the ``ResponseTimeHistogram`` percentile (bucket ``Value`` is **seconds**) × 1000
``source``              ``"xray"``
======================  ==============================================================================

The AWS dependency is a minimal :class:`XRayServiceGraphClient` :class:`~typing.Protocol` (one method,
``get_service_graph``); a unit test drives the source with a hand-written fake and no ``boto3``. The real
client is :class:`Boto3XRayServiceGraphClient`, the only thing here that imports ``boto3`` (lazily), behind
the ``benzene-mesh[aws]`` extra — mirroring how ``benzene-aws`` keeps ``boto3`` optional.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from ..artifacts import TopologyEdge

#: The ``TopologyEdge.source`` tag every edge this source produces carries (the .NET ``xray`` source).
XRAY_SOURCE = "xray"


class XRayServiceGraphClient(Protocol):
    """The one X-Ray call this source needs: fetch a page of the service graph over ``[start, end]``.

    A structural seam over ``boto3``'s X-Ray client (``get_service_graph``): the returned mapping is the
    raw X-Ray response (a ``"Services"`` list, optional ``"NextToken"``). A test implements this with a
    fake that returns canned pages; :class:`Boto3XRayServiceGraphClient` implements it over ``boto3``.
    """

    def get_service_graph(
        self, *, start_time: datetime, end_time: datetime, next_token: str | None = None
    ) -> Mapping[str, Any]: ...


class Boto3XRayServiceGraphClient:
    """A :class:`XRayServiceGraphClient` backed by ``boto3``'s X-Ray client (the only ``boto3`` import here).

    Pass a pre-built ``boto3.client("xray")`` (or a compatible object), or leave it out to construct one
    lazily on first use — region and credentials then come from the ambient AWS environment, matching the
    .NET adapter's default client. Requires the ``benzene-mesh[aws]`` extra.
    """

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def get_service_graph(
        self, *, start_time: datetime, end_time: datetime, next_token: str | None = None
    ) -> Mapping[str, Any]:
        if self._client is None:
            import boto3  # lazy: the [aws] extra's optional dependency

            self._client = boto3.client("xray")
        kwargs: dict[str, Any] = {"StartTime": start_time, "EndTime": end_time}
        if next_token:
            kwargs["NextToken"] = next_token
        result: Mapping[str, Any] = self._client.get_service_graph(**kwargs)
        return result


class XRayTopologySource:
    """A :class:`~benzene.mesh.TopologySource` reading AWS X-Ray's service graph.

    Construct it with an :class:`XRayServiceGraphClient` and the lookback ``window`` (default 1 hour — the
    "what's calling what, now" horizon, as in the .NET recent-flows window); :meth:`topology` queries the
    graph over ``[now - window, now]`` (paging ``NextToken`` to the end) and maps every edge to a
    :class:`~benzene.mesh.TopologyEdge`. ``clock`` is injectable for deterministic tests.
    """

    def __init__(
        self,
        client: XRayServiceGraphClient,
        *,
        window: timedelta = timedelta(hours=1),
        clock: Any | None = None,
    ) -> None:
        self._client = client
        self._window = window
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def topology(self) -> list[TopologyEdge]:
        """Query X-Ray's service graph over the window and map its edges to :class:`~benzene.mesh.TopologyEdge`."""
        end = self._clock()
        start = end - self._window
        services = list(self._fetch_services(start, end))
        window_minutes = self._window.total_seconds() / 60.0
        return _map_service_graph(services, window_minutes)

    def _fetch_services(self, start: datetime, end: datetime) -> Iterable[Mapping[str, Any]]:
        next_token: str | None = None
        while True:
            page = self._client.get_service_graph(
                start_time=start, end_time=end, next_token=next_token
            )
            yield from page.get("Services", []) or []
            next_token = page.get("NextToken") or None
            if not next_token:
                return


def _map_service_graph(
    services: Sequence[Mapping[str, Any]], window_minutes: float
) -> list[TopologyEdge]:
    """Map X-Ray ``Services[]`` (each with ``Edges[]``) into deterministic, sorted topology edges."""
    # X-Ray edges point at a destination service by its ReferenceId, so resolve names first.
    names_by_ref: dict[int, str] = {}
    for service in services:
        ref = service.get("ReferenceId")
        name = service.get("Name")
        if isinstance(ref, int) and name:
            names_by_ref[ref] = str(name)

    edges: list[TopologyEdge] = []
    for service in services:
        client = service.get("Name")
        if not client:
            continue
        for edge in service.get("Edges", []) or []:
            server = names_by_ref.get(edge.get("ReferenceId"))
            if not server or server == client:
                continue
            edges.append(_map_edge(str(client), server, edge, window_minutes))
    edges.sort(key=lambda e: (e.client, e.server))
    return edges


def _map_edge(
    client: str, server: str, edge: Mapping[str, Any], window_minutes: float
) -> TopologyEdge:
    stats = edge.get("SummaryStatistics") or {}
    total = _as_float(stats.get("TotalCount"))
    errors = _as_float((stats.get("ErrorStatistics") or {}).get("TotalCount"))
    faults = _as_float((stats.get("FaultStatistics") or {}).get("TotalCount"))

    requests_per_minute = total / window_minutes if window_minutes > 0 and total is not None else None
    error_rate = (
        ((errors or 0.0) + (faults or 0.0)) / total
        if total is not None and total > 0
        else None
    )
    histogram = edge.get("ResponseTimeHistogram") or []
    return TopologyEdge(
        client=client,
        server=server,
        source=XRAY_SOURCE,
        requests_per_minute=requests_per_minute,
        error_rate=error_rate,
        p50_latency_ms=_percentile_ms(histogram, 0.50),
        p95_latency_ms=_percentile_ms(histogram, 0.95),
        p99_latency_ms=_percentile_ms(histogram, 0.99),
    )


def _percentile_ms(histogram: Sequence[Mapping[str, Any]], quantile: float) -> float | None:
    """The ``quantile`` latency in **milliseconds** from an X-Ray ``ResponseTimeHistogram``.

    Each bucket is ``{"Value": seconds, "Count": n}``. Nearest-rank over the counts: sort buckets by
    latency, walk the cumulative count, and return the first bucket whose running total reaches
    ``quantile × total`` (× 1000 for ms) — the technique the X-Ray console uses to read percentiles off
    the histogram. Returns ``None`` for an empty/degenerate histogram (rendered ``–`` by the mesh UI).
    """
    buckets = sorted(
        (
            (value, count)
            for value, count in (
                (_as_float(b.get("Value")), _as_float(b.get("Count"))) for b in histogram
            )
            if value is not None and count is not None and count > 0
        ),
        key=lambda pair: pair[0],
    )
    total = sum(count for _value, count in buckets)
    if total <= 0:
        return None
    target = quantile * total
    cumulative = 0.0
    for value, count in buckets:
        cumulative += count
        if cumulative >= target:
            return value * 1000.0
    return buckets[-1][0] * 1000.0  # unreachable for a positive total; a defensive fall-through


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None
