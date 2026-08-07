"""The AWS **CloudWatch usage source** — real per-topic usage stats from CloudWatch metrics.

Ports .NET's ``Benzene.Mesh.Usage.CloudWatch``: reads the ``benzene.messages.processed`` counter back from
CloudWatch and reports it as the mesh usage feed — a count per **(topic, transport, status)** over a
window. Every service's metrics middleware emits that counter tagged ``topic``/``transport``/``status``,
exported to CloudWatch (e.g. by the ADOT collector's EMF exporter); this is the aggregator-side reader that
turns those metrics back into :class:`~benzene.mesh.UsageEntry` rows for ``usage.json``.

Beyond the .NET v1 (which leaves ``avgDurationMs`` null — its documented follow-up), this Python source
**also fills mean duration**: for each dimension combination it reads the companion
``benzene.message.duration`` metric's ``Sum`` and ``SampleCount`` over the window and reports
``avgDurationMs = Sum ÷ SampleCount`` (the exact windowed mean, not an average-of-averages). The field
mapping CloudWatch → :class:`~benzene.mesh.UsageEntry`:

======================  ==============================================================================
Entry field             CloudWatch derivation
======================  ==============================================================================
``topic``               the ``topic`` dimension (an entry with no topic dimension is skipped — not ours)
``transport``           the ``transport`` dimension (``None`` if the metric doesn't carry it)
``status``              the ``status`` dimension (the outcome; ``None`` if absent)
``count``               ``Sum`` of the ``benzene.messages.processed`` datapoints over the window
``avgDurationMs``       ``benzene.message.duration`` ``Sum`` ÷ ``SampleCount`` (``None`` if unmeasured)
``version``/``service`` ``None`` — the counter doesn't carry these dimensions (never guessed)
``source``              ``"cloudwatch"``
======================  ==============================================================================

Assumes **delta** temporality (the EMF exporter default), so a ``Sum`` over the window equals the request
count. The AWS dependency is a minimal :class:`CloudWatchClient` :class:`~typing.Protocol`; a unit test
drives the source with a fake and no ``boto3``. :class:`Boto3CloudWatchClient` is the only thing here that
imports ``boto3`` (lazily), behind the ``benzene-mesh[aws]`` extra.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from ..artifacts import UsageEntry

#: The ``UsageEntry.source`` tag every entry this source produces carries (the .NET ``cloudwatch`` source).
CLOUDWATCH_SOURCE = "cloudwatch"


@dataclass(frozen=True)
class CloudWatchUsageOptions:
    """Which CloudWatch metrics :class:`CloudWatchUsageSource` reads, and over what window.

    Defaults match the ``benzene.messages.processed`` counter (tags ``topic``/``transport``/``status``)
    and the ``benzene.message.duration`` timer, in the ``Benzene/Mesh`` namespace — mirrors .NET's
    ``CloudWatchUsageOptions`` (with the added duration metric and a ``status`` dimension carrying the wire
    outcome vocabulary rather than a ``success``/``failure`` class).
    """

    namespace: str = "Benzene/Mesh"
    count_metric_name: str = "benzene.messages.processed"
    duration_metric_name: str = "benzene.message.duration"
    time_window: timedelta = timedelta(hours=24)
    period_seconds: int = 60
    topic_dimension: str = "topic"
    transport_dimension: str = "transport"
    status_dimension: str = "status"


class CloudWatchClient(Protocol):
    """The two CloudWatch calls this source needs — a structural seam over ``boto3``'s CloudWatch client.

    ``list_metrics`` discovers the live dimension combinations of a metric (so every reported entry's
    dimensions are known exactly, not parsed from a grouped label); ``get_metric_statistics`` sums a
    single combination over the window. Returned mappings are the raw CloudWatch responses. A test
    implements this with a fake; :class:`Boto3CloudWatchClient` implements it over ``boto3``.
    """

    def list_metrics(
        self, *, namespace: str, metric_name: str
    ) -> Mapping[str, Any]: ...

    def get_metric_statistics(
        self,
        *,
        namespace: str,
        metric_name: str,
        dimensions: Sequence[Mapping[str, str]],
        start_time: datetime,
        end_time: datetime,
        period: int,
        statistics: Sequence[str],
    ) -> Mapping[str, Any]: ...


class Boto3CloudWatchClient:
    """A :class:`CloudWatchClient` backed by ``boto3``'s CloudWatch client (the only ``boto3`` import here).

    Pass a pre-built ``boto3.client("cloudwatch")`` (or a compatible object), or leave it out to construct
    one lazily on first use — region and credentials then come from the ambient AWS environment, matching
    the .NET adapter's default client. Requires the ``benzene-mesh[aws]`` extra.
    """

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def _resolve(self) -> Any:
        if self._client is None:
            import boto3  # lazy: the [aws] extra's optional dependency

            self._client = boto3.client("cloudwatch")
        return self._client

    def list_metrics(self, *, namespace: str, metric_name: str) -> Mapping[str, Any]:
        result: Mapping[str, Any] = self._resolve().list_metrics(
            Namespace=namespace, MetricName=metric_name
        )
        return result

    def get_metric_statistics(
        self,
        *,
        namespace: str,
        metric_name: str,
        dimensions: Sequence[Mapping[str, str]],
        start_time: datetime,
        end_time: datetime,
        period: int,
        statistics: Sequence[str],
    ) -> Mapping[str, Any]:
        result: Mapping[str, Any] = self._resolve().get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=list(dimensions),
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=list(statistics),
        )
        return result


class CloudWatchUsageSource:
    """A :class:`~benzene.mesh.UsageSource` reading per-topic usage stats from CloudWatch.

    Construct it with a :class:`CloudWatchClient` and :class:`CloudWatchUsageOptions`; :meth:`usage`
    lists the counter's live dimension combinations, sums each over ``[now - time_window, now]``, reads
    the companion duration metric for the mean, and returns one :class:`~benzene.mesh.UsageEntry` per
    combination that carried traffic. ``clock`` is injectable for deterministic tests.
    """

    def __init__(
        self,
        client: CloudWatchClient,
        *,
        options: CloudWatchUsageOptions | None = None,
        clock: Any | None = None,
    ) -> None:
        self._client = client
        self._options = options or CloudWatchUsageOptions()
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def usage(self) -> list[UsageEntry]:
        """List, sum, and map the counter's live dimension combinations into usage entries."""
        opts = self._options
        end = self._clock()
        start = end - opts.time_window

        entries: list[UsageEntry] = []
        for dimensions in self._list_dimension_sets(opts.count_metric_name):
            lookup = {d["Name"]: d["Value"] for d in dimensions}
            topic = lookup.get(opts.topic_dimension)
            if not topic:  # not one of ours (no topic dimension) — never guess
                continue
            count = self._sum(opts.count_metric_name, dimensions, start, end, ("Sum",)).get("Sum", 0.0)
            if count <= 0:  # listed at some point, but no traffic in this window
                continue
            entries.append(
                UsageEntry(
                    topic=str(topic),
                    count=int(round(count)),
                    source=CLOUDWATCH_SOURCE,
                    version=None,
                    service=None,
                    transport=lookup.get(opts.transport_dimension),
                    status=lookup.get(opts.status_dimension),
                    avg_duration_ms=self._avg_duration(dimensions, start, end),
                )
            )
        # Deterministic feed order (the emitter preserves per-topic order when merging).
        entries.sort(key=lambda e: (e.topic, e.transport or "", e.status or ""))
        return entries

    def _list_dimension_sets(self, metric_name: str) -> Iterable[list[dict[str, str]]]:
        response = self._client.list_metrics(namespace=self._options.namespace, metric_name=metric_name)
        for metric in response.get("Metrics", []) or []:
            yield [
                {"Name": str(d["Name"]), "Value": str(d["Value"])}
                for d in metric.get("Dimensions", []) or []
            ]

    def _avg_duration(
        self, dimensions: Sequence[Mapping[str, str]], start: datetime, end: datetime
    ) -> float | None:
        """Mean handling duration in ms: the duration metric's total ``Sum`` ÷ total ``SampleCount``."""
        totals = self._sum(
            self._options.duration_metric_name, dimensions, start, end, ("Sum", "SampleCount")
        )
        samples = totals.get("SampleCount", 0.0)
        if samples <= 0:
            return None
        return totals["Sum"] / samples

    def _sum(
        self,
        metric_name: str,
        dimensions: Sequence[Mapping[str, str]],
        start: datetime,
        end: datetime,
        statistics: Sequence[str],
    ) -> dict[str, float]:
        """Sum each requested statistic across every datapoint in the window (the total over the window)."""
        response = self._client.get_metric_statistics(
            namespace=self._options.namespace,
            metric_name=metric_name,
            dimensions=dimensions,
            start_time=start,
            end_time=end,
            period=self._options.period_seconds,
            statistics=statistics,
        )
        totals: dict[str, float] = dict.fromkeys(statistics, 0.0)
        for datapoint in response.get("Datapoints", []) or []:
            for statistic in statistics:
                value = datapoint.get(statistic)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    totals[statistic] += float(value)
        return totals
