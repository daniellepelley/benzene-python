"""Unit tests for the AWS **CloudWatch usage source** — its metric → usage-entry mapping, with a fake client.

Drives :class:`~benzene.mesh.aws.CloudWatchUsageSource` off a hand-written fake
:class:`~benzene.mesh.aws.CloudWatchClient` (no ``boto3``, no ``moto``), asserting the mapping of the
``benzene.messages.processed`` counter (summed per dimension combination) and the
``benzene.message.duration`` timer (``Sum`` ÷ ``SampleCount``) to the pinned ``usage.json`` entry shape:
count, transport/status dimensions, mean duration, and the ``source: "cloudwatch"`` tag.
"""

from __future__ import annotations

from datetime import datetime, timezone

from benzene.mesh.aws import CloudWatchUsageOptions, CloudWatchUsageSource

_AT = datetime(2026, 7, 16, 9, 15, 0, tzinfo=timezone.utc)


def _dims(topic: str, transport: str, status: str) -> list[dict[str, str]]:
    return [
        {"Name": "topic", "Value": topic},
        {"Name": "transport", "Value": transport},
        {"Name": "status", "Value": status},
    ]


class _FakeCloudWatch:
    """A fake CloudWatch client: a list of dimension combos, and canned Sum/SampleCount per (metric, combo)."""

    def __init__(self, combos, counts, durations) -> None:
        self._combos = combos
        self._counts = counts  # (topic, transport, status) -> total processed count
        self._durations = durations  # (topic, transport, status) -> (sum_ms, sample_count)

    def list_metrics(self, *, namespace, metric_name):
        return {"Metrics": [{"Dimensions": dims} for dims in self._combos]}

    def get_metric_statistics(
        self, *, namespace, metric_name, dimensions, start_time, end_time, period, statistics
    ):
        key = tuple(d["Value"] for d in dimensions)
        if metric_name.endswith("processed"):
            count = self._counts.get(key, 0.0)
            # Split across two datapoints to prove the source sums every bucket in the window.
            return {"Datapoints": [{"Sum": count * 0.4}, {"Sum": count * 0.6}]}
        total_ms, samples = self._durations.get(key, (0.0, 0.0))
        return {"Datapoints": [{"Sum": total_ms, "SampleCount": samples}]}


def _source(combos, counts, durations) -> CloudWatchUsageSource:
    return CloudWatchUsageSource(_FakeCloudWatch(combos, counts, durations), clock=lambda: _AT)


def test_maps_counter_dimensions_to_usage_entries() -> None:
    combos = [
        _dims("orders:get-all", "AspNet", "ok"),
        _dims("orders:create", "Sqs", "created"),
    ]
    counts = {
        ("orders:get-all", "AspNet", "ok"): 41230.0,
        ("orders:create", "Sqs", "created"): 2150.0,
    }
    durations = {
        ("orders:get-all", "AspNet", "ok"): (41230.0 * 14.2, 41230.0),
        ("orders:create", "Sqs", "created"): (2150.0 * 41.7, 2150.0),
    }
    entries = _source(combos, counts, durations).usage()

    by_topic = {e.topic: e for e in entries}
    getall = by_topic["orders:get-all"]
    assert getall.count == 41230
    assert getall.transport == "AspNet"
    assert getall.status == "ok"
    assert getall.avg_duration_ms == 14.2
    assert getall.source == "cloudwatch"
    assert getall.version is None and getall.service is None  # counter carries neither


def test_entry_payload_matches_the_pinned_usage_shape() -> None:
    combos = [_dims("payment:capture", "Sqs", "ok")]
    entries = _source(
        combos, {("payment:capture", "Sqs", "ok"): 10290.0},
        {("payment:capture", "Sqs", "ok"): (10290.0 * 55.0, 10290.0)},
    ).usage()
    payload = entries[0].to_payload()
    assert set(payload) == {
        "topic", "version", "service", "transport", "status", "count", "avgDurationMs", "source",
    }
    assert payload == {
        "topic": "payment:capture", "version": None, "service": None, "transport": "Sqs",
        "status": "ok", "count": 10290, "avgDurationMs": 55.0, "source": "cloudwatch",
    }


def test_combinations_with_no_traffic_in_window_are_dropped() -> None:
    combos = [_dims("orders:create", "Sqs", "created"), _dims("stale:topic", "Sqs", "ok")]
    entries = _source(
        combos, {("orders:create", "Sqs", "created"): 94.0},  # stale:topic summed to 0
        {("orders:create", "Sqs", "created"): (94.0 * 9.3, 94.0)},
    ).usage()
    assert [e.topic for e in entries] == ["orders:create"]


def test_missing_duration_leaves_avg_null_never_zero() -> None:
    combos = [_dims("orders:create", "Sqs", "created")]
    entries = _source(
        combos, {("orders:create", "Sqs", "created"): 94.0}, durations={},
    ).usage()
    assert entries[0].count == 94
    assert entries[0].avg_duration_ms is None  # no samples → honest null, not a fabricated 0


def test_metric_without_topic_dimension_is_skipped() -> None:
    combos = [[{"Name": "transport", "Value": "Sqs"}, {"Name": "status", "Value": "ok"}]]
    entries = _source(combos, {("Sqs", "ok"): 100.0}, {("Sqs", "ok"): (100.0, 100.0)}).usage()
    assert entries == []  # no topic dimension → not one of ours, never guessed


def test_options_drive_namespace_and_metric_names() -> None:
    seen: list[tuple[str, str]] = []

    class _Recording(_FakeCloudWatch):
        def list_metrics(self, *, namespace, metric_name):
            seen.append((namespace, metric_name))
            return super().list_metrics(namespace=namespace, metric_name=metric_name)

    options = CloudWatchUsageOptions(namespace="Acme/Mesh", count_metric_name="acme.processed")
    client = _Recording([_dims("t", "Sqs", "ok")], {("t", "Sqs", "ok"): 5.0}, {})
    CloudWatchUsageSource(client, options=options, clock=lambda: _AT).usage()
    assert seen == [("Acme/Mesh", "acme.processed")]
