"""Fleet tests: static/cloud discovery adapters and the Jaeger/Tempo/X-Ray trace-mappers.

Everything runs in memory — no cloud SDK, no network, no tracing backend. The cloud adapters take an
injected fake client (so the lazy ``boto3`` / ``kubernetes`` import never fires), and the mappers are
fed a real :class:`benzene.mesh.TraceEvent` trace built with the actual mesh types, so the assertions
pin the real field names and — the part that matters most — each backend's time unit.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from benzene.mesh import TraceEvent
from benzene.mesh_fleet import (
    AwsCloudMapDiscovery,
    JaegerTraceMapper,
    KubernetesDiscovery,
    ServiceEndpoint,
    StaticDiscovery,
    TempoTraceMapper,
    XRayTraceMapper,
)


def run(coro):
    return asyncio.run(coro)


# 2024-01-01T00:00:00Z — a round epoch the unit assertions can pin exactly.
_EPOCH_SECONDS = 1_704_067_200
_ROOT_STARTED = "2024-01-01T00:00:00.000000Z"
_CHILD_STARTED = "2024-01-01T00:00:00.005000Z"


def _trace() -> list[TraceEvent]:
    """A two-span trace (parent ``orders`` → child ``inventory``) built from the real mesh type."""
    root = TraceEvent(
        trace_id="a" * 32,
        span_id="1" * 16,
        service="orders",
        topic="order:create",
        status="ok",
        started_at=_ROOT_STARTED,
        duration_ms=12.5,
    )
    child = TraceEvent(
        trace_id="a" * 32,
        span_id="2" * 16,
        parent_span_id="1" * 16,
        service="inventory",
        topic="inventory:reserve",
        status="service-unavailable",
        exception_type="TimeoutError",
        started_at=_CHILD_STARTED,
        duration_ms=3.0,
    )
    return [root, child]


# --- discovery ---------------------------------------------------------------------------------


def test_static_discovery_returns_configured_endpoints():
    endpoints = [
        ServiceEndpoint("orders", "https://orders.svc", {"zone": "a"}),
        ServiceEndpoint("inventory", "https://inventory.svc"),
    ]
    discovery = StaticDiscovery(endpoints)

    result = run(discovery.discover())

    assert result == endpoints
    # a caller mutating the result never disturbs the source
    result.clear()
    assert run(discovery.discover()) == endpoints


def test_static_discovery_empty_is_empty_list():
    assert run(StaticDiscovery().discover()) == []


class _FakeCloudMapClient:
    """A stand-in for a boto3 ``servicediscovery`` client (only the two calls the adapter makes)."""

    def __init__(self) -> None:
        self.filters: list[dict[str, object]] | None = None

    def list_services(self, Filters):  # noqa: N803 - boto3 kwarg name
        self.filters = Filters
        return {"Services": [{"Id": "srv-1", "Name": "orders"}]}

    def list_instances(self, ServiceId):  # noqa: N803 - boto3 kwarg name
        assert ServiceId == "srv-1"
        return {
            "Instances": [
                {
                    "Id": "i-1",
                    "Attributes": {"AWS_INSTANCE_CNAME": "orders.svc", "AWS_INSTANCE_PORT": "8080"},
                },
                {"Id": "i-2", "Attributes": {}},  # no address → skipped, never a raise
            ]
        }


def test_aws_cloud_map_discovery_maps_instances_to_endpoints():
    fake = _FakeCloudMapClient()
    discovery = AwsCloudMapDiscovery("ns-mesh12345", client=fake)  # a Cloud Map namespace id

    endpoints = run(discovery.discover())

    assert endpoints == [
        ServiceEndpoint(
            name="orders",
            address="orders.svc:8080",
            metadata={
                "AWS_INSTANCE_CNAME": "orders.svc",
                "AWS_INSTANCE_PORT": "8080",
                "instanceId": "i-1",
            },
        )
    ]
    assert fake.filters == [{"Name": "NAMESPACE_ID", "Values": ["ns-mesh12345"]}]


class _FakeCoreV1Api:
    """A stand-in for a kubernetes ``CoreV1Api`` (only ``list_namespaced_service``)."""

    def __init__(self) -> None:
        self.namespace: str | None = None
        self.label_selector: str | None = None

    def list_namespaced_service(self, namespace, **kwargs):
        self.namespace = namespace
        self.label_selector = kwargs.get("label_selector")
        service = SimpleNamespace(
            metadata=SimpleNamespace(name="orders", labels={"team": "checkout"}),
            spec=SimpleNamespace(ports=[SimpleNamespace(port=8080)]),
        )
        return SimpleNamespace(items=[service])


def test_kubernetes_discovery_maps_services_to_endpoints():
    fake = _FakeCoreV1Api()
    discovery = KubernetesDiscovery(namespace="mesh", label_selector="app=benzene", client=fake)

    endpoints = run(discovery.discover())

    assert endpoints == [
        ServiceEndpoint(
            name="orders",
            address="orders.mesh.svc.cluster.local:8080",
            metadata={"team": "checkout"},
        )
    ]
    assert fake.namespace == "mesh"
    assert fake.label_selector == "app=benzene"


# --- fleet trace-mappers -----------------------------------------------------------------------


def test_jaeger_mapper_uses_microseconds_and_child_of_references():
    doc = JaegerTraceMapper().map(_trace())

    assert doc["traceID"] == "a" * 32
    assert len(doc["spans"]) == 2
    root, child = doc["spans"]

    assert root["operationName"] == "order:create"
    assert root["startTime"] == _EPOCH_SECONDS * 1_000_000  # Jaeger wants microseconds
    assert root["duration"] == 12_500  # 12.5 ms → 12500 µs
    assert root["references"] == []

    assert child["references"] == [{"refType": "CHILD_OF", "traceID": "a" * 32, "spanID": "1" * 16}]
    assert {"key": "error", "type": "bool", "value": True} in child["tags"]

    # one process per service, referenced by processID
    assert {p["serviceName"] for p in doc["processes"].values()} == {"orders", "inventory"}
    assert root["processID"] in doc["processes"]


def test_tempo_mapper_uses_unix_nanoseconds_and_otlp_shape():
    doc = TempoTraceMapper().map(_trace())

    resource_spans = doc["resourceSpans"]
    assert len(resource_spans) == 2  # grouped by service.name

    spans_by_id = {
        span["spanId"]: span
        for rs in resource_spans
        for scope in rs["scopeSpans"]
        for span in scope["spans"]
    }
    root = spans_by_id["1" * 16]
    child = spans_by_id["2" * 16]

    assert root["name"] == "order:create"
    assert root["startTimeUnixNano"] == _EPOCH_SECONDS * 1_000_000_000  # OTLP wants nanoseconds
    assert root["endTimeUnixNano"] == _EPOCH_SECONDS * 1_000_000_000 + 12_500_000
    assert "parentSpanId" not in root
    assert child["parentSpanId"] == "1" * 16
    assert child["status"]["code"] == "STATUS_CODE_ERROR"
    assert root["status"]["code"] == "STATUS_CODE_OK"


def test_xray_mapper_uses_epoch_seconds_and_subsegment_tree():
    doc = XRayTraceMapper().map(_trace())

    assert doc["name"] == "orders"
    assert doc["id"] == "1" * 16
    assert doc["trace_id"] == f"1-{'a' * 8}-{'a' * 24}"  # X-Ray 1-<8hex>-<24hex>
    assert doc["start_time"] == float(_EPOCH_SECONDS)  # X-Ray wants epoch seconds
    assert doc["end_time"] == float(_EPOCH_SECONDS) + 0.0125

    subsegments = doc["subsegments"]
    assert len(subsegments) == 1
    child = subsegments[0]
    assert child["name"] == "inventory"
    assert child["id"] == "2" * 16
    assert child["fault"] is True
    assert "trace_id" not in child  # subsegments inherit the segment's trace id
