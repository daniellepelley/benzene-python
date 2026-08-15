"""In-memory tests for the mesh's discover -> interrogate -> publish pass (``mesh/discovery_service.py``).

Fakes the two external edges only: a fake :class:`~benzene.mesh_fleet.Discovery` stands in for the real
``ListFunctions``/``ListTags`` call, and a fake ``boto3`` Lambda client routes ``invoke()`` straight to
each service's real, in-memory :class:`~benzene.aws.AwsLambdaApp` (built from the *actual*
``ServiceStartUp`` composition root, exactly as ``service/host.py`` builds it for deployment) — so the
interrogation exercises the real ``benzene:mesh``/``benzene:healthcheck`` handling, not a stub. A fake
``boto3`` S3 client captures what gets published, matching ``tests/test_mesh_s3_artifacts.py``'s pattern
one level up (that file tests ``S3ArtifactStore`` itself; this proves the example wires it correctly end
to end) — proving discover -> poll -> collector -> S3 catalog without a single real AWS call.
"""

from __future__ import annotations

import asyncio
import io
import json

from benzene.aws import AwsLambdaApp
from benzene.core import Container, MessageSender, build_application
from benzene.mesh import S3ArtifactStore, S3CollectorStore, S3TraceInbox
from benzene.mesh_fleet import ServiceEndpoint

from aws_lambda_mesh.mesh.discovery_service import run_mesh_aggregation
from aws_lambda_mesh.service.domain import PAYMENTS_CAPTURE_TOPIC, SERVICE_TOPICS
from aws_lambda_mesh.service.startup import KNOWN_SERVICES, ServiceStartUp

_TAG_METADATA = {"benzene": "true"}


class _FakeDiscovery:
    """A :class:`~benzene.mesh_fleet.Discovery` returning a fixed endpoint list."""

    def __init__(self, endpoints: list[ServiceEndpoint]) -> None:
        self._endpoints = endpoints

    async def discover(self) -> list[ServiceEndpoint]:
        return list(self._endpoints)


class _FakeS3Client:
    def __init__(self) -> None:
        self.puts: dict[str, dict] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 kwarg names
        self.puts[Key] = json.loads(Body.decode("utf-8"))


class _FakeLambdaClient:
    """Routes ``invoke()`` to each service's real, in-memory :class:`AwsLambdaApp` by function name."""

    def __init__(self, apps: dict[str, AwsLambdaApp]) -> None:
        self._apps = apps
        self.invoked_topics: list[tuple[str, str]] = []

    def invoke(self, *, FunctionName, InvocationType="RequestResponse", Payload, Qualifier=None):  # noqa: N803
        event = json.loads(Payload.decode("utf-8"))
        self.invoked_topics.append((FunctionName, event.get("topic", "")))
        response = self._apps[FunctionName].handle(event)
        return {"Payload": io.BytesIO(json.dumps(response).encode("utf-8"))}


def _build_app(name: str) -> AwsLambdaApp:
    def overrides(services: Container) -> None:
        services.add_instance(MessageSender, _NullSender())

    definition, _ = build_application(ServiceStartUp(name), overrides=[overrides])
    return AwsLambdaApp.from_definition(definition)


class _NullSender:
    async def send_message(self, topic, message, headers=None):
        from benzene.results import Result

        return Result.accepted()


class _NoSuchKey(Exception):
    def __init__(self) -> None:
        super().__init__("NoSuchKey")
        self.response = {"Error": {"Code": "NoSuchKey"}}


class _FakeStateS3Client:
    """A stand-in ``boto3`` ``s3`` client backing :class:`~benzene.mesh.S3CollectorStore`
    (``get_object``/``put_object`` only) — the mesh Lambda's durable collector snapshot."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 kwarg names
        self.objects[Key] = Body

    def get_object(self, *, Bucket, Key):  # noqa: N803 - boto3 kwarg names
        if Key not in self.objects:
            raise _NoSuchKey()
        return {"Body": io.BytesIO(self.objects[Key])}


class _FakeTraceInboxS3Client:
    """A stand-in ``boto3`` ``s3`` client backing :class:`~benzene.mesh.S3TraceInbox`
    (``put_object``/``list_objects_v2``/``get_object``/``delete_objects``)."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 kwarg names
        self.objects[Key] = Body

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_objects(self, *, Bucket, Delete):  # noqa: N803
        for entry in Delete["Objects"]:
            self.objects.pop(entry["Key"], None)


def _endpoint(name: str) -> ServiceEndpoint:
    # Mirrors real AwsLambdaDiscovery: name == address == the discovered function name (a Lambda has
    # no separate "friendly name" — see discovery_adapters.AwsLambdaDiscovery).
    function_name = f"aws-lambda-mesh-{name}"
    return ServiceEndpoint(name=function_name, address=function_name, metadata=_TAG_METADATA)


def _fleet() -> tuple[list[ServiceEndpoint], _FakeLambdaClient]:
    endpoints = [_endpoint(name) for name in KNOWN_SERVICES]
    apps = {e.address: _build_app(name) for name, e in zip(KNOWN_SERVICES, endpoints, strict=True)}
    return endpoints, _FakeLambdaClient(apps)


def test_run_mesh_aggregation_discovers_and_interrogates_all_six_services() -> None:
    endpoints, lambda_client = _fleet()
    s3 = _FakeS3Client()
    store = S3ArtifactStore("catalog-bucket", "mesh", client=s3)

    summary = asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=store,
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert summary.discovered == 6
    # Each service was interrogated (by function name) on exactly the two reserved topics.
    called = {name for name, _ in lambda_client.invoked_topics}
    assert called == {e.address for e in endpoints}
    assert {topic for _, topic in lambda_client.invoked_topics} == {
        "benzene:mesh",
        "benzene:healthcheck",
    }


def test_run_mesh_aggregation_publishes_registry_and_full_catalog_to_s3() -> None:
    endpoints, lambda_client = _fleet()
    s3 = _FakeS3Client()
    store = S3ArtifactStore("catalog-bucket", "mesh", client=s3)

    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=store,
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert {
        "mesh/registry.json",
        "mesh/manifest.json",
        "mesh/topology.json",
        "mesh/topics.json",
        "mesh/usage.json",
        "mesh/asyncapi.json",
        "mesh/annotations.json",
        *(f"mesh/services/{name}.json" for name in KNOWN_SERVICES),
    } == set(s3.puts)

    # registry.json reflects DISCOVERY-time identity: the Lambda function names.
    registry = s3.puts["mesh/registry.json"]
    assert {s["name"] for s in registry["services"]} == {e.address for e in endpoints}

    # manifest.json (and the rest of the catalog) reflect INTERROGATION-time identity: each service's
    # own descriptor "service" field (the plain domain name), read off the benzene:mesh response —
    # not necessarily the same string as the function name that was invoked to get it.
    manifest = s3.puts["mesh/manifest.json"]
    manifest_services = {s["name"]: s for s in manifest["services"]}
    assert set(manifest_services) == set(KNOWN_SERVICES)
    assert all(s["status"] == "healthy" for s in manifest_services.values())

    topics = {t["topic"] for t in s3.puts["mesh/topics.json"]["topics"]}
    assert topics == {topic for topics_ in SERVICE_TOPICS.values() for topic in topics_}


def test_run_mesh_aggregation_empty_mesh_is_zero_not_an_error() -> None:
    s3 = _FakeS3Client()
    store = S3ArtifactStore("catalog-bucket", client=s3)

    summary = asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery([]),
            store=store,
            generated_at="2026-08-15T00:00:00+00:00",
        )
    )

    assert summary.discovered == 0
    assert s3.puts["registry.json"]["services"] == []
    assert s3.puts["manifest.json"]["services"] == []


# --- consumer edges: pushed trace batches survive to the NEXT aggregation's published catalog --------
#
# This is the end-to-end proof for the fix: without a durable collector_store, run_mesh_aggregation
# built a fresh in-memory MeshCollector every pass, so any trace pushed between scheduled runs was
# discarded before the next aggregation published anything -- every topic showed a provider but never
# a consumer. Traces now land in a durable S3TraceInbox (a service Lambda's MeshFeedSender pushes
# straight to S3, service/host.py) and only the aggregation pass -- the collector's sole writer --
# drains and merges them in, so a push can never race another concurrent push.


def _trace_event(*, trace_id, span_id, service, topic, parent_span_id=None):
    event = {"traceId": trace_id, "spanId": span_id, "service": service, "topic": topic, "status": "ok"}
    if parent_span_id:
        event["parentSpanId"] = parent_span_id
    return event


def test_a_trace_pushed_between_aggregation_runs_survives_into_the_next_published_catalog() -> None:
    fake_state = _FakeStateS3Client()
    collector_store = S3CollectorStore("state-bucket", client=fake_state)
    fake_traces = _FakeTraceInboxS3Client()
    trace_inbox = S3TraceInbox("state-bucket", client=fake_traces)
    endpoints, lambda_client = _fleet()

    # 1. First scheduled pass: discover + interrogate (registers each service as a provider) and
    #    persist through the same durable store a later trace push will also write through.
    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=S3ArtifactStore("catalog-bucket", "mesh", client=_FakeS3Client()),
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:00:00+00:00",
            collector_store=collector_store,
        )
    )

    # 2. Between aggregation runs, orders calls payments (payments:capture) -- exactly what a service
    #    Lambda's MeshFeedSender pushes into the trace inbox after a real invocation.
    events = [
        _trace_event(trace_id="t1" * 16, span_id="s1" * 8, service="orders", topic="order:create"),
        _trace_event(
            trace_id="t1" * 16,
            span_id="s2" * 8,
            parent_span_id="s1" * 8,
            service="payments",
            topic=PAYMENTS_CAPTURE_TOPIC,
        ),
    ]
    push = asyncio.run(trace_inbox.send_message("benzene:mesh:traces", {"events": events}))
    assert push.is_successful

    # 3. The NEXT scheduled pass re-polls descriptors/health, drains the inbox, and publishes the
    #    merged catalog -- the trace pushed in step 2 must still be there.
    catalog_s3 = _FakeS3Client()
    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=S3ArtifactStore("catalog-bucket", "mesh", client=catalog_s3),
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:05:00+00:00",
            collector_store=collector_store,
            trace_inbox=trace_inbox,
        )
    )

    topics = {t["topic"]: t for t in catalog_s3.puts["mesh/topics.json"]["topics"]}
    capture = topics[PAYMENTS_CAPTURE_TOPIC]
    assert [p["service"] for p in capture["producers"]] == ["payments"]
    assert [c["service"] for c in capture["consumers"]] == ["orders"]
    # Drained objects are gone -- the next pass won't re-count them.
    assert fake_traces.objects == {}


def test_two_fanned_out_pushes_both_survive_into_the_same_pass_no_lost_update() -> None:
    # The bug this whole redesign fixes: order:placed fans out over SNS to BOTH inventory and
    # notifications at once, each pushing its own trace batch independently. The old design
    # (load-mutate-save against one shared S3CollectorStore snapshot) raced under exactly this
    # condition -- whichever push saved last won, silently discarding the other's event. With
    # S3TraceInbox, both pushes land as independent objects with nothing to contend over.
    fake_state = _FakeStateS3Client()
    collector_store = S3CollectorStore("state-bucket", client=fake_state)
    fake_traces = _FakeTraceInboxS3Client()
    trace_inbox = S3TraceInbox("state-bucket", client=fake_traces)
    endpoints, lambda_client = _fleet()

    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=S3ArtifactStore("catalog-bucket", "mesh", client=_FakeS3Client()),
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:00:00+00:00",
            collector_store=collector_store,
        )
    )

    # "Concurrent" pushes: neither read the other's write, exactly as two Lambdas invoked by the same
    # SNS fan-out would each independently drain their own exporter and push.
    parent = _trace_event(trace_id="t2" * 16, span_id="p0" * 8, service="orders", topic="order:placed")
    inventory_push = asyncio.run(
        trace_inbox.send_message(
            "benzene:mesh:traces",
            {
                "events": [
                    parent,
                    _trace_event(
                        trace_id="t2" * 16,
                        span_id="i1" * 8,
                        parent_span_id="p0" * 8,
                        service="inventory",
                        topic="order:placed",
                    ),
                ]
            },
        )
    )
    notifications_push = asyncio.run(
        trace_inbox.send_message(
            "benzene:mesh:traces",
            {
                "events": [
                    parent,
                    _trace_event(
                        trace_id="t2" * 16,
                        span_id="n1" * 8,
                        parent_span_id="p0" * 8,
                        service="notifications",
                        topic="order:placed",
                    ),
                ]
            },
        )
    )
    assert inventory_push.is_successful and notifications_push.is_successful
    assert len(fake_traces.objects) == 2  # two independent objects, nothing overwritten

    catalog_s3 = _FakeS3Client()
    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=S3ArtifactStore("catalog-bucket", "mesh", client=catalog_s3),
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:05:00+00:00",
            collector_store=collector_store,
            trace_inbox=trace_inbox,
        )
    )

    topics = {t["topic"]: t for t in catalog_s3.puts["mesh/topics.json"]["topics"]}
    placed = topics["order:placed"]
    assert [c["service"] for c in placed["consumers"]] == ["orders"]
    usage_by_service = {e["service"] for e in catalog_s3.puts["mesh/usage.json"]["entries"]}
    assert {"inventory", "notifications"} <= usage_by_service
