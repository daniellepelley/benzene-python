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
from benzene.mesh import S3ArtifactStore, S3CollectorStore
from benzene.mesh_fleet import ServiceEndpoint

from aws_lambda_mesh.mesh.discovery_service import ingest_pushed_feed, run_mesh_aggregation
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


# --- consumer edges: a pushed trace batch survives to the NEXT aggregation's published catalog -------
#
# This is the end-to-end proof for the fix: without a durable collector_store, run_mesh_aggregation
# built a fresh in-memory MeshCollector every pass, so any trace pushed between scheduled runs (via
# service Lambdas' MeshFeedSender -> mesh Lambda direct invoke -> ingest_pushed_feed) was discarded
# before the next aggregation published anything -- every topic showed a provider but never a consumer.


def test_a_trace_pushed_between_aggregation_runs_survives_into_the_next_published_catalog() -> None:
    fake_state = _FakeStateS3Client()
    collector_store = S3CollectorStore("state-bucket", client=fake_state)
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

    # 2. Between aggregation runs, orders calls payments (payments:capture) -- exactly the trace batch
    #    a service Lambda's MeshFeedSender pushes after a real invocation (service/host.py). The
    #    mesh Lambda's handler routes this straight to ingest_pushed_feed (mesh/main.py).
    push = {
        "topic": "benzene:mesh:traces",
        "headers": {},
        "body": json.dumps(
            {
                "events": [
                    {
                        "traceId": "t1" * 16,
                        "spanId": "s1" * 8,
                        "service": "orders",
                        "topic": "order:create",
                        "status": "ok",
                    },
                    {
                        "traceId": "t1" * 16,
                        "spanId": "s2" * 8,
                        "parentSpanId": "s1" * 8,
                        "service": "payments",
                        "topic": PAYMENTS_CAPTURE_TOPIC,
                        "status": "ok",
                    },
                ]
            }
        ),
    }
    response = asyncio.run(ingest_pushed_feed(push, collector_store=collector_store))
    assert response["statusCode"] == "ok"

    # 3. The NEXT scheduled pass re-polls descriptors/health and publishes the merged catalog -- the
    #    trace pushed in step 2 must still be there.
    catalog_s3 = _FakeS3Client()
    asyncio.run(
        run_mesh_aggregation(
            discovery=_FakeDiscovery(endpoints),
            store=S3ArtifactStore("catalog-bucket", "mesh", client=catalog_s3),
            lambda_client=lambda_client,
            generated_at="2026-08-15T00:05:00+00:00",
            collector_store=collector_store,
        )
    )

    topics = {t["topic"]: t for t in catalog_s3.puts["mesh/topics.json"]["topics"]}
    capture = topics[PAYMENTS_CAPTURE_TOPIC]
    assert [p["service"] for p in capture["producers"]] == ["payments"]
    assert [c["service"] for c in capture["consumers"]] == ["orders"]
