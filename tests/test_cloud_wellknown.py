"""The Cloud Service Profile's well-known surfaces, served through each cloud HTTP host.

The `/benzene/invoke|health|spec` surfaces live in `BenzeneHttpApp`; every cloud host (GCP, AWS,
Azure) drives its HTTP trigger through an internal `BenzeneHttpApp`, so passing `standard_paths` to a
cloud app exposes the same surfaces there. This parametrised test proves that for all three hosts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from benzene.core import BenzeneMessageApplication, HealthChecks, Registry, ServiceSpec
from benzene.http import HttpRouter, StandardPaths
from benzene.results import Result


@dataclass
class Greet:
    name: str = ""


async def greet(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


def _gcp_host(router, application, standard):
    from benzene.gcp import GcpFunctionsApp
    from benzene.gcp.testing import GcpFunctionsTestHost

    return GcpFunctionsTestHost(
        GcpFunctionsApp(http_router=router, application=application, standard_paths=standard)
    )


def _aws_host(router, application, standard):
    from benzene.aws import AwsLambdaApp
    from benzene.aws.testing import AwsLambdaTestHost

    return AwsLambdaTestHost(
        AwsLambdaApp(http_router=router, application=application, standard_paths=standard)
    )


def _azure_host(router, application, standard):
    from benzene.azure import AzureFunctionsApp
    from benzene.azure.testing import AzureFunctionsTestHost

    return AzureFunctionsTestHost(
        AzureFunctionsApp(http_router=router, application=application, standard_paths=standard)
    )


@pytest.fixture(params=[_gcp_host, _aws_host, _azure_host], ids=["gcp", "aws", "azure"])
def host(request):
    """A cloud HTTP test host with the profile surfaces enabled (health + spec + invoke)."""
    router = HttpRouter()
    router.register("POST", "/greet", "say:hello", greet, request_type=Greet)
    registry = Registry()
    for definition in router.definitions():
        registry.add_definition(definition)
    standard = StandardPaths(
        health=HealthChecks().add("db", lambda: True),
        spec=ServiceSpec.derive(registry, service="greeter"),
    )
    return request.param(router, BenzeneMessageApplication(registry), standard)


def test_health_surface_on_the_cloud_host(host) -> None:
    response = host.send_http("GET", "/benzene/health")
    assert response.status_code == 200
    assert json.loads(response.body)["isHealthy"] is True


def test_spec_surface_on_the_cloud_host(host) -> None:
    response = host.send_http("GET", "/benzene/spec")
    assert response.status_code == 200
    doc = json.loads(response.body)
    assert doc["service"] == "greeter"
    assert "say:hello" in {t["id"] for t in doc["topics"]}


def test_invoke_surface_on_the_cloud_host(host) -> None:
    envelope = {"topic": "say:hello", "headers": {}, "body": json.dumps({"name": "cloud"})}
    response = host.send_http("POST", "/benzene/invoke", body=envelope)
    assert response.status_code == 200
    outer = json.loads(response.body)
    assert outer["statusCode"] == "ok"
    assert json.loads(outer["body"]) == {"greeting": "Hello cloud"}


def test_ordinary_routes_still_work_alongside_the_surfaces(host) -> None:
    response = host.send_http("POST", "/greet", body={"name": "there"})
    assert response.status_code == 200
    assert json.loads(response.body) == {"greeting": "Hello there"}
