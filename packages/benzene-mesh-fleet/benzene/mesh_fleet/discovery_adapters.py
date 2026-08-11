"""Cloud service-discovery adapters (Benzene.Mesh.Discovery.Aws / .Azure / .Kubernetes).

Each adapter reads a cloud registry and projects it into the common
:class:`~benzene.mesh_fleet.discovery.ServiceEndpoint` shape, so a mesh poller or router treats an
AWS Cloud Map namespace, an Azure resource group, and a Kubernetes namespace identically. Every SDK
is an **optional dependency imported lazily** and the underlying client is **injectable**, so the
adapters — and their tests — construct and run with no cloud SDK installed: pass a fake client and
the ``import`` never happens.

Two invariants hold across all three (per the :class:`~benzene.mesh_fleet.discovery.Discovery`
contract): an empty registry is an empty list rather than an error, and a discovered service with no
resolvable address is skipped rather than emitted with a blank one. The extras are ``[aws]``
(``boto3``), ``[azure]`` (``azure-identity`` + a resource client), and ``[kubernetes]`` (the
``kubernetes`` client).
"""

from __future__ import annotations

from typing import Any

from .discovery import ServiceEndpoint, _str_metadata


class AwsCloudMapDiscovery:
    """Discovers endpoints from an AWS Cloud Map namespace (Benzene.Mesh.Discovery.Aws).

    Uses the ``servicediscovery`` API: it lists the services in ``namespace_name`` and, for each,
    the registered instances, turning every healthy instance into a :class:`ServiceEndpoint`. The
    address is the instance's ``AWS_INSTANCE_CNAME`` / ``AWS_INSTANCE_IPV4`` (plus ``AWS_INSTANCE_PORT``
    when present) — the attributes Cloud Map exposes for routing — and the remaining attributes ride
    along as metadata. ``boto3`` is the optional ``[aws]`` extra, imported lazily; inject ``client``
    to run against a fake with no SDK present.
    """

    def __init__(self, namespace_name: str, *, client: Any | None = None) -> None:
        self._namespace_name = namespace_name
        self._client = client

    def _service_discovery(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional [aws] dependency

            self._client = boto3.client("servicediscovery")
        return self._client

    async def discover(self) -> list[ServiceEndpoint]:
        client = self._service_discovery()
        filters = [{"Name": "NAMESPACE_ID", "Values": [self._namespace_name]}]
        endpoints: list[ServiceEndpoint] = []
        for service in client.list_services(Filters=filters).get("Services", []):
            name = service.get("Name")
            if not name:
                continue
            instances = client.list_instances(ServiceId=service["Id"]).get("Instances", [])
            for instance in instances:
                attributes = instance.get("Attributes", {})
                address = attributes.get("AWS_INSTANCE_CNAME") or attributes.get(
                    "AWS_INSTANCE_IPV4"
                )
                if not address:
                    continue
                port = attributes.get("AWS_INSTANCE_PORT")
                if port:
                    address = f"{address}:{port}"
                metadata = _str_metadata({**attributes, "instanceId": instance.get("Id")})
                endpoints.append(ServiceEndpoint(name=name, address=address, metadata=metadata))
        return endpoints


class AzureDiscovery:
    """Discovers endpoints from Azure resources tagged into the mesh (Benzene.Mesh.Discovery.Azure).

    Reads a resource client's ``resources.list``-style feed (App Services / Container Apps carry a
    ``defaultHostName``/FQDN and a ``benzene:service`` tag), turning each tagged resource into a
    :class:`ServiceEndpoint`. The service name is the ``benzene:service`` tag (falling back to the
    resource name) and the address is the resource's hostname; the tags ride along as metadata. The
    ``azure-identity`` credential + resource client are the optional ``[azure]`` extra, imported
    lazily; inject ``client`` to run against a fake.
    """

    def __init__(
        self,
        subscription_id: str,
        *,
        client: Any | None = None,
        service_tag: str = "benzene:service",
    ) -> None:
        self._subscription_id = subscription_id
        self._service_tag = service_tag
        self._client = client

    def _resource_client(self) -> Any:
        if self._client is None:
            from azure.identity import DefaultAzureCredential  # lazy: optional [azure] dependency
            from azure.mgmt.resource import ResourceManagementClient

            self._client = ResourceManagementClient(DefaultAzureCredential(), self._subscription_id)
        return self._client

    async def discover(self) -> list[ServiceEndpoint]:
        client = self._resource_client()
        endpoints: list[ServiceEndpoint] = []
        for resource in client.resources.list():
            tags = _resource_tags(resource)
            name = tags.get(self._service_tag) or _resource_attr(resource, "name")
            address = _resource_attr(resource, "default_host_name") or _resource_attr(
                resource, "fqdn"
            )
            if not name or not address:
                continue
            endpoints.append(
                ServiceEndpoint(name=name, address=address, metadata=_str_metadata(tags))
            )
        return endpoints


class KubernetesDiscovery:
    """Discovers endpoints from a Kubernetes namespace (Benzene.Mesh.Discovery.Kubernetes).

    Lists the ``Service`` objects in ``namespace`` via a ``CoreV1Api``-shaped client and turns each
    into a :class:`ServiceEndpoint` addressed by its in-cluster DNS name
    (``<svc>.<namespace>.svc.cluster.local``, plus the first port when declared). A
    ``label_selector`` narrows the mesh to labelled services; the service's labels ride along as
    metadata. The ``kubernetes`` client is the optional ``[kubernetes]`` extra, imported lazily; inject
    ``client`` (a ready ``CoreV1Api``) to run against a fake with no SDK present.
    """

    def __init__(
        self,
        *,
        namespace: str = "default",
        label_selector: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._namespace = namespace
        self._label_selector = label_selector
        self._client = client

    def _core_v1(self) -> Any:
        if self._client is None:
            from kubernetes import client, config  # lazy: optional [kubernetes] dependency

            config.load_incluster_config()
            self._client = client.CoreV1Api()
        return self._client

    async def discover(self) -> list[ServiceEndpoint]:
        client = self._core_v1()
        kwargs: dict[str, Any] = {}
        if self._label_selector:
            kwargs["label_selector"] = self._label_selector
        services = client.list_namespaced_service(self._namespace, **kwargs)
        endpoints: list[ServiceEndpoint] = []
        for service in getattr(services, "items", []):
            metadata = service.metadata
            name = getattr(metadata, "name", None)
            if not name:
                continue
            address = f"{name}.{self._namespace}.svc.cluster.local"
            port = _first_service_port(service)
            if port is not None:
                address = f"{address}:{port}"
            labels = getattr(metadata, "labels", None) or {}
            endpoints.append(
                ServiceEndpoint(name=name, address=address, metadata=_str_metadata(labels))
            )
        return endpoints


def _resource_tags(resource: Any) -> dict[str, Any]:
    """An Azure resource's ``tags`` as a plain dict, whether it is an object attr or a mapping key."""
    tags = _resource_attr(resource, "tags")
    return dict(tags) if isinstance(tags, dict) else {}


def _resource_attr(resource: Any, name: str) -> Any:
    """Read ``name`` off an Azure resource whether it is an SDK model (attr) or a dict (key)."""
    if isinstance(resource, dict):
        return resource.get(name)
    return getattr(resource, name, None)


def _first_service_port(service: Any) -> int | None:
    """The first declared port of a Kubernetes ``Service``, or ``None`` when it exposes none."""
    spec = getattr(service, "spec", None)
    ports = getattr(spec, "ports", None) or []
    for port in ports:
        value = getattr(port, "port", None)
        if value is not None:
            return int(value)
    return None
