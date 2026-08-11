"""Service discovery — the set of services/endpoints known to the mesh (Benzene.Mesh.Discovery.*).

The mesh already knows how to *describe* a service (the :class:`~benzene.mesh.ServiceDescriptor`) and
how to *poll* a known fleet (the :class:`~benzene.mesh.MeshPoller`). What it does not do is answer the
prior question — *which services are there to poll in the first place?* A :class:`Discovery` is that
seam: an async source that enumerates the mesh's live endpoints, so the fleet a poller or router sees
is read from a cloud registry (AWS Cloud Map, Azure, Kubernetes) rather than hand-listed.

:meth:`Discovery.discover` returns a list of :class:`ServiceEndpoint`\\ s — a deliberately small
identity/address/metadata triple, the lowest common denominator every cloud registry can produce. An
endpoint is *not* a descriptor: discovery tells you a service exists and where to reach it, and the
poller then reads that address for the full contract. An empty mesh is an empty list, never an error.

Mirrors .NET's ``Benzene.Mesh.Discovery`` abstractions; the concrete cloud adapters live in
:mod:`benzene.mesh_fleet.discovery_adapters`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class ServiceEndpoint:
    """One discovered service: its logical name, a reachable address, and free-form metadata.

    ``name`` is the mesh service identity (the same value a :class:`~benzene.mesh.ServiceDescriptor`
    carries in ``service``); ``address`` is a URL or host the poller/router can reach. ``metadata``
    holds whatever the registry knew and the port did not model — availability zone, instance id,
    ARN, labels — so nothing a cloud registry returns is lost. Frozen and hashable so a caller can
    dedupe endpoints across registries.
    """

    name: str
    address: str
    metadata: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class Discovery(Protocol):
    """A source of the mesh's currently known service endpoints (Benzene.Mesh.Discovery.*).

    ``discover`` is async because a real registry is a network call; it returns every endpoint the
    source can see right now. It **must never raise for an empty mesh** — an unpopulated namespace or
    a registry with no matching services is an empty list, not an error — so a caller can treat "no
    services" and "discovery succeeded" as the same, expected outcome.
    """

    async def discover(self) -> list[ServiceEndpoint]: ...


class StaticDiscovery:
    """A :class:`Discovery` over a fixed, in-memory endpoint list — the default and the test double.

    The counterpart of the cloud adapters: it needs no SDK and no network, so it is the impl a test,
    a local run, or a config-file-driven deployment uses. ``discover`` returns a fresh copy of the
    configured endpoints every call, so a caller mutating the result never disturbs the source.
    """

    def __init__(self, endpoints: list[ServiceEndpoint] | None = None) -> None:
        self._endpoints = list(endpoints or [])

    async def discover(self) -> list[ServiceEndpoint]:
        return list(self._endpoints)


def _str_metadata(source: dict[str, Any] | None) -> dict[str, str]:
    """Coerce a registry's raw metadata mapping into the ``dict[str, str]`` an endpoint carries."""
    return {str(k): str(v) for k, v in (source or {}).items() if v is not None}
