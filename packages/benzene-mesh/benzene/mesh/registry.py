"""The mesh **service registry** — the set of services an aggregator polls (the human-maintained config).

:class:`MeshServiceEntry` (one service: its name + where to fetch its spec/health) and
:class:`MeshServiceRegistry` (the set of them) are the aggregator's *input* — mirroring .NET's
``MeshServiceRegistryEntry`` / ``MeshServiceRegistry``. They live in this dependency-light module (no
``benzene-http``) rather than in :mod:`benzene.mesh.aggregator` so a **discovery** source
(:mod:`benzene.mesh.aws.discovery`) can *produce* a registry without pulling in the HTTP-fetching
aggregator: discovery writes this document, the aggregator reads it — the same discovery↔runtime seam the
.NET ``MeshRegistryJson`` is. :mod:`benzene.mesh.aggregator` re-exports both names, so importing them from
there continues to work.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MeshServiceEntry:
    """One service the aggregator polls: its name and where to fetch its spec + health.

    ``base_url`` is the convenience form — ``spec_url``/``health_url`` default to
    ``{base_url}/benzene/spec`` and ``{base_url}/benzene/health`` (the profile's well-known paths). Give
    ``spec_url``/``health_url`` explicitly to override (a relocated prefix, R7). Mirrors .NET's
    ``MeshServiceRegistryEntry`` (name + specUrl + healthUrl), the human-maintained registry input.
    """

    name: str
    base_url: str | None = None
    spec_url: str | None = None
    health_url: str | None = None
    prefix: str = "/benzene"

    def resolved_spec_url(self) -> str:
        if self.spec_url is not None:
            return self.spec_url
        return f"{self._base()}{self.prefix}/spec"

    def resolved_health_url(self) -> str:
        if self.health_url is not None:
            return self.health_url
        return f"{self._base()}{self.prefix}/health"

    def _base(self) -> str:
        if self.base_url is None:
            raise ValueError(
                f"Service {self.name!r} needs either base_url or explicit spec_url/health_url"
            )
        return self.base_url.rstrip("/")


@dataclass(frozen=True)
class MeshServiceRegistry:
    """The set of services an aggregator polls each pass — the human-maintained registry (mesh config).

    Mirrors .NET's ``MeshServiceRegistry``: the input a :class:`~benzene.mesh.MeshAggregator` reads (or a
    discovery source produces).
    """

    services: Sequence[MeshServiceEntry] = ()

    @classmethod
    def from_config(cls, entries: Sequence[Mapping[str, Any]]) -> MeshServiceRegistry:
        """Build a registry from plain dicts (e.g. parsed ``mesh.json``): ``[{"name", "baseUrl", ...}]``."""
        return cls(
            [
                MeshServiceEntry(
                    name=str(entry["name"]),
                    base_url=entry.get("baseUrl") or entry.get("base_url"),
                    spec_url=entry.get("specUrl") or entry.get("spec_url"),
                    health_url=entry.get("healthUrl") or entry.get("health_url"),
                    prefix=str(entry.get("prefix", "/benzene")),
                )
                for entry in entries
            ]
        )
