"""Fleet configuration for the Mesh Host (the .NET ``mesh.json`` analogue).

The host learns which services to poll from JSON — a file (``MESH_CONFIG``, default
``/etc/mesh/mesh.json``, bind-mounted in the container) or, for convenience, an inline
``MESH_SERVICES`` env var. Shape:

    {
      "pollIntervalSeconds": 30,
      "services": [
        {"name": "orders",    "baseUrl": "https://orders.example.com"},
        {"name": "inventory", "baseUrl": "https://inventory.example.com", "prefix": "/benzene"}
      ]
    }
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass

from benzene.mesh import CollectorStore, HttpServiceSource, JsonFileCollectorStore

DEFAULT_CONFIG_PATH = "/etc/mesh/mesh.json"
DEFAULT_POLL_INTERVAL = 30.0


@dataclass
class MeshConfig:
    sources: list[HttpServiceSource]
    poll_interval_seconds: float
    store: CollectorStore | None = None
    artifacts_dir: str | None = None


def load_config(env: Mapping[str, str] | None = None) -> MeshConfig:
    """Load the fleet config from ``MESH_CONFIG`` (a file) or ``MESH_SERVICES`` (inline JSON).

    ``MESH_STORE_PATH``, when set, points the collector at a durable snapshot file on the mounted
    volume, so the fleet view survives a task restart; unset keeps the catalog purely in-memory.
    ``MESH_ARTIFACTS_DIR``, when set, is where the host writes the mesh-ui artifacts after each poll
    and serves them (and the vendored UI) under ``/mesh-ui/``; unset disables the UI.
    """
    env = os.environ if env is None else env
    raw = _read_raw(env)
    services = raw.get("services", [])
    if not isinstance(services, list):
        raise ValueError("mesh config 'services' must be a list")
    sources = [
        HttpServiceSource(
            name=str(entry["name"]),
            base_url=str(entry["baseUrl"]),
            prefix=str(entry.get("prefix", "/benzene")),
        )
        for entry in services
    ]
    interval = float(raw.get("pollIntervalSeconds", DEFAULT_POLL_INTERVAL))
    store_path = env.get("MESH_STORE_PATH")
    store = JsonFileCollectorStore(store_path) if store_path else None
    return MeshConfig(
        sources=sources,
        poll_interval_seconds=interval,
        store=store,
        artifacts_dir=env.get("MESH_ARTIFACTS_DIR") or None,
    )


def _read_raw(env: Mapping[str, str]) -> dict:
    inline = env.get("MESH_SERVICES")
    if inline:
        return json.loads(inline)
    path = env.get("MESH_CONFIG", DEFAULT_CONFIG_PATH)
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)
