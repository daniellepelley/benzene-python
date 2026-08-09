"""What the dashboard shows when a fleet *changes over time* — the mesh's governance signals.

[`demo.py`](demo.py) renders a healthy steady state. This module renders the more interesting
snapshot: a fleet mid-rollout, where the collector's catalog surfaces the contract-evolution signals
the mesh-ui highlights. Everything here goes through the same public ingest API a real service's
``MeshFeedSender`` uses (``register`` + ``heartbeat``); only the *sequence* differs.

The scenario is a single moment during an ``orders`` redeploy:

- **schema-changed** — ``orders`` re-registers ``orders:place`` with a new request contract (a
  ``coupon`` field was added). ``topics.json`` flags the topic ``changes: [{"kind": "schema-changed"}]``
  and ``services/orders.json`` carries both ``specHash`` and ``previousSpecHash``.
- **removedTopics** — the new ``orders`` descriptor no longer provides ``orders:cancel`` (it moved
  elsewhere). No one provides it now, so it drops off the functional map into ``topics.removedTopics``.
- **contractDrift** — one old ``orders`` instance (``orders-1``) is still running the *previous*
  descriptor and heartbeats its old hash, while the redeployed ``orders-2`` beats the new one. The
  hash mismatch marks the service as drifted in ``manifest.json`` and ``services/orders.json`` until
  the last old task is replaced.
- **schemaMismatch** — two services provide ``inventory:reserve`` with *different* request contracts
  (a legacy service was never migrated). ``topics.json`` marks that topic ``schemaMismatch: true``.

Point the canonical ``mesh-ui.html`` at :func:`write_evolution_artifacts`'s output to see each signal
rendered.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from benzene.core import Registry
from benzene.mesh import (
    Heartbeat,
    MeshCollector,
    ServiceDescriptor,
    ServiceInfo,
    build_artifacts,
    write_artifacts,
)
from benzene.results import Result

# --- Contract shapes across the rollout -----------------------------------------------------------


@dataclass
class PlaceOrderV1:
    sku: str = ""


@dataclass
class PlaceOrderV2:  # the redeploy added a coupon field — a changed request contract
    sku: str = ""
    coupon: str = ""


@dataclass
class CancelOrder:
    order_id: str = ""


@dataclass
class ReserveStock:
    sku: str = ""
    quantity: int = 1


@dataclass
class ReserveStockLegacy:  # the un-migrated service's older, incompatible contract
    item: str = ""
    count: int = 1


async def _noop(_request: Any) -> Result:
    return Result.ok({})


def _descriptor(service: str, topics: dict[str, type]) -> ServiceDescriptor:
    """Derive a descriptor from a typed registry (topic id -> request dataclass)."""
    registry = Registry()
    for topic, request_type in topics.items():
        registry.register(topic, _noop, request_type=request_type)
    return ServiceDescriptor.derive(
        registry, ServiceInfo(service=service, service_version="1.0.0", placement={"cloud": "aws"})
    )


def build_evolving_mesh() -> MeshCollector:
    """A collector caught mid-rollout, exhibiting all four contract-evolution signals."""
    collector = MeshCollector()

    # 1. orders v1: provides orders:place (v1 contract) and orders:cancel. One instance beats it.
    orders_v1 = _descriptor("orders", {"orders:place": PlaceOrderV1, "orders:cancel": CancelOrder})
    collector.ingest_register(orders_v1.to_payload())
    collector.ingest_heartbeat(
        Heartbeat(
            service="orders",
            sent_at="2026-01-01T00:00:00Z",
            instance_id="orders-1",
            descriptor_hash=orders_v1.descriptor_hash(),
            is_healthy=True,
        ).to_payload()
    )

    # 2. orders redeploys: orders:place gains a coupon field, orders:cancel is dropped. The new
    #    instance orders-2 beats the new hash; the old orders-1 above never re-beat (the straggler).
    orders_v2 = _descriptor("orders", {"orders:place": PlaceOrderV2})
    collector.ingest_register(orders_v2.to_payload())
    collector.ingest_heartbeat(
        Heartbeat(
            service="orders",
            sent_at="2026-01-01T00:05:00Z",
            instance_id="orders-2",
            descriptor_hash=orders_v2.descriptor_hash(),
            is_healthy=True,
        ).to_payload()
    )

    # 3. Two providers of inventory:reserve with different request contracts — a schema mismatch the
    #    UI flags until the legacy service is migrated.
    inventory = _descriptor("inventory", {"inventory:reserve": ReserveStock})
    collector.ingest_register(inventory.to_payload())
    legacy = _descriptor("inventory-legacy", {"inventory:reserve": ReserveStockLegacy})
    collector.ingest_register(legacy.to_payload())

    return collector


def build_evolution_artifacts() -> dict[str, Any]:
    """The artifact set for the mid-rollout snapshot (see :func:`build_evolving_mesh`)."""
    return build_artifacts(build_evolving_mesh(), generated_at="2026-01-01T00:05:00Z")


def write_evolution_artifacts(directory: str | os.PathLike[str]) -> dict[str, Any]:
    """Write the mid-rollout artifact set under ``directory`` for the UI to render. Returns them."""
    return write_artifacts(directory, build_evolving_mesh(), generated_at="2026-01-01T00:05:00Z")


def _main() -> None:
    import json

    artifacts = build_evolution_artifacts()
    topics = {t["topic"]: t for t in artifacts["topics"]["topics"]}
    print("removedTopics:", artifacts["topics"]["removedTopics"])
    print("orders:place changes:", topics["orders:place"]["changes"])
    print("inventory:reserve schemaMismatch:", topics["inventory:reserve"]["schemaMismatch"])
    drift = {s["name"]: s["contractDrift"] for s in artifacts["manifest"]["services"]}
    print("contractDrift:", json.dumps(drift))


if __name__ == "__main__":
    _main()
