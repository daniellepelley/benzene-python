"""Bring the distributed mesh up on real localhost sockets, drive traffic, run one aggregation pass.

This is the multi-process shape of ``mesh_fleet``: four **separate ASGI apps** — the Mesh Host and the
three domain services — each on its own ephemeral localhost port, talking over genuine HTTP. The
sequence :func:`run_stack` performs is the whole demo:

1. build the three services (peers + host resolved lazily) and start each on its own port;
2. build the Mesh Host over a registry that points at those ports, and start it on its own port;
3. every service **registers + heartbeats into the host over HTTP**;
4. drive ``orders`` traffic over HTTP — ``orders`` calls ``payments`` and ``shipping``, ``payments`` calls
   ``shipping``, each forwarding its mesh span, so the collector can derive the topology;
5. every service **pushes its trace batch to the host over HTTP**;
6. the host's aggregator fetches each service's ``/benzene/spec`` + ``/benzene/health`` over HTTP, queries
   the co-hosted collector, and emits the six mesh-UI artifacts into the directory it serves.

The result: the host's UI URL renders the live multi-process fleet, with the fleet data having reached
the collector via real HTTP feed pushes between processes — the distinguishing win over the in-process
proof. :class:`Stack` hands back the host's base URL and a clean shutdown.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone

from benzene.http import stdlib_get_transport, stdlib_transport
from benzene.mesh import spec_hash
from benzene.mesh.aggregator import MeshServiceEntry, MeshServiceRegistry
from benzene.mesh.host import MeshHost, MeshHostConfig

from .asgi_server import RunningServer, serve
from .services import (
    DomainService,
    build_orders,
    build_payments,
    build_shipping,
)

# The peer each domain topic is called on (used to build orders'/payments' outbound url resolver).
_TOPIC_OWNER = {"payment:capture": "payments", "shipping:book": "shipping"}


@dataclass
class Stack:
    """A running distributed mesh: the host, the three services, their servers, and a shutdown."""

    host: MeshHost
    host_server: RunningServer
    services: dict[str, DomainService]
    servers: dict[str, RunningServer]
    out_dir: str
    created: list[int] = field(default_factory=list)

    @property
    def host_url(self) -> str:
        return self.host_server.base_url

    @property
    def ui_url(self) -> str:
        return f"{self.host_server.base_url}/mesh-ui.html"

    async def drive_traffic(self, *, creates: int = 12, lists: int = 5) -> None:
        """Drive ``orders`` over HTTP: ``creates`` orders (each fanning out) + ``lists`` reads."""
        post = stdlib_transport()
        get = stdlib_get_transport()
        orders = self.servers["orders"].base_url
        for i in range(creates):
            body = json.dumps(
                {"customerEmail": f"c{i}@example.com", "sku": "ABC-0001", "quantity": (i % 3) + 1}
            )
            reply = await post(f"{orders}/orders", {"content-type": "application/json"}, body)
            if reply.status_code in (200, 201):
                self.created.append(i)
        for _ in range(lists):
            await get(f"{orders}/orders")  # GET /orders → the get-all read (a leaf topic)

    async def flush_feeds(self) -> None:
        """Every service drains its trace buffer and pushes it to the host over HTTP."""
        for service in self.services.values():
            await service.flush_traces()

    async def aggregate(self, *, generated_at: datetime | None = None) -> dict:
        """Run one host aggregation pass (fetch spec/health, query collector, emit artifacts)."""
        return await self.host.run_once(generated_at=generated_at)

    async def close(self) -> None:
        await self.host.stop_polling()
        for server in self.servers.values():
            await server.close()
        await self.host_server.close()


async def run_stack(
    *,
    out_dir: str | None = None,
    ui_html: str,
    drive: bool = True,
    creates: int = 12,
    lists: int = 5,
    generated_at: datetime | None = None,
) -> Stack:
    """Start the four apps, wire them over HTTP, drive traffic, and run one aggregation pass."""
    out_dir = out_dir or tempfile.mkdtemp(prefix="mesh-host-")
    urls: dict[str, str] = {}

    def invoke_url(name: str) -> str:
        return f"{urls[name]}/benzene/invoke"

    def peer_url_for(topic: str) -> str:
        return invoke_url(_TOPIC_OWNER[topic])

    def host_invoke_url() -> str:
        return invoke_url("host")

    # 1. Build + start the three domain services (their peers/host resolve lazily from `urls`).
    services: dict[str, DomainService] = {
        "orders": build_orders(peer_url_for, host_invoke_url),
        "payments": build_payments(peer_url_for, host_invoke_url),
        "shipping": build_shipping(host_invoke_url),
    }
    servers: dict[str, RunningServer] = {}
    for name, service in services.items():
        servers[name] = await serve(service.app)
        urls[name] = servers[name].base_url

    # 2. Build + start the Mesh Host over a registry pointing at the now-bound service ports. Seed
    #    payments' previous spec hash to a different shape so contract-drift shows on the first pass.
    payments_previous = spec_hash(
        json.dumps({"service": "payments", "topics": []}, separators=(",", ":"), sort_keys=True)
    )
    registry = MeshServiceRegistry(
        [MeshServiceEntry(name=name, base_url=urls[name]) for name in services]
    )
    annotations = [
        {
            "id": "a1",
            "entity": "service:payments",
            "author": "Dani (PO)",
            "text": "Contract drift here is the planned v2 capture payload — expected. The gateway "
            "health failure is the real thing to chase.",
            "createdAtUtc": (generated_at or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z"),
        }
    ]
    host = MeshHost(
        MeshHostConfig(
            registry=registry,
            out_dir=out_dir,
            ui_html=ui_html,
            poll_interval_seconds=60.0,
            annotations=annotations,
            previous_hashes={"payments": payments_previous},
        )
    )
    host_server = await serve(host)
    urls["host"] = host_server.base_url

    stack = Stack(
        host=host, host_server=host_server, services=services, servers=servers, out_dir=out_dir
    )

    # 3. Every service registers + heartbeats into the host over HTTP.
    sent_at = (generated_at or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")
    for service in services.values():
        await service.announce(sent_at)

    if drive:
        # 4-5. Drive traffic, then push each service's traces to the host over HTTP.
        await stack.drive_traffic(creates=creates, lists=lists)
        await stack.flush_feeds()
        # 6. One aggregation pass emits the artifacts the host serves.
        await stack.aggregate(generated_at=generated_at)

    return stack
