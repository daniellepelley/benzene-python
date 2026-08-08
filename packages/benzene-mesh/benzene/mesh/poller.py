"""The mesh poller — a pull aggregator that feeds the :class:`MeshCollector` (mesh.md §5).

Where :class:`MeshFeedSender` is the *push* side (a service sends its register/heartbeat/traces feeds),
the poller is the *pull* side, mirroring the .NET Mesh Host: it reaches out to a configured fleet on a
timer, reads each service's well-known surfaces (``/benzene/spec`` + ``/benzene/health`` — exposed by
:class:`~benzene.http.StandardPaths`), and folds the result into the same collector. A service needs no
egress wiring to appear in the mesh; it only has to be *pollable*.

    poller = MeshPoller(collector, [
        HttpServiceSource("orders", "https://orders.svc"),
        HttpServiceSource("inventory", "https://inventory.svc"),
    ])
    await poller.poll_once()                 # one sweep; call on a timer for a live fleet
    fleet = collector.query_fleet({})        # now reflects both services

Pull covers identity, topics, and health. **Consumer-edge topology still comes from traces** (the push
feed), so a fleet that wants the call graph also pushes traces to the collector — the two feeds compose
in one collector. A source that is down is recorded as a failed :class:`PollResult` and never breaks the
sweep of the rest of the fleet.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .collector import MeshCollector

#: A GET fetch: a URL → ``(status_code, body_text)``. Injectable so a test drives it with no network.
HttpGet = Callable[[str], Awaitable[tuple[int, str]]]


class PollError(Exception):
    """Raised when a service source cannot be read (network error, non-JSON body)."""


@runtime_checkable
class ServiceSource(Protocol):
    """A pollable service: its name, and how to read its spec + health documents."""

    name: str

    async def fetch_spec(self) -> dict[str, Any]: ...

    async def fetch_health(self) -> dict[str, Any]: ...


@dataclass
class CallableServiceSource:
    """A source backed by two async callables — for tests, or a bespoke transport (e.g. Lambda invoke)."""

    name: str
    spec: Callable[[], Awaitable[dict[str, Any]]]
    health: Callable[[], Awaitable[dict[str, Any]]]

    async def fetch_spec(self) -> dict[str, Any]:
        return await self.spec()

    async def fetch_health(self) -> dict[str, Any]:
        return await self.health()


class HttpServiceSource:
    """Polls a service's ``{prefix}/spec`` and ``{prefix}/health`` over HTTP GET.

    ``fetch`` is the injectable GET (``async (url) -> (status, body)``); the default uses ``urllib`` on
    a worker thread, so a source needs no extra dependency. A ``503`` from the health surface still
    carries the aggregate body, so it is read as data, not an error.
    """

    def __init__(
        self, name: str, base_url: str, *, prefix: str = "/benzene", fetch: HttpGet | None = None
    ) -> None:
        self.name = name
        self._base = base_url.rstrip("/")
        self._prefix = prefix
        self._fetch = fetch or _stdlib_get()

    @property
    def spec_url(self) -> str:
        """The service's ``{prefix}/spec`` URL (also what the mesh-ui manifest links to)."""
        return f"{self._base}{self._prefix}/spec"

    @property
    def health_url(self) -> str:
        """The service's ``{prefix}/health`` URL (also what the mesh-ui manifest links to)."""
        return f"{self._base}{self._prefix}/health"

    async def fetch_spec(self) -> dict[str, Any]:
        return await self._get_json(self.spec_url)

    async def fetch_health(self) -> dict[str, Any]:
        return await self._get_json(self.health_url)

    async def _get_json(self, url: str) -> dict[str, Any]:
        status, body = await self._fetch(url)
        try:
            parsed = json.loads(body) if body else {}
        except (ValueError, TypeError) as exc:
            raise PollError(f"{url} returned a non-JSON body (status {status})") from exc
        if not isinstance(parsed, dict):
            raise PollError(f"{url} returned {type(parsed).__name__}, expected an object")
        return parsed


@dataclass(frozen=True)
class PollResult:
    """The outcome of polling one source in a sweep."""

    service: str
    ok: bool
    error: str | None = None


class MeshPoller:
    """Polls a fleet of :class:`ServiceSource` s and folds each into a :class:`MeshCollector`."""

    def __init__(self, collector: MeshCollector, sources: Iterable[ServiceSource]) -> None:
        self._collector = collector
        self._sources: list[ServiceSource] = list(sources)

    async def poll_once(self) -> list[PollResult]:
        """Poll every source once (concurrently) and ingest what came back. Never raises for one down
        service — its failure is a :class:`PollResult`, and the rest of the fleet still updates."""
        return list(await asyncio.gather(*(self._poll(source) for source in self._sources)))

    async def _poll(self, source: ServiceSource) -> PollResult:
        try:
            spec = await source.fetch_spec()
            health = await source.fetch_health()
        except Exception as exc:  # a down/unreachable service must not break the sweep
            return PollResult(source.name, ok=False, error=str(exc))
        service = str(spec.get("service") or source.name)
        topics = spec.get("topics", [])
        descriptor_hash = _spec_hash(service, topics)
        self._collector.ingest_register(
            {"service": service, "topics": topics, "descriptorHash": descriptor_hash}
        )
        # Carry the same hash on the heartbeat so the collector can match the running instance against
        # the registered contract (drift detection); the poller polls one instance per source.
        self._collector.ingest_heartbeat(
            {
                "service": service,
                "instanceId": source.name,
                "health": health,
                "descriptorHash": descriptor_hash,
            }
        )
        return PollResult(service, ok=True)


def _spec_hash(service: str, topics: Sequence[Any]) -> str:
    """A content hash over the polled contract, so the collector can still detect drift from a pull.

    Canonical JSON (sorted keys, no whitespace) over the service name + topics, matching the descriptor
    hash's shape (``"sha256:" + hex``) even though a spec carries no hash of its own.
    """
    canonical = json.dumps(
        {"service": service, "topics": list(topics)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _stdlib_get(*, timeout: float = 10.0) -> HttpGet:
    """A zero-dependency :data:`HttpGet` using ``urllib`` on a worker thread (GET)."""

    async def get(url: str) -> tuple[int, str]:
        def _do() -> tuple[int, str]:
            try:
                with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - fixed https/http base
                    return response.status, response.read().decode("utf-8")
            except (
                urllib.error.HTTPError
            ) as exc:  # a 503 health reply still carries the aggregate body
                return exc.code, exc.read().decode("utf-8")

        return await asyncio.to_thread(_do)

    return get
