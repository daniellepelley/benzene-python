"""The AWS **Lambda discovery source** — self-discover a mesh's services from tagged Lambda functions.

Ports .NET's ``Benzene.Mesh.Discovery.Aws.AwsLambdaDiscoveryProvider``: instead of hand-feeding the
aggregator a ``mesh.json``, enumerate the account's Lambda functions (paginated ``list_functions``), read
each function's tags (``list_tags``, with bounded concurrency), keep the ones matching a filter (by
default: they carry the ``benzene`` tag), and emit them as :class:`~benzene.mesh.MeshServiceEntry` records
into a :class:`~benzene.mesh.MeshServiceRegistry` the aggregator then polls.

**Interrogation transport.** The .NET provider binds each discovered function to the *Lambda-Invoke*
interrogation source (``LambdaMeshServiceSource`` calls the function directly, no HTTP). The Python
aggregator instead interrogates a service by fetching its ``/benzene/spec`` + ``/benzene/health`` **over
HTTP** (:class:`~benzene.mesh.SpecHealthSource`), so this port produces **HTTP registry entries**: a
discovered function contributes a :class:`~benzene.mesh.MeshServiceEntry` whose ``base_url`` is the HTTP
API the function fronts (a Lambda behind API Gateway / a Function URL), read from the ``benzene:mesh-url``
tag. This keeps discovery aligned with how the Python aggregator already reaches every other service — one
transport, HTTP — rather than introducing a second (Invoke) interrogation path just for discovered
functions. A function tagged for the mesh but *without* a reachable HTTP base URL is still emitted (so it
is visible as a member of the fleet); the aggregator records it as ``unreachable`` until a URL is supplied,
which is the honest state rather than a silent drop.

The optional ``benzene:mesh-path`` tag is carried through as the entry's path **prefix** (the segment the
well-known ``/spec`` and ``/health`` surfaces hang off — ``{base_url}{prefix}/spec``), mirroring the .NET
``SourceOptions["meshPath"]`` descriptor-path override for a service that relocated its ``/benzene`` prefix.

The AWS dependency is a minimal :class:`LambdaClient` :class:`~typing.Protocol` (two methods,
``list_functions`` / ``list_tags``); a unit test drives the provider with a hand-written fake and no
``boto3``. The real client is :class:`Boto3LambdaClient`, the only thing here that imports ``boto3``
(lazily), behind the ``benzene-mesh[aws]`` extra — mirroring :mod:`benzene.mesh.aws.xray`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..registry import MeshServiceEntry, MeshServiceRegistry

#: The tag a function must carry (by default) to be treated as a mesh member — the .NET ``DefaultTagKey``.
DEFAULT_TAG_KEY = "benzene"

#: The optional tag whose value overrides a service's ``/benzene`` path prefix (the .NET ``mesh-path``).
MESH_PATH_TAG = "benzene:mesh-path"

#: The optional tag carrying the HTTP base URL the function fronts (API Gateway stage / Function URL).
#: This is the Python port's interrogation seam: the aggregator fetches ``{base_url}{prefix}/spec`` over
#: HTTP, so a discovered function needs this tag to be reachable (see the module docstring).
MESH_URL_TAG = "benzene:mesh-url"

#: Upper bound on concurrent ``list_tags`` reads, so a large account can't fire hundreds of tag calls at
#: once and trip the Lambda control-plane's request-rate limit (the .NET ``MaxConcurrentTagReads``).
DEFAULT_MAX_CONCURRENT_TAG_READS = 8


@dataclass(frozen=True)
class MeshDiscoveryFilter:
    """Which discovered functions are mesh members — every required tag present, valued tags matched exactly.

    Defaults to "carries the ``benzene`` tag" (:data:`DEFAULT_TAG_KEY`, any value): a required tag whose
    value is ``None`` only needs to be *present*; a required tag with a concrete value must match exactly.
    Mirrors .NET's ``MeshDiscoveryFilter.Matches``.
    """

    required_tags: Mapping[str, str | None] = field(
        default_factory=lambda: {DEFAULT_TAG_KEY: None}
    )

    def matches(self, tags: Mapping[str, str]) -> bool:
        for key, value in self.required_tags.items():
            if key not in tags:
                return False
            if value is not None and tags[key] != value:
                return False
        return True


class LambdaClient(Protocol):
    """The two Lambda calls discovery needs: page the account's functions, and read one function's tags.

    A structural seam over ``boto3``'s Lambda client. ``list_functions`` returns the raw response (a
    ``"Functions"`` list of ``{"FunctionName", "FunctionArn", ...}`` plus an optional ``"NextMarker"``);
    ``list_tags`` returns ``{"Tags": {...}}`` for a function ARN. A test implements this with a fake;
    :class:`Boto3LambdaClient` implements it over ``boto3``.
    """

    def list_functions(self, *, marker: str | None = None) -> Mapping[str, Any]: ...

    def list_tags(self, *, resource: str) -> Mapping[str, Any]: ...


class Boto3LambdaClient:
    """A :class:`LambdaClient` backed by ``boto3``'s Lambda client (the only ``boto3`` import here).

    Pass a pre-built ``boto3.client("lambda")`` (or a compatible object), or leave it out to construct one
    lazily on first use — region and credentials then come from the ambient AWS environment, matching the
    .NET adapter's default client. Requires the ``benzene-mesh[aws]`` extra.
    """

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def _ensure(self) -> Any:
        if self._client is None:
            import boto3  # lazy: the [aws] extra's optional dependency

            self._client = boto3.client("lambda")
        return self._client

    def list_functions(self, *, marker: str | None = None) -> Mapping[str, Any]:
        kwargs: dict[str, Any] = {}
        if marker:
            kwargs["Marker"] = marker
        result: Mapping[str, Any] = self._ensure().list_functions(**kwargs)
        return result

    def list_tags(self, *, resource: str) -> Mapping[str, Any]:
        result: Mapping[str, Any] = self._ensure().list_tags(Resource=resource)
        return result


class AwsLambdaDiscoveryProvider:
    """Discovers mesh services from tagged AWS Lambda functions into a :class:`~benzene.mesh.MeshServiceRegistry`.

    Construct it with a :class:`LambdaClient` and (optionally) a :class:`MeshDiscoveryFilter`; call
    :meth:`discover` to enumerate the account's functions, read their tags, keep the matching ones, and
    emit their :class:`~benzene.mesh.MeshServiceEntry` records. ``list_functions`` is paged to the end via
    its ``NextMarker``; the per-function ``list_tags`` reads run over a bounded thread pool
    (:paramref:`max_concurrent_tag_reads`) and are collected **in source order**, so the discovered
    registry is stable across runs regardless of which tag read finishes first — matching the .NET
    provider's order-preserving ``Task.WhenAll``.
    """

    def __init__(
        self,
        client: LambdaClient,
        *,
        filter: MeshDiscoveryFilter | None = None,  # noqa: A002 - mirrors the .NET DiscoverAsync(filter)
        max_concurrent_tag_reads: int = DEFAULT_MAX_CONCURRENT_TAG_READS,
    ) -> None:
        self._client = client
        self._filter = filter or MeshDiscoveryFilter()
        self._max_concurrent_tag_reads = max(1, max_concurrent_tag_reads)

    def discover(self) -> MeshServiceRegistry:
        """Enumerate + tag-filter the account's Lambda functions into a :class:`~benzene.mesh.MeshServiceRegistry`."""
        functions = list(self._list_functions())
        # Bounded-concurrency, order-preserving tag reads: ThreadPoolExecutor.map keeps source order, so
        # the emitted registry is deterministic (the .NET provider's Task.WhenAll ordering guarantee).
        with ThreadPoolExecutor(max_workers=self._max_concurrent_tag_reads) as pool:
            tag_maps = list(pool.map(self._read_tags, functions))

        entries: list[MeshServiceEntry] = []
        for function, tags in zip(functions, tag_maps, strict=True):
            if self._filter.matches(tags):
                entries.append(_entry_for(function, tags))
        return MeshServiceRegistry(entries)

    def _list_functions(self) -> Iterable[Mapping[str, Any]]:
        marker: str | None = None
        while True:
            page = self._client.list_functions(marker=marker)
            yield from page.get("Functions", []) or []
            marker = page.get("NextMarker") or None
            if not marker:
                return

    def _read_tags(self, function: Mapping[str, Any]) -> Mapping[str, str]:
        arn = function.get("FunctionArn")
        if not arn:
            return {}
        response = self._client.list_tags(resource=str(arn))
        return response.get("Tags", {}) or {}


def _entry_for(function: Mapping[str, Any], tags: Mapping[str, str]) -> MeshServiceEntry:
    """Build one HTTP :class:`~benzene.mesh.MeshServiceEntry` from a discovered function + its tags."""
    name = str(function.get("FunctionName") or "")
    base_url = tags.get(MESH_URL_TAG) or None
    mesh_path = tags.get(MESH_PATH_TAG)
    prefix = mesh_path if mesh_path and mesh_path.strip() else "/benzene"
    return MeshServiceEntry(name=name, base_url=base_url, prefix=prefix)
