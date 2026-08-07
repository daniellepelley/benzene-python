"""The **Mesh Host** as a long-lived ASGI app for AWS (App Runner / Fargate), driven by the environment.

This is the hosted counterpart of :mod:`deploy.mesh.stack`'s in-process host: one long-running container
that *is* the collector + aggregator + UI, but wired to a **real AWS account** instead of localhost peers:

- **discovery** — by default it self-discovers the fleet from **tagged Lambda functions**
  (:class:`~benzene.mesh.aws.AwsLambdaDiscoveryProvider` over ``lambda:ListFunctions`` / ``ListTags``),
  reading each service's API base URL off its ``benzene:mesh-url`` tag; set ``BENZENE_MESH_DISCOVERY=static``
  to instead read a hand-written / Terraform-output registry from ``BENZENE_MESH_REGISTRY`` (JSON);
- **enrichment** — it constructs an :class:`~benzene.mesh.aws.XRayTopologySource` (real ``client → server``
  edges + latency percentiles from X-Ray's service graph) and a
  :class:`~benzene.mesh.aws.CloudWatchUsageSource` (per-topic usage from CloudWatch metrics) and hands them
  to the aggregator, so every pass carries real topology timing + usage;
- **feeds + auth** — it receives the services' ``register`` / ``heartbeat`` / ``traces`` feeds over its
  ``/benzene/invoke`` surface, guarded by the optional shared secret ``BENZENE_MESH_KEY``;
- **UI** — every non-``/benzene/*`` GET serves the freshly-emitted artifacts + the vendored ``mesh-ui.html``.

It exposes a single module-level ``app`` (an ASGI 3 application) run under **uvicorn**
(``uvicorn host_app:app``): a lifespan wrapper builds the :class:`~benzene.mesh.host.MeshHost` on startup
(discovery + enrichment + an initial aggregation pass), starts the background poll loop, and stops it on
shutdown. The MeshHost keeps its collector catalog **in memory in this one process**, so run exactly one
instance (App Runner min=max=1) — a multi-replica host would need a shared collector store (out of scope).

Config (all optional; sensible defaults):

=================================  =================================================================
Env var                            Meaning (default)
=================================  =================================================================
``BENZENE_MESH_DISCOVERY``         ``lambda`` (tag discovery) | ``static`` (``lambda``)
``BENZENE_MESH_REGISTRY``          static-mode registry JSON: ``[{"name","baseUrl"[,"prefix"]}]``
``BENZENE_MESH_TAG_KEY``           discovery filter tag a function must carry (``benzene``)
``BENZENE_MESH_KEY``               shared secret guarding the ingest feeds (unset → open)
``BENZENE_MESH_ENRICH``            ``1`` to wire X-Ray + CloudWatch, ``0`` to skip (``1``)
``BENZENE_MESH_POLL_INTERVAL``     seconds between aggregation passes (``60``)
``BENZENE_MESH_OUT_DIR``           where artifacts are written + served (``/tmp/mesh-artifacts``)
``BENZENE_MESH_UI_HTML``           path to the vendored ``mesh-ui.html`` (bundled in the image)
``BENZENE_MESH_ANNOTATIONS``       optional human-annotations JSON seeding the discussion artifact
=================================  =================================================================
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from typing import Any

from benzene.mesh.aggregator import MeshServiceRegistry
from benzene.mesh.host import MeshHost, MeshHostConfig

_LOGGER = logging.getLogger("benzene.mesh.aws.host")
logging.basicConfig(level=os.environ.get("BENZENE_LOG_LEVEL", "INFO"))

_DEFAULT_OUT_DIR = "/tmp/mesh-artifacts"  # noqa: S108 - a container-local scratch dir, by design
_DEFAULT_UI_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mesh-ui.html")


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _static_registry() -> MeshServiceRegistry:
    """Read the static registry from ``BENZENE_MESH_REGISTRY`` (JSON): ``[{"name","baseUrl"[,"prefix"]}]``."""
    raw = os.environ.get("BENZENE_MESH_REGISTRY", "[]")
    entries = json.loads(raw)
    _LOGGER.info("Static registry: %d service(s)", len(entries))
    return MeshServiceRegistry.from_config(entries)


def _discovery_provider() -> Callable[[], MeshServiceRegistry]:
    """A callable that self-discovers the fleet from tagged Lambda functions — called each pass.

    Re-discovering every pass (rather than once at boot) is what makes the tag-discovery deployment
    correct: the host can come up before the service Lambdas are fully created (they carry its URL) and
    still find them on the next pass, and it tracks the fleet as functions come and go.
    """
    from benzene.mesh.aws import (
        AwsLambdaDiscoveryProvider,
        Boto3LambdaClient,
        MeshDiscoveryFilter,
    )

    tag_key = os.environ.get("BENZENE_MESH_TAG_KEY", "benzene").strip()
    client = Boto3LambdaClient()  # one client, reused across passes
    filter_ = MeshDiscoveryFilter({tag_key: None})

    def discover() -> MeshServiceRegistry:
        registry = AwsLambdaDiscoveryProvider(client, filter=filter_).discover()
        _LOGGER.info(
            "Lambda discovery: %d service(s) tagged %r: %s",
            len(registry.services), tag_key, [s.name for s in registry.services],
        )
        return registry

    return discover


def _build_enrichment() -> tuple[Any, Any]:
    """Construct the X-Ray topology + CloudWatch usage sources from the account (or ``(None, None)``)."""
    if not _bool_env("BENZENE_MESH_ENRICH", True):
        return None, None
    from benzene.mesh.aws import (
        Boto3CloudWatchClient,
        Boto3XRayServiceGraphClient,
        CloudWatchUsageSource,
        XRayTopologySource,
    )

    topology = XRayTopologySource(Boto3XRayServiceGraphClient())
    usage = CloudWatchUsageSource(Boto3CloudWatchClient())
    return topology, usage


def _build_config() -> MeshHostConfig:
    ui_html = os.environ.get("BENZENE_MESH_UI_HTML", _DEFAULT_UI_HTML)
    annotations_raw = os.environ.get("BENZENE_MESH_ANNOTATIONS")
    annotations = json.loads(annotations_raw) if annotations_raw else ()
    topology, usage = _build_enrichment()

    # Static mode → a fixed registry baked from the environment; lambda mode → a re-discovery provider
    # called every pass (see _discovery_provider). The provider takes precedence when set.
    mode = os.environ.get("BENZENE_MESH_DISCOVERY", "lambda").strip().lower()
    if mode == "static":
        registry = _static_registry()
        provider = None
    else:
        registry = MeshServiceRegistry(())  # seeded empty; the provider fills it each pass
        provider = _discovery_provider()

    return MeshHostConfig(
        registry=registry,
        registry_provider=provider,
        out_dir=os.environ.get("BENZENE_MESH_OUT_DIR", _DEFAULT_OUT_DIR),
        ui_html=ui_html if os.path.isfile(ui_html) else None,
        poll_interval_seconds=float(os.environ.get("BENZENE_MESH_POLL_INTERVAL", "60")),
        mesh_key=os.environ.get("BENZENE_MESH_KEY") or None,
        annotations=annotations,
        topology_source=topology,
        usage_source=usage,
    )


class MeshHostAsgiApp:
    """ASGI wrapper: build the :class:`~benzene.mesh.host.MeshHost` on lifespan startup, then delegate HTTP.

    The bare :class:`~benzene.mesh.host.MeshHost` only speaks the ``http`` scope; this adds the ``lifespan``
    protocol uvicorn drives — building the host (discovery + enrichment), running one aggregation pass so
    the UI has data immediately, and starting the background poll loop on startup; stopping it on shutdown.
    """

    def __init__(self) -> None:
        self._host: MeshHost | None = None

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        kind = scope.get("type")
        if kind == "lifespan":
            await self._lifespan(receive, send)
            return
        if kind == "http":
            if self._host is None:  # started outside a lifespan-aware server — build lazily
                self._host = MeshHost(_build_config())
            await self._host(scope, receive, send)
            return
        raise ValueError(f"MeshHostAsgiApp handles 'lifespan'/'http' scopes, got {kind!r}")

    async def _lifespan(self, receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    config = _build_config()
                    self._host = MeshHost(config)
                    try:
                        await self._host.run_once()  # seed the UI before the first poll interval
                    except Exception:  # noqa: BLE001 - a bad first pass must not stop the host booting
                        _LOGGER.exception("Initial aggregation pass failed; will retry on the poll loop")
                    self._host.start_polling()
                    _LOGGER.info("Mesh Host started (polling every %ss)",
                                 config.poll_interval_seconds)
                    await send({"type": "lifespan.startup.complete"})
                except Exception as exc:  # noqa: BLE001
                    _LOGGER.exception("Mesh Host startup failed")
                    await send({"type": "lifespan.startup.failed", "message": str(exc)})
            elif message["type"] == "lifespan.shutdown":
                if self._host is not None:
                    await self._host.stop_polling()
                await send({"type": "lifespan.shutdown.complete"})
                return


#: The ASGI application uvicorn serves: ``uvicorn host_app:app --host 0.0.0.0 --port 8080``.
app = MeshHostAsgiApp()
