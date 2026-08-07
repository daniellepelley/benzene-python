# `mesh_fleet` — a live Python fleet rendered in the canonical Mesh UI

A three-service Benzene mesh (`orders` / `payments` / `shipping`) that **produces the mesh-UI catalog
artifacts** and proves the canonical [Mesh UI](https://github.com/daniellepelley/Benzene/blob/main/docs/guides/mesh-ui.md)
renders a live Python fleet from them. This is the Python port's answer to the .NET `examples/Mesh`.

## The two data planes

The [`MeshArtifactEmitter`](../../packages/benzene-mesh/benzene/mesh/artifacts.py) writes the six
artifacts the UI reads (`manifest.json`, `services/<name>.json`, `topics.json`, `topology.json`,
`usage.json`, `annotations.json`) by combining:

1. **The catalog spine** — each service's derived `/benzene/spec`
   ([`ServiceSpec`](../../packages/benzene-core/benzene/core/spec.py)) and `/benzene/health`
   aggregate. Gives topics, schemas, HTTP mappings, health, and contract-drift.
2. **Live enrichment from the collector** — a [`MeshCollector`](../../packages/benzene-mesh/benzene/mesh/collector.py)
   that each service registers + heartbeats + traces into. Gives *who calls whom* (topology edges,
   derived from **trace parentage**) and *how much* (per-topic usage) — signals no service knows
   about itself.

`orders` calls `payments` and `shipping`, and `payments` calls `shipping`, each **forwarding its mesh
span**, so the collector derives the `orders → payments`, `orders → shipping`, and `payments → shipping`
edges. The three services demonstrate the three manifest states: `orders` healthy, `payments`
unhealthy **and** contract-drifted, `shipping` unreachable on its spec/health endpoints (but still
alive in the collector).

## Run the proof

Renders the fleet in **headless Chromium** (via Playwright) and asserts the live DOM shows the three
services, their health, and the collector-derived edge, then saves a screenshot to the repo root:

```bash
# from the repo root, with the namespace packages + examples on the path (the same PYTHONPATH the
# README uses for the conformance runner); an editable install of the packages works too.
PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers \
PYTHONPATH=packages/benzene-results:packages/benzene-core:packages/benzene-http:packages/benzene-mesh:examples \
  python -m mesh_fleet.prove
# -> ./mesh-fleet-proof.png
```

The browser binary is auto-detected under `PLAYWRIGHT_BROWSERS_PATH` — no `playwright install` needed.

The browser-free path (fleet → emitter → artifact-shape assertions) runs in the normal test suite:

```bash
pytest examples/mesh_fleet tests/test_mesh_artifacts.py
```

## Layout

| File | What it is |
|---|---|
| `domain.py` | Payload models + handler factories for the three services |
| `fleet.py` | Builds the in-process fleet, wires the collector, drives traffic → `build_fleet()` |
| `prove.py` | Emit artifacts, serve them next to the vendored UI, drive Chromium, screenshot |
| `mesh-ui.html` | A **verbatim vendored copy** of the canonical `mesh-ui/mesh-ui.html` (never forked) |
| `test_mesh_fleet.py` | Integration test: the fleet's traffic really populates the artifacts |
