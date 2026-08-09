# `mesh_dashboard` — the observer side of the mesh

Where [`mesh_fleet`](../mesh_fleet) is the *sending* side (services reporting `register` + `traces`
into a collector), this example is the *observer* side: it assembles a realistic mesh in memory and
projects the collector's catalog into the full set of **mesh-ui artifacts** — the cross-language
read-model the canonical `mesh-ui.html` renders. It's the runnable, in-memory counterpart to what
[`deploy/mesh`](../../deploy/mesh/README.md) does on Fargate.

Two scenarios, each driving the projection from real collector state (no hand-written artifacts):

| Module | Scenario | What it shows |
|---|---|---|
| [`demo.py`](demo.py) | a healthy fleet under load | typed descriptors → real request/response **schemas** + a versioned topic; **heartbeats** → per-check health; real traced invocations → **topology** + **usage** |
| [`evolution.py`](evolution.py) | a fleet mid-rollout | the governance signals: **schema-changed**, **removedTopics**, **contractDrift**, **schemaMismatch** |
| [`profile.py`](profile.py) | a service self-checking | grade the wiring against the **Cloud Service Profile** (R1–R8) and carry the verdict on the descriptor's `profile` field |

## Run it

```bash
# Print the healthy-fleet manifest (and write the full artifact set to a temp dir):
python examples/mesh_dashboard/demo.py

# Print the mid-rollout governance signals:
python examples/mesh_dashboard/evolution.py
```

From Python:

```python
import asyncio
from mesh_dashboard.demo import write_demo_artifacts

asyncio.run(write_demo_artifacts("/tmp/mesh-ui", orders=5))
```

## View it in the real Mesh UI

The artifacts are data-driven: `mesh-ui.html` fetches `manifest.json` (and `topics.json`,
`topology.json`, `services/{name}.json`, …) by path relative to itself, so co-locate the vendored page
with the output and serve the directory over HTTP:

```bash
python - <<'PY'
import asyncio, shutil
from mesh_dashboard.demo import write_demo_artifacts
asyncio.run(write_demo_artifacts("/tmp/mesh-ui", orders=5))
shutil.copy("deploy/mesh/collector/ui/mesh-ui.html", "/tmp/mesh-ui/mesh-ui.html")
PY
python -m http.server 8080 --directory /tmp/mesh-ui
# open http://localhost:8080/mesh-ui.html
```

Swap `write_demo_artifacts` for `mesh_dashboard.evolution.write_evolution_artifacts` to render the
mid-rollout snapshot instead.

## What degrades to null

The projection never invents fields. With no metrics feed, the topology edges' `requestsPerMinute` /
`p50/p95/p99LatencyMs` and usage's `windowStartUtc` / `avgDurationMs` stay `null` (the UI renders those
as reduced) — the structural map, schemas, health, and exercise counts are fully populated. See the
[mesh reference](../../docs/reference/mesh.md) and the [Observing the mesh](../../docs/cookbooks/observing-the-mesh.md)
cookbook for the full contract.
