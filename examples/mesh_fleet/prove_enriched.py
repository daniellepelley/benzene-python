"""Prove the mesh UI renders **AWS-enriched** artifacts: X-Ray latency percentiles + a CloudWatch usage feed.

The sibling of :mod:`mesh_fleet.prove`, but wiring the two AWS enrichment sources into the emitter. It
feeds **canned X-Ray service-graph** and **canned CloudWatch metric** responses through the *real*
:class:`~benzene.mesh.aws.XRayTopologySource` / :class:`~benzene.mesh.aws.CloudWatchUsageSource` (so the
actual mapping code runs — this is a proof of the adapters, not of hand-built edges), passes them to
:class:`~benzene.mesh.MeshArtifactEmitter` as ``topology_source`` / ``usage_source``, emits the six
artifacts, and drives headless Chromium over the canonical mesh UI. It then asserts against the live DOM
that the topology table now shows an ``xray`` source badge with **non-empty** P50/P95/P99 latency, and
that the usage feed carries ``cloudwatch``-sourced rows — the enrichment the collector plane can't provide.

Run it directly::

    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers python -m mesh_fleet.prove_enriched

It exits non-zero if any assertion fails. A screenshot is saved to ``mesh-enriched-proof.png``.
"""

from __future__ import annotations

import os
import shutil
from datetime import timedelta

from benzene.mesh import MeshArtifactEmitter
from benzene.mesh.aws import CloudWatchUsageSource, XRayTopologySource

from .fleet import build_fleet
from .prove import _HERE, _REPO_ROOT, _chromium_executable, _serve

_SCREENSHOT = os.path.join(_REPO_ROOT, "mesh-enriched-proof.png")

# One page.evaluate reading the DOM facts this proof asserts: the service health badges (unchanged by
# enrichment) and the topology rows *with* their latency cells (tds 5/6/7 are p50/p95/p99, rendered "–"
# when null) and the source badge — so the assertions below can check the xray edges carry real latency.
_EXTRACT_JS = """
() => {
  const cards = Array.from(document.querySelectorAll('article.svc'));
  const services = cards.map(c => {
    const badges = Array.from(c.querySelectorAll('.svc-head .badge')).map(b => b.className + '=' + b.textContent.trim());
    return { name: c.dataset.service, badges };
  });
  const rows = Array.from(document.querySelectorAll('#topology-table tbody tr'));
  const edges = rows.map(r => {
    const tds = r.querySelectorAll('td');
    const src = r.querySelector('.badge');
    return { client: tds[0] && tds[0].textContent.trim(), server: tds[1] && tds[1].textContent.trim(),
             source: src ? src.textContent.trim() : null,
             p50: tds[5] && tds[5].textContent.trim(), p95: tds[6] && tds[6].textContent.trim(),
             p99: tds[7] && tds[7].textContent.trim() };
  });
  return { services, edges };
}
"""


class _CannedXRay:
    """A canned X-Ray client: the service graph a real fleet would have produced over the last hour."""

    def get_service_graph(self, *, start_time, end_time, next_token=None):
        return {
            "Services": [
                {
                    "ReferenceId": 0,
                    "Name": "orders",
                    "Edges": [
                        {
                            "ReferenceId": 1,
                            "SummaryStatistics": {
                                "TotalCount": 5184,
                                "OkCount": 4251,
                                "ErrorStatistics": {"TotalCount": 900},
                                "FaultStatistics": {"TotalCount": 33},
                            },
                            "ResponseTimeHistogram": [
                                {"Value": 0.045, "Count": 500},
                                {"Value": 0.42, "Count": 40},
                                {"Value": 0.89, "Count": 10},
                            ],
                        },
                        {
                            "ReferenceId": 2,
                            "SummaryStatistics": {
                                "TotalCount": 1446,
                                "ErrorStatistics": {"TotalCount": 6},
                                "FaultStatistics": {"TotalCount": 0},
                            },
                            "ResponseTimeHistogram": [
                                {"Value": 0.012, "Count": 1200},
                                {"Value": 0.035, "Count": 200},
                                {"Value": 0.058, "Count": 46},
                            ],
                        },
                    ],
                },
                {
                    "ReferenceId": 1,
                    "Name": "payments",
                    "Edges": [
                        {
                            "ReferenceId": 2,
                            "SummaryStatistics": {
                                "TotalCount": 372,
                                "ErrorStatistics": {"TotalCount": 0},
                                "FaultStatistics": {"TotalCount": 0},
                            },
                            "ResponseTimeHistogram": [
                                {"Value": 0.008, "Count": 300},
                                {"Value": 0.015, "Count": 60},
                                {"Value": 0.022, "Count": 12},
                            ],
                        }
                    ],
                },
                {"ReferenceId": 2, "Name": "shipping", "Edges": []},
            ]
        }


# The canned CloudWatch feed: per (topic, transport, status) processed counts and duration sum/samples.
_CW_COUNTS = {
    ("orders:get-all", "AspNet", "ok"): 41230.0,
    ("orders:get-all", "AspNet", "service-unavailable"): 310.0,
    ("orders:create", "AspNet", "created"): 8460.0,
    ("orders:create", "Sqs", "created"): 2150.0,
    ("orders:create", "Sqs", "validation-error"): 94.0,
    ("payment:capture", "Sqs", "ok"): 10290.0,
    ("payment:capture", "Sqs", "service-unavailable"): 412.0,
}
_CW_AVG_MS = {
    ("orders:get-all", "AspNet", "ok"): 14.2,
    ("orders:get-all", "AspNet", "service-unavailable"): 2101.4,
    ("orders:create", "AspNet", "created"): 38.1,
    ("orders:create", "Sqs", "created"): 41.7,
    ("orders:create", "Sqs", "validation-error"): 9.3,
    ("payment:capture", "Sqs", "ok"): 55.0,
    ("payment:capture", "Sqs", "service-unavailable"): 1893.0,
}


class _CannedCloudWatch:
    """A canned CloudWatch client backed by the tables above (proves the list→sum→map path end to end)."""

    def list_metrics(self, *, namespace, metric_name):
        dims = [
            [
                {"Name": "topic", "Value": topic},
                {"Name": "transport", "Value": transport},
                {"Name": "status", "Value": status},
            ]
            for (topic, transport, status) in _CW_COUNTS
        ]
        return {"Metrics": [{"Dimensions": d} for d in dims]}

    def get_metric_statistics(
        self, *, namespace, metric_name, dimensions, start_time, end_time, period, statistics
    ):
        key = tuple(d["Value"] for d in dimensions)
        if metric_name.endswith("processed"):
            return {"Datapoints": [{"Sum": _CW_COUNTS.get(key, 0.0)}]}
        count = _CW_COUNTS.get(key, 0.0)
        avg = _CW_AVG_MS.get(key, 0.0)
        return {"Datapoints": [{"Sum": avg * count, "SampleCount": count}]}


def emit_enriched(out_dir: str) -> dict:
    """Build the fleet, wire the canned X-Ray + CloudWatch sources into the emitter, and emit the artifacts."""
    fleet = build_fleet()
    topology_source = XRayTopologySource(_CannedXRay(), window=timedelta(hours=1))
    usage_source = CloudWatchUsageSource(_CannedCloudWatch())
    emitter = MeshArtifactEmitter(
        fleet.services,
        fleet.collector,
        generated_at=fleet.generated_at,
        window_start=fleet.generated_at - timedelta(hours=24),
        window_end=fleet.generated_at,
        topology_source=topology_source,
        usage_source=usage_source,
    )
    manifest = emitter.emit(out_dir)
    shutil.copy(os.path.join(_HERE, "mesh-ui.html"), os.path.join(out_dir, "mesh-ui.html"))
    return manifest


def _drive_and_assert(port: int, screenshot: str) -> dict:
    """Load the UI in headless Chromium, assert the enriched DOM, and screenshot it. Returns the DOM facts."""
    from playwright.sync_api import sync_playwright

    url = f"http://127.0.0.1:{port}/mesh-ui.html"
    executable = _chromium_executable()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=executable, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1280, "height": 1600})
        page.goto(url, wait_until="networkidle")
        page.wait_for_selector("article.svc", timeout=15000)
        page.wait_for_selector("#topology-table tbody tr", state="attached", timeout=15000)
        facts = page.evaluate(_EXTRACT_JS)
        # Expand the (collapsed-by-default) Topology section so the latency-bearing edge table is visible
        # in the screenshot — the assertions read it from the DOM regardless of visibility.
        toggle = page.query_selector('button[data-sec-toggle="topology-content"]')
        if toggle:
            toggle.click()
            page.wait_for_timeout(400)
        page.screenshot(path=screenshot, full_page=True)
        browser.close()

    _assert_enriched(facts)
    return facts


_EMPTY_CELL = {"–", "-", "", "—"}


def _assert_enriched(facts: dict) -> None:
    # Services are unchanged by enrichment — the same three demonstrated states.
    names = {svc["name"] for svc in facts["services"]}
    assert names == {"orders", "payments", "shipping"}, f"expected the 3 Python services, got {names}"

    xray_edges = [e for e in facts["edges"] if e["source"] == "xray"]
    assert xray_edges, f"expected xray-sourced topology edges, got {facts['edges']}"
    for edge in xray_edges:
        for key in ("p50", "p95", "p99"):
            value = edge.get(key)
            assert value and value not in _EMPTY_CELL, (
                f"xray edge {edge['client']}→{edge['server']} shows empty {key}: {value!r}"
            )
    keys = {(e["client"], e["server"], e["source"]) for e in facts["edges"]}
    assert ("orders", "payments", "xray") in keys, f"orders→payments xray edge missing: {keys}"


def main() -> None:
    work = os.path.join(_REPO_ROOT, ".mesh-enriched-artifacts")
    os.makedirs(work, exist_ok=True)
    emit_enriched(work)
    httpd, port = _serve(work)
    try:
        facts = _drive_and_assert(port, _SCREENSHOT)
    finally:
        httpd.shutdown()

    print("Mesh UI rendered the AWS-enriched Python fleet:")
    print("Topology edges (X-Ray-enriched):")
    for edge in facts["edges"]:
        latency = " ".join(
            f"{k}={edge.get(k)}" for k in ("p50", "p95", "p99") if edge.get(k)
        )
        print(f"  - {edge['client']} -> {edge['server']} [{edge['source']}] {latency}".rstrip())
    print(f"Screenshot: {_SCREENSHOT}")


if __name__ == "__main__":
    main()
