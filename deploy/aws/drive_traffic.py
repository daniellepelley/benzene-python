"""Drive traffic at the deployed orders API so the mesh's edges form (orders → payments → shipping).

Each ``POST /orders`` makes ``orders`` call ``payments`` and ``shipping`` (and ``payments`` call
``shipping``) over their ``/benzene/invoke`` legs, forwarding the mesh span — so the collector derives
the consumer edges and X-Ray records the service-graph edges the host's :class:`XRayTopologySource`
reads. A few ``GET /orders`` reads exercise the leaf topic. Zero dependencies (stdlib ``urllib``), so it
runs anywhere without installing anything.

Usage (the orders API URL is a ``terraform output``)::

    python deploy/aws/drive_traffic.py https://abc123.execute-api.eu-west-1.amazonaws.com
    # or:  ORDERS_API_URL=... python deploy/aws/drive_traffic.py --creates 40 --lists 8

Then open the host URL (``terraform output host_url``) — within one poll interval the UI shows the fleet
with the orders → payments / orders → shipping / payments → shipping edges and real latency once X-Ray has
aggregated the window.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def _post(url: str, body: dict) -> tuple[int, str]:
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"content-type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def _get(url: str) -> tuple[int, str]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Drive the deployed Benzene mesh orders API.")
    parser.add_argument(
        "orders_api_url",
        nargs="?",
        default=os.environ.get("ORDERS_API_URL"),
        help="The orders API base URL (terraform output orders_api_url), or set ORDERS_API_URL.",
    )
    parser.add_argument("--creates", type=int, default=24, help="How many orders to POST.")
    parser.add_argument("--lists", type=int, default=6, help="How many GET /orders reads to do.")
    args = parser.parse_args(argv)

    if not args.orders_api_url:
        parser.error("provide the orders API base URL (argument or ORDERS_API_URL env var)")

    base = args.orders_api_url.rstrip("/")
    created = 0
    for i in range(args.creates):
        status, body = _post(
            f"{base}/orders",
            {"customerEmail": f"c{i}@example.com", "sku": "ABC-0001", "quantity": (i % 3) + 1},
        )
        if status in (200, 201):
            created += 1
        else:
            print(f"  POST /orders #{i} -> {status}: {body[:120]}", file=sys.stderr)

    for _ in range(args.lists):
        _get(f"{base}/orders")

    print(f"Drove {created}/{args.creates} orders + {args.lists} reads at {base}.")
    print("Open the host URL (terraform output host_url) to watch the mesh fill in.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
