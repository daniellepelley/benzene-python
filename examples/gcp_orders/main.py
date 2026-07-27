"""Google Cloud Functions entry points (what the Functions Framework / deploy target loads).

Deploy the HTTP function with entry point ``orders_http`` and the Pub/Sub function with entry point
``orders_pubsub``, both from this module. Locally:

    functions-framework --target orders_http --debug
    curl -X POST localhost:8080/orders -d '{"sku": "ABC", "quantity": 2}'
"""

from __future__ import annotations

from benzene.gcp import http_function, pubsub_function

from .host import build_gcp_orders_app

_app = build_gcp_orders_app()

orders_http = http_function(_app)
orders_pubsub = pubsub_function(_app)
