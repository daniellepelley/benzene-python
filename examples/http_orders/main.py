"""ASGI entry point for the standalone HTTP orders host.

``build_http_orders_app()`` returns a standard ASGI application, so any ASGI server hosts it::

    BENZENE_ORDERS_EVENTS_URL=http://localhost:9000 uvicorn http_orders.main:app

The single app serves ``POST /orders`` and ``GET /orders/{id}``; placing an order publishes
``orders:created`` to the downstream service over HTTP.
"""

from __future__ import annotations

from .host import build_http_orders_app

app = build_http_orders_app()
