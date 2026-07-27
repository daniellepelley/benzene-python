"""Azure Functions host for the shared order domain (HTTP + Service Bus + Event Hub + egress)."""

from __future__ import annotations

from .host import build_azure_orders_app

__all__ = ["build_azure_orders_app"]
