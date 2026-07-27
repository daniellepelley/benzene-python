"""GCP Cloud Functions host for the shared order domain (HTTP + Pub/Sub + egress)."""

from __future__ import annotations

from .host import build_gcp_orders_app

__all__ = ["build_gcp_orders_app"]
