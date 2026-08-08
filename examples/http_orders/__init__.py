"""Standalone HTTP host for the shared order domain (benzene-http ASGI + HTTP egress)."""

from __future__ import annotations

from .host import build_http_orders_app

__all__ = ["build_http_orders_app"]
