"""A gRPC host for the shared order domain — the "host anywhere" proof over gRPC.

Same handlers, same ``OrdersStartUp`` composition root as the three cloud examples; only this package
is gRPC-specific. Because the gRPC binding serves every topic as a unary method (method = topic), the
domain's HTTP routes ``POST /orders`` / ``GET /orders/{id}`` are reached here as the topics
``orders:place`` / ``orders:get`` — no per-route registration, one generic handler.
"""

from __future__ import annotations

from .host import build_grpc_orders_server

__all__ = ["build_grpc_orders_server"]
