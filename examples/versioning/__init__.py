"""Payload/handler versioning demo — handler-version dispatch by the ``benzene-version`` header."""

from __future__ import annotations

from .app import build_versioning_app
from .contracts import (
    ORDER_CREATE_TOPIC,
    V1,
    V2,
    CreateOrderV1,
    CreateOrderV2,
    OrderAcceptedV1,
    OrderAcceptedV2,
)
from .handlers import ProcessedLog

__all__ = [
    "ORDER_CREATE_TOPIC",
    "V1",
    "V2",
    "CreateOrderV1",
    "CreateOrderV2",
    "OrderAcceptedV1",
    "OrderAcceptedV2",
    "ProcessedLog",
    "build_versioning_app",
]
