"""Benzene — a Python port of the Benzene message-handler framework.

Spec-first: implements the language-neutral Benzene specification (core-concepts, wire-contracts,
transport-bindings) idiomatically in Python, rather than translating the C# API. See the README and
https://github.com/daniellepelley/Benzene/tree/main/docs/specification.
"""

from __future__ import annotations

from .container import Container, Lifetime, Scope
from .context import Context
from .handler import Handler, HandlerDefinition, message
from .http_status import from_http, to_http
from .envelope import BenzeneMessageApplication, encode_response, error_payload
from .pipeline import Middleware, MiddlewarePipeline, Next
from .registry import DuplicateHandlerError, Registry
from .result import Result
from .router import message_router
from .status import (
    FAILURE_STATUSES,
    KNOWN_STATUSES,
    SUCCESS_STATUSES,
    Status,
    is_successful,
)

__version__ = "0.0.1"

__all__ = [
    "BenzeneMessageApplication",
    "Container",
    "Context",
    "DuplicateHandlerError",
    "FAILURE_STATUSES",
    "Handler",
    "HandlerDefinition",
    "KNOWN_STATUSES",
    "Lifetime",
    "Middleware",
    "MiddlewarePipeline",
    "Next",
    "Registry",
    "Result",
    "SUCCESS_STATUSES",
    "Scope",
    "Status",
    "encode_response",
    "error_payload",
    "from_http",
    "is_successful",
    "message",
    "message_router",
    "to_http",
]
