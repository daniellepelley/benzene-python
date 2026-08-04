"""``benzene.core`` — the transport-neutral message-handling engine.

The middle layer of the Benzene Python port (distribution ``benzene-core``): the handler registry
and ``@message`` decorator, the middleware pipeline, the per-invocation DI container, request/
response mapping, the message-router terminal middleware, and the ``BenzeneMessageApplication``
envelope entry point. Depends only on ``benzene-results``.

Install this to run handlers through the transport-neutral ``BenzeneMessage`` envelope without
pulling in any specific transport binding:

    pip install benzene-core

Mirrors .NET's ``Benzene.Core`` + ``Benzene.Core.MessageHandlers`` + ``Benzene.Dependencies``
(collapsed — the C# split existed for assembly isolation, which Python does not need). Contributes
the ``benzene.core`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .clients import MessageSender
from .context import Context
from .dependencies import Container, Lifetime, Scope, ServiceNotRegisteredError
from .envelope import (
    VERSION_HEADER,
    VERSION_HEADER_NAMES,
    BenzeneMessageApplication,
    encode_response,
    error_payload,
    resolve_version,
)
from .errors import MessageHandlingError
from .health import (
    HEALTH_TOPIC,
    DuplicateHealthCheckError,
    HealthCheck,
    HealthCheckResult,
    HealthChecks,
    HealthReport,
    health_interception,
)
from .handler import Handler, HandlerDefinition, definition_of, message
from .mapping import encode_body, to_camel, to_jsonable, to_request
from .pipeline import Middleware, MiddlewarePipeline, Next
from .registry import (
    DuplicateHandlerError,
    Registry,
    VersionSelector,
    exact_version,
    highest_version,
)
from .startup import AppDefinition, BenzeneStartUp, application_from, build_application
from .router import message_router

__all__ = [
    "BenzeneMessageApplication",
    "Container",
    "Context",
    "DuplicateHandlerError",
    "DuplicateHealthCheckError",
    "HEALTH_TOPIC",
    "Handler",
    "HandlerDefinition",
    "HealthCheck",
    "HealthCheckResult",
    "HealthChecks",
    "HealthReport",
    "Lifetime",
    "MessageHandlingError",
    "MessageSender",
    "Middleware",
    "MiddlewarePipeline",
    "Next",
    "AppDefinition",
    "BenzeneStartUp",
    "Registry",
    "application_from",
    "build_application",
    "Scope",
    "ServiceNotRegisteredError",
    "VERSION_HEADER",
    "VERSION_HEADER_NAMES",
    "VersionSelector",
    "definition_of",
    "encode_body",
    "encode_response",
    "error_payload",
    "exact_version",
    "health_interception",
    "highest_version",
    "message",
    "message_router",
    "resolve_version",
    "to_camel",
    "to_jsonable",
    "to_request",
]
