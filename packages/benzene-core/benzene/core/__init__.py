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
from .dependencies import Container, Lifetime, Scope
from .discovery import (
    CompositeHandlers,
    ExplicitHandlers,
    HandlerSource,
    ModuleHandlers,
    PackageHandlers,
    UnregisteredHandlerWarning,
    handlers_in_module,
    handlers_in_package,
    unregistered_handlers,
    warn_unregistered_handlers,
)
from .envelope import (
    VERSION_HEADER,
    BenzeneMessageApplication,
    encode_response,
    error_payload,
)
from .errors import MessageHandlingError
from .handler import Handler, HandlerDefinition, definition_of, message
from .mapping import encode_body, to_camel, to_jsonable, to_request
from .metadata import TOPIC_KEY, take_topic
from .pipeline import Middleware, MiddlewarePipeline, Next
from .registry import DuplicateHandlerError, Registry
from .startup import AppDefinition, BenzeneStartUp, build_application
from .router import message_router

__all__ = [
    "TOPIC_KEY",
    "take_topic",
    "warn_unregistered_handlers",
    "unregistered_handlers",
    "handlers_in_package",
    "handlers_in_module",
    "UnregisteredHandlerWarning",
    "PackageHandlers",
    "ModuleHandlers",
    "HandlerSource",
    "ExplicitHandlers",
    "BenzeneMessageApplication",
    "Container",
    "CompositeHandlers",
    "Context",
    "DuplicateHandlerError",
    "Handler",
    "HandlerDefinition",
    "Lifetime",
    "MessageHandlingError",
    "MessageSender",
    "Middleware",
    "MiddlewarePipeline",
    "Next",
    "AppDefinition",
    "BenzeneStartUp",
    "Registry",
    "build_application",
    "Scope",
    "VERSION_HEADER",
    "definition_of",
    "encode_body",
    "encode_response",
    "error_payload",
    "message",
    "message_router",
    "to_camel",
    "to_jsonable",
    "to_request",
]
