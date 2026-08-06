"""The gRPC server binding — serve Benzene handlers over gRPC unary calls (transport-bindings §1).

A Benzene gRPC server is a **generic** gRPC handler: it dispatches by method name, and the method name
*is* the Benzene topic (the ``grpc`` topic source is "method"). Each unary call carries the message
``body`` as its bytes and the Benzene headers as request metadata; the response carries the mapped
``StatusCode`` and — always, success or failure — a ``benzene-status`` trailer with the raw status
verbatim, so the exact status survives the codes that collapse to one gRPC code.

    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    add_benzene_handler(server, BenzeneMessageApplication(registry))
    server.add_insecure_port("[::]:50051"); server.start()
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.core import BenzeneMessageApplication
from benzene.results import is_successful

import grpc

from .codes import status_to_code
from .status import BENZENE_STATUS_TRAILER

#: The gRPC method-path prefix; the segment after it is the Benzene topic.
METHOD_PREFIX = "/benzene.Benzene/"


def method_for(topic: str) -> str:
    """The gRPC method path a topic is served/called on."""
    return METHOD_PREFIX + topic


def topic_for(method: str) -> str:
    """The Benzene topic a gRPC method path resolves to (the segment after the service prefix)."""
    if method.startswith(METHOD_PREFIX):
        return method[len(METHOD_PREFIX):]
    return method.rsplit("/", 1)[-1]


class BenzeneGrpcHandler(grpc.GenericRpcHandler):
    """Dispatches every gRPC unary method to a :class:`~benzene.core.BenzeneMessageApplication`.

    The method name is the topic; request metadata is the headers; the response body is the handler's
    encoded payload, with the mapped ``StatusCode`` and the ``benzene-status`` trailer set.
    """

    def __init__(self, application: BenzeneMessageApplication) -> None:
        self._application = application

    def service(self, handler_call_details: Any) -> Any:
        topic = topic_for(handler_call_details.method)
        return grpc.unary_unary_rpc_method_handler(
            lambda request, context: self._invoke(topic, request, context)
        )

    def _invoke(self, topic: str, request: bytes, context: Any) -> bytes:
        headers = {str(key): str(value) for key, value in context.invocation_metadata()}
        body = request.decode("utf-8") if request else ""
        response = asyncio.run(
            self._application.handle({"topic": topic, "headers": headers, "body": body})
        )
        status = response["statusCode"]
        context.set_trailing_metadata(((BENZENE_STATUS_TRAILER, status),))
        if not is_successful(status):
            context.set_code(status_to_code(status))
            context.set_details(_detail(response))
        return response["body"].encode("utf-8")


def _detail(response: dict[str, Any]) -> str:
    body = response.get("body") or ""
    try:
        parsed = json.loads(body) if body else {}
    except (ValueError, TypeError):
        return response["statusCode"]
    return parsed.get("detail") if isinstance(parsed, dict) and parsed.get("detail") else response["statusCode"]


def add_benzene_handler(server: Any, application: BenzeneMessageApplication) -> None:
    """Register a Benzene application on a ``grpc.Server`` — it then serves every topic as a method."""
    server.add_generic_rpc_handlers((BenzeneGrpcHandler(application),))
