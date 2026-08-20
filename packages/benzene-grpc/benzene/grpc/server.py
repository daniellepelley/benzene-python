"""The gRPC server binding — serve Benzene handlers over gRPC unary calls (transport-bindings §1).

A Benzene gRPC server is a **generic** gRPC handler: it dispatches by method name, and the method name
*is* the Benzene topic (the ``grpc`` topic source is "method"). Each unary call carries the message
``body`` as its bytes and the Benzene headers as request metadata; the response carries the mapped
``StatusCode`` and — always, success or failure — a ``benzene-status`` trailer with the raw status
verbatim, so the exact status survives the codes that collapse to one gRPC code. A **failure** also
carries a ``grpc-status-details-bin`` trailer with the problem's structured errors as a
``google.rpc.BadRequest`` (§4.2), because gRPC discards the body of a non-OK call.

    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    add_benzene_handler(server, BenzeneMessageApplication(registry))
    server.add_insecure_port("[::]:50051"); server.start()
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    application_from,
    successful_from,
)
from benzene.results import problem_errors

import grpc

from .codes import status_to_code
from .details import details_trailer
from .status import BENZENE_STATUS_TRAILER

#: The gRPC method-path prefix; the segment after it is the Benzene topic.
METHOD_PREFIX = "/benzene.Benzene/"


def method_for(topic: str) -> str:
    """The gRPC method path a topic is served/called on."""
    return METHOD_PREFIX + topic


def topic_for(method: str) -> str:
    """The Benzene topic a gRPC method path resolves to (the segment after the service prefix)."""
    if method.startswith(METHOD_PREFIX):
        return method[len(METHOD_PREFIX) :]
    return method.rsplit("/", 1)[-1]


class BenzeneGrpcHandler(grpc.GenericRpcHandler):
    """Dispatches every gRPC unary method to a :class:`~benzene.core.BenzeneMessageApplication`.

    The method name is the topic; request metadata is the headers; the response body is the handler's
    encoded payload, with the mapped ``StatusCode`` and the ``benzene-status`` trailer set.
    """

    def __init__(self, application: BenzeneMessageApplication) -> None:
        self._application = application

    @classmethod
    def from_definition(cls, definition: AppDefinition) -> BenzeneGrpcHandler:
        """Build the handler from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition))

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
        trailers: list[tuple[str, str | bytes]] = [(BENZENE_STATUS_TRAILER, status)]
        # isSuccessful is authoritative (wire-contracts.md 1.2) and is exactly what section 4.2's
        # mapping needs for an **application-defined** status: one the handler marked successful is
        # OK, not Internal. Classifying from the status text instead would answer Internal to a
        # caller the handler meant to answer successfully - and would leave to_grpc's rule for it
        # implemented but unreachable. Mapping first and branching on the resulting code (rather
        # than on the classification) keeps that single rule in one place: a status that maps to OK
        # sets no code and no details, whatever route it took there.
        code = status_to_code(status, successful_from(response))
        if code is not grpc.StatusCode.OK:
            problem = _problem(response)
            detail = _detail(response, problem)
            context.set_code(code)
            context.set_details(detail)
            # gRPC drops the body of a non-OK call, so the problem document's structured errors
            # would die here (section 4.2: they map onto google.rpc.BadRequest instead).
            # problem_errors is the shared section 1.3 decoder, so what goes on the trailer is what
            # an HTTP caller reads off the body. code.value is (number, name) - the numeric half is
            # what a google.rpc.Status carries.
            rich = details_trailer(code.value[0], detail, problem_errors(problem))
            if rich is not None:
                trailers.append(rich)
        context.set_trailing_metadata(tuple(trailers))
        return response["body"].encode("utf-8")


def _problem(response: dict[str, Any]) -> dict[str, Any]:
    """The failed response's body parsed as a problem document, or ``{}`` if it isn't one."""
    body = response.get("body") or ""
    try:
        parsed = json.loads(body) if body else {}
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _detail(response: dict[str, Any], problem: dict[str, Any]) -> str:
    if problem.get("detail"):
        return str(problem["detail"])
    return str(response["statusCode"])


def add_benzene_handler(server: Any, application: BenzeneMessageApplication) -> None:
    """Register a Benzene application on a ``grpc.Server`` — it then serves every topic as a method."""
    server.add_generic_rpc_handlers((BenzeneGrpcHandler(application),))
