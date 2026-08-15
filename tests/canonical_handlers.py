"""The canonical conformance handlers every runner must register (conformance/README.md).

They demonstrate idiomatic request mapping: dataclass request types constructed from the decoded
JSON body by the framework's request mapper, and typed responses so the mesh descriptor can derive
a request/response schema per topic. A required field is one with **no default** (so it appears in
the derived schema's ``required``); optional fields carry a default.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from benzene.core import Registry, message
from benzene.mesh import OutboundRegistry
from benzene.results import Result, is_successful


@dataclass
class GreetRequest:
    name: str  # required — no default, so the derived schema lists it in `required`


@dataclass
class GreetResponse:
    greeting: str


@dataclass
class StatusRequest:
    status: str  # required
    errors: list[str] = field(default_factory=list)  # optional — has a default


@dataclass
class StatusResponse:
    applied: str


@message("conformance:greet", request_type=GreetRequest, response_type=GreetResponse)
async def greet(request: GreetRequest) -> Result:
    return Result.ok(GreetResponse(greeting=f"Hello {request.name}"))


@message("conformance:status", request_type=StatusRequest, response_type=StatusResponse)
async def status_handler(request: StatusRequest) -> Result:
    # Return the requested status verbatim: a typed payload for success, the given errors for failure.
    if is_successful(request.status):
        return Result(request.status, StatusResponse(applied=request.status))
    return Result(request.status, None, tuple(request.errors))


@message("conformance:panic")
async def panic(request: dict) -> Result:
    """Throws unconditionally — pins that a handler panic is traced as ``service-unavailable``."""
    raise RuntimeError("conformance:panic always throws")


@dataclass
class LogRequest:
    message: str  # required — no default


def register_canonical(registry: Registry) -> Registry:
    """Register the two canonical handlers (greet + status) — the descriptor's exact topic set."""
    return registry.add(greet).add(status_handler)


def register_canonical_outbound(outbound: OutboundRegistry) -> OutboundRegistry:
    """Register the one canonical outbound topic (``conformance:log``, no handler anywhere) —
    what ``ServiceDescriptor.produces`` derives from in the descriptor fixture (mesh.md §2.3)."""
    return outbound.register("conformance:log", request_type=LogRequest)


def register_with_panic(registry: Registry) -> Registry:
    """The canonical handlers plus ``conformance:panic`` (for the trace cases)."""
    return register_canonical(registry).add(panic)
