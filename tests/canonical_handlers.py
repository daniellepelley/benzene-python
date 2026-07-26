"""The canonical conformance handlers every runner must register (conformance/README.md).

They demonstrate idiomatic request mapping: dataclass request types constructed from the decoded
JSON body by the framework's request mapper.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from benzene import Registry, Result, message
from benzene.status import is_successful


@dataclass
class GreetRequest:
    name: str = ""


@dataclass
class StatusRequest:
    status: str = ""
    errors: list[str] = field(default_factory=list)


@message("conformance:greet", request_type=GreetRequest)
async def greet(request: GreetRequest) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


@message("conformance:status", request_type=StatusRequest)
async def status_handler(request: StatusRequest) -> Result:
    # Return the requested status verbatim: a payload for success, the given errors for failure.
    if is_successful(request.status):
        return Result(request.status, {"applied": request.status})
    return Result(request.status, None, tuple(request.errors))


def register_canonical(registry: Registry) -> Registry:
    return registry.add(greet).add(status_handler)
