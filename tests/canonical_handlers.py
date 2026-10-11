"""The canonical conformance handlers every runner must register (conformance/README.md).

They demonstrate idiomatic request mapping: dataclass request types constructed from the decoded
JSON body by the framework's request mapper, and typed responses so the Contract Document can derive
a request/response schema per topic. A required field is one with **no default** (so it appears in
the derived schema's ``required``); optional fields carry a default.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from benzene.core import Handler, Registry, message
from benzene.results import BenzeneError, ProblemDetails, Result, Status, is_successful


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
    return Result.failure(request.status, *request.errors)


@dataclass
class ProblemRequest:
    message: str  # required
    field: str | None = None  # optional
    code: str | None = None  # optional
    appType: str | None = None  # optional


@message("conformance:problem", request_type=ProblemRequest)
async def problem_handler(request: ProblemRequest) -> Result:
    """The canonical ``conformance:problem`` handler (conformance/README.md).

    Always a ``validation-error`` carrying exactly one structured error built from the request's
    message/field/code. When ``appType`` is given, the emitted problem document's ``type`` is that
    value verbatim instead of the registry URI - the application-authored-problem case
    (wire-contracts.md section 1.3); ``benzeneStatus`` is still ``validation-error`` and ``errors``
    still carries the one structured error either way.
    """
    error = BenzeneError(message=request.message, field=request.field, code=request.code)

    if request.appType:
        return Result.problem(
            ProblemDetails(
                benzene_status=Status.VALIDATION_ERROR,
                type=request.appType,
                errors=(error,),
            )
        )

    return Result.validation_error(error)


@message("conformance:versioned", version="v1", default=True)
async def versioned_v1(_request: dict) -> Result:
    """``conformance:versioned`` at ``v1``, the topic's declared default handler (conformance/README)."""
    return Result.ok({"version": "v1"})


@message("conformance:versioned", version="v2")
async def versioned_v2(_request: dict) -> Result:
    """``conformance:versioned`` at ``v2`` — reached only by ``benzene-version: v2``."""
    return Result.ok({"version": "v2"})


def validate_handler() -> Handler | None:
    """``conformance:validate``, validated through this port's own integration — the Pydantic
    adapter — or ``None`` when pydantic is not installed (its cases are then ``requires``-skipped).

    Rules (conformance/README.md): ``name`` non-empty, and every ``items[].sku`` present and
    non-empty. The handler body does no checking of its own: that is the integration's job.
    """
    try:
        from benzene.pydantic import validated
        from pydantic import BaseModel, Field
    except ImportError:
        return None

    class ValidateItem(BaseModel):
        sku: str = Field(min_length=1)

    class ValidateRequest(BaseModel):
        name: str = Field(min_length=1)
        items: list[ValidateItem] = Field(default_factory=list)

    @message("conformance:validate")
    @validated(ValidateRequest)
    async def validate(_request: ValidateRequest) -> Result:
        return Result.ok()

    return validate


#: Whether this environment can run the ``"requires": "validation"`` cases.
VALIDATION_AVAILABLE = validate_handler() is not None


@message("conformance:panic")
async def panic(request: dict) -> Result:
    """Throws unconditionally — pins that a handler panic is traced as ``service-unavailable``."""
    raise RuntimeError("conformance:panic always throws")


@dataclass
class LogEntry:
    message: str  # required — no default


# --- the canonical payload types of the contract-document cases (conformance/README.md) ----------
#
# Every property is always emitted, so every property is required: a nullable field has no default
# and is dumped as None. GreetRequest and GreetResponse above are the canonical ones too.


@dataclass
class Greeted:
    greeting: str


@dataclass
class LookupRequest:
    key: str


@dataclass
class LookupResponse:
    value: str


@dataclass
class ShapesChild:
    name: str


@dataclass
class ShapesRequest:
    count: int
    ratio: float
    active: bool
    tags: list[str]
    labels: dict[str, str]
    note: str | None
    child: ShapesChild
    nullable_child: ShapesChild | None  # wire name nullableChild


@dataclass
class TreeNode:
    name: str
    parent: TreeNode | None
    children: list[TreeNode]


#: The canonical payload types by their fixed names (conformance/README.md "Canonical payload
#: types"). The class names already are those names, so no declared name is needed.
CANONICAL_PAYLOAD_TYPES: dict[str, type] = {
    cls.__name__: cls
    for cls in (
        GreetRequest,
        GreetResponse,
        Greeted,
        LogEntry,
        LookupRequest,
        LookupResponse,
        ShapesRequest,
        ShapesChild,
        TreeNode,
    )
}


def register_canonical(registry: Registry) -> Registry:
    """Register the two canonical handlers (greet + status) — the ``benzene:spec`` cases' topic set."""
    return registry.add(greet).add(status_handler)


def register_canonical_with_problem(registry: Registry) -> Registry:
    """The canonical handlers plus ``conformance:problem`` (for the problem-details envelope cases).

    Separate from :func:`register_canonical` on purpose: the ``benzene:spec`` cases derive their
    document from greet and status alone, so the problem handler stays out of that registry. The
    same split .NET's runner makes.
    """
    return register_canonical(registry).add(problem_handler)


def register_all_canonical(registry: Registry) -> Registry:
    """Every canonical handler the envelope and problem-details cases run against (README): greet,
    status, problem, versioned (v1 default + v2), and validate when the integration is installed."""
    register_canonical_with_problem(registry).add(versioned_v1).add(versioned_v2)
    validate = validate_handler()
    if validate is not None:
        registry.add(validate)
    return registry


def register_with_panic(registry: Registry) -> Registry:
    """The canonical handlers plus ``conformance:panic`` (for the trace cases)."""
    return register_canonical(registry).add(panic)
