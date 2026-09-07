# Parity gap analysis — core framework depth, codegen and developer tooling

**Reference:** `/workspace/benzene-dotnet` (.NET port)
**Target:** `/home/user/benzene-python` — `packages/benzene-core`, `packages/benzene-codegen-client`, `packages/benzene-testing`
**Scope:** `Benzene.Core*`, `Benzene.Abstractions*`, `Benzene.CodeGen.*`, `Benzene.EventSourcing*`, `Benzene.MapReduce`, `Benzene.Spec.Ui`, `Benzene.Testing`, `Benzene.Grpc.Versioning`
**Date:** 2026-09-07. Analysis only — no code was changed.

---

## Headline

Most of this domain is .NET plumbing that must **not** be ported (see the long
"Not worth porting" section — it is the majority of the surface, and that is the correct
finding). But the sweep turned up one thing that is not a parity nicety at all:

> **The language-neutral spec has moved and this port has not.** `.NET`'s vendored conformance
> fixtures are at spec commit `d7aed44057302707fb0a56158af5fed259c9908b`; this repo's
> `conformance/SPEC_VERSION` is `b732a743b391248d28c8f08b7283e8e1457f9c3b`. The newer spec
> rewrites the **failure wire body** to RFC 9457 problem details and adds an `isSuccessful`
> field to the response envelope. Python emits neither. A .NET Benzene client calling a Python
> Benzene service **throws** on any failure response (it deserializes the body into
> `ProblemDetails`, whose `Status` member is `int?`, and Python sends `"status": "bad-request"`).

That is Gap 1 and it dominates everything else here.

---

## Gap index

| # | Gap | Production severity |
|---|-----|---------------------|
| 1 | RFC 9457 problem-details failure body + structured `BenzeneError` + envelope `isSuccessful` | **critical** |
| 2 | No Contract Document producer — the codegen loop is open at the producer end | **high** |
| 3 | No `benzene diff` contract-compatibility CI gate | **high** |
| 4 | No pipeline exception containment (`use_exception_handler`) | **high** |
| 5 | Versioning: no eager cast-graph validation, no auto field-mapping caster | **medium** |
| 6 | Codegen: no remote spec sources (`--url`) in the CLI | **medium** |
| 7 | Codegen: no Markdown/README output from a Contract Document | **medium** |
| 8 | Unknown-status `isSuccessful` row in the HTTP/gRPC mapping tables (cross-domain flag) | **medium** |
| 9 | Scatter-gather (map-reduce) helper over a `MessageSender` | **low–medium** |
| 10 | Codegen: no handler-stub scaffolding (`message-handlers` output) | **low** |
| 11 | Event-sourcing store port (`EventStore` protocol + in-memory + DynamoDB) | **low** |
| 12 | Spec UI (topic-centric browsable viewer) | **low** |
| 13 | Pipeline branching / request–response taps | **low** |
| 14 | Profile-probe CLI polish (`--fail-on`, console script) | **low** |

---

## Gap 1 — RFC 9457 problem-details failure body, structured errors, and envelope `isSuccessful`

**Severity: CRITICAL.** This is a live cross-language interop break, not a feature gap.

### ⚠ Wire-contract flag

This touches the **frozen wire contract and the conformance fixtures**. It is not a unilateral
change: it is a **spec-version bump**, from `b732a743…` to `d7aed440…`. Do not hand-edit
`conformance/*.json`; re-vendor them from the canonical `benzene` spec repo
(`docs/specification/conformance/*.json`) at the new commit and update
`conformance/SPEC_VERSION`, exactly as .NET's `test/conformance-fixtures/README.md` describes.
Coordinate with whoever owns the wire-contract workstream before landing; the *analysis* below
is complete enough to implement once that call is made.

### What .NET has

- `src/Benzene.Abstractions/Results/BenzeneError.cs` — one structured error:
  `Message`, optional `Field` (property path / JSON Pointer), optional `Code`
  (machine-readable, producer-specific, e.g. FluentValidation's `ErrorCode`).
  `IBenzeneResult.Errors` is `IReadOnlyList<BenzeneError>`, never null.
- `src/Benzene.Results/ProblemDetails.cs` — the RFC 9457 document Benzene emits **in place of the
  payload whenever a result is unsuccessful, on every transport**. Members, all optional, all
  omitted (not `null`) when absent: `type`, `title`, `status` (HTTP int — **HTTP bindings only**),
  `detail`, `instance`, `benzeneStatus`, `errors[]`.
- `src/Benzene.Results/ProblemTypes.cs` — the spec-pinned registry: `type` URI + `title` +
  HTTP status per failure status. Base URI `https://benzene.app/problems/`.
- `src/Benzene.Clients/Common/ClientResultExtensions.cs` — the reader: prefers the envelope's
  `isSuccessful` (authoritative per wire-contracts §1.2), falls back to known-status
  classification when absent; deserializes a failure body as `ProblemDetails` and rebuilds the
  result's structured errors from `errors[]`, falling back to a single message-only error from
  `detail` for an older producer.
- `test/conformance-fixtures/envelope-cases.json` and `problem-details-cases.json` pin all of it,
  including `bodyExclude` negatives (`status` and `instance` MUST be absent off HTTP).

### What Python has

- `packages/benzene-results/benzene/results/result.py` — `Result.errors` is
  `tuple[str, ...]`. **Plain strings.** No `BenzeneError`, no `field`, no `code`, no
  `ProblemDetails`, no problem-type registry.
- `packages/benzene-core/benzene/core/envelope.py`:
  - `error_payload()` emits `{"status": result.status, "detail": ", ".join(result.errors)}` —
    where `status` carries the *Benzene status string*. Under the new spec `status` means the
    HTTP status **integer** and MUST be omitted off HTTP; the Benzene status belongs in
    `benzeneStatus`. Python is emitting a spec member with the wrong type and the wrong meaning.
  - `encode_response()` returns `{statusCode, headers, body}` — **no `isSuccessful`**
    (`grep -rn "isSuccessful" packages/` returns nothing).
  - `decode_response()` reconstructs errors by splitting `detail` on `", "` — lossy, and blind to
    a peer's authoritative `errors[]` array.
- `packages/benzene-http/benzene/http/app.py:222` emits the same `{status, detail}` body over
  HTTP, with no `application/problem+json` content type.
- `packages/benzene-pydantic/benzene/pydantic/validation.py` has per-field validation failures
  available and throws the field away into a flat string.

### Consequences today

1. **.NET client → Python service, any failure: hard exception.**
   `serializer.Deserialize<ProblemDetails>(body)` on `{"status":"bad-request",…}` binds the JSON
   string `"bad-request"` to `int? Status` → `JsonException`.
2. **Application-defined success statuses misclassify.** With no envelope `isSuccessful`, a .NET
   peer falls back to its own known-status table, so a Python service returning an extension
   status that *is* successful is read as a failure.
3. **Validation detail is destroyed on the wire.** A pydantic failure knows the field; the
   consumer receives one joined sentence.

### Implementation spec

**Package `benzene-results`** (no new deps).

New module `benzene/results/errors.py`:

```python
@dataclass(frozen=True)
class BenzeneError:
    message: str
    field: str | None = None
    code: str | None = None

    @classmethod
    def coerce(cls, value: "BenzeneError | str") -> "BenzeneError": ...
    def to_payload(self) -> dict[str, Any]:
        """camelCase, omitting absent members: {"message":…, "field"?:…, "code"?:…}"""
    @classmethod
    def from_payload(cls, raw: Mapping[str, Any] | str) -> "BenzeneError": ...
```

Change `Result.errors` to `tuple[BenzeneError, ...]`, coercing in `__post_init__` so every
existing `Result.bad_request("msg")` / `result_with_errors(status, ["a","b"])` call site keeps
working untouched. **Back-compat requirement:** keep a `Result.error_messages -> tuple[str, ...]`
property and make `", ".join(...)` sites use it — several packages join `result.errors` today
(`benzene-core/envelope.py`, `benzene-otel/response_events.py`); they must be updated, not left
to stringify dataclasses. Add failure factories accepting either form:
`Result.bad_request(BenzeneError("Name required", field="name", code="NotEmpty"))`.

New module `benzene/results/problems.py`:

```python
PROBLEM_BASE_URI = "https://benzene.app/problems/"

#: status -> (type URI, title, http status). Exactly the 11 rows in
#: conformance/problem-details-cases.json's `registry`.
PROBLEM_REGISTRY: Mapping[str, tuple[str, str, int]] = {...}

def problem_type(status: str) -> str | None:      # None for an app-defined status
def problem_title(status: str) -> str | None:
def problem_http_status(status: str) -> int:      # unknown -> 500

@dataclass(frozen=True)
class ProblemDetails:
    type: str | None = None
    title: str | None = None
    status: int | None = None          # HTTP bindings ONLY
    detail: str | None = None
    instance: str | None = None
    benzene_status: str | None = None
    errors: tuple[BenzeneError, ...] = ()

    @classmethod
    def from_result(cls, result: Result[Any], *, http_status: int | None = None,
                    type_override: str | None = None) -> "ProblemDetails": ...
    def to_payload(self) -> dict[str, Any]:
        """Omit every absent member. Never emit JSON null. Key `benzeneStatus`."""
    @classmethod
    def from_payload(cls, raw: Mapping[str, Any]) -> "ProblemDetails": ...
```

Rules the fixtures pin:
- `type` = `type_override` if the application supplied one, else `problem_type(status)`, else omitted.
- `benzeneStatus` = the result status verbatim, **always** on framework-produced documents.
- `status` = present **only** when an HTTP binding built the document, and then equal to the
  actual HTTP response code. Off HTTP it must be **absent** (`bodyExclude: ["status"]`).
- `instance` is never fabricated by the framework (`bodyExclude: ["instance"]` in the
  problem-details cases).
- `detail` = `", ".join(error.message for error in errors)` when there are errors, else omitted.
- `errors` omitted when empty; otherwise ordered, each entry omitting absent `field`/`code`.
- Success results produce **no** problem document at all.

**Package `benzene-core`** — `benzene/core/envelope.py`:
- Reimplement `error_payload(result, *, http_status=None)` as
  `ProblemDetails.from_result(...).to_payload()`. Keep the name — it is exported from
  `benzene.core.__all__` and referenced by `benzene-openapi`.
- `encode_response()` adds `"isSuccessful": result.is_successful` to the envelope dict.
- `decode_response()` reads `isSuccessful` when present (authoritative), falling back to
  `is_successful(status)`; parses a failure body via `ProblemDetails.from_payload` and takes
  `errors[]` when non-empty, else a single message-only error from `detail`.
- Update `benzene-openapi/generator.py`'s `_ERROR_SCHEMA` to the full problem-details shape.

**Package `benzene-http`** — `app.py`: build the problem document with the resolved HTTP status
in `status`, and set `content-type: application/problem+json` on failure responses (the
`httpRules` block of `problem-details-cases.json`). Success responses unchanged
(`application/json`).

**Package `benzene-pydantic`** — map each pydantic `ValidationError` entry to a
`BenzeneError(message=msg, field=".".join(str(x) for x in loc), code=err["type"])`.

**Conformance / tests**
- Re-vendor `conformance/*.json` at spec `d7aed440…`; add `problem-details-cases.json`;
  update `conformance/SPEC_VERSION`.
- `tests/conformance_runner.py`: teach the envelope runner (a) exact `isSuccessful` assertion,
  (b) `bodyExclude` (named members must be **absent** from the parsed body), and (c) a new
  `run_problem_details_cases()` covering the `registry` rows, the `envelopeCases`, and — since
  this port ships HTTP — the `httpRules` block. Register a canonical `conformance:problem`
  handler that turns `{message, field?, code?, appType?}` into a `validation-error` result with
  one structured error and an optional application `type`.
- Unit tests: `BenzeneError` coercion and payload omission; `ProblemDetails.from_result` for a
  registry status, an app-defined status, and an HTTP-bound document; round-trip
  `encode_response` → `decode_response` preserving `field`/`code` and ordering.

---

## Gap 2 — No Contract Document producer

**Severity: HIGH.**

### What .NET has

The `benzene`-format spec document (`docs/spec.md`) **is** the Contract Document:
`{openapi, info, messageEndpoint?, requests[], events[], components.schemas}`. It is served live
from the `spec` topic (`GET /spec?type=benzene`) by `Benzene.Schema.OpenApi`, and emitted as a
build artifact `{Service}.spec.json` by the `benzene-descriptor` dotnet tool from a **built,
non-running** assembly (`docs/contract-artifacts.md`). That one document is the input to every
downstream tool: client codegen, `benzene diff`, the Spec UI, the Lambda test-payload generator.

### What Python has

`packages/benzene-codegen-client/benzene/codegen_client/document.py` **parses** the Contract
Document, conformantly — `conformance/contract-document-cases.json` and
`contract-hash-cases.json` are byte-identical to .NET's. But nothing in the port **emits** one.
`benzene.core.ServiceSpec.to_payload()` (`packages/benzene-core/benzene/core/spec.py`) emits a
different, ad-hoc shape — `{"service", "topics":[{"id","version","requestSchema","responseSchema"}]}`
— and that is what `/benzene/spec` serves (`packages/benzene-http/benzene/http/standard.py`).

So: **a Python service cannot produce the artifact that this port's own codegen tool consumes.**
Every `benzene-codegen` invocation today is fed a document produced by something else. The
source-of-truth loop that makes the whole tooling story work is open at the producer end.

Everything needed to close it already exists in the port: `Registry` (requests),
`HttpRouter.definitions()` (`httpMappings`), `benzene.mesh.OutboundRegistry` (events),
`benzene.core.json_schema` (component schemas), and `codegen_client.contract_hash.compute`.

### Implementation spec

Put it in **`benzene-openapi`** (it already owns "project the registry into a standard
document", already depends on `benzene-core` + `benzene-http`, and this keeps `benzene-core`
free of the extra surface). New module `benzene/openapi/contract.py`:

```python
def contract_document(
    registry: Registry,
    *,
    service: str,
    version: str = "1.0.0",
    description: str = "",
    router: Any | None = None,              # benzene.http.HttpRouter, for httpMappings
    outbound: Any | None = None,            # benzene.mesh.OutboundRegistry, for events[]
    message_endpoint: str | None = None,    # e.g. StandardPaths().invoke_path
    include_reserved: bool = True,
) -> dict[str, Any]: ...
```

Output shape (contract-document.md §§1–4; mirror `conformance/contract-document-cases.json`'s
`documents` exactly):

```json
{
  "openapi": "3.0.1",
  "info": {"title": "<service>", "description": "", "version": "<version>"},
  "messageEndpoint": "/benzene/invoke",
  "requests": [
    {"topic": "orders:create", "version": "v2",
     "httpMappings": [{"method": "POST", "path": "/orders"}],
     "reserved": false,
     "request":  {"$ref": "#/components/schemas/CreateOrder"},
     "response": {"$ref": "#/components/schemas/OrderDto"}}
  ],
  "events": [{"topic": "order:created", "message": {"$ref": "#/components/schemas/OrderCreated"}}],
  "components": {"schemas": {"CreateOrder": {...}}}
}
```

Rules:
- Every request/response/message is a `$ref` into `components.schemas`; `$ref` prefix is
  `#/components/schemas/` and nothing else (§4). Reuse `benzene-openapi`'s existing
  `_schema_name`/`_suffixed` collision handling so two topics with the same PascalCase name do
  not collide.
- `version` is **omitted** when empty (never `""`, never `null`) — matches `TopicSpec.to_payload`'s
  existing rule and the fixtures' `version_present` distinction.
- `reserved: true` for `benzene:`-prefixed topics; `include_reserved=False` drops them entirely.
- Deterministic ordering: `requests` and `events` sorted by `(topic, version)`, component keys
  sorted lexicographically. The document must be byte-stable — `contract_hash.compute` is applied
  to it and the hashes are conformance-pinned.
- **Frozen-format flag:** the Contract Document format and `contractHash` algorithm are pinned by
  `conformance/contract-document-cases.json` + `contract-hash-cases.json`. This gap is *additive*
  — a producer for an already-specified format — so it changes no fixture. Verify by round-trip:
  every document this emits must parse cleanly through `codegen_client.parse_document` and hash
  identically before and after a no-op re-derivation.

CLI: add a `benzene-codegen contract` subcommand (or a small `benzene-descriptor`-style console
script) that imports a `BenzeneStartUp` by dotted path, runs `build_application`, and writes the
document — the Python analogue of `benzene-descriptor --assembly`, minus the assembly-loading
complexity (`importlib.import_module` + `getattr`).

Optionally serve it: `StandardPaths(spec=...)` could accept a `contract=` source and answer
`GET /benzene/spec?type=benzene`. Check first whether the profile spec (R5) pins the
`{service, topics}` shape at `/benzene/spec` — if it does, add the Contract Document under a
query-parameter type selector rather than replacing the existing body.

Tests: golden document for a two-topic + one-event registry with an HTTP router; round-trip
through `parse_document`; hash stability across re-derivation; reserved-topic inclusion/exclusion;
`$ref` closure (every `$ref` resolves, no orphan components) reusing
`codegen_client.schema_closure.reachable_names`.

---

## Gap 3 — No `benzene diff` contract-compatibility gate

**Severity: HIGH** (blocked on Gap 2 for a live producer, but implementable against
checked-in documents immediately).

### What .NET has

`src/Benzene.CodeGen.Cli.Core/Commands/Diff/DiffCommand.cs` — compares two Contract Documents
(a committed baseline and the current build's output) for backward compatibility and **exits
non-zero** when the report trips `--fail-on`. Flags: `--baseline`, `--current`,
`--fail-on breaking|warning|none` (default `breaking`), `--warn-only`, `--format text|json`.
Text output is one line per change:
`[Breaking] <Kind> <Topic> <Path> (<Direction>): <Description>`, then a
`Summary: N change(s) — B breaking, W warning, C compatible`. The comparison engine itself is
`Benzene.Schema.OpenApi.Compatibility.SchemaCompatibilityComparer` (a different domain's project,
but the algorithm is small and well-defined).

### What Python has

Nothing. `grep -rn "compat\|breaking" packages/` finds only unrelated matches. There is no way to
fail a Python service's CI on a breaking contract change.

### Implementation spec

New module `packages/benzene-codegen-client/benzene/codegen_client/compatibility.py`
(no new deps — it walks the `dict` schemas the existing parser already produces).

```python
class Compatibility(str, Enum):
    BREAKING = "breaking"; WARNING = "warning"; COMPATIBLE = "compatible"

class Direction(str, Enum):
    REQUEST = "request"; RESPONSE = "response"; MESSAGE = "message"

@dataclass(frozen=True)
class Change:
    compatibility: Compatibility
    kind: str          # "topic-removed" | "property-removed" | "property-added" |
                       # "required-added" | "type-changed" | "enum-narrowed" | ...
    topic: str
    path: str          # JSON-Pointer-ish path within the schema, "" for topic-level
    direction: Direction
    description: str

@dataclass(frozen=True)
class CompatibilityReport:
    changes: tuple[Change, ...]
    @property
    def has_breaking(self) -> bool: ...
    @property
    def has_warnings(self) -> bool: ...
    def to_payload(self) -> dict[str, Any]: ...

def compare(baseline: ContractDocument, current: ContractDocument) -> CompatibilityReport: ...
```

Classification rules (direction matters — a producer's request and response evolve in opposite
directions):

| Change | Request | Response / event message |
|---|---|---|
| Topic removed | breaking | breaking |
| Topic added | compatible | compatible |
| Property removed | breaking (consumer may still send it → tolerated; but the *server* stops honouring it) | breaking |
| Property added, optional | compatible | compatible |
| Property added, **required** | breaking | compatible |
| Property made required (was optional) | breaking | compatible |
| Property made optional (was required) | compatible | breaking |
| `type` changed | breaking | breaking |
| `enum` values removed | breaking | warning |
| `enum` values added | warning | compatible |
| `format` changed | warning | warning |
| Numeric/length constraint tightened | breaking | warning |
| Constraint loosened | compatible | warning |
| Version added for an existing topic | compatible | compatible |
| Version removed | breaking | breaking |
| HTTP mapping removed / path changed | breaking | — |

Resolve `$ref`s through each document's own `components.schemas` before comparing (a `$ref`
rename with an identical target is **not** a change). Compare `(topic, version)` pairs — an
unversioned baseline entry matches an unversioned current entry.

CLI, added to `benzene/codegen_client/cli.py` as a `diff` subcommand:

```
benzene-codegen diff --baseline orders.spec.json --current build/orders.spec.json \
                     [--fail-on breaking|warning|none] [--format text|json]
```

Exit `0` clean, `1` when the threshold trips, `2` on a bad argument or unparseable document.
Match .NET's text output line format verbatim so both ports' CI logs read the same.

Tests: table-driven — one case per row above, both directions; `--fail-on` threshold behaviour;
identical documents yield zero changes; JSON output shape.

---

## Gap 4 — No pipeline exception containment

**Severity: HIGH.**

### What .NET has

`ExceptionHandlerMiddleware<TContext>` +
`.UseExceptionHandler((context, exception) => …)` (`Benzene.Core.Middleware`,
`docs/common-middleware.md` §UseExceptionHandler): centralized handling around the *rest* of the
pipeline — anything thrown by downstream middleware **or** the handler is caught and passed to
the callback, which logs it and/or maps it onto the context's result. It rethrows an
`OperationCanceledException` **only when its token was actually cancelled**, so a genuine
host-shutdown cancellation still propagates and the transport redelivers.

### What Python has

`benzene/core/router.py` catches everything **inside the terminal router only** (handler
exception → `service-unavailable`; request-mapping failure → `bad-request`). Any exception raised
by a middleware *before* the router — auth, tracing, mesh interception, rate limiting, a
user-written middleware — propagates straight out of `MiddlewarePipeline.handle`, out of
`BenzeneMessageApplication.handle`, and into the transport adapter. `grep` confirms no
exception-handling middleware exists in any package.

That is the difference between a queue message being NACKed with a Benzene failure result and a
Lambda/consumer adapter blowing up with an unhandled traceback.

### Implementation spec

`packages/benzene-core/benzene/core/pipeline.py` (or a new `middleware.py`), exported from
`benzene.core`:

```python
OnException = Callable[[Context, BaseException], None | Awaitable[None]]

def exception_handler(
    on_exception: OnException | None = None,
    *,
    status: str = Status.UNEXPECTED_ERROR,
    detail: Callable[[BaseException], str] = str,
) -> Middleware:
    """Contain any exception raised downstream, turning it into a failure Result on the context.

    Registered FIRST (outermost) so it wraps every later middleware and the router.
    ``asyncio.CancelledError`` is re-raised, never swallowed: a cancelled invocation must stay
    cancelled so the transport redelivers rather than settling a fabricated failure.
    ``on_exception`` may be sync or async; an exception raised *by the callback itself* is
    suppressed after the result is set, so a broken logger cannot mask the original error.
    """
```

Behaviour:
- `except asyncio.CancelledError: raise` — first, before the generic clause. (Python's
  `CancelledError` derives from `BaseException`, so `except Exception` already misses it; make
  the intent explicit and add a test, because it is the exact edge .NET documents as a known
  footgun.)
- `except Exception as exc:` → await/call `on_exception`, then set
  `context.result = Result.failure(status, detail(exc))` **only if `context.result` is still
  `None`** (a middleware that already produced a result and then failed while unwinding should
  not have its result clobbered).
- Never call `next()` again.

Also harden the entry point: `BenzeneMessageApplication.handle` should not depend on the user
having registered the middleware. Wrap `await self._pipeline.handle(context)` in the same
`CancelledError`-transparent `try/except`, so the envelope contract ("always returns a response
envelope") holds unconditionally — the module docstring in `envelope.py` already claims this for
malformed bodies; make it true for pipeline faults too.

Tests: a middleware raising before the router yields an `unexpected-error` envelope, not a raised
exception; `on_exception` receives the context and the exception; an async `on_exception` is
awaited; `CancelledError` propagates; a pre-set result survives; a raising `on_exception` does not
mask the original failure result.

---

## Gap 5 — Versioning: no eager cast-graph validation, no auto field-mapping caster

**Severity: MEDIUM.**

### What .NET has (`Benzene.Core.Versioning`)

Python's `SchemaCasters` already matches the *engine*: adjacency map + BFS shortest chain,
direct-cast preference. Three things sit on top of it in .NET that Python lacks:

1. **`AddPayloadVersioning(...)` — one fluent call that validates eagerly.** You declare each
   version once (name ↔ type), supply only the **upcasts**, and it (a) *synthesises* the
   field-drop downcasts, (b) **validates the whole caster graph at registration** — a missing path
   throws at startup, not on the first live message — and (c) enables the decorators per context.
   `SchemaCastDefinitionsExpander` does the BFS expansion up front.
2. **`CasterFactory<TFrom,TTo>` / `CasterFuncBuilder`** — builds a caster automatically by mapping
   same-named properties (nested classes, lists, enums, nullables, polymorphic bases), with
   `RegisterInitValue` to seed a property new in the target schema and `RegisterTypeMapping` for a
   renamed type. Convention: identical type names in per-version namespaces.
3. `SchemaTypeMatcher` — same-simple-name resolution across version namespaces.

### What Python has

`packages/benzene-core/benzene/core/casting.py`: `SchemaCasters.cast_between(from, to, fn)` +
`casting_handler(...)`. Every cast is a hand-written lambda, both directions must be registered by
hand, and a missing path raises `NoCastPathError` **on the first message that needs it** — a
production incident rather than a startup failure.

### Implementation spec

`packages/benzene-core/benzene/core/casting.py`, additive (no behaviour change to existing API):

```python
def auto_cast(
    from_type: type, to_type: type, *,
    init: Mapping[str, Any] | None = None,     # seed fields new in to_type (also overrides mapped)
    rename: Mapping[str, str] | None = None,   # from-field -> to-field
) -> Cast:
    """Build a same-name field-copy cast between two dataclasses (or pydantic models).

    Copies every field present in both by name. Fields only in ``to_type`` come from ``init``, or
    from the target's own default/default_factory; a field with neither raises ValueError **at
    build time**, not on the first message. Fields only in ``from_type`` are dropped (the
    field-drop downcast .NET synthesises). Recurses into nested dataclass fields and into
    ``list``/``tuple``/``dict``-of-dataclass fields when a same-simple-name type is resolvable
    in the target field's annotation. Enums copy by value.
    """

def add_payload_versioning(
    registry: Registry,
    topic: str,
    *,
    versions: Sequence[tuple[str, type, type]],  # (version, request_type, response_type),
                                                 # oldest -> newest; last is canonical
    handler: Handler,
    casters: SchemaCasters | None = None,
    auto: bool = True,
) -> SchemaCasters:
    """Register the canonical handler plus a casting_handler for every older version, and
    VALIDATE the whole graph now.

    For each older version, resolves (or, with ``auto=True``, synthesises via ``auto_cast``) the
    request upcast old->canonical and the response downcast canonical->old, raising
    ``NoCastPathError`` at *registration* if either is missing. This is the eager-validation
    property .NET's AddPayloadVersioning has and this port currently lacks.
    """

class SchemaCasters:
    def validate(self, pairs: Iterable[tuple[type, type]]) -> None:
        """Raise NoCastPathError for any (from, to) pair with no chain. Call at startup."""
```

Deliberately **not** ported: topic-scoped caster keys (Python keys by type, which is simpler and
sufficient), the `ISchemaCasters` DI singleton and request/response *mapper decorators* (Python
casts at the registry/handler layer, which is transport-agnostic by construction — see the
`Benzene.Grpc.Versioning` note under "Not worth porting"), and compiled expression trees
(`auto_cast` builds a closure once at registration; that is the same amortisation).

Tests: `auto_cast` field copy / drop / `init` seed / rename / nested / list-of-nested / enum;
missing-field-with-no-default raises at build time; `add_payload_versioning` registers N versions
and routes each to the canonical handler; a missing cast raises at registration, not at dispatch;
`validate()` on a partially-registered graph.

---

## Gap 6 — Codegen CLI: no remote spec sources

**Severity: MEDIUM.**

.NET: `src/Benzene.CodeGen.Cli.Core/Commands/Spec/SpecSourceResolver.cs` picks exactly one of
`--file` / `--url` / `--mesh <manifest> --service <name>` / `--lambda-name [--profile]`, shared by
both `spec` and `build`. So a consumer team can generate a client straight from a running
service's spec endpoint or from a mesh manifest.

Python: `benzene/codegen_client/cli.py` reads a local file path only.

**Spec.** Add a `_load_document(...)` source resolver to `cli.py`:
- `--spec <path>` (existing, keep as the default positional behaviour),
- `--url <base>` — `GET {base}/benzene/spec` (or the full URL if it already ends in a path) via
  `urllib.request` (stdlib — no new dep, same choice `benzene.mesh.probe` already made),
- `--mesh <manifest-url> --service <name>` — fetch the mesh manifest, find the service entry, then
  fetch its spec URL. Gate behind the `benzene-mesh` package being importable, or just parse the
  manifest JSON inline (it is a flat `{services: [{name, specUrl}]}` read).

Enforce "exactly one source" with the same error wording as .NET
(`No spec source given: pass exactly one of --spec, --url, --mesh.` /
`Multiple spec sources given (…)`). Deliberately **omit** `--lambda-name`/`--profile`: that would
add a hard `boto3` dependency to a codegen package for one AWS-specific fetch path; a user can
`aws lambda invoke` into a file and pass `--spec`.

Add a `benzene-codegen spec --url … [--out …]` subcommand that just prints the fetched document
(the fetch-and-store half of .NET's `spec` command) — that is what feeds `diff`'s `--current`.

Tests: source-selection errors; `--url` with a stubbed opener; the fetched document flows into
`service`/`topic` generation unchanged.

---

## Gap 7 — Codegen: no Markdown/README output

**Severity: MEDIUM.**

.NET: `src/Benzene.CodeGen.Markdown/LambdaServiceMarkdownBuilder.cs` +
`MarkdownTypeBuilder.cs` — `--output readme` renders a `README.md` from a Contract Document: one
section per topic with request/response type tables, validation constraints, and example payloads.
Infrastructure *and* documentation generated from the same source of truth is exactly the kind of
capability worth having; a Python service that has just gained a Contract Document producer
(Gap 2) gets browsable API docs for free.

Python has nothing equivalent.

**Spec.** New module `packages/benzene-codegen-client/benzene/codegen_client/markdown.py`:

```python
def generate_markdown(
    document: ContractDocument, *, service_name: str, header: str = "",
    include_reserved: bool = False,
) -> str: ...
```

Structure (pure string building, no template dependency — the port's generator already builds
source with `parts.append`):

```
# {service_name}
{header}

## Topics
### `orders:create`  (POST /orders)
**Request** — `CreateOrder`
| Field | Type | Required | Constraints |
**Response** — `OrderDto`
| Field | Type | Required | Constraints |
```

Reuse `schema_closure.reachable_names` for the type set and `types.py`'s existing schema→type
rendering for the type column, so the Markdown names match the generated client's names exactly.
Render `format`, `enum`, `minLength`/`maxLength`, `minimum`/`maximum`, `pattern`, `nullable` as a
constraints column. Nested object types get their own subsection, referenced by name, rather than
being inlined recursively (avoids unbounded nesting on recursive schemas — dedupe via a visited
set, same guard `schema_closure` already uses).

Deterministic output: topics in document order, fields in schema-declaration order. Reserved
topics excluded by default (`benzene:` prefix), included behind the flag — same rule as the
client generator's `--include-reserved`.

CLI: `benzene-codegen markdown --spec … --service … [--out README.md] [--include-reserved]`.

Tests: golden `.md` for the `minimal` and `versioned` fixture documents in
`conformance/contract-document-cases.json`; recursive schema terminates; reserved-topic flag.

---

## Gap 8 — Unknown-status `isSuccessful` row in the status-mapping tables

**Severity: MEDIUM. Cross-domain flag — belongs to whoever owns `benzene-http`/`benzene-grpc`,
recorded here because it surfaced from the same fixture drift as Gap 1.**

The newer spec adds a second `<unknown>` row to both mapping tables, selected by the result's
`isSuccessful`:

- `conformance/http-status-mapping.json`: Python has `{"from": "<unknown>", "to": "500"}`;
  .NET has `{"<unknown>" → "500", isSuccessful: false}` **and**
  `{"<unknown>" → "200", isSuccessful: true}`.
- `conformance/grpc-status-mapping.json`: likewise `<unknown>` → `Internal` (failure) **and**
  `<unknown>` → `OK` (success).

So an application-defined **successful** extension status must map to HTTP 200 / gRPC OK, not
500 / Internal. `benzene.http.to_http` and `benzene.grpc`'s forward mapper need an `is_successful`
input. This lands naturally alongside Gap 1 (the envelope gains `isSuccessful`, which is the
signal these tables now key on) and should be sequenced with it and with the fixture re-vendor.

---

## Gap 9 — Scatter-gather (map-reduce)

**Severity: LOW–MEDIUM.**

`Benzene.MapReduce` is four files, and its own `CLAUDE.md` is candid that it is "the thin,
supported form of scatter-gather … composed from parts already present … not a framework."
Judged on merit rather than size, though, the *policy* it encodes is the valuable part and is
genuinely easy to get wrong by hand: bounded concurrency, source-order results, and — the real
content — an explicit **partial-failure mode** so an incomplete total is never silently mistaken
for a complete one.

Python has `InProcessFanOutSender` (`packages/benzene-core/benzene/core/inprocess.py`), which
fans one message out to several local pipelines; it is not scatter-gather over a real sender and
has no reduce or partial-failure policy. `benzene-resilience` already ships a `Bulkhead` with a
semaphore, so the concurrency primitive exists.

**Spec.** New module `packages/benzene-core/benzene/core/scatter.py` (core, because it depends
only on the `MessageSender` protocol already defined in `clients.py`):

```python
class PartialFailureMode(str, Enum):
    RAISE = "raise"          # default — any failed shard raises ScatterGatherPartialFailure
    BEST_EFFORT = "best-effort"

@dataclass(frozen=True)
class ScatterGatherResult(Generic[S, A]):
    value: A
    failed_shards: tuple[tuple[S, Result[Any] | BaseException], ...]
    @property
    def is_complete(self) -> bool: return not self.failed_shards

class ScatterGatherPartialFailure(Exception): ...

async def scatter_gather(
    sender: MessageSender, topic: str, shards: Iterable[S], *,
    seed: A, reduce: Callable[[A, Any], A],
    headers: dict[str, str] | None = None,
    max_concurrency: int | None = None,      # None = unbounded
    on_failure: PartialFailureMode = PartialFailureMode.RAISE,
) -> ScatterGatherResult[S, A]: ...
```

Implementation: `asyncio.Semaphore` when `max_concurrency` is set, `asyncio.gather(...,
return_exceptions=True)` for the fan-out, results zipped back to shards **in source order** (so a
caller's positional logic is unaffected by completion order). A shard "fails" if its `Result` is
unsuccessful *or* it raised. `reduce` folds only over successful payloads. `CancelledError` from
`gather` propagates.

Tests: order preservation under staggered completion; `max_concurrency` actually caps (count
concurrent entries); `RAISE` raises and names the failed shards; `BEST_EFFORT` reduces over
successes and reports `is_complete is False`; a raising shard is treated identically to an
unsuccessful result.

---

## Gap 10 — Codegen: no handler-stub scaffolding

**Severity: LOW.**

.NET `--output message-handlers` (`src/Benzene.CodeGen.Client/MessageHandlerBuilder.cs`) emits, per
topic in a Contract Document, a handler class stub plus the DTO types — the "implement this
contract" starting point, the mirror image of the client generator.

Python has no equivalent. It is a modest amount of work given `generator.py` and `types.py`
already do all the schema→Python-type rendering: a `generate_handlers(document, *, module_name)`
that emits, per topic, a `@message("<topic>", version=…)`-decorated `async def` stub with the
right typed request/response dataclasses and a `Result.ok(...)` `TODO` body.

Real but modest value — a scaffolding convenience, once, at the start of a service. Worth doing
**after** Gaps 2 and 7, and only if the Contract Document producer lands (a scaffolder with no
document to scaffold from is not useful).

---

## Gap 11 — Event-sourcing store port

**Severity: LOW.**

`Benzene.EventSourcing` is deliberately minimal and honest about it: an `IEventStore` with
`AppendAsync(streamId, expectedVersion, events)` (optimistic concurrency, throws
`EventStoreConcurrencyException`) and `ReadAsync(streamId, fromVersion)`, plus
`EventEnvelope`/`StoredEvent` (serialization-agnostic — the caller owns payload bytes and an
`EventType` discriminator) and an `InMemoryEventStore`. `Benzene.EventSourcing.DynamoDb` adds the
production store: one item per event keyed `(streamId, version)`, append as a single
`TransactWriteItems` with `attribute_not_exists(#pk)` conditions — optimistic concurrency without
a lock, bounded by DynamoDB's 100-item transaction limit — and reads as a sorted, paginated query.
Everything else (rehydration, snapshots, projections, replay) is explicitly app-level.

**Does it carry its weight?** Marginally, and only because of the DynamoDB half. The abstraction
itself is ~40 lines a team would write anyway; the value is the *correct* conditional-transaction
append, which is easy to get subtly wrong. But it is orthogonal to everything Benzene actually
does — no handler, middleware, envelope, or contract touches it — and Python teams reaching for
event sourcing have established libraries. **Recommendation: defer.** If it is ever wanted, the
shape is:

- `packages/benzene-core`: `EventStore` `Protocol` + `EventEnvelope`/`StoredEvent` frozen
  dataclasses + `EventStoreConcurrencyError` + `InMemoryEventStore` (~80 lines, no deps).
- `packages/benzene-aws`, optional extra `[eventsourcing]`: `DynamoDbEventStore` using the
  existing aiobotocore/boto3 client, `transact_write_items` with
  `ConditionExpression="attribute_not_exists(pk)"` per event, `query` with
  `ScanIndexForward=True` and pagination. Raise on >100 events per append with an explicit message
  rather than letting DynamoDB's error surface.

Do not port a "framework" on top; .NET does not either.

---

## Gap 12 — Spec UI

**Severity: LOW** (developer experience, not production capability).

`Benzene.Spec.Ui` serves a single self-contained HTML file (inline CSS/JS, no CDN, works offline
and behind strict CSPs) that renders a Benzene spec topic-centrically — expandable per-topic
cards, request/response field tables, validation-constraint chips, transport chips, example
payloads with copy buttons, a "Try it" panel gated on the spec advertising a `messageEndpoint`,
and reserved `benzene:` topics split into a collapsed "utilities" panel. It is `UseSwaggerUI` for
topics rather than paths.

Python has no viewer (`grep -rn "spec.ui\|swagger"` finds nothing), though `benzene-openapi`
already emits an OpenAPI 3.1 document a user could point stock Swagger UI at — which covers a good
fraction of the value at zero cost, and is the honest reason this is LOW rather than MEDIUM.

If built: a `benzene-openapi` (or new `benzene-specui`) module exposing
`spec_ui_html(spec_url: str = "/benzene/spec") -> str` reading an embedded
`spec-ui.html` via `importlib.resources`, plus a `StandardPaths(spec_ui=True)` route serving it as
`text/html`. Do **not** port the .NET HTML by translation — write it against whatever document
shape this port actually serves (see Gap 2: if `/benzene/spec` starts serving a Contract Document,
the viewer targets that; if it keeps `{service, topics}`, it targets that). Keep it dependency-free
and self-contained, as .NET does.

---

## Gap 13 — Pipeline branching and request/response taps

**Severity: LOW.**

.NET's `IMiddlewarePipelineBuilder<TContext>` has `.Split(predicate, …)` (with
`ContextPredicateBuilder` / `HeaderContextPredicate` / `MediaTypeHeaderContextPredicate`),
`.Convert<TIn,TOut>()`, `.OnRequest(action)`, `.OnResponse(action)`, and `.UseStream()`.
Python's `MiddlewarePipeline` has `use()` and nothing else.

Judgement: mostly **not needed**. `.Convert()` exists because .NET pipelines are generic on
`TContext` and need an explicit conversion step; Python subclasses `Context` and needs no such
seam. `.OnRequest`/`.OnResponse` are three-line closures in Python:

```python
def on_request(action): 
    async def mw(ctx, nxt): action(ctx); await nxt()
    return mw
```

`.Split()` is the only one with real content, and even it is a small helper:

```python
def split(predicate: Callable[[Context], bool], *branch: Middleware) -> Middleware:
    """Run ``branch`` (as a nested pipeline) instead of the rest, when ``predicate`` matches."""
```

Worth adding `on_request`, `on_response` and `split` to `benzene.core.pipeline` as convenience
exports **if and only if** a real use case appears. Do not build a `ContextPredicateBuilder`
class hierarchy — a plain `Callable[[Context], bool]` is the Python idiom, matching how
`VersionSelector` is already expressed in `registry.py`.

---

## Gap 14 — Profile-probe CLI polish

**Severity: LOW.**

`packages/benzene-mesh/benzene/mesh/probe.py` is at or ahead of .NET's
`CloudServiceProfileCheckCommand` on substance (tri-state verdicts, documented inconclusive-by-design
cases, `--json`, non-zero exit when not clean). Two small gaps:

- No `--fail-on not-satisfied|inconclusive|none` threshold — .NET has one and documents that it
  added it precisely because the command was useless as a CI gate without it. Python already
  exits `1` on `not_satisfied`, so only the `inconclusive` and `none` thresholds are missing.
- Not exposed as a console script; it is `python -m benzene.mesh.probe` only. Add
  `benzene-probe = "benzene.mesh.probe:_main"` to `benzene-mesh`'s `[project.scripts]`.

---

# NOT worth porting

This section is the longest, and that is the substantive finding: **the large majority of the
projects in this domain are .NET-ecosystem artifacts with no Python meaning.** Each entry gives
the reason, so nobody re-litigates it.

## A. .NET assembly-layering artifacts — no Python analogue at all

**`Benzene.Abstractions`, `.Abstractions.MessageHandlers`, `.Abstractions.Messages`,
`.Abstractions.Middleware`, `.Abstractions.Pipelines`, `.Abstractions.Validation`** — six
projects that contain interfaces and constants only. The split exists so that a plugin package can
reference `IMiddleware<T>` without dragging in an implementation assembly: a compile-time,
assembly-graph concern. Python has no assembly boundary, and the port already collapses the
equivalent split (`benzene-core`'s own docstring says so explicitly). Where a *port* is genuinely
needed, this repo already uses `typing.Protocol` (`MessageSender` in `clients.py`,
`SupportsDefinitions` in `registry.py`) — which is the right shape and needs no separate package.

**`Benzene.Core` / `.Core.MessageHandlers` / `.Core.Messages` / `.Core.Middleware` as separate
packages** — same reasoning; `benzene-core` correctly collapses them.

Note in passing: three .NET packages named in the brief (`Benzene.CodeGen.Cli` vs `.Cli.Core`)
are also a pure packaging split — one holds `Program.cs`, the other everything else.

## B. Compile-time / build-system tooling

**`Benzene.CodeGen.SourceGenerators`** — a Roslyn `IIncrementalGenerator` emitting two
diagnostics: `BENZ001` duplicate message topic, `BENZ002` `[HttpEndpoint]` handler with no
`[Message]`. Python has no compile step. And the *value* is already delivered by construction:
`Registry.register` raises `DuplicateHandlerError` eagerly at import/startup
(`packages/benzene-core/benzene/core/registry.py`), and `OutboundRegistry` does the same for
outbound edges. A Python "analyzer" would be a linter plugin with a fraction of the confidence.
**Not applicable.**

**`Benzene.CodeGen.Build`** — a targets-only NuGet: an MSBuild `.targets` file with a
`<BenzeneServiceContract>` item type, `Inputs`/`Outputs` stamp-file incrementality, and two
carefully-split targets working around MSBuild property-evaluation-order and metadata-batching
traps. Every line of it is MSBuild arcana. Python's equivalent — "regenerate the client when the
contract file changes" — is a `Makefile` rule, a `pre-commit` hook, or three lines in CI; it is
not a package, and shipping one would be inventing a build system. **Not applicable.** (Worth one
sentence in `docs/codegen-client.md` showing the `pre-commit` recipe, if anything.)

**`Benzene.CodeGen.Cli.Core/Parsing/*`** — `CommandParser`, `AttributesParser`, `ArgAttribute`,
`CommandSplitter`, `HelpGenerator`, `PayloadMapper`: a hand-rolled attribute-driven CLI
argument-parsing framework, plus an interactive REPL loop in `Program.cs`. Python has `argparse`
in the stdlib and the port already uses it. **Not applicable** — port the *commands* (Gaps 3, 6, 7),
never the parsing framework.

## C. Deployment-specific / not-shipped generators

**`Benzene.CodeGen.Terraform`** — the brief asks specifically whether "infrastructure generated
from the same source of truth" is valuable. In principle yes; in practice **no, not this one**.
Its own `CLAUDE.md` opens with "**NOT part of the 1.0 release (`IsPackable=false`)** … at a fork in
the road: either it grows into a complete, opinionated infra generator … or it stays a
separate/experimental artifact." It is excluded from .NET's own NuGet release. It also generates
exactly one deployment topology — an AWS Lambda function with SNS subscriptions and EventBridge
rules — which is one of several this Python port targets (AWS, Azure, GCP, Kubernetes, gRPC,
RabbitMQ, Kafka). Porting an unshipped, single-cloud, single-topology generator would be adopting
someone else's unresolved design decision. **Do not port.** Revisit only if the .NET fork-in-the-road
resolves toward "complete infra generator."

**`Benzene.CodeGen.ApiGateway`** — generates an AWS API Gateway extended-OpenAPI document with
`x-amazon-apigateway-integration` VTL templates. Even .NET has withdrawn it: `CodeBuilderFactory`
deliberately excludes `api-gateway` from its advertised `ValidOutputs` list ("deprecation/removal
froze for a later decision"). Deeply AWS-and-deployment-specific (CORS token substitution,
authorizer names, identity-header VTL mappings). **Do not port.**

**`Benzene.CodeGen.LambdaTestTool`** — emits per-topic test-payload JSON files (one per topic per
transport) for firing at a locally running service. The capability — "give me a ready-to-send
payload for this topic" — is real, but this port already delivers it *better*, in code:
`packages/benzene-testing` (`create_test_host(...).build_aws()`, `MessageBuilder`) plus each
transport package's `testing.py` (`benzene.aws.testing`, `benzene.azure.testing`,
`benzene.gcp.testing`, `benzene.grpc.testing`) build native transport events in-process, which is
strictly more useful than writing JSON files to a directory for a GUI test tool that has no Python
counterpart. The only genuinely missing piece is *deterministic example payloads derived from a
schema* — and that belongs with the Contract Document / spec workstream (`ExamplePayloadBuilder`
lives in `Benzene.Schema.OpenApi`, not here), where it would also feed Gaps 7 and 12.
**Not worth porting as a file-emitting tool.**

## D. Runtime mechanics with no Python equivalent, or already solved differently

**Warm-up (`IWarmUpTask`, `SerializationWarmUpTask`, `AddBenzeneWarmUp`,
`IServiceResolverFactory.WarmUp()`)** — exists to force System.Text.Json to build per-type
metadata and FluentValidation to construct validators during the Lambda INIT phase, so the first
real message does not pay a ~18ms JIT cost. Python has no JIT and no per-type serializer metadata
compilation; its cold-start cost is module import, which no runtime hook can defer or pre-pay.
A pydantic-model-building warm-up would be measurable but tiny, and pydantic already builds its
core schema at class-definition (import) time. **Not applicable.**

**Handler-pipeline structure caching (`HandlerPipelineStructureCache`,
`HandlerMiddlewareBuilderSetComparer`, `ResolvedTopicCache<TContext>`)** — .NET caches the
middleware *structure* keyed by builder-array reference identity to avoid rebuilding the chain per
message (408–792 B → 32 B per message), with a documented unbounded-leak caveat for
non-singleton builders. Python's `MiddlewarePipeline` holds a plain list built once at startup and
closes over it; there is nothing rebuilt per message to cache. **Not applicable** — and note that
this port avoided the leak footgun by construction.

**`PresetTopicHolder` / `PresetTopicMiddleware` / `PresetTopicMessageTopicGetter` /
`DeriveTopicMiddleware` / `UseTopicFrom`, and the whole "scoped DI state, not context" pattern
documented in `Benzene.Abstractions.Middleware/CLAUDE.md`** — an elaborate mechanism to let one
pipeline override the topic without adding a property to a `TContext` type shared app-wide across
every pipeline. It exists because .NET's `IMessageTopicGetter<TContext>` is a single app-wide DI
registration. Python's `Context` carries `topic` as a plain attribute, transports subclass
`Context`, and "derive the topic from the message" is a two-line middleware
(`ctx.topic = ctx.topic or derive(ctx)`). The *capability* (a queue whose producer sets no topic
attribute) is worth documenting as a cookbook recipe; the *machinery* is not worth porting.
**Not applicable.**

**`MessageErrorState`** — scoped DI state recording a handler-converted exception's type name for
readers that outlive the tracing span. Python's mesh/OTel middleware sits in the same pipeline and
can read `context.result` directly. **Not applicable.**

**`IBenzeneInvocation` / `IBenzeneInvocationAccessor` / `GetFeature<T>()` /
`UseBenzeneInvocation()`** — a portable escape hatch to reach a native host object
(`ILambdaContext`, `HttpContext`) from a handler without coupling to it, needed because .NET's
context types are closed and generic. Python transports already subclass `Context` and attach the
native call object as an attribute, which is the same capability with none of the DI ceremony.
**Not applicable.**

**Null-object DI (`NullBenzeneServiceContainer`, `NullServiceResolver`,
`NullServiceResolverFactory`)** — Python uses `None` and `Optional`. **Not applicable.**

**`RegistrationCheck` / `IRegistrationCheck` / `RegistrationRecorder` / `RegistrationMatch`** —
diagnostics that scan a DI container's *exception message text* (wording-agnostic, culture-agnostic,
across Microsoft DI / Autofac / third parties) to guess which `.AddXxx()` call the user forgot. It
is impressive engineering entirely in service of a problem Python does not have:
`packages/benzene-core/benzene/core/dependencies.py`'s `Container` is hand-rolled and its
`ServiceNotRegisteredError` names the missing type directly. **Not applicable.**

**Media-format negotiation (`IMediaFormat<TContext>`, `MediaFormatNegotiator`,
`AcceptHeaderMediaFormatBase`, `MultiSerializerOptionsRequestMapper`, and the
`Benzene.Xml`/`Benzene.MessagePack` formats)** — real capability (content-type-negotiated
request/response bodies), but there is no Python consumer for it: the port is JSON-only end to end,
the wire contract specifies a JSON body string, and nothing in `packages/` implements another
format. Adding a negotiation seam with exactly one implementation is speculative generality.
**Defer** until a second format is actually wanted; the natural insertion point would be
`benzene.core.mapping.encode_body` / `envelope.encode_response`.

**`BoundedFanOut` / `MaxDegreeOfParallelism` on `MiddlewareMultiApplication`** — caps how many
records of a batch run concurrently. Python's transport bindings
(`packages/benzene-aws/benzene/aws/app.py`) process batch records **sequentially** in a `for`
loop, which is ordered, back-pressure-free and cannot exhaust a connection pool — a defensible
different choice, not a gap. If concurrent batch processing is ever wanted, `benzene-resilience`'s
`Bulkhead` already provides the semaphore. The one place the *policy* is worth having is
scatter-gather, which is Gap 9. **Not a gap on its own.**

**Cancellation plumbing (`ICancellationTokenAccessor`, `SeedCancellationToken`, the token
overloads on every application type)** — .NET must thread a `CancellationToken` explicitly through
every signature. Python's `asyncio` cancellation is ambient: cancelling the task cancels the
awaited call chain, and `asyncio.CancelledError` propagates without any accessor. The port already
handles it correctly in `benzene-resilience/circuit_breaker.py` (catching `BaseException` so a
cancelled probe is not counted). The only thing to preserve is the *rule*, which Gap 4 states
explicitly: never swallow `CancelledError`. **Not applicable as machinery.**

**`Benzene.Core.MessageHandlers` handler-discovery family (`ReflectionMessageHandlersFinder`,
`DependencyMessageHandlersFinder`, `CacheMessageHandlersFinder`, `CompositeMessageHandlersFinder`,
`MessageHandlerCandidateTypes`, `MessageHandlerDefinitionLookUp`)** — assembly-scanning
infrastructure with a documented composition bug history (multiple `AddMessageHandlers` calls
silently dropping a finder). Python registers handlers explicitly via the `@message` decorator and
`Registry`; there is no assembly to scan and no discovery to cache. **Not applicable.**

**`StartUpChecks` (`DuplicateTopicStartUpCheck`, `EmptyHandlerRegistryStartUpCheck`,
`PipelineResolutionStartUpCheck`, `TerminalMiddlewareStartUpCheck`,
`AddBenzeneStartUpChecks(Enforce|Advisory|Disabled)`)** — a whole opt-in check phase, added
because .NET's wiring is lazy and reflective so mistakes only surfaced on the first message.
`packages/benzene-core/benzene/core/inprocess.py` already notes this has "no analogue in this port
… because there is nothing lazy to check." Confirmed: `DuplicateHandlerError` fires at
registration; `encode_response(None)` already returns a clear
`"The pipeline produced no result"` failure if the router were somehow absent; the container
resolves eagerly. **Not applicable** — with the one exception that the *eager cast-graph
validation* idea is worth borrowing, which is Gap 5.

**`Benzene.Grpc.Versioning`** — one file, whose entire reason for existing is that .NET's casting
decorator wraps the *default serializer request mapper*, and gRPC has a bespoke mapper that the
decorator therefore cannot wrap. Python casts at the registry/handler layer
(`casting_handler` in `casting.py`), which is transport-agnostic by construction — a gRPC-served
topic gets version casting for free with no gRPC-specific package. **Not applicable, and the
Python design is better here.** Worth one line in `docs/reference/grpc.md` saying so.

**`Benzene.Testing`** — `packages/benzene-testing` is at or ahead of parity:
`create_test_host(StartUp).with_services(overrides).build_aws()` matches
`BenzeneTestHost.Create<TStartUp>().WithServices(...).BuildAwsLambdaHost()` one-for-one, and the
Python harness additionally exposes the resolved root scope (`host.scope`) for assertions and
covers more transports (`build_aws`, `build_gcp`, `build_azure`, `build_http`, `build_grpc`,
`build_kafka`, `build_rabbitmq`, `build_sqs_consumer`). `MessageBuilder`/`HttpBuilder` have direct
counterparts. **No gap.**

**`Benzene.Core.Versioning` engine internals (`CompositeCaster`, `SchemaCastDefinition`,
`ISchemaCasters` topic-keyed lookup, `SchemaCastersBuilder`, DI registration extensions,
`SchemaTypeMatcher` namespace convention)** — Python's `SchemaCasters` covers the engine
(BFS shortest chain, direct-cast preference) in 137 lines with no DI. The topic dimension in the
key is unnecessary when casts are keyed by type. **No gap beyond Gap 5.**

---

## Suggested sequencing

1. **Gap 1** (+ Gap 8, same fixture re-vendor) — coordinate the spec-version bump, then land
   `BenzeneError` / `ProblemDetails` / `isSuccessful` / the mapping-table rows together. Nothing
   else in this list matters if cross-language failure responses are broken.
2. **Gap 4** — small, self-contained, removes a real crash class.
3. **Gap 2** — closes the contract loop; unblocks 3, 6, 7, 10, 12.
4. **Gap 3**, then **Gap 6**, then **Gap 7**.
5. **Gap 5**, **Gap 9**, then the LOW items as demand appears.
