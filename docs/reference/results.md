# `benzene.results`

The bottom layer: the `Result` type and the status vocabulary. **Distribution: `benzene-results`
(zero dependencies).**

```bash
pip install benzene-results
```

## Overview

Every Benzene handler returns a `Result` — a value describing the outcome, never an exception for an
expected domain case (core-concepts §5). A `Result` carries a **status** (from the wire vocabulary),
an optional **payload**, and, on failure, **structured errors**.

## `Result`

A frozen, generic dataclass:

```python
from benzene.results import BenzeneError, ProblemDetails, Result

Result(
    status: str,
    payload=None,
    errors: tuple[BenzeneError, ...] = (),
    problem_document: ProblemDetails | None = None,
    successful: bool | None = None,
)
```

| Member | Description |
|---|---|
| `.status` | a status-vocabulary string (or an application extension) |
| `.payload` | the success payload (may also be present on failure) |
| `.errors` | tuple of [`BenzeneError`](#benzeneerror), populated on failure |
| `.problem_document` | an application-authored [`ProblemDetails`](#problemdetails) the wire edge emits verbatim; `None` unless set by `Result.problem` |
| `.successful` | an explicit classification overriding the status-derived one; `None` (the normal case) means derive it |
| `.messages` | the error messages alone — `tuple(e.message for e in .errors)` — for callers that only wanted prose |
| `.is_successful` | `.successful` when it is set, otherwise `is_successful(.status)` |

Plain strings are accepted anywhere `errors` is, and coerced — `Result(status, None, ("boom",))`
stores a `BenzeneError("boom")` — so a message-only failure stays a one-liner and the type
annotation stays true.

### `BenzeneError`

One structured error (wire-contracts §1.3). `message` is the only required member; `field` (the
producer's own property path) and `code` (its machine-readable rule identifier) are emitted verbatim
and never normalized or reworded.

```python
BenzeneError(message: str, field: str | None = None, code: str | None = None)
```

A validator that knows all three can say all three, and they reach the caller intact — the
difference between an error a UI can attach to an input and one it can only print.

### Success factories

`Result.ok(payload=None)`, `Result.created(...)`, `Result.accepted(...)`, `Result.updated(...)`,
`Result.deleted(...)`, `Result.ignored(...)`.

### Failure factories

`Result.failure(status, *errors)` is the general form. Each failure status also has a shortcut:
`Result.bad_request(*errors)`, `Result.validation_error(...)`, `Result.unauthorized(...)`,
`Result.forbidden(...)`, `Result.not_found(...)`, `Result.conflict(...)`,
`Result.too_many_requests(...)`, `Result.timeout(...)`, `Result.not_implemented(...)`,
`Result.service_unavailable(...)`, and `Result.unexpected_error(...)`. Each accepts a plain string, a
`BenzeneError`, or the mapping the wire decoder produces.

```python
Result.ok({"id": 1})
Result.created({"id": 2})
Result.not_found("no such order")
Result.failure("conflict", "already exists")   # any status by name
Result.validation_error(BenzeneError("Name must not be empty", field="name", code="NotEmpty"))
```

### `Result.set` — stating success outright

```python
Result.set(status: str, payload=None, successful: bool | None = None)
```

The one place `.successful` is set. It decouples the success classification from the status, for the
case wire-contracts §1.3 names: a health check answering `service-unavailable` so an HTTP probe sees
a 503 and a load balancer drains the instance, while still rendering its **report** as the body
rather than a problem document. It is also how an application-defined status is carried as a
success — `isSuccessful` is what every receiver reads (§1.2), so without it a status outside the
shared vocabulary reads as a failure to every peer.

```python
Result.set("cache-warm", {"entries": 12}, successful=True)   # a success, whatever the status says
```

For ordinary results prefer `ok` / `failure` and the status-derived default; reach for this only
when the transport outcome and the body's meaning genuinely diverge.

### `Result.problem` — an application-authored problem document

```python
Result.problem(document: ProblemDetails)
```

For a service that owns its own problem vocabulary and wants its own `type` URI to reach the caller.
Every other factory derives the document from the status; this one is emitted **verbatim**, because
deriving instead would overwrite the application's `type` with the registry URI, which is the entire
reason for authoring one.

#### `ProblemDetails`

```python
ProblemDetails(
    benzene_status: str,
    type: str | None = None,
    title: str | None = None,
    detail: str | None = None,
    instance: str | None = None,
    errors: tuple[BenzeneError, ...] = (),
)
```

`.to_payload()` renders it as the wire body, omitting every member it does not carry. RFC 9457's
`status` is deliberately not a member: it is the **integer HTTP code**, which an application does not
author — an HTTP binding sets it to the code it is actually sending, and it is absent on every other
transport.

## Problem details on the wire (wire-contracts §1.3)

A failure's body is an RFC 9457 problem document. `benzene.core.error_payload` builds it and
`benzene.http` adds the two things §4.1 requires of an HTTP failure, so a failed response looks like:

```jsonc
{
  "type": "https://benzene.app/problems/validation-error",  // omitted for an app-defined status
  "title": "Validation Error",
  "status": 422,                    // RFC 9457's INTEGER HTTP code — HTTP responses only
  "detail": "Name must not be empty",
  "benzeneStatus": "validation-error",   // the transport-neutral Benzene status, always present
  "errors": [ { "message": "Name must not be empty", "field": "name", "code": "NotEmpty" } ]
}
```

Two rules are easy to get wrong:

- **`benzeneStatus`, not `status`.** The earlier shape put the Benzene status *string* in a member
  named `status`, colliding with RFC 9457's integer member. §1.3 withdrew it; the status now travels
  as `benzeneStatus`, and `status` is the integer HTTP code (absent wherever there is no HTTP
  response, which is every transport but HTTP).
- **`errors` is authoritative and ordered.** When it is present it is the error list; `detail` stands
  in as ONE opaque message only when it is absent. Recovering errors by splitting `detail` on `", "`
  was withdrawn — messages contain commas.

Helpers for reading and building these:

```python
from benzene.results import problem_errors, result_with_errors
from benzene.results.problems import problem_http_status, problem_title, problem_type

problem_errors(document)          # a peer's document -> tuple[BenzeneError, ...], in §1.3's precedence
result_with_errors(status, errs)  # a failure Result from a status + an errors sequence (wire decoder)

problem_type("not-found")         # "https://benzene.app/problems/not-found"; None if app-defined
problem_title("not-found")        # the registry title;                       None if app-defined
problem_http_status("not-found")  # 404; 500 for an application-defined status
```

`problem_type` and `problem_title` return `None` for a status the §3.1 registry does not know:
an application-defined failure carries its own URI or omits the member, and the framework has no
business inventing one under the `benzene.app` namespace on the application's behalf.

## Status vocabulary

Statuses are **lowercase-kebab-case strings**, not an enum, so applications can extend them; an
unknown status is treated as a **failure** (wire-contracts §3).

```python
from benzene.results import Status, is_successful

Status.OK              # "ok"
Status.CREATED         # "created"
Status.NOT_FOUND       # "not-found"

is_successful("ok")            # True
is_successful("not-found")     # False
is_successful("app-specific")  # False  (unknown -> failure)
```

The `Status` class exposes constants for every framework-defined status. Membership sets are also
exported: `SUCCESS_STATUSES`, `FAILURE_STATUSES`, and `KNOWN_STATUSES`.

Note what `is_successful` is and is not: it classifies a status *string*, and an unknown one is a
failure by that rule. It is **not** how a receiver decides whether a message succeeded — the
envelope's `isSuccessful` is (§1.2), read through `benzene.core.successful_from`. A result built with
`Result.set(..., successful=True)` on an application-defined status is a success that
`is_successful(status)` alone would report as a failure.

| Success class | Failure class |
|---|---|
| `ok`, `created`, `accepted`, `updated`, `deleted`, `ignored` | `bad-request`, `validation-error`, `unauthorized`, `forbidden`, `not-found`, `conflict`, `too-many-requests`, `timeout`, `not-implemented`, `service-unavailable`, `unexpected-error` |

## See also

- [`benzene.core`](core.md) — where results are produced and encoded onto the wire.
- [wire-contracts §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)
  — the authoritative status vocabulary.
