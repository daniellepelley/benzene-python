# `benzene.results`

The bottom layer: the `Result` type and the status vocabulary. **Distribution: `benzene-results`
(zero dependencies).**

```bash
pip install benzene-results
```

## Overview

Every Benzene handler returns a `Result` — a value describing the outcome, never an exception for an
expected domain case (core-concepts §5). A `Result` carries a **status** (from the wire vocabulary),
an optional **payload**, and, on failure, human-readable **errors**.

## `Result`

A frozen, generic dataclass:

```python
from benzene.results import Result

Result(status: str, payload=None, errors: tuple[str, ...] = ())
```

| Member | Description |
|---|---|
| `.status` | a status-vocabulary string (or an application extension) |
| `.payload` | the success payload (may also be present on failure) |
| `.errors` | tuple of error messages, populated on failure |
| `.is_successful` | `True` iff `.status` is in the success class |

### Success factories

`Result.ok(payload=None)`, `Result.created(...)`, `Result.accepted(...)`, `Result.updated(...)`,
`Result.deleted(...)`, `Result.ignored(...)`.

### Failure factories

`Result.failure(status, *errors)` is the general form. Each failure status also has a shortcut:
`Result.bad_request(*errors)`, `Result.validation_error(...)`, `Result.unauthorized(...)`,
`Result.forbidden(...)`, `Result.not_found(...)`, `Result.conflict(...)`,
`Result.too_many_requests(...)`, `Result.timeout(...)`, `Result.not_implemented(...)`,
`Result.service_unavailable(...)`, and `Result.unexpected_error(...)`.

```python
Result.ok({"id": 1})
Result.created({"id": 2})
Result.not_found("no such order")
Result.failure("conflict", "already exists")   # any status by name
```

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

| Success class | Failure class |
|---|---|
| `ok`, `created`, `accepted`, `updated`, `deleted`, `ignored` | `bad-request`, `validation-error`, `unauthorized`, `forbidden`, `not-found`, `conflict`, `too-many-requests`, `timeout`, `not-implemented`, `service-unavailable`, `unexpected-error` |

## See also

- [`benzene.core`](core.md) — where results are produced and encoded onto the wire.
- [wire-contracts §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)
  — the authoritative status vocabulary.
