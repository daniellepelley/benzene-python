# benzene-results

The bottom layer of the [Benzene Python port](https://github.com/daniellepelley/benzene-python):
the `Result` type and the status vocabulary. **Zero dependencies.**

A domain handler returns a value, never raising for an expected outcome:

```python
from benzene.results import Result

def create_order(request) -> Result:
    if not request.get("sku"):
        return Result.bad_request("sku is required")
    return Result.created({"id": "ord_123"})
```

## Errors that carry a field and a code

The failure factories take plain strings, and that stays the short path. When the producer knows
more than a sentence — a validator knows *which* field failed and *which* rule rejected it — say so,
and it travels all the way to the caller's RFC 9457 problem document instead of being flattened into
prose the caller has to parse:

```python
from benzene.results import BenzeneError, Result

return Result.validation_error(
    BenzeneError("sku is required", field="sku", code="missing"),
)
# → {"type": "https://benzene.app/problems/validation-error", "title": "Validation failed",
#    "detail": "sku is required", "benzeneStatus": "validation-error",
#    "errors": [{"message": "sku is required", "field": "sku", "code": "missing"}]}
```

Every factory takes strings and structured errors interchangeably, so mixing them is fine and
nothing needs a second `*_with` variant. `benzene-pydantic`'s `@validated` emits these
automatically: pydantic already knows each error's location and rule, and they cross straight into
`field` and `code`.

`Result.messages` is the messages alone, for code that only ever wanted prose.

Two escape hatches sit alongside:

- `Result.problem(ProblemDetails(...))` — an application-authored problem document, emitted
  verbatim, for a service that owns its own problem vocabulary and wants its own `type` URI on the
  wire rather than the registry URI Benzene would derive from the status.
- `Result.set(status, payload, successful=...)` — state the success classification outright,
  decoupled from the status. The reserved health check uses it to answer `service-unavailable` (so
  an HTTP probe sees a 503 and a load balancer drains the instance) while still rendering its
  report body instead of a problem document.

Install just this layer if all you want is Benzene's result/status model (e.g. to type a domain
service's return values) without any pipeline or transport code:

```bash
pip install benzene-results
```

Provides `Result`, `BenzeneError`, `ProblemDetails`, `Status` (the wire-contract status constants),
`is_successful`, and the `SUCCESS_STATUSES` / `FAILURE_STATUSES` / `KNOWN_STATUSES` sets. It contributes the `benzene.results`
subpackage to the shared `benzene` namespace. Mirrors .NET's `Benzene.Results` plus the status
vocabulary from `Benzene.Abstractions`.
