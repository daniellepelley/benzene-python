# `benzene.pydantic`

Validate a handler's request with a [pydantic](https://docs.pydantic.dev) model — the Python
ecosystem's standard for parsing and validating request data. **Distribution: `benzene-pydantic`
(depends on `benzene-core` and `pydantic`).** The core stays pydantic-free; this optional adapter is
the one place the dependency lives.

```bash
pip install benzene-pydantic
```

## `validated`

`validated(model)` wraps a handler so the decoded request body is validated into `model` (a pydantic
`BaseModel`) **before** the handler runs. Apply `@message(topic)` above it and leave `request_type`
unset — the raw body flows in and `@validated` checks it:

```python
from benzene.core import message
from benzene.pydantic import validated
from benzene.results import Result
from pydantic import BaseModel

class PlaceOrder(BaseModel):
    sku: str
    quantity: int = 1

@message("orders:place")
@validated(PlaceOrder)
async def place(order: PlaceOrder) -> Result:
    return Result.created(order)
```

- **Valid** — the handler receives a validated `PlaceOrder` instance (defaults applied, types coerced).
- **Invalid** — a `pydantic.ValidationError` becomes a `validation-error` `Result` that names each bad
  field; the handler is never called and the pipeline never crashes:

  ```python
  # body {"quantity": "x"} -> statusCode "validation-error",
  #   detail "sku: Field required, quantity: Input should be a valid integer, ..."
  ```

## Responses

A pydantic model returned as a success payload is serialized by `benzene.core`'s wire mapper via
`model_dump(by_alias=True)`. Note the asymmetry with a dataclass response: a **dataclass**'s fields
are auto-camelCased by the wire mapper, but a **pydantic model** is dumped under its own field
names, so a plain model's `order_id` crosses the wire as `order_id`, not `orderId`. Give the model a
camelCase `alias_generator` to put it back in the Benzene naming policy, exactly like a dataclass
response:

```python
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

class Receipt(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    order_id: str          # -> "orderId" on the wire
```

## Schema derivation

Importing this package also registers a
[schema provider](core.md#schema-derivation-and-its-providers) with `benzene.core`, so a handler
that declares a pydantic model publishes that model's real schema in every document derived from the
registry: the Contract Document at `/benzene/spec`, the native `ServiceSpec` at
`?type=native`, the mesh `ServiceDescriptor`, and the [OpenAPI](openapi.md) document. Without it
`json_schema` has no rule for a `BaseModel` and falls through to the open schema `{}` — a published
contract that says nothing, from which a client generator can only emit an untyped client.

```python
from benzene.core import json_schema

class Address(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    line_one: str

json_schema(Address)
# {"type": "object", "properties": {"lineOne": {"type": "string"}}, "required": ["lineOne"]}
```

The schema is derived with `model_json_schema(by_alias=True)`, matching the `model_dump(by_alias=True)`
the wire mapper serializes with — the schema describes the names that actually cross the wire.
Constraints are contract and travel with it (`Field(min_length=…, pattern=…, ge=…)` →
`minLength`/`pattern`/`minimum`), as do `description`, `default`, `enum`, `const` and `format`.

Two adjustments make the result publishable:

- **`$defs` are inlined.** pydantic hoists nested models into `$defs` and points at them with
  `#/$defs/<name>`; `contract-document.md` §4 allows only `#/components/schemas/<name>` anywhere in
  the document, and the mesh descriptor embeds each topic schema standalone and hashes it. Refs are
  therefore resolved in place and `$defs` dropped. A recursive model cuts the cycle with `{}`, the
  same rule core applies to a recursive dataclass. `inline_defs` is exported for direct use.
- **Synthesised `title`s are stripped.** pydantic derives one for every model and property from the
  identifier (`line_one` → `"Lineone"`); it is annotation, not contract, and dataclass-derived
  schemas carry none. Prose belongs in `description`, which is kept.

> **This changes a pydantic-using service's `descriptorHash` and contract hash — once.** Those hashes
> are content-derived drift detection over the schemas above. A service whose schemas were `{}`
> hashed a contract it did not have; now it hashes the real one, so the value moves the first time it
> reports after upgrading. That is the bug being fixed, not a wire change: the envelope, status
> vocabulary and document shapes are untouched, and a service with no pydantic types hashes exactly
> what it did before.

### The one path still unclaimed: `@validated`'s request schema

A schema is derived from the type a handler *declares*. `@validated(Model)` deliberately takes the
raw decoded body (`@message` above it, `request_type` unset) so the decorator — not the wire
mapper — is what validates, which is what makes a bad request a `validation-error` naming each
field. The cost is that there is no declared type to derive from, so the published **request**
schema for such a topic is still `{}`. Its response schema, and every schema of a handler that
declares its model, are unaffected.

Declaring the model instead (`async def place(order: PlaceOrder)`, or `request_type=PlaceOrder`)
publishes the request schema, but today the wire mapper then constructs the model itself and a bad
body surfaces as `bad-request` with pydantic's message glued into `detail`, rather than
`validation-error` with a structured error per field. Pick per topic: `@validated` for the better
failure, a declared type for the published schema. Closing the choice needs the wire mapper to route
a pydantic type through `model_validate` and the router to classify the resulting `ValidationError`
as `validation-error`; that is tracked separately.

## Exports

`validated`, `format_validation_errors`, `pydantic_schema`, `inline_defs`.

## See also

- [`benzene.core`](core.md) — handlers, `@message`, and the `Result` handlers return.
- [Packages](../packages.md) — the layered distribution stack.
