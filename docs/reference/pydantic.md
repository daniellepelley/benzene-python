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
- **Invalid** — a `pydantic.ValidationError` becomes a `validation-error` `Result` carrying one
  structured error per bad field; the handler is never called and the pipeline never crashes:

  ```python
  # body {"quantity": "x"} -> statusCode "validation-error", body errors:
  #   [{"message": "Field required", "field": "/sku", "code": "missing"},
  #    {"message": "Input should be a valid integer, ...", "field": "/quantity", "code": "int_parsing"}]
  ```

### `field` is a JSON Pointer

Each error's `field` is an [RFC 6901](https://www.rfc-editor.org/rfc/rfc6901) JSON Pointer to the
offending value **in the request as it was sent** (wire-contracts §1.3) — `/name`, `/items/0/sku` — so
a caller in any language can resolve it against its own body. pydantic reports a location as a tuple
(`("items", 0, "sku")`); the adapter converts it, using the names the model **serializes** under
(`model_dump(by_alias=True)`: an `alias_generator`, a field's `alias` or `serialization_alias`), since
that is the JSON on the wire. Union-member tags pydantic adds to a location (`("extra", "Line",
"skuCode")`) are not part of the JSON and are dropped. A rule with no single location — a
`@model_validator`, a cross-field rule — omits `field` rather than inventing one. `pydantic`'s error
`type` travels as `code`, verbatim.

`validation_errors(exc, model)` and `field_pointer(loc, model)` are the conversion on its own, for a
handler that validates by hand.

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

## Exports

`validated`, `validation_errors`, `field_pointer`, `format_validation_errors`.

## See also

- [`benzene.core`](core.md) — handlers, `@message`, and the `Result` handlers return.
- [Packages](../packages.md) — the layered distribution stack.
