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

## Exports

`validated`, `format_validation_errors`.

## See also

- [`benzene.core`](core.md) — handlers, `@message`, and the `Result` handlers return.
- [Packages](../packages.md) — the layered distribution stack.
