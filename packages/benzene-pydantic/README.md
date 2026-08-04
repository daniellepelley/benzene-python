# benzene-pydantic

Validate a Benzene handler's request with a [pydantic](https://docs.pydantic.dev) model — the Python
ecosystem's standard for parsing and validating request data — for the
[Benzene Python port](https://github.com/daniellepelley/benzene-python).

Depends on [`benzene-core`](https://pypi.org/project/benzene-core/) and `pydantic`. The core stays
pydantic-free; this optional adapter is the one place the dependency lives.

```bash
pip install benzene-pydantic
```

```python
from benzene.core import message
from benzene.pydantic import validated
from benzene.results import Result
from pydantic import BaseModel

class PlaceOrder(BaseModel):
    sku: str
    quantity: int = 1

@message("orders:place")          # no request_type — the raw body flows in and @validated checks it
@validated(PlaceOrder)
async def place(order: PlaceOrder) -> Result:
    return Result.created(order)  # a pydantic model returned as payload serializes on the wire
```

`@validated(Model)` validates the decoded body into `Model` **before** your handler runs. A
`pydantic.ValidationError` becomes a `validation-error` `Result` that names each bad field, so a
malformed request never reaches your handler and never crashes the pipeline:

```python
# POST {"quantity": "not-an-int"}  ->  status "validation-error"
#   detail: "sku: Field required, quantity: Input should be a valid integer, ..."
```

A pydantic model returned as a success payload is serialized by `benzene.core`'s wire mapper
(`model_dump(by_alias=True)`). Unlike a dataclass — whose fields the mapper auto-camelCases — a
pydantic model is dumped under its own field names, so give the model a camelCase `alias_generator`
to cross the wire in the Benzene naming policy (then it matches a dataclass response).

Contributes the `benzene.pydantic` subpackage to the shared `benzene` namespace.
