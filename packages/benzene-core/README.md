# benzene-core

The transport-neutral message-handling engine of the
[Benzene Python port](https://github.com/daniellepelley/benzene-python): the handler registry and
`@message` decorator, the middleware pipeline, a minimal per-invocation DI container, request/
response mapping, and the `BenzeneMessageApplication` envelope entry point.

Depends only on [`benzene-results`](https://pypi.org/project/benzene-results/). No transport code.

```bash
pip install benzene-core
```

```python
from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result

@message("say:hello")
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneMessageApplication(Registry().add(hello))

# Drive it with a transport-neutral Benzene message envelope:
response = await app.handle_async(
    {"topic": "say:hello", "headers": {}, "body": '{"name": "world"}'}
)
# -> {"statusCode": "ok", "headers": {"content-type": "application/json"},
#     "body": '{"greeting": "Hello world"}'}
```

Install a transport binding on top (e.g. [`benzene-http`](https://pypi.org/project/benzene-http/))
to host these handlers over a real protocol. Mirrors .NET's `Benzene.Core` +
`Benzene.Core.MessageHandlers` + `Benzene.Dependencies`, and contributes the `benzene.core`
subpackage to the shared `benzene` namespace.
