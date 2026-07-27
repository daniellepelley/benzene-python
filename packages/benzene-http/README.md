# benzene-http

The inbound **HTTP (ASGI)** transport binding for the
[Benzene Python port](https://github.com/daniellepelley/benzene-python). Host the same handlers you
wrote against `benzene-core` behind a real HTTP server — a standard ASGI app you can run with
uvicorn/hypercorn.

Depends on [`benzene-core`](https://pypi.org/project/benzene-core/) (which pulls in
`benzene-results`).

```bash
pip install benzene-http
```

```python
from benzene.core import message
from benzene.results import Result
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

@http_endpoint("GET", "/greet/{name}")   # where it arrives
@message("say:hello")                    # which handler it resolves to
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneHttpApp(HttpRouter().add(hello))   # run: uvicorn module:app
```

The topic is resolved from route/method conventions; the Benzene status maps to an HTTP code
(wire-contracts §4.1): `ok` → 200, `created` → 201, `not-found` → 404, and so on. Path params
(`{name}`), the query string, and the JSON body merge into the handler's request (path wins, then
query, then body). Unmatched route → 404, invalid JSON body → 400, uncaught handler error → 503 —
the host is never crashed by request content.

Mirrors .NET's `Benzene.Http`, and contributes the `benzene.http` subpackage to the shared
`benzene` namespace.
