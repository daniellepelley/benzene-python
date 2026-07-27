# Getting started

Build a small Benzene service in Python — from an empty folder to a running HTTP endpoint — and see
how the layered packages let you adopt exactly as much of Benzene as you need.

## Prerequisites

- **Python 3.10+**
- `pip` and a virtual environment

## 1. Set up a project

```bash
mkdir hello-benzene && cd hello-benzene
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

## 2. Write a handler (level 1 + 2)

A Benzene handler is a plain `async` function from a request to a `Result`. It never sees the
transport. You need two layers for this: `benzene-core` to run it, which pulls in `benzene-results`
for the return type.

```bash
pip install benzene-core
```

Create `app.py`:

```python
from dataclasses import dataclass

from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result


@dataclass
class Greet:
    name: str = "world"


@message("say:hello", request_type=Greet)
async def hello(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


application = BenzeneMessageApplication(Registry().add(hello))
```

The `@message("say:hello")` decorator registers the handler under a topic. `request_type=Greet`
tells Benzene to build a `Greet` from the decoded JSON body before calling you.

## 3. Drive it with the transport-neutral envelope

Before adding any HTTP, you can already exercise the handler through the `BenzeneMessage` envelope —
this is the same wire contract every Benzene port speaks. Add to the bottom of `app.py`:

```python
import asyncio

if __name__ == "__main__":
    response = asyncio.run(
        application.handle_async(
            {"topic": "say:hello", "headers": {}, "body": '{"name": "Benzene"}'}
        )
    )
    print(response)
```

```bash
python app.py
# {'statusCode': 'ok', 'headers': {'content-type': 'application/json'},
#  'body': '{"greeting": "Hello Benzene"}'}
```

Note the response is transport-neutral: a Benzene **status** (`ok`), headers, and a pre-serialized
JSON **body**.

## 4. Host it over HTTP (level 3)

Now put the same handler behind a real HTTP server. Install the HTTP binding and an ASGI server:

```bash
pip install benzene-http uvicorn
```

Add an HTTP route to the handler and expose an ASGI app. Update `app.py`:

```python
from dataclasses import dataclass

from benzene.core import message
from benzene.results import Result
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint


@dataclass
class Greet:
    name: str = "world"


@http_endpoint("GET", "/greet/{name}")
@message("say:hello", request_type=Greet)
async def hello(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


app = BenzeneHttpApp(HttpRouter().add(hello))
```

`@http_endpoint("GET", "/greet/{name}")` says *where* the request arrives; `@message("say:hello")`
says *which handler* it resolves to. The `{name}` path parameter is merged into the handler's
request.

Run it:

```bash
uvicorn app:app
```

In another terminal:

```bash
curl -i http://127.0.0.1:8000/greet/Benzene
# HTTP/1.1 200 OK
# content-type: application/json
#
# {"greeting": "Hello Benzene"}
```

The handler's Benzene status (`ok`) was mapped to HTTP `200` by the binding
([wire-contracts §4.1](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md)).
Try a route that doesn't exist and you'll get a `404` with a `not-found` body — the binding never
crashes on bad input.

## What just happened

You adopted Benzene one layer at a time:

1. `benzene-results` — the `Result` you returned.
2. `benzene-core` — registering and running the handler through the envelope.
3. `benzene-http` — hosting it over HTTP, unchanged.

The handler code never changed as you added the transport — that is the whole point of the
ports-and-adapters design. See **[Packages & adoption levels](packages.md)** for why the layering is
split this way, and the reference docs for each package:
[`benzene.results`](reference/results.md), [`benzene.core`](reference/core.md),
[`benzene.http`](reference/http.md).

## Troubleshooting

- **`ModuleNotFoundError: No module named 'benzene.http'`** — you installed a lower layer only.
  `pip install benzene-http` (it pulls in the rest).
- **`404` for a route you defined** — the HTTP method must match too; `@http_endpoint("GET", ...)`
  won't answer a `POST`.
- **Handler raised an exception** — Benzene turns an uncaught error into a `service-unavailable`
  result (HTTP `503`) rather than crashing the server; check the response body's `detail`.
