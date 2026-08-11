# Evolving a handler's payload (versions, casting-handlers, transparent casting)

You shipped `orders:place` with a `PlaceOrder` payload. Now a field needs to change — `count` becomes
`quantity`, say — but the old callers are still out there. Benzene versions the *payload*, not the URL:
a message carries its version in a header, each version maps to its own handler, and a small toolkit
lets one real implementation serve every version at once. This walkthrough takes you from "I shipped v1,
now I need v2" through that toolkit, in the order you'd actually reach for it.

## Prerequisites

- Python 3.10+
- `pip install benzene-core` (add `benzene-http` for the HTTP route-segment section)
- A handler you want to evolve — here `orders:place`, with a `dataclass` payload per version.

## 1. How a version is carried and resolved

Inbound, a message names its version in **one of three headers**, tried in order —
`benzene-version` (the canonical one Benzene writes outbound), then `version`, then `x-version`. A peer
in any language (.NET, Go, TS) may send whichever it has; the first present wins. `resolve_version`
reads them, and `VERSION_HEADER_NAMES` is that ordered list:

```python
from benzene.core import VERSION_HEADER_NAMES, resolve_version

VERSION_HEADER_NAMES                                  # ('benzene-version', 'version', 'x-version')

resolve_version({"benzene-version": "v2"})            # 'v2'
resolve_version({"x-version": "v2"})                  # 'v2'  (fallback header still works)
resolve_version({"version": "a", "x-version": "b"})   # 'a'   (order-sensitive: first present wins)
resolve_version({})                                   # ''    (no signal -> the unversioned handler)
```

Over HTTP you don't touch headers at all — a `{version}` **route segment** drives selection. Register
one route with the segment and let the message registry hold the versions:

```python
from benzene.http import BenzeneHttpApp, HttpRouter
from benzene.core import BenzeneMessageApplication, Registry
from benzene.results import Result

async def get_v1(_req: dict) -> Result: return Result.ok({"served": "v1"})
async def get_v2(_req: dict) -> Result: return Result.ok({"served": "v2"})

registry = Registry()
registry.register("orders:get", get_v1, version="v1")
registry.register("orders:get", get_v2, version="v2")

router = HttpRouter()
router.register("GET", "/{version}/orders", "orders:get", get_v1)   # one route, {version} segment
app = BenzeneHttpApp(router, application=BenzeneMessageApplication(registry))

# GET /v1/orders -> {"served": "v1"} ;  GET /v2/orders -> {"served": "v2"}
```

The captured segment becomes the resolved version, so `/v1/orders` selects the `"v1"` handler and
`/v2/orders` the `"v2"` one. (Prefer the literal prefix outside the capture — `"/v{version}/orders"`
captures just `"2"`, so register those handlers as `version="2"`; match your route to your version
strings either way.) The segment drives selection only — it never leaks into the request the handler
sees.

## 2. Registering versioned handlers

A `(topic, version)` pair maps to **at most one handler**. Register each version explicitly; the
router builds the request into each handler's payload type, which it reads from the handler's
first-parameter annotation (`place_v1(request: PlaceOrderV1)`) — no `request_type=` to repeat:

```python
from dataclasses import dataclass
from benzene.core import Registry
from benzene.results import Result

@dataclass
class PlaceOrderV1:
    sku: str
    count: int = 1        # v1 called it `count`

@dataclass
class PlaceOrderV2:
    sku: str
    quantity: int = 1     # v2 renamed it `quantity`

async def place_v1(request: PlaceOrderV1) -> Result:
    return Result.created({"sku": request.sku, "quantity": request.count})

async def place_v2(request: PlaceOrderV2) -> Result:
    return Result.created({"sku": request.sku, "quantity": request.quantity})

registry = (
    Registry()
    .register("orders:place", place_v1, version="v1")   # request_type inferred: PlaceOrderV1
    .register("orders:place", place_v2, version="v2")   # request_type inferred: PlaceOrderV2
)
```

By default selection is **exact match** (`exact_version`): the requested version must be registered, or
the message is a loud `not-found`. A message with no version signal is served by the unversioned handler
(version `""`). This fail-loud default is deliberate — an unknown version is a mistake worth surfacing,
not a request to silently route to whatever handler happens to be newest.

When you *do* want latest-wins fallback — an unknown or absent version served by the highest registered
version — opt into the `highest_version` selector on the application:

```python
from benzene.core import BenzeneMessageApplication, highest_version

app = BenzeneMessageApplication(registry, version_selector=highest_version)
# a request for v9 (or none) now falls back to v2; v10 would beat v2 by natural order, not string order
```

Reach for `highest_version` when you control every caller and want new deploys to pick up the newest
contract automatically; keep the exact-match default when unknown versions are third-party callers you'd
rather reject than mis-route. A selector is just a `VersionSelector` callable, so you can supply your own
policy (semantic versioning, an allow-list) in its place.

## 3. Serving several versions with one real implementation

Registering a full handler per version means duplicating logic. Better: keep **one** real
implementation on the newest payload and make every older version forward to it.

### The casting-handler pattern (by hand)

Write a thin forwarding `async def` per retired version that upcasts the old payload to the current one
and calls the shared implementation. No framework code — just an extra registration:

```python
from benzene.core import Handler, Registry

async def place_v2(request: PlaceOrderV2) -> Result:          # the one real implementation
    return Result.created({"sku": request.sku, "quantity": request.quantity})

def make_place_v1(latest: Handler) -> Handler:
    async def place_v1(request: PlaceOrderV1) -> Result:
        return await latest(PlaceOrderV2(sku=request.sku, quantity=request.count))  # upcast v1 -> v2
    return place_v1

registry = (
    Registry()
    .register("orders:place", place_v2, version="v2")                    # inferred: PlaceOrderV2
    .register("orders:place", make_place_v1(place_v2), version="v1")     # inferred: PlaceOrderV1
)
```

Both `place_v2` and the forwarding `place_v1` annotate their request, so the router builds each into
the right per-version type with no `request_type=`. A v1 caller (old `count` field) and a v2 caller
(`quantity`) now both reach the single `place_v2`
implementation. This is enough when you have one or two old versions and no response to reshape.

### Transparent casting (register the casts, not the forwarders)

When the versions pile up — or the *response* also changed — hand-writing a forwarder each time gets
repetitive. **Transparent casting** replaces the forwarders with one-step **casts between payload
types**, registered once on a `SchemaCasters`, and lets `casting_handler` build the forwarder for you.
Register a cast in each direction you need — request **up**casts (older → canonical) and response
**down**casts (canonical → older):

```python
from dataclasses import dataclass
from benzene.core import SchemaCasters, casting_handler, Registry
from benzene.results import Result

@dataclass
class OrderPlacedV2:
    id: str
    quantity: int

@dataclass
class OrderPlacedV1:
    id: str            # v1's response never carried the quantity

async def place_v2(request: PlaceOrderV2) -> Result:
    return Result.created(OrderPlacedV2(id=f"ord-{request.sku}", quantity=request.quantity))

casters = (
    SchemaCasters()
    .cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(sku=v1.sku, quantity=v1.count))
    .cast_between(OrderPlacedV2, OrderPlacedV1, lambda v2: OrderPlacedV1(id=v2.id))   # response down
)

registry = (
    Registry()
    .register("orders:place", place_v2, version="v2",                      # the real implementation
              response_type=OrderPlacedV2)                                 # request_type inferred
    .register("orders:place",                                              # v1, served transparently
              casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
              version="v1", request_type=PlaceOrderV1, response_type=OrderPlacedV1)
)
```

Now a v1 caller sends `{"sku": "A", "count": 3}`; `casting_handler` upcasts it to `PlaceOrderV2`, the
handler runs on the canonical type and returns `OrderPlacedV2`, and the wrapper downcasts that to
`OrderPlacedV1` — so `quantity` is dropped from the reply. The v2 caller is untouched. A few points
worth pinning down:

- **The handler only ever sees the canonical type.** `place_v2` never learns v1 exists; all the version
  knowledge lives in the casts and the registrations.
- **Casts compose.** Register `V1 → V2`, then later `V2 → V3`, and a `V1 → V3` upcast is found
  automatically by a breadth-first search over the steps (a direct cast always wins over a chain). A
  new version only needs a cast **from the one before it** — add a `PlaceOrderV0` with a `V0 → V1` cast
  and the v0 registration reaches v2 through `V0 → V1 → V2` with no new code.
- **The `casting_handler` registration must declare `request_type` explicitly.** Unlike the plain
  handlers above — whose annotated `request` parameter lets Benzene *infer* the type — the wrapper's
  parameter isn't the older type, so there is nothing to infer. The router builds the body into that
  type *before* the wrapper runs, and the wrapper casts *from that built type*. Omit it and the request
  arrives as a `dict`, giving a puzzling `No cast path from dict to PlaceOrderV2` instead of a clean
  upcast.
- **A failure passes through un-downcast.** Only a successful payload is downcast — a domain failure
  (say `Result.bad_request(...)`) flows straight out unchanged, so it never trips a stray
  `NoCastPathError` on a payload that was never going to be serialized anyway.

> **Why one registration per served version?** The spec's transparent-casting mechanism serves *any*
> incoming version off a single registration, upcasting whatever arrives. That presumes non-exact
> routing — accept and cast an unrecognised version rather than reject it — which contradicts this
> port's fail-loud default (an unknown version is a `not-found`). So the port keeps §4's substance (a
> shared `SchemaCasters`, casts that compose) but each *served* version stays one deliberate,
> discoverable registration; `casting_handler` just collapses it to a one-liner. This is spelled out
> under "Transparent casting" in the [`benzene.core` reference](../reference/core.md#versioning).

## Test it

Drive the whole thing through `BenzeneMessageApplication.handle` with a version header and assert on the
mapped envelope — the v1 reply should have shed the field the v2 response added:

```python
import asyncio, json
from benzene.core import BenzeneMessageApplication

app = BenzeneMessageApplication(registry)

v1 = asyncio.run(app.handle({
    "topic": "orders:place", "headers": {"version": "v1"}, "body": '{"sku": "A", "count": 3}',
}))
assert v1["statusCode"] == "created"
assert json.loads(v1["body"]) == {"id": "ord-A"}          # downcast dropped `quantity`

v2 = asyncio.run(app.handle({
    "topic": "orders:place", "headers": {"benzene-version": "v2"}, "body": '{"sku": "A", "quantity": 3}',
}))
assert json.loads(v2["body"]) == {"id": "ord-A", "quantity": 3}   # canonical response, untouched
```

## See also

- [`benzene.core` reference — Versioning](../reference/core.md#versioning) — every signature, the
  selector contract, and the documented bend in full.
- [Calling other services](calling-other-services.md) — outbound clients that write the canonical
  `benzene-version` header for you.
- [versioning specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/versioning.md)
  — §2 headers, §3 selection, §3.1 casting-handler, §4 transparent casting.
