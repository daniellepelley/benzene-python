# Versioning example

A small, tested demonstration of **handler-version dispatch**
([versioning.md §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/versioning.md),
"Mechanism A") — the Python port of the .NET `Versioning` example's first mechanism.

The version always travels as **metadata** — the `benzene-version` header (with the `version` /
`x-version` fallbacks a cross-language peer might send) — **never inside the payload body**. A
message with no version signal is treated as the topic's default.

## Handler-version dispatch (`order:create`)

Two genuinely different request shapes, two handlers, no casting: the incoming version picks the
handler.

| Version | Handler | Request shape |
|---|---|---|
| `v1` | [`create_order_v1`](handlers.py) | flat `customer_name` |
| `v2` | [`create_order_v2`](handlers.py) | `first_name` / `last_name` + `currency` |

Both are registered against the same topic under different versions
(`registry.register("order:create", ..., version="v1")` / `"v2"`). The app opts into the
[`highest_version`](../../packages/benzene-core/benzene/core/registry.py) selector, so when a producer
sends **no** version it falls back to the highest registered version (`v2`) — an exact `v1` still
routes to V1. Proven end to end through the transport-neutral front door in
[`tests/test_versioning.py`](tests/test_versioning.py).

## Why only Mechanism A here

The .NET example also demonstrates *Mechanism B* — transparent payload casting with caster chaining.
The Python core already ships the casting primitives (`SchemaCasters` + `casting_handler` in
[`benzene.core.casting`](../../packages/benzene-core/benzene/core/casting.py)); a dedicated,
multi-hop casting example is a natural follow-up. This example stays focused on the handler-dispatch
axis, which the core supports directly through the version selector.

## Run the tests (no cloud needed)

```bash
pytest examples/versioning
```

Everything is in-memory: `InMemoryBenzeneHost` from `benzene.testing` drives the real
`BenzeneMessageApplication`, each test pushing an envelope whose `benzene-version` header selects the
handler.
