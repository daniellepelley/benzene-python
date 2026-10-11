# Versioning example

A small, tested demonstration of **handler-version dispatch**
([versioning.md §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/versioning.md),
"Mechanism A") — the Python port of the .NET `Versioning` example's first mechanism.

The version always travels as **metadata** — the one `benzene-version` header — **never inside the
payload body**. A producer's own `version` or `x-version` header is an ordinary header and selects
nothing. A message with no version goes to the topic's **default handler**.

## Handler-version dispatch (`order:create`)

Two genuinely different request shapes, two handlers, no casting: the incoming version picks the
handler.

| Version | Handler | Request shape |
|---|---|---|
| `v1` | [`create_order_v1`](handlers.py) | flat `customer_name` |
| `v2` | [`create_order_v2`](handlers.py) | `first_name` / `last_name` + `currency` |

Both are registered against the same topic under different versions
(`registry.register("order:create", ..., version="v1", default=True)` / `version="v2"`). Selection is
exact: `v1` runs V1, `v2` runs V2, and an unknown version such as `v9` is `not-found` — never "the
nearest" or "the highest". V1 is **declared the default**, so a producer that sends no version keeps
reaching the handler it reached before versioning was turned on. Proven end to end through the
transport-neutral front door in [`tests/test_versioning.py`](tests/test_versioning.py).

(A topic whose handlers are all versioned with none declared default is valid, but every message
without a version to it is `not-found`, so the router warns about it at start-up with a
`NoDefaultHandlerWarning`. An app that wants latest-wins instead opts in explicitly with
`BenzeneMessageApplication(registry, version_selector=highest_version)`.)

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
