# Proposal: optional per-topic HTTP route mappings on the derived spec / ServiceDescriptor

**Status:** draft — Python port ships it as an additive, backward-compatible extension; cross-language
ratification in the [benzene spec](https://github.com/daniellepelley/Benzene/tree/main/docs/specification)
is a separate follow-up.

**Owner:** benzene-python (mesh)

## Problem — the mesh "producer gap" on the distributed path

The mesh catalog wants to show, per topic, the HTTP `(method, path)` endpoints a consuming service exposes
for it — the `topics.json` `consumers[].httpMappings` field the mesh UI renders in its **HTTP** column.

But the derived spec (Cloud Service Profile **R5**) and the `ServiceDescriptor` (**R6** / `mesh.md` §2)
are deliberately **transport-neutral**: a descriptor carries a *service-level* `binding` (`"http"`,
`"grpc"`, …) and, per topic, only `{ id, version, requestSchema, responseSchema }`. There is **no per-topic
`{method, path}` route table** anywhere in the neutral contract — by design, since the same handler is
hosted identically across transports and the route table is one binding's detail.

Consequence: a **distributed** aggregator, which learns about a peer only by fetching its `/benzene/spec`
over HTTP, can never recover that peer's route mappings — so `consumers[].httpMappings` comes back empty
and HTTP-entrypoint topics (e.g. `orders:create`, `orders:get-all`) render with a `gap` status. The
**in-process** demo does not have this problem only because it reads the mappings straight off the live
`HttpRouter` object, which the distributed path cannot share.

This is a genuine gap in the neutral contract, not an implementation quirk — verified against
`wire-contracts.md`, `cloud-service-profile.md` (R5/R6), and `mesh.md` §2.

## Proposed extension — additive, optional `topics[].http`

Add an **optional** per-topic field to the derived spec / descriptor topic entry:

```jsonc
{
  "id": "orders:create",
  "version": "",
  "requestSchema":  { "...": "..." },
  "responseSchema": { "...": "..." },
  "http": [                                 // NEW — optional, transport-binding detail
    { "method": "POST", "path": "/orders" }
  ]
}
```

Rules that keep it safe:

- **Optional and additive.** The field is present only for topics that actually have HTTP routes. A topic
  with no route, a service with no HTTP binding, and every port that has not implemented this all emit the
  spec exactly as before — so existing conformance fixtures stay green (extra optional field only).
- **A binding detail, not the neutral core.** `http` is category **D** in `wire-contracts.md` terms — a
  detail of the HTTP binding. It is sourced from the service's actual routing table (the truth-is-derived
  principle), never hand-maintained. The transport-neutral `benzene:spec` interception MUST NOT carry it;
  only the HTTP `/benzene/spec` surface (which knows the route table) adds it.
- **Excluded from `descriptorHash`.** Like `degraded` / `profile`, this is self-description that varies by
  binding/deployment; it MUST NOT participate in the descriptor hash (`mesh.md` §2.2), so it never causes
  spurious contract-drift across a fleet of mixed-binding ports.
- **Read side is tolerant.** A consumer of the spec treats a missing/empty `http` as "no mappings known"
  (exactly today's behavior), so old producers and new readers, and vice-versa, interoperate.

## How the Python port implements it (reference)

Producer side — `benzene-http`:
- `StandardPaths.spec_http_mappings: bool = True` gates the behavior (opt-out available).
- `BenzeneHttpApp`, when serving `/benzene/spec`, annotates each topic entry with the `(method, path)`
  routes read off its own `HttpRouter` (`_attach_http_mappings`). The core `ServiceSpec.to_payload` and the
  transport-neutral `benzene:spec` interception are untouched.

Reader side — `benzene-mesh`:
- `MeshAggregator._catalog` parses `topics[].http` off each fetched spec into the
  `ServiceCatalog.http_mappings` map (`_http_mappings_from_spec`) — the same `topic id → [HttpMapping]`
  shape the in-process demo builds from the router — which the existing `MeshArtifactEmitter` already
  threads into `topics.json` `consumers[].httpMappings`.

Net effect: the distributed mesh now renders the same populated HTTP column the in-process demo does, and
the `gap` status on HTTP-entrypoint topics is resolved — with zero change to the neutral wire contract or
to any other language port.

## Cross-language follow-up

To ratify this beyond Python, raise it in the benzene spec repo:
- add the optional `topics[].http: [{ method, path }]` field to the `ServiceDescriptor` / derived-spec
  definition in `mesh.md` §2 and R5, with the "optional, binding-detail, excluded-from-hash, HTTP-surface-
  only" rules above;
- extend the relevant conformance fixtures with an optional-field case (present when routes exist, absent
  otherwise), so every port can converge on the same shape.
