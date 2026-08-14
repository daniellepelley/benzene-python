# `benzene-codegen-client` — generated clients from a Contract Document

Generates a typed, topic-scoped Python client SDK from a Benzene **Contract Document**
(`{Service}.spec.json`) — the language-neutral file any Benzene service (.NET, Go, TypeScript, or
Python) derives from its handler registry and serves at `/benzene/spec`
([contract-document.md](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/contract-document.md)).
A Python consumer of *any* Benzene service gets a typed client from that one file — no .NET SDK, no
hand-written DTOs, no manual topic-string typos.

**Distribution: `benzene-codegen-client` (depends on `benzene-core`; a build-time/CLI dependency,
not a runtime one).**

```bash
pip install benzene-codegen-client
```

This is a **code generator**, not a library your service imports at runtime. The code it *emits*
depends on nothing but `benzene.core.clients.MessageSender` and `benzene.results.Result` — the
generator package itself (its OpenAPI-schema walking, its `rfc8785` dependency) never ships inside
a generated client or a deployed service.

## Quick start

```bash
benzene-codegen service --spec payments.spec.json --service Payments --out payments_client.py
benzene-codegen topic   --spec payments.spec.json --topic payments:capture --out payments_capture_client.py
```

```python
from benzene.core import MessageSender
from payments_client import CapturePayment, create_payments_client

def configure_services(services, config):
    services.try_add_singleton(
        PaymentsClient,
        lambda scope: create_payments_client(scope.get_service(MessageSender)),
    )

# elsewhere:
result = await payments_client.capture_payments(CapturePayment(order_id="o1", amount=42.42, currency="GBP"))
if result.is_successful:
    payment = result.payload   # a PaymentDto
```

Handler/client discovery in this port is **import-driven, not reflective**
(`benzene.core.discovery`'s own docstring says the same) — import the generated module (or name its
package in whatever discovery/composition-root scan your service uses) to register it. There is no
scan that finds a generated client on its own.

## The two output shapes

| Command | Shape | `components.schemas` | `events[]` | Use when |
|---|---|---|---|---|
| `benzene-codegen service` | One class, one method per in-scope topic | Whole document's catalogue (unnarrowed) | Passed through, unused | You call several topics on one service |
| `benzene-codegen topic` | One self-contained module for exactly one topic | Narrowed to just that topic's reachable schemas (the **schema closure**, spec §5.3) | Dropped — a single request/response topic carries no events | You depend on one topic and want the smallest possible, most decoupled client |

Both shapes are pinned by the spec (contract-document.md §5): which topics are in scope, which
schemas a topic-client narrows down to, and the embedded contract hash are **not** this port's
choice to make differently from any other language's generator. Method naming and file layout *are*
this port's choice (§5.5) — see [Naming](#naming) below.

## Topic scoping

By default, `service` covers a service's **domain** topics only — reserved Benzene utility topics
(`benzene:spec`, `benzene:healthcheck`, `benzene:mesh`, …) are excluded, whether marked with
`"reserved": true` or merely prefixed `benzene:` (contract-document.md §5.1: a generated client is
for a service's business surface; not every consumer wants to register an outbound route for
framework plumbing it never asked for).

- `--topics a,b,c` — an explicit include-list. Only these topics are in scope, and naming a reserved
  topic here admits it regardless of any other setting.
- `--include-reserved` — include every reserved topic in the *default* scope (ignored once
  `--topics` is given — an explicit ask always wins).
- Naming a topic that isn't in the document is a **fail-loud** error (non-zero exit), listing both
  the topic(s) that weren't found and the document's actual topics.

`benzene-codegen topic --topic <t>` always admits `<t>` even if it's reserved — naming one topic
explicitly is itself an include-list of one.

## Types

Every `components.schemas` entry becomes a stdlib `@dataclass` — **not** a `pydantic` model (there
is no pydantic usage anywhere in this port). Generated dataclasses are meant to be used with the
existing `benzene.core.mapping` serialization idiom (`to_jsonable`/`to_request`), which already
camelCases on write and case-/separator-folds on read — so a generated dataclass needs no
per-field metadata to round-trip against a .NET/Go/TypeScript peer.

- Every field gets a default (`""`/`0`/`0.0`/`False`/an empty collection/`None`), regardless of the
  schema's `required` list. This is a deliberate simplification: Python doesn't enforce "required"
  on a dataclass at runtime any more than it enforces the wire contract does (that's `to_request`'s
  job, not the type's), and giving every field a default means an `allOf`-composed subclass never
  runs into dataclass's required-before-optional field-ordering rule, however deep the inheritance.
- `allOf` (one `$ref` branch as a base, inline branches as own properties) becomes real Python
  inheritance — a subclass of the referenced dataclass.
- A bare `oneOf` with no properties of its own (a pure union) becomes a `Union[...]` **type alias**,
  not a dataclass — it has no fields to hold. A `oneOf` union-member type site elsewhere is typed the
  same way.
- `format` is not consulted (`date-time`, `uuid`, `int64`, …) — only the schema's declared `type`.
  `benzene.core.mapping` has no `datetime`/UUID/decimal awareness today, and this generator doesn't
  add unrequested serializer support as a side effect of code generation. If `mapping` grows that
  awareness, the type builder can follow. See
  [`types.py`](../packages/benzene-codegen-client/benzene/codegen_client/types.py)'s module
  docstring for the full reasoning (including why this is *not* the same divergence as the .NET
  reference's `format` heuristics).

## Naming

Method naming and file layout are explicitly out of the spec's conformance scope
(contract-document.md §5.5) — every language port picks its own idiom. This port's default:

- **Service-client method name** — reversed topic segments, `snake_case`: `payments:capture` →
  `capture_payments` (translates the .NET reference's `TopicReversedMethodName`, which Pascal-cases
  and concatenates instead).
- **Topic-client class/module identifier** — segments in original order, `snake_case`:
  `payments:capture` → `payments_capture` (translates `TopicMethodName`).
- **Schema name → class name** — `PascalCase` (`CSharpNameFormatter`'s direct analog).
- **`Namespace`/module option** — .NET's `ClientSdkOptions.Namespace` (used exactly, no magic
  suffix) has no Python equivalent to name *inside* generated source (Python has no C#-style
  namespace): the idiomatic translation is that `--out <path>` is used **exactly** as given, with no
  suffix appended — wherever you put the file *is* its import path.

## `RequiredTopics` and the contract hash

Every generated module exports two module-level constants:

- `REQUIRED_TOPICS: tuple[str, ...]` — every topic the client's method(s) call. There's no
  reflective startup-validation hook in this port to wire this into automatically (unlike .NET's
  `[OutboundRoutingContract]`) — a service that wants to fail loud at boot on a missing outbound
  route reads this constant itself in its own composition root. Imperatively-wired ports failing
  loud at construction, rather than at a reflective startup scan, is the accepted answer here.
- `CONTRACT_HASH` — the spec-pinned `contractHash` (contract-document.md §6): `"sha256:" +
  lowercase-hex(sha256(canonicalJSON(normalize(document))))`, canonicalized per RFC 8785 (JCS) via
  the `rfc8785` PyPI package. Comparable **only** against an identically-scoped hash — a
  service-level client's hash against another service-level hash of the same include-list; a
  topic-client's hash against that same topic's topic-scoped hash. Never compare across scopes (§6.4).

## Registration (no DI container to hook into)

This port has no reflective DI container convention the way .NET's
`Add{Service}ServiceClient()` extension has one — `benzene.core.dependencies.Container` is a
hand-rolled, composition-root-style container (`add_singleton`/`try_add_singleton`), not a
reflection-scanned one. The idiomatic equivalent this generator emits is a plain **factory
function**, `create_{name}_client(sender)`, that a caller wires into its own
`BenzeneStartUp.configure_services` exactly like any other collaborator:

```python
services.try_add_singleton(
    PaymentsClient, lambda scope: create_payments_client(scope.get_service(MessageSender))
)
```

## Regenerating as part of your build (Phase 6)

There's no universal Python build hook (no `dotnet build`-style MSBuild target every Python project
shares) — regeneration is a script you run and commit the output of, gated by CI re-checking it's
clean. This repo's own dogfood example (`examples/orders_payments_client/`) is the pattern:

```python
# examples/orders_payments_client/generate.py
from benzene.codegen_client import generate_topic_client, parse_document
document = parse_document(json.loads(spec_path.read_text()))
generated = generate_topic_client(document, topic="payments:capture")
out_path.write_text(generated.source)
```

```bash
python examples/orders_payments_client/generate.py
git diff --exit-code -- examples/orders_payments_client/generated   # CI fails if regeneration drifted
```

Adopt the same two-line shape in a `make regenerate` / `nox` session / hatch script in your own
service, and add the matching `git diff --exit-code` line as a required CI check — that's the
"regeneration stays clean" gate this port offers, honestly, with no framework magic behind it.

## Conformance

`conformance/contract-document-cases.json` and `conformance/contract-hash-cases.json` (vendored from
the [spec repo](https://github.com/daniellepelley/Benzene/tree/main/docs/specification/conformance),
pinned at [`conformance/SPEC_VERSION`](../conformance/SPEC_VERSION)) are run by
`tests/conformance_runner.py`'s `run_contract_document_cases()` / `run_contract_hash_cases()`, and
surfaced individually under `pytest` by `tests/test_conformance.py`. Passing them is what "client-
generation conformant" means for this port (contract-document.md §7).

## See also

- [Packages & adoption levels](packages.md) — where this sits in the stack (a build-time-only leaf,
  nothing depends on it).
- [`examples/orders_payments_client/`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_payments_client) —
  the dogfood example: a real, .NET-produced `payments.spec.json`, a generated topic-client for
  `payments:capture`, wired to `FakeMessageSender` and tested end to end.
- [contract-document.md](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/contract-document.md) —
  the normative spec this generator implements.
