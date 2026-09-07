# Parity gap analysis — serialization, schema and validation

**Reference:** `/workspace/benzene-dotnet` (.NET port — the widest implementation)
**Target:** `/home/user/benzene-python` (Python port)
**Domain:** `Benzene.Avro`, `Benzene.MessagePack`, `Benzene.Xml`, `Benzene.NewtonsoftJson`, `Benzene.JsonSchema`,
`Benzene.SchemaRegistry.Core`, `Benzene.DataAnnotations`, `Benzene.FluentValidation`, `Benzene.Descriptor`,
`Benzene.Schema.OpenApi` — against Python's `packages/benzene-core` (serialization + schema derivation),
`packages/benzene-pydantic`, `packages/benzene-openapi`.

**This document is analysis only. No code was modified.**

---

## 0. Baseline — what Python actually has today

Read before anything else: three of the gaps below only make sense against these facts.

### 0.1 A note on branch state

This analysis was performed while the working tree was on `claude/roadmap-implementation-z5k8zw`,
which is **behind `origin/main`** in this domain. `origin/main` additionally contains:

- `packages/benzene-core/benzene/core/contract.py` — `ContractDocument.derive` / `.from_spec`, the
  cross-language Contract Document producer (`{openapi, info, messageEndpoint, transports?, requests[],
  events[], components.schemas}`) that R5 names and that every port's client generator parses.
- `packages/benzene-pydantic/benzene/pydantic/validation.py` — `validation_errors()` returning
  structured `BenzeneError(message, field, code)` from pydantic's `loc`/`msg`/`type`.
- `packages/benzene-openapi/benzene/openapi/generator.py` — RFC 9457 `BenzeneProblem` +
  `BenzeneError` components, `application/problem+json`.

**Everything below is written against `origin/main` as the target's true state.** Two things I
initially flagged as gaps are *not* gaps once main is read: Python **does** produce a Contract
Document, and pydantic validation failures **do** already carry `field`/`code`. Both are noted where
relevant. An implementer must work from `origin/main`, not from this branch.

### 0.2 How a Benzene Python service chooses a serializer today

**It does not.** There is no serializer abstraction anywhere in the Python port.

| Direction | Where | What it does |
|---|---|---|
| Inbound envelope decode | `packages/benzene-core/benzene/core/envelope.py:73` | `json.loads(body)`, hardcoded. Failure → `bad-request`. |
| Outbound envelope encode | `packages/benzene-core/benzene/core/envelope.py:105,107` | `json.dumps(to_jsonable(...))`, hardcoded, with `headers = {"content-type": "application/json"}` at line 103, also hardcoded. |
| Payload naming policy | `packages/benzene-core/benzene/core/mapping.py` | `to_jsonable` (camelCase dataclass fields, verbatim dict keys, `bytes`→base64, `datetime`→ISO, duck-typed `model_dump(by_alias=True)`), `to_request` (case/separator-insensitive field matching), `encode_body` (the single outbound entry point). |
| Outbound clients | kafka/rabbitmq/gcp/aws/azure senders | Each constructor takes `serializer: Callable[[Any], str] | None = None`, defaulting to `encode_body` — e.g. `packages/benzene-kafka/benzene/kafka/producer.py:50`, `packages/benzene-gcp/benzene/gcp/pubsub.py:66`, `packages/benzene-aws/benzene/aws/clients.py:55,90,164,213,276`. |
| Inbound transports | every consumer/decoder | Produce a **UTF-8 `str`** body and hand it to the envelope, which JSON-parses it. E.g. `packages/benzene-kafka/benzene/kafka/consumer.py::decode_kafka_message` (`bytes(raw).decode("utf-8")`), `packages/benzene-aws/benzene/aws/events.py:197 _b64_to_text` (base64 → **UTF-8 text**). |

So the only seam that exists is **outbound, per-client-constructor, and `str`-returning**. There is:

- no inbound counterpart at all,
- no content negotiation (`grep content-type` across `packages/` returns only hardcoded
  `"application/json"` literals),
- no way to express a binary body (`_b64_to_text` decodes to text; the envelope body is typed `str`).

**Answer to the brief's question (a): the seam is not pluggable enough. A non-JSON format cannot be
added at all today, not even by a determined application author, because the inbound path has no
extension point.** This is Gap 1.

.NET, by contrast, has `ISerializer` / `IPayloadSerializer`
(`src/Benzene.Abstractions/Serialization/`), `IMediaFormat<TContext>` /
`IMediaFormatNegotiator<TContext>` (`src/Benzene.Abstractions.MessageHandlers/MediaFormats/`), and
`AcceptHeaderMediaFormatBase<TContext>` (`src/Benzene.Core.MessageHandlers/MediaFormats/`) — a
per-message `content-type`(read)/`accept`(write) negotiation over a set of registered formats, with
JSON as the fall-back default.

### 0.3 Schema derivation

`packages/benzene-core/benzene/core/schema.py::json_schema` maps a Python type to a fixed JSON Schema
2020-12 subset: primitives, `datetime`, `bytes`, `T | None`, `list`/`tuple`/`set`, `dict[str, T]`, and
`@dataclass` (camelCased property names, `required` iff no default, declaration order, recursion cut
with `{}`). **Anything else — including every pydantic `BaseModel` — falls through to `{}`, the open
schema.** That single line is the root of Gap 2.

Four documents consume `json_schema` and therefore inherit that hole:

- `benzene.core.ServiceSpec` (`spec.py`) → `/benzene/spec?type=native`, reserved topic `benzene:spec`
- `benzene.core.ContractDocument` (`contract.py`, on main) → `/benzene/spec`, the cross-language file
  `benzene-codegen` and every other port's generator read
- `benzene.mesh.ServiceDescriptor` (`packages/benzene-mesh/benzene/mesh/descriptor.py`) → and its
  `descriptor_hash()` at line 138-142
- `benzene.openapi.openapi_document` (`packages/benzene-openapi/benzene/openapi/generator.py`)

### 0.4 Validation

`packages/benzene-pydantic/benzene/pydantic/validation.py` — one decorator, `@validated(Model)`,
applied per handler beneath `@message(topic)`, validating the raw decoded body into a pydantic model
and turning `ValidationError` into `Result.validation_error(...)` with structured `BenzeneError`s
(on main). That is the whole of it. There is no pipeline-level validation middleware, no
schema-derived validation, and no outbound (pre-send) validation.

### 0.5 What is frozen — the boundary every proposal below respects

- **The envelope is JSON.** `{topic, headers, body}` in, `{statusCode, headers, body}` out. Field
  names, camelCase naming policy (`mapping.to_camel`), status vocabulary, and the pinned response
  header `content-type: application/json` (asserted in `conformance/envelope-cases.json`,
  case `greet-ok`) do not move.
- **`body` is a string whose *contents* are the payload.** This is exactly where .NET puts its media
  formats too — `BenzeneMessageRequest.Body` is a `string`
  (`src/Benzene.Core.Messages/BenzeneMessage/BenzeneMessageRequest.cs`), and `IMediaFormat` chooses
  how to read/write *that string*, never the envelope around it. **This is the one legitimate seam
  for a pluggable format in Python, and only when a `content-type` header explicitly asks for it.**
- **Off limits:** the envelope shape itself; the descriptor/Contract Document JSON (which is
  canonical-JSON hashed — `descriptor.py:138`, `codegen_client/contract_hash.py`); any transport's
  metadata/header convention; the status↔HTTP/gRPC tables.
- **Every conformance fixture sends no `content-type` and expects `application/json` back.** A
  negotiator that defaults to JSON when no `content-type` is present leaves all of
  `conformance/*.json` byte-identical. Gap 1's design turns on this.

---

# Gaps, in priority order

---

## Gap 1 — No serializer / media-format seam: a non-JSON body cannot be added at all

**Severity: HIGH** (enabling seam — Gaps 3 and the not-worth-porting items below all depend on it)

### What .NET has

- `src/Benzene.Abstractions/Serialization/ISerializer.cs` — `Serialize(Type, object) → string`,
  `Serialize<T>(T) → string`, `Deserialize(Type, string) → object?`, `Deserialize<T>(string) → T?`.
- `src/Benzene.Abstractions/Serialization/IPayloadSerializer.cs` — the additive byte-oriented
  extension (`Serialize(Type, object, IBufferWriter<byte>)`, `Deserialize(Type, ReadOnlySpan<byte>)`),
  explicitly documenting that a binary-only format may throw `NotSupportedException` from the string
  members.
- `src/Benzene.Abstractions.MessageHandlers/MediaFormats/IMediaFormat.cs` — `ContentType`,
  `CanRead(context, resolver)`, `CanWrite(context, resolver)`, `GetSerializer(resolver)`.
- `src/Benzene.Abstractions.MessageHandlers/MediaFormats/IMediaFormatNegotiator.cs` — `SelectRead`
  (first `CanRead` match, else the process default JSON), `SelectWrite` (first `CanWrite` match, else
  `SelectRead`'s format). Evaluated once per message.
- `src/Benzene.Core.MessageHandlers/MediaFormats/AcceptHeaderMediaFormatBase.cs` — reads by
  `content-type`, writes by `accept`, `;`-parameter and case tolerant, and deliberately does **not**
  let a bare `*/*` match a specific format.
- `src/Benzene.Core.MessageHandlers/Request/RequestMapper.cs` — the consumer: prefers the byte path
  when both an `IPayloadSerializer` and an `IMessageBodyBytesGetter` are present, else the string path.

### What Python has

Nothing on the inbound side (§0.2). `encode_body` on the outbound side is a bare function, not a
protocol, and is `str`-only.

### Judgement

This is a real capability, not a .NET shape. Concretely it buys:

1. **Legacy/partner interop** — a service that must accept one non-JSON topic today has no option but
   to fork the envelope entry point.
2. **The Kafka schema-registry story (Gap 3)** — Confluent framing is a *body* encoding; without a
   body-encoding seam there is nowhere to put it.
3. **A drop-in faster JSON codec** (`orjson`, `msgspec`) as a pure application concern, which is the
   idiomatic Python answer to `Benzene.NewtonsoftJson` and costs the framework nothing.

It is worth building **only** as a narrow, default-off seam at the body. Do not port .NET's
`IMediaFormat<TContext>` generic-over-context shape; Python's envelope already has the headers in
hand at the one place that matters.

### Implementation spec

**Package:** `benzene-core` (no new dependency — the JSON format is stdlib `json`).
**New module:** `packages/benzene-core/benzene/core/serialization.py`
**Optional extra:** none. Third-party codecs are the *application's* dependency, plugged in via the
protocol; the framework never imports one.

```python
# packages/benzene-core/benzene/core/serialization.py
from __future__ import annotations
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class Serializer(Protocol):
    """Encodes/decodes an envelope *body* (never the envelope itself)."""

    #: The media type this serializer produces, e.g. ``"application/json"``.
    content_type: str

    def encode(self, payload: Any) -> str:
        """Payload -> the wire body string. Must apply the wire naming policy."""

    def decode(self, body: str) -> Any:
        """Wire body string -> a decoded payload (typically a dict). Raise ValueError on malformed."""


class JsonSerializer:
    """The default. Exactly today's behaviour, extracted verbatim — no behaviour change."""
    content_type = "application/json"
    def encode(self, payload: Any) -> str: ...   # == mapping.encode_body
    def decode(self, body: str) -> Any: ...      # == json.loads(body) if body else {}


class MediaFormats:
    """The registry + negotiator. JSON is always registered and is always the default."""

    def __init__(self, *serializers: Serializer, default: Serializer | None = None) -> None: ...
    def register(self, serializer: Serializer) -> MediaFormats: ...      # chainable
    def select_read(self, headers: Mapping[str, str]) -> Serializer: ...
    def select_write(self, headers: Mapping[str, str]) -> Serializer: ...
```

Negotiation rules — copy .NET's semantics exactly, they are correct and cheap:

- `select_read`: match the request's `content-type` header (strip `;`-parameters, casefold) against
  each registered `content_type`, first match wins; **no header, empty header, or no match → the
  default (JSON)**.
- `select_write`: split the `accept` header on `,`, strip parameters/`q=`, match any token against a
  registered `content_type`; a bare `*/*` **does not** match a specific format; no match → whatever
  `select_read` chose. (Same rule and same reasoning as
  `AcceptHeaderMediaFormatBase.CanWrite` — see its `<remarks>`.)

**Wiring — the only two lines that change in `envelope.py`:**

```python
class BenzeneMessageApplication:
    def __init__(self, registry, pipeline=None, container=None, *,
                 version_selector=None,
                 media_formats: MediaFormats | None = None) -> None:
        self._formats = media_formats or MediaFormats()   # JSON-only by default
```

- `handle()`: replace `json.loads(body)` with `self._formats.select_read(headers).decode(body)`;
  keep the existing `except (ValueError, TypeError)` → `bad_request("Request body is not valid JSON")`
  guard, generalising the message to name the negotiated content type.
- `encode_response()`: gains an optional `serializer: Serializer | None = None` parameter (default
  `JsonSerializer()`), uses `serializer.encode(...)` and emits
  `{"content-type": serializer.content_type}`. `handle()` passes `select_write(headers)`.

**Conformance safety:** with no `content-type`/`accept` header, `select_read`/`select_write` both
return `JsonSerializer`, so the body encoding and the pinned `content-type: application/json`
response header are byte-identical. Every fixture in `conformance/` sends no `content-type`.

**What must NOT change:** `error_payload()` stays JSON unconditionally. A failure body is
problem-details (wire-contracts §1.3 / RFC 9457 on main) and is part of the frozen contract — a
negotiated format applies to the *success payload* only. Say this explicitly in the module docstring;
it is the one thing an implementer will get wrong.

**Outbound clients:** leave `serializer: Callable[[Any], str]` alone. It is already a working seam and
`Serializer.encode` is structurally compatible with it (`client(serializer=my_format.encode)`).
Do **not** widen those constructors to take a `Serializer` — a needless breaking change.

**Tests** (`tests/test_serialization.py`, new):

1. `test_default_is_json_and_unchanged` — an app with no `media_formats` produces byte-identical
   envelopes to today for a representative fixture body.
2. `test_read_selects_by_content_type` — register a fake `text/csv` serializer; a request with
   `content-type: text/csv` is decoded by it; without the header, JSON.
3. `test_content_type_parameters_and_case_are_tolerated` — `Application/JSON; charset=utf-8` matches.
4. `test_write_selects_by_accept` — `accept: text/csv` gets a CSV body **and**
   `content-type: text/csv` on the response.
5. `test_accept_star_does_not_match_a_specific_format` — `accept: */*` falls back to the read format.
6. `test_accept_falls_back_to_read_format` — `content-type: text/csv`, no `accept` → CSV out.
7. `test_unregistered_content_type_falls_back_to_json` — never a 415, never a crash.
8. `test_failure_body_is_always_json` — a `not-found` under `accept: text/csv` still returns the
   problem document as JSON.
9. `test_malformed_body_in_negotiated_format_is_bad_request` — the fake serializer raises; result is
   `bad-request`, not a crash.
10. Re-run `python -m tests.conformance_runner` and say so in the PR (wire-adjacent).

---

## Gap 2 — Schema derivation ignores pydantic: a pydantic-modelled service publishes an empty contract

**Severity: CRITICAL**

### What .NET has

- `src/Benzene.Schema.OpenApi/SchemaBuilder.cs` + `ISchemaBuilder.cs` — CLR type → `OpenApiSchema` via
  Swashbuckle's `SchemaGenerator` over the System.Text.Json contract resolver, catalogued into
  `components.schemas`. Resolved from DI, so it is replaceable.
- `src/Benzene.Schema.OpenApi/OpenApiValidationSchemaBuilder.cs` — decorates generated schemas with
  `minLength`/`maxLength`/`pattern`/`enum`/`required`/`uuid`/`email` pulled from a registered
  `IValidationSchemaBuilder` (`Benzene.FluentValidation`'s
  `Schema/FluentValidationSchemaBuilder.cs`). Validation rules reach the published contract.
- `src/Benzene.Schema.OpenApi/SuppliedSchemaCatalog.cs` / `SuppliedSchemaBuilder.cs` and
  `src/Benzene.JsonSchema/SuppliedJsonSchemaCatalog.cs` — bring-your-own hand-authored schemas for
  payloads reflection cannot express, served as `$ref`s with reflection as the fallback.
- `src/Benzene.Schema.OpenApi/SchemaGenerationOptions.cs` + `JsonPolymorphism.cs` — opt-in `allOf`
  inheritance and `oneOf` + `discriminator` polymorphism, resolved from the models' own
  `[JsonDerivedType]`/`[JsonPolymorphic]`.

### What Python has / lacks

`packages/benzene-core/benzene/core/schema.py::_schema` — the final line is
`return {}  # unknown/custom type — an open schema matches anything`. A pydantic `BaseModel` is an
unknown/custom type. There is no builder seam, no supplied-schema catalog, no constraint folding, and
no polymorphism rendering.

The failure is silent and total:

- `@message("orders:place") @validated(PlaceOrder)` — the wrapper's annotation is `request: object`
  (`validation.py`'s `async def wrapper(request: object)`), so `infer_request_type`
  (`handler.py`) yields `object` → `json_schema(object)` → `{}`.
- `async def place(order: PlaceOrder)` with `PlaceOrder(BaseModel)` — inference yields the model class
  → `json_schema(PlaceOrder)` → `{}`. (And `to_request` then hits
  `mapping.py`'s `request_type(**data)` branch, so a validation failure surfaces as
  `bad-request "Could not map request to PlaceOrder: ..."` via `router.py`, not `validation-error` —
  a second, smaller defect worth fixing in the same change.)

Either way `/benzene/spec`, the Contract Document, the mesh `ServiceDescriptor` and the OpenAPI
document all advertise `{}` for that topic. A consumer's `benzene-codegen` generates an untyped
client. The mesh cannot detect a breaking schema change (`collector.py`'s per-topic schema-change
detection compares schemas that are always `{}`). The port ships a pydantic adapter and then discards
everything pydantic knows.

### Judgement

This is the highest-value item in the domain, and it is not a port of anything — it is closing a hole
Python's own idiom opened. pydantic is *the* Python request-model library, `model_json_schema()`
already emits JSON Schema 2020-12 with constraints, formats, enums and discriminated unions, and the
port already depends on pydantic in the one optional package where it belongs. `benzene-core` must
stay pydantic-free, so the fix is a **provider hook in core + a provider in `benzene-pydantic`**.

Designing the hook as an ordered provider chain also subsumes .NET's `SuppliedSchemaCatalog`
(Gap 6) and its `IValidationSchemaBuilder` folding (Gap 5) for free — a pydantic model's
`Field(min_length=…, pattern=…)` lands in the derived schema automatically, and a hand-authored
schema is just another provider registered ahead of the default.

### Implementation spec

**Packages:** `benzene-core` (the hook), `benzene-pydantic` (the provider).
**Optional extra:** none new — `benzene-pydantic` already declares `pydantic>=2`.

**(a) The hook, in `packages/benzene-core/benzene/core/schema.py`:**

```python
#: A schema provider: return a Schema for types it recognises, ``None`` to defer to the next one.
SchemaProvider = Callable[[Any], "Schema | None"]

_providers: list[SchemaProvider] = []

def register_schema_provider(provider: SchemaProvider) -> None:
    """Register a derivation strategy consulted *before* the built-in dataclass/primitive rules.

    Providers are consulted in registration order; the first non-``None`` result wins. Import-time
    registration (``benzene.pydantic`` registers its own on import) keeps ``benzene-core``
    dependency-free.
    """

def clear_schema_providers() -> None:
    """Test seam — drop every registered provider."""
```

`_schema(py_type, seen)` gains, as its **first** step after the `None`/`Any` guard:

```python
for provider in _providers:
    derived = provider(py_type)
    if derived is not None:
        return derived
```

Providers run before the primitive/dataclass table so a provider can override, and are given the
raw type (not the origin), so a provider sees `list[Model]` too if it wants it — the pydantic
provider below deliberately does not, letting the existing `list`/`dict`/union machinery recurse into
it.

**(b) The provider, new module `packages/benzene-pydantic/benzene/pydantic/schema.py`:**

```python
def pydantic_schema(py_type: Any) -> Schema | None:
    """Derive a Benzene payload schema from a pydantic ``BaseModel`` subclass, else ``None``."""
```

Requirements, in order of how easy they are to get wrong:

1. **Guard:** `isinstance(py_type, type) and issubclass(py_type, BaseModel)`, else `None`.
   `TypeAdapter`-style non-model types are out of scope for v1.
2. **`by_alias=True`** — call `py_type.model_json_schema(by_alias=True, ref_template="#/$defs/{model}")`.
   This is not optional: `mapping.to_jsonable` serialises a model with `model_dump(by_alias=True)`,
   so the *schema* must describe the aliased names or the contract lies about the wire.
   (`by_alias=True` is pydantic v2's default for `model_json_schema`; pass it explicitly anyway so the
   intent survives a pydantic default change.)
3. **Inline every `$ref`.** `benzene.core.json_schema` returns **self-contained, `$ref`-free** schemas
   today: `contract.py`'s Contract Document allows only the `#/components/schemas/` ref prefix (§4,
   `SCHEMA_REF_PREFIX`), and the mesh descriptor embeds each topic schema standalone and hashes it.
   A raw `model_json_schema()` result carries `$defs` + `#/$defs/...` refs and would break both.
   So: resolve each `$ref` against the document's own `$defs`, substitute in place, drop the `$defs`
   key, and **cut a recursive model with `{}`** — the identical rule `_dataclass_schema` already uses
   for cycles (`schema.py`, `if cls in seen: return {}`). Write this as a small pure helper
   `_inline_defs(document: dict) -> Schema` and unit-test it directly.
4. **Strip pydantic-only noise** — remove the top-level `title` that pydantic injects from the class
   name (the port's dataclass schemas carry no `title`, and a spurious one changes the contract hash
   for no contract reason). Keep `description`, `enum`, `const`, `format`, and every constraint
   keyword (`minLength`, `maxLength`, `pattern`, `minimum`, `maximum`, `multipleOf`, `minItems`,
   `maxItems`, `uniqueItems`) — those *are* contract.
5. **Register on import** in `packages/benzene-pydantic/benzene/pydantic/__init__.py`:
   `register_schema_provider(pydantic_schema)`. Installing `benzene-pydantic` is the opt-in;
   `benzene-core` alone behaves exactly as today. Export `pydantic_schema` so a service can register
   it explicitly if it prefers not to rely on import side effects.

**(c) Fix the request-mapping path** (same change, `packages/benzene-core/benzene/core/mapping.py`):
`to_request` should route a pydantic request type through the model's own validation rather than
`request_type(**data)`. Idiomatic and dependency-free: duck-type it.

```python
    validate = getattr(request_type, "model_validate", None)
    if callable(validate) and isinstance(data, dict):
        return validate(data)          # ValidationError -> router's bad-request guard
```

Then, so the failure is classified correctly, `router.py`'s `except Exception` around `to_request`
should keep `bad-request` for a genuine mapping error but let a pydantic `ValidationError` become
`validation-error` with structured `BenzeneError`s. Do this **without importing pydantic in core** —
have `benzene.pydantic` register a small mapping hook, or (simpler and preferred) duck-type on
`hasattr(ex, "errors") and callable(ex.errors)` in a `benzene.pydantic`-registered classifier.
If that seam feels too clever for the value, leave the classification alone in v1 and note it; the
schema half is what matters.

**Contract-hash impact — call this out in the PR.** For a service using pydantic request/response
types, the derived schema goes from `{}` to a real schema, so its `descriptorHash`
(`packages/benzene-mesh/benzene/mesh/descriptor.py:138`) and Contract Document `contractHash`
change. This is **correct** — the hash is content-derived drift detection, and today's value is the
hash of a contract the service does not actually have. No conformance fixture uses pydantic
(`tests/canonical_handlers.py` and every `conformance/*.json` are dataclass/dict-only), so
`tests/conformance_runner` is unaffected. Say so explicitly in the PR body.

**Tests** (`tests/test_pydantic.py`, extended):

1. `test_model_derives_properties_and_required` — a `BaseModel` with a required and a defaulted field
   derives `type: object`, both properties, `required` naming only the first.
2. `test_alias_generator_reaches_the_schema` — a model with
   `model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)` derives camelCase
   property names, matching what `to_jsonable` puts on the wire. Assert schema keys == serialised
   body keys for the same instance — this is the property that actually matters.
3. `test_field_constraints_reach_the_schema` — `Field(min_length=2, max_length=8, pattern=r"^a")`
   and `Field(ge=0, le=10)` appear as the corresponding keywords.
4. `test_nested_model_is_inlined_not_reffed` — assert the derived schema contains no `"$ref"` and no
   `"$defs"` anywhere (walk it recursively).
5. `test_recursive_model_terminates` — a self-referencing model derives without recursion error, with
   the cycle cut as `{}`.
6. `test_enum_and_literal` — `Enum` / `Literal["a","b"]` derive an `enum` array.
7. `test_discriminated_union_keeps_oneof` — a `Field(discriminator=...)` union survives inlining.
8. `test_optional_is_nullable` — `str | None` derives a schema accepting null, consistent with
   `_as_nullable`'s rendering for dataclasses.
9. `test_core_without_pydantic_is_unchanged` — `clear_schema_providers()`, then a `BaseModel`
   derives `{}` (proves core stays dependency-free and the hook is the only path in).
10. `test_spec_descriptor_openapi_all_see_the_model` — build a registry with a pydantic-typed handler
    and assert `ServiceSpec.derive`, `ContractDocument.derive`, `ServiceDescriptor.derive` and
    `openapi_document` all carry the real schema (one test proving the hook reaches all four
    projections).
11. `test_contract_document_has_no_foreign_refs` — assert every `$ref` in a derived Contract Document
    starts with `#/components/schemas/` (pins requirement 3 at the document level).
12. `test_validated_handler_request_type` — document the `@validated` + `object` annotation
    interaction; if you also declare `request_type=Model` on `@message`, the schema is derived.

---

## Gap 3 — No schema registry: Benzene Kafka payloads cannot interoperate with the Confluent ecosystem

**Severity: HIGH** (for any deployment where Benzene shares Kafka topics with non-Benzene consumers;
low otherwise — it is genuinely optional)

### What .NET has

`src/Benzene.SchemaRegistry.Core/` — BCL-only, deliberately vendor-free:

- `ConfluentWireFormat.cs` — `Encode(schemaId, payload)` prepends `0x00` + 4-byte **big-endian**
  schema id; `Decode(framed, out id)` validates magic + length and strips the 5-byte header.
  `HeaderLength = 5`. This is the interop-critical piece.
- `ISchemaRegistryClient.cs` — `RegisterAsync(SchemaDefinition) → int` (idempotent),
  `GetByIdAsync(int)`, `GetLatestAsync(subject)`, `IsCompatibleAsync(SchemaDefinition)`.
- `SchemaDefinition.cs` (subject + text + format), `RegisteredSchema.cs` (+ id + 1-based version),
  `SchemaFormat.cs` (Avro/Json/Protobuf), `SchemaCompatibilityMode.cs` (None/Backward/Forward/Full).
- `InMemorySchemaRegistryClient.cs` — reference impl and test double; monotonic ids, per-subject
  versions, idempotent re-registration, compatibility via a pluggable checker.
- `ISchemaCompatibilityChecker.cs` + `TextualSchemaCompatibilityChecker` — deliberately conservative:
  first schema always OK, `None` accepts anything, otherwise **byte-identical text** only. It never
  falsely approves a structural change; real evolution rules come from the registry server or a
  format-aware checker.
- `ISchemaResolver.cs` / `DelegateSchemaResolver` — CLR type → `SchemaDefinition`, kept pluggable so
  the package stays Avro-free.
- `SchemaRegistrar.cs` — startup helper: `RegisterAsync(types) → id map`,
  `EnsureCompatibleAsync(types)` (fail-fast gate listing every incompatible subject),
  `CreateSerializerAsync(inner, types)`.
- `SchemaRegistrySerializer.cs` — an `IPayloadSerializer` decorator framing any inner serializer's
  output with the resolved id. **Ids resolved once at startup**, so serialisation stays synchronous —
  no registry call on the hot path. Unregistered type → throws.

### What Python has

Nothing. `grep -ri "schema.registry\|confluent.*schema\|magic byte" packages/` is empty.
`packages/benzene-kafka` produces `encode_body(message).encode("utf-8")` as the record value
(`producer.py:73`) and consumes `bytes(raw).decode("utf-8")` (`consumer.py::decode_kafka_message`).
A Confluent-framed record arriving from a non-Benzene producer would blow up on the UTF-8 decode; a
Benzene-produced record cannot be read by a Confluent consumer.

### Judgement

**Worth porting — this is the one item in the "binary formats" cluster that is a genuine production
capability rather than a .NET adapter.** A real Kafka estate has a schema registry, and a service that
cannot frame its payloads is a second-class citizen on shared topics. The framing is 20 lines and
fully testable with no broker and no SDK, which is exactly the port's house style.

Two Python-specific improvements over the .NET design — **take both**:

1. **No Base64 armor.** .NET Base64-armors framed bytes because its `ISerializer` string members are
   the universal path. Python does not need that: the Kafka record `value` is already `bytes` at the
   SDK boundary (`producer.py` does `.encode("utf-8")` itself), so framing can produce and consume
   **real bytes** there. Do the framing in the Kafka binding, not in the envelope, and the awkward
   compromise disappears.
2. **Async-native client protocol.** `.NET`'s `Task<int> RegisterAsync` maps directly onto
   `async def register(...) -> int`. Startup registration is `await`ed once in the composition root;
   nothing is sync-over-async.

**Where the seam sits — and does not:** the Kafka *record value*. Never the Benzene envelope, never
the descriptor, never the Contract Document. A registry-framed Benzene message is a Kafka-transport
concern; the envelope a Benzene consumer reconstructs from it is unchanged JSON.

### Implementation spec

**Package:** new distribution `benzene-schema-registry`, importing as `benzene.schema_registry`,
depending on `benzene-core` only. (Not folded into `benzene-kafka`: the framing and the client
protocol are transport-neutral — Kinesis/Event Hubs estates use the same registry — and
`benzene-kafka` should depend on it, not own it. This mirrors .NET's `SchemaRegistry.Core`
being separate from `Benzene.Avro`.)
**Optional extra:** `benzene-schema-registry[confluent]` → `confluent-kafka[schemaregistry]`, used
**only** by the adapter in Gap 3(d). Nothing in the core module imports a third-party package.

**(a) `benzene/schema_registry/wire.py` — the interop-critical codec, pure stdlib:**

```python
MAGIC_BYTE = 0x00
HEADER_LENGTH = 5

def encode(schema_id: int, payload: bytes) -> bytes:
    """0x00 || big-endian uint32 schema id || payload (the Confluent framing)."""

def decode(framed: bytes) -> tuple[int, bytes]:
    """Inverse. Raises ValueError when too short or the magic byte is wrong."""
```

Use `struct.pack(">BI", MAGIC_BYTE, schema_id)`. Reject `schema_id` outside `0 <= id <= 0xFFFFFFFF`
with `ValueError` (.NET writes a signed int32; unsigned is what the registry actually assigns, and
rejecting out-of-range is safer than silently wrapping).

**(b) `benzene/schema_registry/registry.py` — the model and the seam:**

```python
class SchemaFormat(str, Enum):
    AVRO = "AVRO"; JSON = "JSON"; PROTOBUF = "PROTOBUF"   # the registry's own spellings

class CompatibilityMode(str, Enum):
    NONE = "NONE"; BACKWARD = "BACKWARD"; FORWARD = "FORWARD"; FULL = "FULL"

@dataclass(frozen=True)
class SchemaDefinition:
    subject: str
    schema: str
    format: SchemaFormat = SchemaFormat.AVRO

@dataclass(frozen=True)
class RegisteredSchema:
    id: int
    subject: str
    version: int          # 1-based within the subject
    schema: str
    format: SchemaFormat

class SchemaRegistryClient(Protocol):
    async def register(self, schema: SchemaDefinition) -> int: ...
    async def get_by_id(self, schema_id: int) -> RegisteredSchema | None: ...
    async def get_latest(self, subject: str) -> RegisteredSchema | None: ...
    async def is_compatible(self, schema: SchemaDefinition) -> bool: ...

class CompatibilityChecker(Protocol):
    def __call__(self, latest: RegisteredSchema | None, candidate: SchemaDefinition,
                 mode: CompatibilityMode) -> bool: ...

def textual_compatibility(latest, candidate, mode) -> bool:
    """The conservative default: first schema OK, NONE accepts anything, else byte-identical text.

    Deliberately never *falsely approves* a structural change. BACKWARD/FORWARD/FULL are accepted as
    configuration but enforced only as textual identity here — document this in the docstring exactly
    as .NET does, so nobody mistakes it for real evolution analysis.
    """

class InMemorySchemaRegistry:
    """Reference implementation + test double. Single process; does not coordinate ids."""
    def __init__(self, mode: CompatibilityMode = CompatibilityMode.BACKWARD,
                 checker: CompatibilityChecker = textual_compatibility) -> None: ...
```

`InMemorySchemaRegistry` needs an `asyncio.Lock` (not a thread lock) around register, monotonic ids
from 1, per-subject version lists, and idempotent re-registration keyed on `(schema text, format)`.

**(c) `benzene/schema_registry/registrar.py` — startup registration:**

```python
SchemaResolver = Callable[[str], SchemaDefinition]   # Benzene topic -> its schema definition

class SchemaRegistrar:
    def __init__(self, client: SchemaRegistryClient, resolver: SchemaResolver) -> None: ...
    async def register(self, topics: Iterable[str]) -> dict[str, int]: ...
    async def ensure_compatible(self, topics: Iterable[str]) -> None:
        """Raise SchemaIncompatibleError listing *every* incompatible subject, not just the first."""
    async def create_framer(self, topics: Iterable[str]) -> "SchemaFramer": ...
```

**Key deviation from .NET, and the right one for Python:** key the id map by **Benzene topic**
(`str`), not by CLR/Python type. The Kafka producer's `send_message(topic, message, headers)` already
has the topic in hand; the payload's Python type is not reliably known there (a handler may send a
dict). The conventional Confluent subject is `f"{topic}-value"`; make that the default resolver's
subject rule and document it.

Ship one ready-made resolver so the package is useful with zero user code:

```python
def json_schema_resolver(registry: Registry) -> SchemaResolver:
    """Resolve each topic's schema from the handler registry via ``benzene.core.json_schema``.

    Subject is ``f"{topic}-value"``, format JSON, schema text the canonical JSON of the derived
    request schema. With Gap 2 landed this covers dataclass *and* pydantic payloads, which makes
    a JSON-Schema-registry deployment work out of the box with no Avro anywhere.
    """
```

An Avro user supplies their own resolver (typically over `fastavro`'s schema dict) — the package
stays Avro-free, exactly as `Benzene.SchemaRegistry.Core` does.

**(d) `benzene/schema_registry/framing.py` — the body codec:**

```python
class SchemaFramer:
    """Frames an inner encoder's bytes with the Confluent header, using a topic -> id map."""
    def __init__(self, schema_ids: Mapping[str, int],
                 encode_payload: Callable[[Any], bytes] = ...,   # default: encode_body(...).encode()
                 decode_payload: Callable[[bytes], Any] = ...) -> None: ...
    def encode(self, topic: str, message: Any) -> bytes:  # KeyError -> a teaching LookupError
    def decode(self, framed: bytes) -> tuple[int, Any]:
```

**(e) Wiring into `benzene-kafka` — additive, default-off:**

- `KafkaMessageSender.__init__` gains `framer: SchemaFramer | None = None`. When present,
  `data = framer.encode(topic, message)` replaces `self._serialize(message).encode("utf-8")` at
  `producer.py:73`. `serializer` and `framer` are mutually exclusive — raise `ValueError` in
  `__init__` if both are given, rather than silently ignoring one.
- `decode_kafka_message` gains `framer: SchemaFramer | None = None`; when present, decode the value
  bytes through it instead of `bytes(raw).decode("utf-8")`, and put the resolved schema id into a
  header (suggest `benzene-schema-id`) so a handler/middleware can see which writer schema arrived.
  When absent: today's UTF-8 path, unchanged.
- `benzene-kafka` gains `benzene-schema-registry` as an **optional extra**
  (`benzene-kafka[schema-registry]`), imported lazily inside the framer branch with the usual
  teaching `ImportError`.

**(f) The vendor adapter is documented, not shipped** — same boundary .NET draws
(`Benzene.SchemaRegistry.Core/CLAUDE.md`, "the registry *clients* are the vendor-coupled,
un-CI-testable part — kept out"). Put a copy-paste `ConfluentSchemaRegistryClient` (wrapping
`confluent_kafka.schema_registry.SchemaRegistryClient` in `asyncio.to_thread`) in a new
`docs/cookbooks/schema-registry.md`, alongside the Azure Schema Registry equivalent.

**Tests** (`tests/test_schema_registry.py`, new — all in-memory, no broker, no SDK):

1. `test_encode_prepends_magic_and_big_endian_id` — assert the exact 5 header bytes for a known id
   (e.g. id 1 → `b"\x00\x00\x00\x00\x01"`). Pin the byte layout, not just the round trip; this is the
   interop contract.
2. `test_round_trip` and `test_decode_rejects_short_buffer` / `test_decode_rejects_wrong_magic`.
3. `test_schema_id_out_of_range_is_rejected`.
4. `test_register_is_idempotent_and_ids_are_monotonic`.
5. `test_versions_are_per_subject_and_1_based`.
6. `test_backward_rejects_a_changed_schema_none_allows_it`; `test_first_schema_is_always_compatible`.
7. `test_get_by_id_and_get_latest`.
8. `test_registrar_builds_the_topic_id_map`.
9. `test_ensure_compatible_lists_every_incompatible_subject` — two bad subjects, both named in the
   raised error's message.
10. `test_framer_frames_with_the_registered_id`; `test_framer_unregistered_topic_raises_teaching_error`.
11. `test_json_schema_resolver_uses_the_derived_schema` — including a pydantic-typed topic once
    Gap 2 lands.
12. In `tests/test_kafka.py`: `test_producer_frames_when_a_framer_is_given`,
    `test_consumer_deframes_and_exposes_the_schema_id_header`,
    `test_without_a_framer_the_bytes_are_unchanged` (the regression guard that matters),
    `test_serializer_and_framer_together_raise`.

---

## Gap 4 — No build-time contract emitter: the spec can only be obtained by running the service

**Severity: MEDIUM**

### What .NET has

`src/Benzene.Descriptor/` — a `dotnet` tool (`benzene-descriptor`) that emits `{name}.spec.json` (the
`EventServiceDocument` = the Contract Document) and `{name}.service.json` (the mesh §2
`ServiceDescriptor` wire shape) from a **built, non-running, non-deployed** service, by constructing
it in-process and reading the descriptors it already computes. `DescriptorEmitter.Emit(EmitOptions)`
is the core; `Program.cs` is a thin CLI shell; `HostAdapters.cs` runs `ConfigureServices` without the
run/listen step; `ServiceLoadContext.cs` handles assembly isolation and fails loudly on a
`Benzene.Core` version skew. `--emit spec|descriptor|both`.

### What Python has

Nothing. `benzene-codegen-client` **consumes** a Contract Document
(`packages/benzene-codegen-client/benzene/codegen_client/cli.py` — `--spec payments.spec.json`) but
no Python service can *produce* that file except by starting itself and `curl`ing `/benzene/spec`.
`ContractDocument.derive` (on main) does the projection in-process — the CLI wrapper around it is
missing.

### Judgement

Worth doing, and **far cheaper in Python than in .NET**: most of `Benzene.Descriptor` is
`AssemblyLoadContext` plumbing and host adapters that Python simply does not need. Python needs
`importlib.import_module(...)` on the composition root and one call. It unlocks:

- committing `{service}.spec.json` and diffing it in CI (the natural home for Gap 7's compatibility
  gate),
- generating clients for a Python service from CI without deploying it,
- publishing the spec as a build artifact for the mesh/Spec UI.

Do **not** port `OutboundRouteInspector` (unused even in .NET) or `ServiceLoadContext`.

### Implementation spec

**Package:** `benzene-core` (it already owns `ContractDocument`/`ServiceSpec` and has no third-party
deps; a build-time console script there costs a runtime service nothing). Add
`[project.scripts] benzene-contract = "benzene.core.cli:main"`.
**New module:** `packages/benzene-core/benzene/core/cli.py`.

```
benzene-contract --app mypkg.composition:build_registry \
                 --service orders [--version 1.4.0] [--description "..."] \
                 [--emit spec|descriptor|both] [--out DIR] [--indent 2]
```

- `--app` is a `module:attribute` target resolving to either a `Registry`, or a zero-arg callable
  returning one, or an `AppDefinition` (`benzene.core.startup`). Resolve with
  `importlib.import_module` + `getattr`; call it if callable. This is the whole of what
  `HostAdapters` does in .NET — the composition root builds the registry, nothing listens.
- `--emit spec` → `{service}.spec.json` from `ContractDocument.derive(...).to_payload()`.
- `--emit descriptor` → `{service}.service.json` from `benzene.mesh.ServiceDescriptor.derive(...)`
  **imported lazily**; a clear error if `benzene-mesh` is not installed. `benzene-core` must not gain
  a mesh dependency.
- Deterministic output: `json.dumps(..., indent=2, ensure_ascii=False, sort_keys=False)` — preserve
  the documents' own deliberate key order (both `to_payload()` methods order keys intentionally), so
  the file diffs cleanly commit to commit.
- Exit codes: `0` ok, `2` target could not be resolved/imported (message naming the `module:attr`
  string), `1` any other failure.
- Keep the path-derivation logic (`--out` given / defaulting to cwd) in a separate pure function so
  it is testable without touching the filesystem — the one structural idea worth copying from
  `DescriptorEmitter.ResolveOutputPaths`.

**Tests** (`tests/test_contract_cli.py`, new):

1. `test_emits_spec_json_for_a_registry_target` — a fixture module exporting a registry; assert the
   file parses and matches `ContractDocument.derive(...).to_payload()` exactly.
2. `test_emits_both_documents`.
3. `test_callable_and_app_definition_targets_are_accepted`.
4. `test_unresolvable_target_exits_2_with_a_teaching_message`.
5. `test_output_is_byte_stable_across_two_runs`.
6. `test_descriptor_emit_without_benzene_mesh_fails_clearly` (monkeypatch the import).
7. `test_output_paths_resolve` — the pure path function, table-driven.

---

## Gap 5 — No `example` payload in the Contract Document

**Severity: MEDIUM**

### What .NET has

`src/Benzene.Schema.OpenApi/Examples/ExamplePayloadBuilder.cs` (+ `IExamplePayloadBuilder`,
`ISchemaGetter`, `SchemaGetter`, `OpenApiAnyConverter`) — a **deterministic** example payload built
from a schema. Precedence: caller-supplied known values (keyed by property path
`order.customer.email`, falling back to bare name) → the schema's own `example` → `default` → first
`enum` value → a fixed value per type/format (`uuid`, `date-time`, `date`, `email`, `uri`), sized and
clamped into `minLength`/`maxLength`/`minimum`/`maximum` so the generated example passes the
validation the spec advertises. `pattern` is not reverse-generated. Reference cycles terminate as
`{}`/`[]` (ancestry-tracked, max depth 8). No randomness.

Emitted as the `example` field on each `requests[]`/`events[]` entry (`docs/spec.md`, "Example
payloads"), and reused by `TestPayloadsBuilder` to produce ready-to-POST envelopes per transport.

### What Python has / lacks

`packages/benzene-core/benzene/core/contract.py` — `ContractRequest.to_payload` /
`ContractEvent.to_payload` emit no `example` (`grep -n example contract.py` is empty). There is no
example generator anywhere in the port.

### Judgement

Worth porting the **builder**, not the `TestPayloads` machinery. The example is what makes a Contract
Document usable by a human with `curl` and by the Spec UI, and it costs one pure function over a
schema the port already derives. Deterministic-by-design is essential and is the part most likely to
be got wrong (a random or dict-ordered example would churn every artifact diff).

**Hash safety — this is why it is safe to add:** `benzene-codegen-client`'s
`contract_hash.normalize()` already strips `example` from every `requests[]` entry
(`packages/benzene-codegen-client/benzene/codegen_client/contract_hash.py:49`, §6.2). Adding examples
therefore cannot change any `contractHash`. The mesh `descriptor_hash` is computed over the
`ServiceDescriptor`, which has no `example` field at all — also unaffected. Verify both with a test.

Skip `TestPayloadsBuilder` / `ITestPayloadDresser` (per-transport payload "dressing", the AWS
dressers): Python's transport bindings are far fewer and the port has `benzene-testing` +
`/benzene/invoke` covering the same need. Revisit only if a Spec UI ships for Python.

### Implementation spec

**Package:** `benzene-core`. **New module:** `packages/benzene-core/benzene/core/examples.py`.
**Optional extra:** none.

```python
def example_payload(schema: Schema, *, known: Mapping[str, Any] | None = None,
                    max_depth: int = 8) -> Any:
    """A deterministic example value for a derived schema (never random, never dict-order dependent).

    Precedence per node: ``known`` by dotted property path, then by bare property name, then the
    schema's ``example``, ``default``, first ``enum`` entry, then a fixed value for the
    ``type``/``format`` pair. Strings are sized into ``minLength``/``maxLength``; numbers clamped
    into ``minimum``/``maximum``. ``pattern`` is not reverse-generated (a value that fails a pattern
    is still emitted — documented, same as .NET). Cycles and depth overflow terminate as ``{}``/``[]``.
    """
```

Fixed values (pin these in the tests; they are the golden output):
`string → "value"`, `integer → 0`, `number → 0.0`, `boolean → true`, `array → [<item example>]`
(one element), `object → {property: example}`, `null`/open `{}` → `None`.
Formats: `uuid → "00000000-0000-0000-0000-000000000000"`,
`date-time → "2020-01-01T00:00:00Z"`, `date → "2020-01-01"`, `email → "user@example.com"`,
`uri → "https://example.com"`. Nullable (`type: ["string","null"]`) uses the non-null member.

Wire it in: `ContractRequest` gains `example: Any = None`, emitted **only when not `None`** and
positioned per `docs/spec.md` (after `response`); same for `ContractEvent`.
`ContractDocument.derive` gains `examples: bool = True` and builds each entry's example from the
schema it just derived. `from_spec` does the same from the inline schemas. Keep the flag so a service
that wants the leanest possible document can turn it off.

**Tests** (`tests/test_examples.py` + additions to the contract-document tests):

1. `test_deterministic` — same schema built twice, and built from two dicts with different insertion
   order, yields the identical example.
2. `test_precedence_known_then_example_then_default_then_enum` — one test per rung.
3. `test_known_values_by_dotted_path_and_by_bare_name`.
4. `test_string_is_sized_into_min_and_max_length`; `test_number_is_clamped`.
5. `test_formats` — table-driven over the six formats above.
6. `test_cycle_terminates` and `test_depth_cap`.
7. `test_contract_document_emits_example_and_omitting_it_is_opt_out`.
8. `test_examples_do_not_change_the_contract_hash` — derive with and without examples, run both
   through `benzene.codegen_client.contract_hash` (or its `normalize`), assert equal.
9. `test_examples_do_not_appear_in_the_descriptor` — guard against leaking into the hashed mesh
   document.

---

## Gap 6 — No schema/contract backward-compatibility gate for CI

**Severity: MEDIUM**

### What .NET has

`src/Benzene.Schema.OpenApi/Compatibility/` — `SchemaCompatibility.Compare(baseline, current)` and
`EnsureBackwardCompatible(...)` (throws `SchemaCompatibilityException`; JSON-string overloads
deserialize a committed baseline `spec.json`), supported by `SchemaCompatibilityComparer`,
`SchemaCompatibilityRules`, `SchemaCompatibilityReport`, `SchemaChange`/`SchemaChangeKind`,
`ChangeCompatibility`, `SchemaDirection`, and `OpenApiSchemaComparer` (structural schema diff). The
documented usage is "drop `EnsureBackwardCompatible` into a test to fail CI when the `benzene`
contract stops being backward compatible with a baseline".

`SchemaRegistrar.EnsureCompatibleAsync` (Gap 3) is the runtime/registry-side sibling.

### What Python has / lacks

- `ServiceDescriptor.descriptor_hash()` (`packages/benzene-mesh/benzene/mesh/descriptor.py:138`) —
  detects *that* the contract changed, never *whether the change is safe*.
- `packages/benzene-mesh/benzene/mesh/collector.py:98,171` — the collector keeps prior contracts and
  flags which topics changed schema, but again only as change detection, mesh-side, after deploy.
- Nothing a service can run in its own CI, before merge.

### Judgement

Worth building — it is the natural CI companion to Gap 4's emitter, it is pure Python over
dictionaries the port already produces, and it needs no dependency. Scope it honestly to the JSON
Schema subset the port actually derives (§0.3 plus whatever pydantic adds under Gap 2); do not
attempt general JSON Schema subsumption.

### Implementation spec

**Package:** `benzene-core`. **New module:** `packages/benzene-core/benzene/core/compatibility.py`.

```python
class ChangeKind(str, Enum):
    TOPIC_ADDED = "topic-added"; TOPIC_REMOVED = "topic-removed"
    PROPERTY_ADDED = "property-added"; PROPERTY_REMOVED = "property-removed"
    PROPERTY_TYPE_CHANGED = "property-type-changed"
    REQUIRED_ADDED = "required-added"; REQUIRED_REMOVED = "required-removed"
    CONSTRAINT_TIGHTENED = "constraint-tightened"; CONSTRAINT_RELAXED = "constraint-relaxed"
    ENUM_VALUE_ADDED = "enum-value-added"; ENUM_VALUE_REMOVED = "enum-value-removed"

@dataclass(frozen=True)
class SchemaChange:
    kind: ChangeKind
    topic: str
    path: str            # JSON Pointer into the schema, e.g. "/properties/sku"
    detail: str
    breaking: bool

@dataclass(frozen=True)
class CompatibilityReport:
    changes: tuple[SchemaChange, ...]
    @property
    def is_compatible(self) -> bool: ...        # no breaking change
    @property
    def breaking(self) -> tuple[SchemaChange, ...]: ...
    def describe(self) -> str: ...              # one line per breaking change, for an assert message

def compare_contracts(baseline: Mapping[str, Any], current: Mapping[str, Any]) -> CompatibilityReport:
    """Compare two Contract Document payloads (``ContractDocument.to_payload()`` shape)."""

def ensure_backward_compatible(baseline, current) -> None:
    """Raise IncompatibleContractError naming *every* breaking change, or return silently."""
```

**The rules — state the direction explicitly, this is where such tools go wrong.** Backward
compatibility here means *a producer written against the baseline can still call the current
service*, i.e. the request direction is contravariant and the response direction is covariant:

| Change | Request schema | Response schema |
|---|---|---|
| property added, not required | compatible | compatible |
| property added **to `required`** | **breaking** | compatible |
| property removed | compatible | **breaking** |
| property removed from `required` | compatible | compatible (relaxed) |
| type changed | **breaking** | **breaking** |
| `enum` value removed | **breaking** | compatible |
| `enum` value added | compatible | **breaking** |
| constraint tightened (`minLength`↑, `maxLength`↓, `minimum`↑, `maximum`↓, `pattern` added/changed) | **breaking** | compatible |
| constraint relaxed | compatible | **breaking** |
| nullable removed (`["string","null"]` → `"string"`) | **breaking** | compatible |
| topic removed, or a served version removed | **breaking** | — |
| topic added | compatible | — |

Reserved topics (`benzene:` prefix — `contract.is_reserved_topic`) are excluded from the comparison,
consistent with `contract_hash.normalize()`. `example` is ignored. Recursion terminates on the same
`{}` cut the derivation uses.

Give it a CLI mode too, so Gap 4's emitter and this compose in one CI step:
`benzene-contract check --baseline orders.spec.json --current orders.spec.json` (exit `1` +
`describe()` on the stderr when incompatible).

**Tests** (`tests/test_compatibility.py`, new): one case per row of the table above, in both
directions (request and response), plus:
`test_identical_documents_are_compatible`, `test_reserved_topics_are_ignored`,
`test_examples_are_ignored`, `test_report_names_every_breaking_change_not_just_the_first`,
`test_nested_property_paths_are_reported_as_json_pointers`,
`test_ensure_raises_with_all_breaks_in_the_message`.

---

## Gap 7 — No pipeline-level, schema-derived request validation

**Severity: MEDIUM–LOW**

### What .NET has

`src/Benzene.JsonSchema/` — `JsonSchemaMiddleware<TContext>` validates the **raw wire body, before
deserialization**, against a schema resolved per topic by `IJsonSchemaProvider<TContext>`. The default
provider (`DefaultJsonSchemaProvider.cs`) generates the schema from the registered handler's request
type (camelCase, draft 2020-12, cached per type); `SuppliedJsonSchemaCatalog`/`SuppliedJsonSchemaProvider`
supply hand-authored schemas. A `null` schema means "skip". Failures short-circuit with the shared
validation contract: one `BenzeneError` per failed keyword, `Field` = the failing value's JSON Pointer,
`Code` = the failed schema keyword (`JsonSchemaValidationErrors.Format`). Missing and malformed bodies
are caught too — a thing post-deserialization validators structurally cannot do.

### What Python has / lacks

Only `@validated(Model)` — per handler, opt-in, post-decode, pydantic-only
(`packages/benzene-pydantic/benzene/pydantic/validation.py`). There is no way to say "validate every
topic against the contract I publish", and nothing validates that the published schema and the
runtime acceptance agree.

### Judgement

**Real, but the smallest of the four "worth doing" items, and it must not become a dependency
liability.** The genuine value is the *whole-registry* guarantee — "what I publish is what I accept" —
which no per-handler decorator gives. A Python service that already uses pydantic everywhere gets
most of this from Gap 2 + `@validated`.

Recommendation: build it as a **separate optional distribution** with the third-party evaluator behind
an extra, and a small built-in evaluator covering the derived subset so the common case needs nothing.
Do **not** put a JSON Schema evaluator in `benzene-core`.

### Implementation spec

**Package:** new distribution `benzene-jsonschema`, importing as `benzene.jsonschema`, depending on
`benzene-core` only.
**Optional extra:** `benzene-jsonschema[full]` → `jsonschema>=4` for full draft 2020-12 evaluation.

```python
SchemaLookup = Callable[[str, str], Schema | None]   # (topic, version) -> schema, None = skip

def registry_schemas(registry: Registry) -> SchemaLookup:
    """The default lookup: each topic's ``json_schema(definition.request_type)``, cached.

    An open schema ``{}`` (an untyped handler) resolves to ``None`` -> validation skipped, matching
    .NET's "no request type => no schema => pass through, not a rejection".
    """

def supplied_schemas(catalog: Mapping[str, Schema], fallback: SchemaLookup | None = None) -> SchemaLookup:
    """Hand-authored schemas by topic, falling back to the derived ones (.NET's SuppliedJsonSchemaCatalog)."""

def json_schema_validation(lookup: SchemaLookup, *, evaluate: Evaluator | None = None) -> Middleware:
    """Middleware validating ``context.request`` against the topic's schema before the router runs."""
```

- The middleware runs **after** the envelope has decoded the body (Python decodes in
  `BenzeneMessageApplication.handle` before the pipeline, unlike .NET where the mapper is downstream),
  so it validates `context.request`. Missing/malformed bodies are already `bad-request` at the
  envelope; do not try to reclaim that.
- On failure: `context.result = Result.validation_error(*errors)` with one `BenzeneError` per failed
  keyword — `field` = the JSON Pointer of the failing value (`/sku`, `None` at the root; **not**
  folded into the message text, exactly as `JsonSchemaValidationErrors` decided), `code` = the failed
  keyword (`required`, `maxLength`, `type`). Short-circuit; do not call `next()`.
- Default evaluator: a ~150-line pure-Python walker covering exactly the derived subset —
  `type` (incl. the `["x","null"]` list form), `properties`, `required`, `additionalProperties`,
  `items`, `enum`, `const`, `minLength`/`maxLength`, `minimum`/`maximum`, `pattern`, `minItems`/
  `maxItems`. An unknown keyword is **ignored, never a failure**. `Evaluator` is a Protocol so
  `benzene.jsonschema.strict.jsonschema_evaluator()` (the `[full]` extra) drops in for full coverage.

**Tests** (`tests/test_jsonschema.py`, new): pass/fail per supported keyword; JSON-Pointer `field`
and keyword `code` on each error; multiple failures all reported; unknown keyword ignored; open
schema `{}` → skipped; unregistered topic → skipped; supplied catalog overrides derived and falls
back; the optional strict evaluator produces the same `field`/`code` shape (skipped when `jsonschema`
is absent); a pydantic-derived schema (Gap 2) validates end to end.

---

## Gap 8 — No per-service AsyncAPI document

**Severity: LOW**

.NET: `src/Benzene.Schema.OpenApi/AsyncApi/AsyncApiDocumentBuilder.cs` — AsyncAPI 3.0 from the
service's own registry, with the 3.0 perspective done correctly (a handler `receive`s its request and
models the reply with the native `reply` object against a `<topic>:response` channel; broadcasts and
senders are `send`), map keys sanitised to `^[A-Za-z0-9.\-_]+$` with the raw topic kept in the
channel `address`, `id` = `urn:benzene:service:<title>`, `defaultContentType: application/json`.

Python: `packages/benzene-mesh/benzene/mesh/artifacts.py::_asyncapi` builds one **fleet-wide**
AsyncAPI 3.0 document from collector snapshots (channels per non-reserved topic, `receive` for
consumers, `send` for providers). A **per-service** document derived from the service's own registry —
the direct analogue — does not exist.

**Judgement:** worth doing eventually, not now. The mesh document covers the "what does this estate
look like" use case, which is the one people actually ask for; the per-service document mostly serves
AsyncAPI Studio deep-links. If it is built, it belongs in `benzene-openapi` as
`asyncapi_document(registry, *, title, version)` next to `openapi_document`, reusing `json_schema`
verbatim and copying the `_channel_key` sanitisation already in `artifacts.py`. Do **not** copy .NET's
`reply` channel modelling without first confirming Python's response semantics per transport — Kafka
in this port has no response channel at all (`consumer.py`'s docstring: "acknowledge/log only").

---

## Gap 9 — No outbound (pre-send) request validation

**Severity: LOW**

.NET: `src/Benzene.FluentValidation/ValidationClientMiddleware.cs` +
`ValidationClientMiddlewareBuilder.cs` — validates a request **before** `IBenzeneClient` sends it,
always as `ValidationError` (no status mapper on this path), with the same `Field`/`Code` mapping as
the inbound middleware. Catches a malformed outbound message at its source rather than as a remote
`bad-request`.

Python: `packages/benzene-core/benzene/core/clients.py` / `outbound.py` have no validation seam; a bad
outbound payload is discovered by the callee.

**Judgement:** genuinely small value and easy to add later — an application can already validate before
calling `send_message`. Note it, do not schedule it. If it is ever wanted, the shape is a
`validated_sender(inner: MessageSender, validate: Callable[[str, Any], Result | None]) -> MessageSender`
decorator in `benzene-core`, with a pydantic-model-per-topic convenience in `benzene-pydantic` — a
decorator, not middleware, because Python's outbound path is a port object, not a pipeline.

---

# Not worth porting — and why

Listed so a future reader does not re-litigate these.

### `Benzene.NewtonsoftJson` — not applicable

`src/Benzene.NewtonsoftJson/JsonSerializer.cs` exists because .NET has two mainstream JSON libraries
with materially different semantics (`System.Text.Json` vs Json.NET), and porting a Json.NET-shaped
model onto Benzene needed an adapter. Python has **one** JSON model: `json` in the stdlib, with
`orjson`/`ujson`/`msgspec` as drop-in *speed* replacements that produce the same values. There is no
"the other JSON library" to adapt to.

The legitimate residue — "I want a faster JSON codec" — is fully served by Gap 1's `Serializer`
protocol: an application writes a six-line `OrjsonSerializer` and registers it. The framework must not
take the dependency. Worth one paragraph in the Gap 1 docs; not a package.

### `Benzene.Xml` — not worth porting

`src/Benzene.Xml/` is ~4 files because .NET gets `System.Xml.Serialization.XmlSerializer` free in the
BCL: an attribute-driven, bidirectional type↔XML mapper. Python has no equivalent idiom —
`xml.etree` is a tree API, not an object mapper, and there is no conventional dataclass↔XML mapping
for a `to_camel`-style naming policy to plug into. Writing one would mean inventing a mapping
convention and then freezing it, for a format essentially nobody sends to a Python message-topic
service in 2026.

If a SOAP-adjacent integration ever appears, Gap 1's seam makes it an application-side
`XmlSerializer` of maybe 60 lines, scoped to that one service's actual XML dialect — which is better
than a framework-blessed guess. **Ship Gap 1; do not ship an XML package.**

### `Benzene.MessagePack` — not worth a package

The .NET package is honest about its own compromise (`Benzene.MessagePack/CLAUDE.md`): because every
Benzene transport body is a `string`, it **Base64-armors** the msgpack bytes. Python is in the same
position — the envelope `body` is `str`, and every inbound transport decodes to UTF-8 text
(`aws/events.py::_b64_to_text`, `kafka/consumer.py`). Do the arithmetic: MessagePack is roughly
0.6–0.8× JSON for typical payloads, Base64 costs 1.33×, so the armored result lands at roughly
0.8–1.0× JSON — the compression win is spent on the armor. For a format whose entire reason to exist
is size, that is not a capability, it is a wash.

The one place binary genuinely survives is the Kafka record value, which is already `bytes` at the SDK
boundary — and that is exactly the path Gap 3 opens, with the framing that actually buys interop.
So: **ship Gap 3; let an application register a msgpack `Serializer` through Gap 1 if it has measured
a win on its own traffic.** No `benzene-messagepack` package.

### `Benzene.Avro` — port the registry, not the Avro adapter

`src/Benzene.Avro/` is nine files, and most of them (`AvroSchemaGenerator`, `AvroDatumConverter`,
`AvroSchemaResolver`, `BoundedBinaryDecoder`, `AvroOptions`) exist to do reflection-based CLR→`.avsc`
generation and POCO↔`GenericRecord` conversion — work that `fastavro` does natively in Python from a
plain dict schema, with no reflection layer needed. Porting the generator would mean re-implementing,
in Python, a mapping table (`AvroSchemaGenerator`'s `uint→long`, `decimal/Guid/DateTime→string`)
whose whole purpose is to paper over C# type-system details Python does not have.

What is worth taking from the package is the *lesson*, and it is already captured elsewhere:

- The **security hardening is worth stealing** if anyone ever adds a binary decoder here.
  `BoundedBinaryDecoder` + `AvroPayloadTooLargeException` exist because Avro length-prefixes each
  `bytes`/`string` field, so a hostile body can declare a huge length and drive a large allocation
  before any data is read. The bound is *always* the decoded input size (no legitimate field is longer
  than the whole message), optionally tightened. Any Python binary `Serializer` written against Gap 1
  must carry the same rule; put it in the Gap 1 module docstring as a requirement on binary
  implementations, and repeat it in the Gap 3 cookbook.
- The **`ISchemaResolver` boundary** (schema source is pluggable and format-specific, so the registry
  package stays Avro-free) is carried into Gap 3's `SchemaResolver`.

### `Benzene.DataAnnotations` and `Benzene.FluentValidation` — not applicable

Both are adapters to **.NET validation libraries**. `Benzene.DataAnnotations` wraps
`System.ComponentModel.DataAnnotations` (BCL attributes: `[Required]`, `[Range]`, `[StringLength]`);
`Benzene.FluentValidation` wraps the FluentValidation NuGet package's `AbstractValidator<T>` fluent
rules. Python's answer to both is **pydantic**, which is already adapted
(`packages/benzene-pydantic`). Porting either would mean inventing a second validation vocabulary for
Python services to choose between — strictly worse than one good one.

Three *behaviours* inside them are worth carrying, and each is accounted for above rather than as a
port:

1. **Structured `BenzeneError(message, field, code)` instead of prose strings** — the shared failure
   contract across all three .NET validation packages. **Already done on `origin/main`**
   (`benzene/pydantic/validation.py::validation_errors` maps pydantic's `loc`→`field`,
   `type`→`code`). Nothing to do; note it so it is not "re-ported".
2. **Validation rules reaching the published schema** — .NET needs
   `IValidationSchemaBuilder`/`OpenApiValidationSchemaBuilder` to bridge FluentValidation's rule
   objects into OpenAPI. Python needs **no bridge at all**: a pydantic model's constraints are already
   in `model_json_schema()`, so **Gap 2 delivers this for free**. Do not port the bridge.
3. **Per-rule result-status mapping** (`.WithStatus(...)`, `[ValidationStatus]`,
   `IValidationStatusMapper`) — a genuine .NET feature with no pydantic equivalent, and low value:
   a handler that wants a non-default status for one rule can branch on `validation_errors()` itself.
   Skip.

Also skip `Benzene.FluentValidation/Common/*` (`IsGuid`, `IsDoubleGuid`, `IsJson`,
`IsAlphaNumericAndSymbols`, …): every one is a pydantic `Field(pattern=...)`, an annotated type, or a
`field_validator` in three lines.

### `Benzene.Schema.OpenApi`'s Swashbuckle schema builder and polymorphism rendering — subsumed

`SchemaBuilder.cs`/`ISchemaBuilder.cs` exist because .NET needs a reflection engine (Swashbuckle's
`SchemaGenerator` over the STJ contract resolver) to turn CLR types into schemas.
`SchemaGenerationOptions`/`JsonPolymorphism` add opt-in `allOf` inheritance and `oneOf` +
`discriminator`, resolved from the models' own `[JsonDerivedType]`/`[JsonPolymorphic]`.

Python's equivalent already exists in two halves: `benzene.core.json_schema` for dataclasses, and —
once Gap 2 lands — `model_json_schema()` for pydantic, which emits `oneOf` + `discriminator` for a
`Field(discriminator=...)` union natively and correctly. Porting a builder abstraction on top would
be a seam with one implementation. The **replaceability** that `ISchemaBuilder`'s DI seam provides is
delivered instead by Gap 2's `register_schema_provider` chain, which is smaller and covers the
supplied-schema case in the same mechanism.

Likewise skip `SpecCache` (.NET memoizes because Swashbuckle generation is expensive; Python's
derivation is a dict walk over a handful of types — measure before caching), `SuppliedSchemaBuilder`
as a separate type (it is a provider), and `JsonOpenApiSchemaBuilder` (schema-from-sample-JSON — a
one-off tool, not framework surface).

### `Benzene.Descriptor`'s `ServiceLoadContext` and `OutboundRouteInspector` — not applicable / dead

`ServiceLoadContext` is `AssemblyLoadContext` plumbing to keep type identity across a plugin boundary
and detect a `Benzene.Core` version skew — a problem `importlib` does not have.
`OutboundRouteInspector` is documented as **currently unused** even in .NET. Gap 4 ports
`DescriptorEmitter`'s idea and nothing else.

---

# Suggested sequencing

1. **Gap 2** (pydantic schema derivation) — critical, self-contained, unblocks the honest value of
   Gaps 5, 6 and 7, and needs no new package. Do it first.
2. **Gap 1** (`Serializer` + `MediaFormats`) — the enabling seam; small, default-off, conformance-safe.
3. **Gap 3** (schema registry + Confluent framing) — the largest new surface, but self-contained in a
   new distribution and testable with no broker.
4. **Gap 4** (`benzene-contract` CLI) then **Gap 6** (compatibility gate) — they compose into one CI
   story and should ship together or back to back.
5. **Gap 5** (examples), **Gap 7** (JSON-Schema middleware) — opportunistic.
6. **Gaps 8, 9** — note and defer.

# Standing constraints for every item above

- No new **required** third-party dependency in any package. Third-party code lives behind an optional
  extra, imported lazily, with a teaching `ImportError` naming the extra (the pattern
  `packages/benzene-kafka/benzene/kafka/producer.py:57-64` already uses).
- Python idiom over .NET shape: `Protocol` not interface hierarchies, `@dataclass(frozen=True)` not
  classes with constructor-set properties, `async def` where .NET has `Task`, module-level functions
  where .NET has a class with one method, duck-typed fakes in tests rather than mock frameworks.
- **The wire contract is frozen.** Nothing above changes the envelope shape, the camelCase naming
  policy, the status vocabulary, the status↔HTTP/gRPC tables, or any hashed document's canonical
  form. Gaps 1, 2, 3 and 5 are wire-adjacent: each PR must re-run
  `python -m tests.conformance_runner` and say so explicitly.
- Gap 2 **will** change `descriptorHash`/`contractHash` for services using pydantic types. That is the
  point of the fix, no fixture is affected, and it must be called out in the PR body and the changelog.
