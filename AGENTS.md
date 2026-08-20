# Benzene for Python — Project Guide for AI Coding Agents

## What this is

The **Python port** of [Benzene](https://github.com/daniellepelley/Benzene), a hexagonal
(ports-and-adapters) architecture for message-driven services. The language-neutral **specification**
and the **conformance fixtures** are owned by the cross-language
[`Benzene`](https://github.com/daniellepelley/Benzene) repo (`docs/specification/**`); this repo
implements them in Python. The sibling ports are
[benzene-dotnet](https://github.com/daniellepelley/benzene-dotnet) (the original),
[benzene-go](https://github.com/daniellepelley/benzene-go) and
[benzene-typescript](https://github.com/daniellepelley/benzene-typescript).

## Structure

A monorepo of **nineteen independently-publishable distributions** under `packages/`, all sharing the
`benzene` [PEP 420 namespace](https://peps.python.org/pep-0420/) so an adopter installs only the
layers they use. The dependency direction is strict and one-way:

- **`benzene-results`** — `Result`, `BenzeneError`, `ProblemDetails`, the status vocabulary. Zero
  dependencies; everything else sits on it.
- **`benzene-core`** — registry + `@message`, the middleware pipeline, per-invocation DI, the
  composition root (`BenzeneStartUp` / `AppDefinition`), and the transport-neutral `BenzeneMessage`
  envelope. Depends on `benzene-results` only, and stays transport-free.
- **Transport bindings** — `benzene-http` (ASGI), `benzene-grpc`, `benzene-gcp`, `benzene-aws`,
  `benzene-azure`, `benzene-kafka`, `benzene-rabbitmq`.
- **Cross-cutting middleware** — `benzene-resilience`, `benzene-auth`, `benzene-cache`,
  `benzene-otel`, `benzene-openapi`.
- **Mesh** — `benzene-mesh` (descriptor, tracing, collector), `benzene-mesh-fleet`.
- **Adapters and tools** — `benzene-pydantic`, `benzene-testing`, `benzene-codegen-client`.

Everything else: `conformance/` (the vendored fixture snapshot), `tests/` (cross-package tests + the
dependency-free conformance runner), `examples/` (runnable, dogfood-tested demos — collected by
pytest, so they are a gate), `templates/` (Copier project templates; inert Jinja, not collected),
`deploy/mesh/` (the Fargate Mesh Host + Terraform), `docs/`.

## Running things

No build step — the layers resolve straight off `sys.path` via the `pythonpath` in `pyproject.toml`:

```bash
pytest                              # the whole suite, including examples/ and deploy/mesh/
ruff check . && mypy                # the same lint + type-check gate CI runs
python -m tests.conformance_runner  # the fixtures, without pytest (needs PYTHONPATH — see below)
```

The conformance runner is deliberately runnable **without pytest**, so give it the same paths pytest
reads from `[tool.pytest.ini_options].pythonpath`:

```bash
PYTHONPATH=$(python -c "import tomllib;print(':'.join(tomllib.load(open('pyproject.toml','rb'))['tool']['pytest']['ini_options']['pythonpath']))") \
  python -m tests.conformance_runner
```

Only `rfc8785` is required (by `benzene-codegen-client`); `grpcio` and `pydantic` unlock the gRPC
transport and validation tests, which otherwise skip.

## The conformance fixtures are not ours to edit

`conformance/*.json` is a **vendored snapshot** of the canonical fixtures in the `Benzene` repo, and
`conformance/SPEC_VERSION` pins the commit it came from.

- **Never edit a fixture to make an implementation pass.** Fix the implementation, or change the
  spec deliberately in the `Benzene` repo and re-vendor here. The fixture is the neutral truth that
  keeps four ports interoperable; editing it locally makes this port pass and the wire wrong.
- `.github/workflows/conformance-drift-check.yml` diffs the snapshot against canonical **both ways**
  and fails on any difference, including a canonical fixture missing from the snapshot.
- `tests/conformance_runner.py` fails if any vendored fixture is never opened by a runner. A fixture
  this port genuinely should not run goes in `UNRUN_FIXTURES` **with its reason in writing** — not
  because it is failing, but because `conformance/README.md` marks it conditional on a capability
  this port does not implement.

## Conventions

- **The spec's names on the wire, Python's names in the API.** `snake_case` in Python, camelCase on
  the wire (`benzene.core.to_camel` / `to_jsonable` do the translation) — the envelope, status
  vocabulary and status mappings are interop contracts and never bend to local idiom.
- **A `Result`, never an exception, for a domain outcome.** Exceptions are for defects.
- **`isSuccessful` is the authoritative success signal** on a response envelope (wire-contracts
  §1.2). A receiver reads `benzene.core.successful_from(envelope)`, never
  `is_successful(status_string)` — an application-defined status means nothing to a receiver
  classifying by text.
- **No `Async` suffixes**, explicit registration over reflection, middleware as a declarative
  `AppDefinition` field. See "Notes for readers of the .NET original" in `README.md`.
- Every package needs complete PyPI metadata and a `twine`-clean sdist + wheel; a **new package must
  be added to the trusted-publisher lists** in `.github/workflows/release.yml` and
  `docs/publishing.md`, and its pending publisher registered on PyPI, or the next release fails for
  every package at once (`tests/test_packaging_lists.py` guards the lists).

## Do NOT

- Do not edit a conformance fixture, or add one to `UNRUN_FIXTURES`, to turn a failure green.
- Do not put transport knowledge in `benzene-core`, or reach across the layering (a lower package
  never imports a higher one).
- Do not add a runtime dependency to a package that does not already have one; optional SDKs
  (`grpcio`, `boto3`, cloud SDKs, `redis`, …) go in an extra and import lazily.
- Do not leave a doc claiming shipped work is pending, or pending work shipped.

## Workflow expectations

- Plan-first for non-trivial changes.
- `pytest` **and** `python -m tests.conformance_runner` green before committing, plus `ruff` and
  `mypy`.
- Keep commits scoped to one logical change, and say what was wrong rather than what was typed.
