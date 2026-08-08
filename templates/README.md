# Benzene Python starter templates (Copier)

The `dotnet new` equivalent for the Benzene Python port: generate a runnable Benzene service — a
composition root, a demo handler with one injected service, the transport host entry point, a
`pyproject.toml`, and an optional pytest component test — with a single command.

We use [**Copier**](https://copier.readthedocs.io/) rather than Cookiecutter for one reason:
`copier update`. A project generated from these templates records its answers in
`.copier-answers.yml`, so when the templates improve you re-run `copier update` inside your project
and pull the changes in (three-way-merged against your edits) — Cookiecutter has no equivalent.

## Usage

```bash
pip install copier

# From GitHub (the templates live in this subdirectory of the repo):
copier copy gh:daniellepelley/benzene-python/templates my-service

# ...or from a local checkout of this repo:
copier copy templates my-service
```

Copier then asks four questions:

| Question        | Meaning                                                            | Default              |
| --------------- | ----------------------------------------------------------------- | -------------------- |
| `project_name`  | Human-readable service name (used in prose/docstrings)            | `My Benzene Service` |
| `project_slug`  | Python package name — importable, lowercase, underscores          | derived from name    |
| `transport`     | Which host/transport to wire (see below)                          | `aws-apigateway`     |
| `include_tests` | Generate a pytest component test that drives the real pipeline?   | `true`               |

Answer non-interactively with `--data`:

```bash
copier copy --defaults --data project_name="Orders Service" --data transport=aws-sqs templates my-service
```

### One template, a `transport` choice (not one template per transport)

The three starters share almost everything — the same composition root, the same demo handler, the
same injected `Greeter` — differing only in the host wiring and the test's front door. That is a
better fit for **one Copier template with a `transport` question** (Jinja conditionals switch the few
transport-specific lines) than three near-duplicate template trees. `include_tests` is a second
toggle that omits the `tests/` directory (and its dev-deps) when off.

## Transports

| `transport`      | Host                                             | Demo handler wired as        |
| ---------------- | ------------------------------------------------ | ---------------------------- |
| `aws-apigateway` | AWS Lambda behind API Gateway (HTTP)             | `GET /hello/{name}` route     |
| `aws-sqs`        | AWS Lambda triggered by SQS (fire-and-forget)    | `hello:world` topic           |
| `grpc`           | a gRPC server (method = topic, host anywhere)    | `hello:world` topic           |

These are the **common cross-language core** every Benzene port ships as starters. The generated
project's own `README.md` covers deploying/running that specific transport.

## What each project contains

```
my-service/
  pyproject.toml            # deps on the real benzene-* PyPI packages (+ a [test] extra)
  README.md
  .copier-answers.yml       # your answers, for `copier update`
  <project_slug>/
    __init__.py
    handlers.py             # the Greeter service + the demo hello handler (your logic goes here)
    startup.py              # the composition root (<Name>StartUp) — every host and test boots from it
    host.py                 # builds the AWS Lambda app / the gRPC server (the only transport-specific file)
    main.py                 # entry point: the Lambda handler, or `python -m <slug>.main` for gRPC
  tests/                    # only when include_tests=true
    test_hello.py           # component test: boots StartUp, swaps Greeter for a spy, pushes a message
```

The component test mirrors the .NET templates' pattern: boot the **same** app `StartUp` configures
for a real deployment via `create_test_host(StartUp)`, override the `Greeter` with a spy through the
`with_services` seam, then push a message through the whole pipeline via the transport's own front
door (`send_http` / `send_sqs` / `send_grpc`) and assert the handler ran.

## Published vs. local dependencies

Generated projects depend on the **published** package names (`benzene-aws`, `benzene-grpc`,
`benzene-testing`, …) exactly as an adopter will consume them once they are on PyPI. **Until they
are published**, install the `benzene-*` deps from a local checkout of this repo first (editable),
then install the generated project with `--no-deps`:

```bash
pip install -e /path/to/benzene-python/packages/benzene-core \
            -e /path/to/benzene-python/packages/benzene-results \
            -e /path/to/benzene-python/packages/benzene-http \
            -e /path/to/benzene-python/packages/benzene-aws \
            -e /path/to/benzene-python/packages/benzene-testing
cd my-service && pip install -e . --no-deps && pytest
```

The generated project's own `README.md` repeats the local-install recipe scoped to its transport.

## Note for maintainers

The `templates/` tree is inert Jinja — it is **not** collected by the repo's `pytest` (the suite's
`testpaths` list `tests/`, `examples/`, and the mesh dirs, never `templates/`), and the `.jinja`
files are not importable Python. Keep the demo handler and `StartUp` shape in sync with
`examples/orders_domain` and the `benzene-testing` harness; these templates are the polished,
parametrized distillation of those examples.
