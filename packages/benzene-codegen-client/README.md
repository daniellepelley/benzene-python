# benzene-codegen-client

Generates a typed, topic-scoped Python client SDK from a Benzene **Contract Document**
(`{Service}.spec.json`, produced by a Benzene service's own descriptor build — .NET, Go, TypeScript,
or Python) — so a Python consumer of any Benzene service gets a typed client with no .NET SDK
required.

This package is a **build-time / CLI tool**, not a runtime dependency of a Benzene service. It
depends on `benzene-core` (the generated code's only runtime dependency is
`benzene.core.clients.MessageSender` and `benzene.results.Result`), but nothing depends on it.

See [`docs/codegen-client.md`](../../docs/codegen-client.md) at the repo root for the full guide,
and `docs/specification/contract-document.md` in the
[Benzene spec repo](https://github.com/daniellepelley/Benzene) for the normative format this
generator implements.

## Quick start

```bash
pip install benzene-codegen-client
benzene-codegen service --spec payments.spec.json --service Payments --package payments_client
benzene-codegen topic --spec payments.spec.json --topic payments:capture --out payments_capture_client.py
```
