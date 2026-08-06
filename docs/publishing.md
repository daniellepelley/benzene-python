# Publishing the packages

Benzene ships as ten independent distributions on PyPI (see [packages](packages.md)). They are
released **in lockstep at one shared version** and published by the
[`release`](../.github/workflows/release.yml) workflow using **PyPI trusted publishing** — OpenID
Connect, so no API tokens are stored in the repository.

## What a release does

Pushing a tag `vX.Y.Z` (or running the workflow manually) triggers two jobs:

1. **build** — checks that all ten `pyproject.toml` files carry the *same* version (and, for a tag,
   that it equals `v<version>`), then builds an sdist + wheel for every package and runs
   `twine check` on all twenty artifacts.
2. **publish** — uploads every artifact to PyPI from the `pypi` GitHub Environment, authenticating via
   OIDC. PyPI routes each distribution to its project and verifies the trusted-publisher identity per
   project.

## One-time maintainer setup

Trusted publishing must be configured **once per project** before the first release. On PyPI, for each
of the ten project names —

```
benzene-results  benzene-core     benzene-http    benzene-grpc   benzene-mesh
benzene-pydantic benzene-testing  benzene-gcp     benzene-aws    benzene-azure
```

add a *pending* trusted publisher (Account → Publishing) pointing at:

| Field | Value |
|---|---|
| PyPI Project Name | the project (e.g. `benzene-core`) |
| Owner | `daniellepelley` |
| Repository | `benzene-python` |
| Workflow name | `release.yml` |
| Environment | `pypi` |

A *pending* publisher lets the very first upload create the project, so the names don't need to exist
yet. Then create a GitHub Environment named **`pypi`** on the repository (Settings → Environments) —
optionally with required reviewers so a human approves each publish.

## Cutting a release

1. Bump the version in **all ten** `packages/*/pyproject.toml` to the new `X.Y.Z` (they must match —
   the workflow enforces it). Inter-package dependencies are pinned `>=0.0.1`, so a coordinated bump
   keeps the stack installable.
2. Commit, then tag and push:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

3. Watch the `release` workflow. On success, `pip install benzene-http` (and friends) resolves the new
   version, pulling its `benzene-*` dependencies from PyPI.

## Trying it without publishing

- **Build locally** exactly as CI does — no credentials needed:

  ```bash
  for pkg in packages/*/; do python -m build --outdir dist "$pkg"; done
  python -m twine check dist/*
  ```

- **TestPyPI**: add a second `pypi`-style environment and a trusted publisher on
  [test.pypi.org](https://test.pypi.org), then a publish step with
  `with: { repository-url: https://test.pypi.org/legacy/ }`. Useful to rehearse the first release.
