# Publishing the packages

Benzene ships as eighteen independent distributions on PyPI (see [packages](packages.md)). They are
released **in lockstep at one shared version** and published by the
[`release`](../.github/workflows/release.yml) workflow using **PyPI trusted publishing** — OpenID
Connect, so no API tokens are stored in the repository.

## Pre-1.0: every release is a real PyPI prerelease

Until Benzene for Python reaches 1.0, every version published carries a
[PEP 440](https://peps.python.org/pep-0440/) prerelease segment — `0.1.0b1`, `0.1.0b2`, …, `0.1.0rc1`,
never a bare `0.1.0`. This is not just a label: `packaging.version.Version("0.1.0b1").is_prerelease`
is `True`, and pip's resolver **skips prereleases by default**. That is deliberate: these packages
are for early testing, not production, and the version string says so rather than relying on a
reader noticing a low number or a "beta" note in the README.

**Inter-package dependencies must carry a prerelease floor — `benzene-core>=0.1.0b1`, not
`benzene-core>=0.0.1`.** PEP 440 permits prereleases into a resolution only when the specifier
*itself* names one as a bound. With a plain `>=0.0.1` floor and nothing but prereleases published,
pip has no candidate it is willing to pick, and the install fails outright:

```
$ pip install benzene-core
ERROR: Could not find a version that satisfies the requirement benzene-results>=0.0.1
       (from benzene-core) (from versions: 0.1.0b1)
```

That failed with **and without** `--pre`, so it was not a gate — it made every multi-package
install impossible. Fixed by raising all inter-package floors to `>=0.1.0b1`; raise them again with
each version bump.

> **A note on what the prerelease version does and does not gate.** It does not stop a plain
> `pip install`. When the *only* published version satisfying a requirement is a prerelease, pip
> takes it anyway — so `pip install benzene-results` today installs `0.1.0b1` with no `--pre`
> (verified 2026-08-14). The prerelease segment is therefore honest signalling, not enforcement; it
> starts excluding these builds only once a stable version exists to prefer instead. Do not
> reintroduce a non-prerelease floor believing it gates anything — the exclusion happens at the
> top level, not through the dependency floors.

## What a release does

Pushing a tag `vX.Y.Z` (or running the workflow manually) triggers two jobs:

1. **build** — checks that all eighteen `pyproject.toml` files carry the *same* version (and, for a
   tag, that it equals `v<version>`), then builds an sdist + wheel for every package and runs
   `twine check` on all thirty-six artifacts.
2. **publish** — uploads every artifact to PyPI from the `pypi` GitHub Environment, authenticating via
   OIDC. PyPI routes each distribution to its project and verifies the trusted-publisher identity per
   project.

## One-time maintainer setup

Trusted publishing must be configured **once per project** before the first release. On PyPI, for each
of the eighteen project names —

```
benzene-results   benzene-core      benzene-http     benzene-grpc      benzene-mesh
benzene-pydantic  benzene-testing   benzene-gcp      benzene-aws       benzene-azure
benzene-kafka     benzene-rabbitmq  benzene-auth     benzene-cache     benzene-resilience
benzene-openapi   benzene-otel      benzene-mesh-fleet
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

1. Bump the version in **all eighteen** `packages/*/pyproject.toml` to the new version (they must
   match — the workflow enforces it). Pre-1.0, that means a prerelease identifier every time —
   `0.1.0b2`, `0.1.0b3`, … (bump the counter), or `0.1.0rc1` once the `0.1.0` line is stabilizing.
   Inter-package dependencies are pinned `>=0.0.1`, so a coordinated bump keeps the stack installable.
   Only drop the prerelease suffix (ship a bare `X.Y.Z`) for an actual 1.0-and-beyond stable release,
   deliberately decided, not as a default.
2. Commit, then tag and push:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

3. Watch the `release` workflow. On success, `pip install benzene-http==<version>` (and friends)
   resolves the new version, pulling its `benzene-*` dependencies from PyPI. A bare
   `pip install benzene-http` continues to skip it, same as every prerelease before it.

## Trying it without publishing

- **Build locally** exactly as CI does — no credentials needed:

  ```bash
  for pkg in packages/*/; do python -m build --outdir dist "$pkg"; done
  python -m twine check dist/*
  ```

- **TestPyPI**: add a second `pypi`-style environment and a trusted publisher on
  [test.pypi.org](https://test.pypi.org), then a publish step with
  `with: { repository-url: https://test.pypi.org/legacy/ }`. Useful to rehearse the first release.
