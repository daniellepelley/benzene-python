"""The lists a release depends on, checked against `packages/` rather than against a reader's memory.

The release workflow builds with a glob (`for pkg in packages/*/`) and uploads every artifact in one
`pypi-publish` call, so the enumerated trusted-publisher list is not what decides *what* ships — it
is what decides whether the upload is accepted. PyPI verifies the OIDC identity per project, so one
project with no pending publisher fails the whole release, and nothing in the repo noticed that
`benzene-codegen-client` had been missing from the list since it was added.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _distributions() -> set[str]:
    """Every distribution the release workflow's `packages/*/` glob will build."""
    return {path.parent.name for path in (_ROOT / "packages").glob("*/pyproject.toml")}


def test_every_distribution_has_a_trusted_publisher_in_the_release_workflow() -> None:
    workflow = (_ROOT / ".github" / "workflows" / "release.yml").read_text()
    missing = sorted(name for name in _distributions() if name not in workflow)
    assert not missing, (
        f"{missing} would be built and uploaded but are not in release.yml's trusted-publisher "
        "list; add them there and register the pending publisher on PyPI (docs/publishing.md)"
    )


def test_the_publishing_guide_lists_every_distribution_too() -> None:
    guide = (_ROOT / "docs" / "publishing.md").read_text()
    missing = sorted(name for name in _distributions() if name not in guide)
    assert not missing, f"{missing} are absent from docs/publishing.md's one-time-setup list"
