"""Persistence for the collector — the seam that lets the fleet view survive a restart (mesh.md §6).

:class:`MeshCollector` keeps its catalog in memory. That is exactly right for tests and for a single
run, but a long-lived Mesh Host (the Fargate container) should not forget the whole fleet every time
its task is replaced. A :class:`CollectorStore` is the seam between the two: the collector snapshots
its state through it and restores from it on start.

The default is :class:`NullCollectorStore` — a no-op, so in-memory usage and every test pay nothing and
behave exactly as before. The container swaps in :class:`JsonFileCollectorStore`, pointed at a file on
a mounted volume (EFS), so a redeployed task rehydrates the fleet it already knew.

The snapshot is a plain JSON-able ``dict`` (see :meth:`MeshCollector.snapshot`); a store only has to
move that dict to and from durable bytes.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CollectorStore(Protocol):
    """Durable home for a collector snapshot: load it on start, save it after each ingest.

    Two methods, both dealing in the JSON-able snapshot dict:

    - :meth:`load` returns the last saved snapshot, or ``None`` when there is nothing to restore
      (first boot, or an unreadable/corrupt store — a broken store must never stop the host coming up).
    - :meth:`save` durably records the given snapshot, replacing any previous one.
    """

    def load(self) -> dict[str, Any] | None: ...

    def save(self, snapshot: dict[str, Any]) -> None: ...


class NullCollectorStore:
    """The default store: keeps nothing. The collector stays purely in-memory (tests, single runs)."""

    def load(self) -> dict[str, Any] | None:
        return None

    def save(self, snapshot: dict[str, Any]) -> None:
        return None


class JsonFileCollectorStore:
    """A snapshot on disk as JSON — the durable store the container mounts on a volume.

    ``save`` writes atomically (temp file in the same directory, then :func:`os.replace`) so a task
    killed mid-write can never leave a half-written, unparseable snapshot behind. ``load`` is
    forgiving: a missing file is a first boot (``None``), and a corrupt file is treated the same way
    rather than crashing the host — a fresh catalog refills from the fleet within one poll interval.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[str, Any] | None:
        try:
            text = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            snapshot = json.loads(text)
        except json.JSONDecodeError:
            return None  # a truncated/corrupt snapshot: start clean rather than fail to boot
        return snapshot if isinstance(snapshot, dict) else None

    def save(self, snapshot: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Write to a sibling temp file then atomically replace, so readers never see a partial file.
        fd, tmp_name = tempfile.mkstemp(
            dir=self._path.parent, prefix=f".{self._path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise
