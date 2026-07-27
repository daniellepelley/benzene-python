"""Re-exports a handler on purpose: the same function is then an attribute of two modules, which
a scan must de-duplicate rather than register twice."""

from .orders import create_order  # noqa: F401
