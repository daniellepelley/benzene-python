"""``benzene.pydantic`` — validate a handler's request with a `pydantic <https://docs.pydantic.dev>`_ model.

An optional adapter (distribution ``benzene-pydantic``) for the Python ecosystem's standard
validation library. The :func:`validated` decorator lets a handler take a pydantic ``BaseModel`` as
its request: the decoded body is validated into the model before the handler runs, and a
``ValidationError`` becomes a ``validation-error`` :class:`~benzene.results.Result` naming each bad
field — so a malformed request never reaches your handler and never crashes the pipeline.

    pip install benzene-pydantic

Depends on ``benzene-core`` and ``pydantic``. Contributes the ``benzene.pydantic`` subpackage to the
shared ``benzene`` namespace. The core stays pydantic-free — this is the one place the dependency
lives, so a service opts in only where it wants pydantic validation.
"""

from __future__ import annotations

from .validation import format_validation_errors, validated

__all__ = ["format_validation_errors", "validated"]
