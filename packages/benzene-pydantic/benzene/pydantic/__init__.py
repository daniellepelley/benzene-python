"""``benzene.pydantic`` — validate a handler's request with a `pydantic <https://docs.pydantic.dev>`_ model.

An optional adapter (distribution ``benzene-pydantic``) for the Python ecosystem's standard
validation library. The :func:`validated` decorator lets a handler take a pydantic ``BaseModel`` as
its request: the decoded body is validated into the model before the handler runs, and a
``ValidationError`` becomes a ``validation-error`` :class:`~benzene.results.Result` naming each bad
field — so a malformed request never reaches your handler and never crashes the pipeline.

    pip install benzene-pydantic

Importing this package also teaches ``benzene.core`` to derive a payload schema from a
``BaseModel`` (:func:`~benzene.pydantic.pydantic_schema`, registered below), so a service modelled
in pydantic publishes a real contract on ``/benzene/spec``, in its mesh descriptor and in its
OpenAPI document instead of the open schema ``{}``.

Depends on ``benzene-core`` and ``pydantic``. Contributes the ``benzene.pydantic`` subpackage to the
shared ``benzene`` namespace. The core stays pydantic-free — this is the one place the dependency
lives, so a service opts in only where it wants pydantic validation.
"""

from __future__ import annotations

from benzene.core import register_schema_provider

from .schema import inline_defs, pydantic_schema
from .validation import format_validation_errors, validated

# The opt-in, and the reason `benzene-core` can stay pydantic-free: a service that installs and
# imports this adapter gets pydantic-aware schema derivation everywhere the core derives one; a
# service that does not is byte-for-byte unaffected. Registration is idempotent in effect (the
# provider is pure and claims only BaseModel subclasses) and Python imports a module once, so a
# second `import benzene.pydantic` does not stack a second provider.
register_schema_provider(pydantic_schema)

__all__ = [
    "format_validation_errors",
    "inline_defs",
    "pydantic_schema",
    "validated",
]
