"""A handler nothing registers — the case warn_unregistered_handlers exists to surface."""

from __future__ import annotations

from benzene.core import message
from benzene.results import Result


@message("sample:stray")
async def stray(_request: dict) -> Result:
    return Result.ok()
