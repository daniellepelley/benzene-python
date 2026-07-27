"""Deliberately imported by nothing else: only a package walk reaches this."""

from __future__ import annotations

from benzene.core import message
from benzene.results import Result


@message("sample:deep")
async def deep(_request: dict) -> Result:
    return Result.ok({"deep": True})
