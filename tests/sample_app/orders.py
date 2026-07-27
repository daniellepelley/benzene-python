"""Sample handlers for the discovery tests."""

from __future__ import annotations

from benzene.core import message
from benzene.results import Result


@message("sample:order:create")
async def create_order(_request: dict) -> Result:
    return Result.ok({"created": True})


@message("sample:order:cancel")
async def cancel_order(_request: dict) -> Result:
    return Result.ok({"cancelled": True})
