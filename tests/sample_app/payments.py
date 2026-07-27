"""A second module, plus a versioned handler."""

from __future__ import annotations

from benzene.core import message
from benzene.results import Result


@message("sample:payment:capture")
async def capture(_request: dict) -> Result:
    return Result.ok({"captured": True})


@message("sample:payment:capture", version="2")
async def capture_v2(_request: dict) -> Result:
    return Result.ok({"captured": True, "v": 2})
