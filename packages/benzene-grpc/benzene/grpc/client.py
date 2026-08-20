"""The gRPC client binding — a :class:`~benzene.core.MessageSender` over a gRPC channel.

The reverse direction: :class:`GrpcMessageSender` publishes a message as a gRPC unary call whose method
is the topic (``/benzene.Benzene/<topic>``), forwarding the Benzene headers as request metadata and
mapping the outcome back to a :class:`~benzene.results.Result`. A ``benzene-status`` trailer, when the
peer sets one, wins verbatim; otherwise the gRPC ``StatusCode`` is mapped (the reverse table). On a
failure the ``grpc-status-details-bin`` trailer's ``google.rpc.BadRequest`` is read back into the
result's structured ``errors`` (§4.2) — a non-OK call has no body to carry a problem document, so
without that the ``field`` a validator knew would not survive the hop.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.core import encode_body
from benzene.results import KNOWN_STATUSES, Result

import grpc

from .codes import code_to_status
from .details import errors_from_trailers
from .server import method_for
from .status import BENZENE_STATUS_TRAILER


class GrpcMessageSender:
    """Sends Benzene messages over a ``grpc.Channel`` (a :class:`~benzene.core.MessageSender`)."""

    def __init__(self, channel: Any) -> None:
        self._channel = channel

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        invoke = self._channel.unary_unary(method_for(topic))
        metadata = tuple((str(key), str(value)) for key, value in (headers or {}).items())
        request = encode_body(message).encode("utf-8")
        try:
            # The sync gRPC call runs on a worker thread so it never blocks the event loop.
            response, call = await asyncio.to_thread(
                lambda: invoke.with_call(request, metadata=metadata)
            )
        except grpc.RpcError as exc:
            trailers = exc.trailing_metadata()
            status = _trailer_status(trailers) or code_to_status(exc.code())
            # The peer's structured errors, when it sent any, are authoritative and ordered - the
            # same precedence section 1.3 gives the problem document's `errors` over its `detail`
            # (see problem_errors). A peer that sends no details trailer lands on the message-only
            # fallback below, unchanged.
            errors = errors_from_trailers(trailers)
            if errors:
                return Result.failure(status, *errors)
            detail = exc.details()
            return Result.failure(status, detail) if detail else Result.failure(status)
        status = _trailer_status(call.trailing_metadata()) or "ok"
        return Result(status, _parse(response.decode("utf-8")), successful=_successful(status))


def _successful(status: str) -> bool | None:
    """The success classification for a call that returned no ``RpcError`` — so, gRPC code ``OK``.

    gRPC has no ``isSuccessful`` member to carry section 1.2's authoritative signal; the code is the
    only place it survives. For a status **in** the section 3 vocabulary the status decides and
    there is nothing to state (``None`` - derive it). For an **application-defined** status the code
    is the whole signal, and an ``OK`` answer means the peer classified it successful: state that,
    or the escape hatch the peer used to say "successful" would decode back as a failure here.
    """
    return None if status in KNOWN_STATUSES else True


def _trailer_status(trailing_metadata: Any) -> str | None:
    for key, value in trailing_metadata or ():
        if key == BENZENE_STATUS_TRAILER:
            return value
    return None


def _parse(body: str) -> Any:
    if not body:
        return None
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return body
