"""The gRPC client binding — a :class:`~benzene.core.MessageSender` over a gRPC channel.

The reverse direction: :class:`GrpcMessageSender` publishes a message as a gRPC unary call whose method
is the topic (``/benzene.Benzene/<topic>``), forwarding the Benzene headers as request metadata and
mapping the outcome back to a :class:`~benzene.results.Result`. A ``benzene-status`` trailer, when the
peer sets one, wins verbatim; otherwise the gRPC ``StatusCode`` is mapped (the reverse table).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.core import encode_body
from benzene.results import Result, Status

import grpc

from .codes import code_to_status
from .server import method_for
from .status import BENZENE_STATUS_TRAILER


class GrpcMessageSender:
    """Sends Benzene messages over a ``grpc.Channel`` (a :class:`~benzene.core.MessageSender`)."""

    def __init__(self, channel: Any) -> None:
        self._channel = channel

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        """Invoke ``topic`` as a unary call and map the outcome to a Result — never raises.

        A gRPC error maps through the reverse code table; anything else the channel throws (a closed
        channel raises ``ValueError``, a broken transport raises whatever it likes) becomes
        ``service-unavailable`` — or ``timeout`` for a :class:`TimeoutError` — so a retrying sender,
        which retries on *statuses* rather than exceptions, can see it.
        """
        invoke = self._channel.unary_unary(method_for(topic))
        metadata = tuple((str(key), str(value)) for key, value in (headers or {}).items())
        request = encode_body(message).encode("utf-8")
        try:
            # The sync gRPC call runs on a worker thread so it never blocks the event loop.
            response, call = await asyncio.to_thread(
                lambda: invoke.with_call(request, metadata=metadata)
            )
        except grpc.RpcError as exc:
            status = _trailer_status(exc.trailing_metadata()) or code_to_status(exc.code())
            detail = exc.details()
            return Result.failure(status, detail) if detail else Result.failure(status)
        except ImportError:
            raise  # a missing grpcio plugin is a deployment error, never a service-unavailable
        except TimeoutError as ex:
            return Result.failure(Status.TIMEOUT, str(ex) or "the gRPC call timed out")
        except Exception as ex:  # a closed/broken channel is a transport failure, not a crash
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex) or type(ex).__name__)
        status = _trailer_status(call.trailing_metadata()) or "ok"
        return Result(status, _parse(response.decode("utf-8")))


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
