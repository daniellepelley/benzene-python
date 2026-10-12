"""An envelope-wrapping outbound HTTP client — the Python analogue of .NET's ``HttpBenzeneMessageClient``.

``benzene.http.HttpMessageSender`` (the port's usual outbound HTTP client) POSTs a message's raw
serialized body to a URL, with the topic riding along as a header — the right shape for a peer whose
inbound side is a per-topic REST route (see ``docs/cookbooks/calling-other-services.md``).

The K8s mesh example instead wants what .NET's ``HttpBenzeneMessageClient`` gives it: **one fixed
target URL that serves every topic**, addressed by POSTing the actual wire envelope
``{topic, headers, body}`` — exactly what this port's own Cloud Service Profile ``/benzene/invoke``
surface (``benzene.http.StandardPaths``, R4) expects and answers. ``EnvelopeHttpMessageSender`` is that
client: it wraps the message in an envelope, POSTs it to one URL, and decodes the response envelope
back into a :class:`~benzene.results.Result` via :func:`benzene.core.decode_response` — the same decode
step an in-process or Lambda-invoke caller uses for a peer that speaks the envelope directly.

Used for both service-to-service chaining (``DOWNSTREAM_MSG_URL``, pointed at the next service's own
``/benzene/invoke``) and reporting into the mesh collector (``MESH_COLLECTOR_ENVELOPE_URL``, pointed at
the mesh's ``/benzene/invoke``) — one mechanism, matching .NET's K8sMesh example, where both hops speak
the same lightweight BenzeneMessage-over-HTTP client.

This is example-local code (not a change to ``benzene-http``): the packages already ship the
REST-style outbound client that fits *most* Benzene services, and this repo's own convention (see
``docs/cookbooks/calling-other-services.md``) is to reach for it first. A small, local client is the
right size for the one example that specifically wants the generic single-endpoint shape.
"""

from __future__ import annotations

import asyncio
import json

from benzene.core import decode_response, encode_body, failed_call_result
from benzene.http import from_http
from benzene.http.client import HttpTransport, stdlib_transport
from benzene.results import Result, is_successful, problem_errors, problem_status


class EnvelopeHttpMessageSender:
    """A :class:`~benzene.core.MessageSender` that POSTs a ``{topic, headers, body}`` envelope to a
    single fixed URL and decodes the response envelope — the client side of ``/benzene/invoke``.
    """

    def __init__(self, url: str, *, transport: HttpTransport | None = None) -> None:
        self._url = url
        self._transport = transport or stdlib_transport()

    async def send_message(
        self, topic: str, message: object, headers: dict[str, str] | None = None
    ) -> Result:
        envelope = {"topic": topic, "headers": dict(headers or {}), "body": encode_body(message)}
        try:
            reply = await self._transport(
                self._url, {"content-type": "application/json"}, json.dumps(envelope)
            )
        except (OSError, asyncio.TimeoutError) as exc:
            # No reply at all - a dead peer is a timeout or service-unavailable Result like any
            # other failure, not an exception (transport-bindings §1).
            return failed_call_result(exc, f"POST {self._url}")
        # The envelope endpoint answers 200 for every envelope it processed and carries the outcome in
        # the envelope, so read the body whatever the status; only a body that is not an envelope
        # falls back to the reverse HTTP table (transport-bindings §1, "Outbound clients").
        try:
            response_envelope = json.loads(reply.body) if reply.body else None
        except (ValueError, TypeError):
            response_envelope = None
        if isinstance(response_envelope, dict) and "statusCode" in response_envelope:
            return decode_response(response_envelope)
        status = from_http(reply.status_code)
        if is_successful(status):
            return Result(status)
        if isinstance(response_envelope, dict) and "benzeneStatus" in response_envelope:
            # A carrier failure answered with a problem document (e.g. a body that was not an
            # envelope, 400): its benzeneStatus and errors win over the bare table (wire-contracts 4.1).
            return Result.failure(
                problem_status(response_envelope) or status, *problem_errors(response_envelope)
            )
        return Result.failure(
            status, f"{self._url} answered HTTP {reply.status_code} without a response envelope"
        )
