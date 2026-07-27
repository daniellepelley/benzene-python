"""``benzene.gcp`` — the Google Cloud Functions host and its transport bindings.

Distribution ``benzene-gcp``: host the same Benzene handlers behind Cloud Functions HTTP and Pub/Sub
triggers, plus a Pub/Sub outbound client. Depends on ``benzene-core`` and ``benzene-http``. Mirrors
.NET's ``Benzene.GoogleCloud.Functions.*``.

    from benzene.gcp import GcpFunctionsApp, http_function, pubsub_function

The ``google-cloud-pubsub`` SDK is only needed for the real outbound client and is an optional
extra (``pip install benzene-gcp[pubsub]``); the inbound bindings and the test host need no cloud
SDK. Contributes the ``benzene.gcp`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .functions import GcpFunctionsApp, http_function, pubsub_function
from .pubsub import TOPIC_ATTRIBUTE, PubSubMessageSender, decode_pubsub_message

__all__ = [
    "GcpFunctionsApp",
    "PubSubMessageSender",
    "TOPIC_ATTRIBUTE",
    "decode_pubsub_message",
    "http_function",
    "pubsub_function",
]
