"""``benzene.testing`` — in-memory test host and doubles for testing Benzene apps.

The transport-neutral testing layer (distribution ``benzene-testing``): drive an application in
memory and fake the external edges, without deploying to a cloud. Transport packages add native
event builders on top (e.g. ``benzene.gcp.testing``, ``benzene.aws.testing``). Mirrors .NET's
``Benzene.Testing`` + the per-transport ``*.TestHelpers`` packages; this is a dev/test dependency.
"""

from __future__ import annotations

from .fakes import FakeMessageSender, SentMessage
from .harness import TestHostBuilder, create_test_host
from .host import InMemoryBenzeneHost, MessageBuilder

__all__ = [
    "FakeMessageSender",
    "InMemoryBenzeneHost",
    "MessageBuilder",
    "SentMessage",
    "TestHostBuilder",
    "create_test_host",
]
