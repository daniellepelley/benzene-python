"""Dogfoods ``benzene-codegen-client`` against a real, .NET-produced Contract Document.

``contracts/payments.spec.json`` is a copy of ``benzene-dotnet``'s
``examples/AwsMesh/Orders/contracts/payments.spec.json`` — a real ``Benzene.Descriptor``-emitted
document, proving the generator reads a document authored by a *different* language's producer, not
just its own fixtures. ``generate.py`` regenerates ``generated/payments_capture_client.py`` (a
topic-scoped client for ``payments:capture`` only); ``tests/test_payments_client.py`` wires it to a
``FakeMessageSender`` and asserts the send, the payload type, the embedded contract hash, and that
no ``benzene:*`` reserved topic leaks into the generated output.
"""

from __future__ import annotations
