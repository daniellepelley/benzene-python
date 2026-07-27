# benzene-testing

In-memory test host and test doubles for [Benzene Python](https://github.com/daniellepelley/benzene-python)
— drive your handlers through the real pipeline, faking only the external edges, without deploying to
a cloud. Mirrors .NET's `Benzene.Testing`.

```bash
pip install benzene-testing        # dev/test dependency
```

```python
from benzene.testing import InMemoryBenzeneHost, FakeMessageSender

host = InMemoryBenzeneHost(registry)
response = await host.send_message("order:create", {"sku": "ABC"})
assert response["statusCode"] == "created"
```

`FakeMessageSender` records outbound publishes so a test can prove ingress → handler → egress
carried the payload through. Transport packages add native event builders on top
(`benzene.gcp.testing`, `benzene.aws.testing`, …). Depends on `benzene-core`; contributes the
`benzene.testing` subpackage to the shared `benzene` namespace.
