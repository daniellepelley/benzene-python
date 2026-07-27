"""The provider-agnostic end-to-end test harness — ``create_test_host(StartUp)``.

Boot the *real* application from its :class:`~benzene.core.BenzeneStartUp`, override any registration
with a fake, and specialize to a cloud host in a **single call** — the only thing that changes
between an AWS Lambda test and a GCP test is ``.build_aws()`` vs ``.build_gcp()``. Mirrors .NET's
``BenzeneTestHost.Create<StartUp>().WithServices(...).BuildAwsLambdaHost()``.

```python
def overrides(services):
    services.add_instance(OrderService, store)
    services.add_instance(MessageSender, fake)     # only the external edges are faked

host = create_test_host(OrdersStartUp).with_services(overrides).build_aws()
response = host.send_sqs("orders:created", order)  # native event in the front door
assert response == {"batchItemFailures": []}       # assert on the transport response
assert fake.last_topic == "orders:created"         # ...and on the client's egress
```

The cloud packages are imported lazily inside each ``build_*`` so this stays a ``benzene-core``-only
dependency; ``.build_aws()`` raises a clear error if ``benzene-aws`` isn't installed.
"""

from __future__ import annotations

from typing import Callable, Mapping

from benzene.core import BenzeneStartUp, Container, build_application


class TestHostBuilder:
    """Transport- and cloud-neutral setup; a single ``build_<cloud>()`` picks the host."""

    def __init__(self, startup: BenzeneStartUp | type[BenzeneStartUp]) -> None:
        self._startup = startup
        self._overrides: list[Callable[[Container], None]] = []
        self._config: dict[str, str] = {}

    def with_services(self, override: Callable[[Container], None]) -> "TestHostBuilder":
        """Register services over the startup's own (last wins) — the seam for fakes/mocks."""
        self._overrides.append(override)
        return self

    def with_config(self, values: Mapping[str, str]) -> "TestHostBuilder":
        """Layer configuration values on top of the startup's defaults."""
        self._config.update(values)
        return self

    def _build(self):
        return build_application(self._startup, overrides=self._overrides, config=self._config)

    def build_gcp(self):
        """Specialize to a GCP Cloud Functions test host (requires ``benzene-gcp``)."""
        try:
            from benzene.gcp import GcpFunctionsApp
            from benzene.gcp.testing import GcpFunctionsTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError("build_gcp() requires the 'benzene-gcp' package to be installed") from ex
        definition, _ = self._build()
        return GcpFunctionsTestHost(
            GcpFunctionsApp(http_router=definition.router, registry=definition.registry)
        )

    def build_aws(self):
        """Specialize to an AWS Lambda test host (requires ``benzene-aws``)."""
        try:
            from benzene.aws import AwsLambdaApp
            from benzene.aws.testing import AwsLambdaTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError("build_aws() requires the 'benzene-aws' package to be installed") from ex
        definition, _ = self._build()
        return AwsLambdaTestHost(
            AwsLambdaApp(http_router=definition.router, registry=definition.registry)
        )

    def build_azure(self):
        """Specialize to an Azure Functions test host (requires ``benzene-azure``)."""
        try:
            from benzene.azure import AzureFunctionsApp
            from benzene.azure.testing import AzureFunctionsTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError("build_azure() requires the 'benzene-azure' package to be installed") from ex
        definition, _ = self._build()
        return AzureFunctionsTestHost(
            AzureFunctionsApp(http_router=definition.router, registry=definition.registry)
        )


def create_test_host(startup: BenzeneStartUp | type[BenzeneStartUp]) -> TestHostBuilder:
    """Start building a provider-agnostic end-to-end test host from a ``BenzeneStartUp``."""
    return TestHostBuilder(startup)
