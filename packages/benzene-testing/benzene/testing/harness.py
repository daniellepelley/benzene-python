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
assert response.batch_item_failures == []          # assert on the transport response
assert fake.last_topic == "orders:created"         # ...and on the client's egress
```

Need a service for an assertion? Every built host exposes the resolved root scope as ``host.scope``,
so ``host.scope.get_service(OrderService)`` reaches the same instance the handlers ran against.

The cloud packages are imported lazily inside each ``build_*`` so this stays a ``benzene-core``-only
dependency; ``.build_aws()`` raises a clear error if ``benzene-aws`` isn't installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from benzene.core import BenzeneStartUp, ServiceOverride, build_application

if TYPE_CHECKING:
    # Return types for editor/mypy help only — the cloud packages stay lazy runtime imports so
    # benzene-testing keeps its benzene-core-only dependency.
    from benzene.aws.testing import AwsLambdaTestHost, SqsConsumerTestHost
    from benzene.azure.testing import AzureFunctionsTestHost
    from benzene.core import AppDefinition, Scope
    from benzene.gcp.testing import GcpFunctionsTestHost
    from benzene.grpc.testing import GrpcTestHost
    from benzene.http.testing import HttpTestHost
    from benzene.kafka.testing import KafkaTestHost
    from benzene.rabbitmq.testing import RabbitMqTestHost


class TestHostBuilder:
    """Transport- and cloud-neutral setup; a single ``build_<cloud>()`` picks the host."""

    def __init__(self, startup: BenzeneStartUp | type[BenzeneStartUp]) -> None:
        self._startup = startup
        self._overrides: list[ServiceOverride] = []
        self._config: dict[str, str] = {}

    def with_services(self, override: ServiceOverride) -> TestHostBuilder:
        """Register services over the startup's own (last wins) — the seam for fakes/mocks.

        The override's return value is ignored, so the one-liner
        ``.with_services(lambda c: c.add_instance(Greeter, fake))`` type-checks; Container's ``add_*``
        methods are fluent, so that lambda returns a Container rather than None.
        """
        self._overrides.append(override)
        return self

    def with_config(self, values: Mapping[str, str]) -> TestHostBuilder:
        """Layer configuration values on top of the startup's defaults."""
        self._config.update(values)
        return self

    def _build(self) -> tuple[AppDefinition, Scope]:
        return build_application(self._startup, overrides=self._overrides, config=self._config)

    def build_http(self) -> HttpTestHost:
        """Specialize to a standalone HTTP test host (requires ``benzene-http``).

        Boots the same app onto the plain ASGI binding (:class:`~benzene.http.BenzeneHttpApp`) — no
        cloud — so a service hosted on a standalone HTTP server is tested through the same
        ``send_http`` front door as the API Gateway / Cloud Functions hosts.
        """
        try:
            from benzene.http import BenzeneHttpApp
            from benzene.http.testing import HttpTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_http() requires the 'benzene-http' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = HttpTestHost(BenzeneHttpApp.from_definition(definition))
        host.scope = scope
        return host

    def build_gcp(self) -> GcpFunctionsTestHost:
        """Specialize to a GCP Cloud Functions test host (requires ``benzene-gcp``)."""
        try:
            from benzene.gcp import GcpFunctionsApp
            from benzene.gcp.testing import GcpFunctionsTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_gcp() requires the 'benzene-gcp' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = GcpFunctionsTestHost(GcpFunctionsApp.from_definition(definition))
        host.scope = scope
        return host

    def build_aws(self) -> AwsLambdaTestHost:
        """Specialize to an AWS Lambda test host (requires ``benzene-aws``)."""
        try:
            from benzene.aws import AwsLambdaApp
            from benzene.aws.testing import AwsLambdaTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_aws() requires the 'benzene-aws' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = AwsLambdaTestHost(AwsLambdaApp.from_definition(definition))
        host.scope = scope
        return host

    def build_grpc(self) -> GrpcTestHost:
        """Specialize to a gRPC test host (requires ``benzene-grpc[transport]`` — grpcio).

        The gRPC binding serves every topic as a generic unary method, so there is no router to
        mount: the whole registry is driven through one :class:`BenzeneGrpcHandler`, in memory.
        """
        try:
            from benzene.grpc import BenzeneGrpcHandler
            from benzene.grpc.testing import GrpcTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_grpc() requires 'benzene-grpc[transport]' (grpcio) to be installed"
            ) from ex
        definition, scope = self._build()
        host = GrpcTestHost(BenzeneGrpcHandler.from_definition(definition))
        host.scope = scope
        return host

    def build_kafka(self) -> KafkaTestHost:
        """Specialize to a self-hosted Kafka consumer test host (requires ``benzene-kafka``).

        The Kafka binding is a consumer loop, not an HTTP host, so there is no router to mount: the
        registry is driven one record at a time through a :class:`KafkaConsumerApp`, in memory. Feed
        records with ``await host.send_kafka(topic, body, headers)``.
        """
        try:
            from benzene.kafka import KafkaConsumerApp
            from benzene.kafka.testing import KafkaTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_kafka() requires the 'benzene-kafka' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = KafkaTestHost(KafkaConsumerApp.from_definition(definition))
        host.scope = scope
        return host

    def build_rabbitmq(self) -> RabbitMqTestHost:
        """Specialize to a self-hosted RabbitMQ consumer test host (requires ``benzene-rabbitmq``).

        The RabbitMQ binding is a consumer loop, not an HTTP host, so there is no router to mount: the
        registry is driven one delivery at a time through a :class:`RabbitMqConsumerApp`, in memory.
        Feed deliveries with ``await host.send_rabbitmq(topic, body, headers)``.
        """
        try:
            from benzene.rabbitmq import RabbitMqConsumerApp
            from benzene.rabbitmq.testing import RabbitMqTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_rabbitmq() requires the 'benzene-rabbitmq' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = RabbitMqTestHost(RabbitMqConsumerApp.from_definition(definition))
        host.scope = scope
        return host

    def build_sqs_consumer(self) -> SqsConsumerTestHost:
        """Specialize to a self-hosted SQS consumer test host (requires ``benzene-aws``).

        The SQS binding is a poll loop, not an HTTP host, so there is no router to mount: the
        registry is driven one message at a time through an :class:`SqsConsumerApp`, in memory. Feed
        messages with ``await host.send_sqs_consumer(topic, body, headers)``. Distinct from
        ``.build_aws()`` — that specializes to the Lambda *event-source* binding, this one to the
        self-hosted poller.
        """
        try:
            from benzene.aws import SqsConsumerApp
            from benzene.aws.testing import SqsConsumerTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_sqs_consumer() requires the 'benzene-aws' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = SqsConsumerTestHost(SqsConsumerApp.from_definition(definition))
        host.scope = scope
        return host

    def build_azure(self) -> AzureFunctionsTestHost:
        """Specialize to an Azure Functions test host (requires ``benzene-azure``)."""
        try:
            from benzene.azure import AzureFunctionsApp
            from benzene.azure.testing import AzureFunctionsTestHost
        except ImportError as ex:  # pragma: no cover - environment-specific
            raise ImportError(
                "build_azure() requires the 'benzene-azure' package to be installed"
            ) from ex
        definition, scope = self._build()
        host = AzureFunctionsTestHost(AzureFunctionsApp.from_definition(definition))
        host.scope = scope
        return host


def create_test_host(startup: BenzeneStartUp | type[BenzeneStartUp]) -> TestHostBuilder:
    """Start building a provider-agnostic end-to-end test host from a ``BenzeneStartUp``."""
    return TestHostBuilder(startup)
