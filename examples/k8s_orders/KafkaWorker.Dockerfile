# Build context is the repo root - see Api.Dockerfile's comment on the local-packages install.
#   docker build -f examples/k8s_orders/KafkaWorker.Dockerfile -t orders-kafka-worker:local .
FROM python:3.12-slim
WORKDIR /app

COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-kafka packages/benzene-kafka
RUN pip install --no-cache-dir "./packages/benzene-results" "./packages/benzene-core" \
      "./packages/benzene-kafka[kafka]"

COPY examples/orders_domain orders_domain
COPY examples/kafka_orders kafka_orders

CMD ["python", "-m", "kafka_orders.host"]
