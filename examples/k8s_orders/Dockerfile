# Build context is the repo root (the example imports the local packages/, matching this monorepo's
# pre-PyPI-publish state - once the benzene-* packages are published this collapses to
# `pip install benzene-http benzene-aws[boto3] benzene-kafka[kafka] uvicorn`).
#   docker build -f examples/k8s_orders/Dockerfile -t k8s-orders:local .
FROM python:3.12-slim
WORKDIR /app

COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-http packages/benzene-http
COPY packages/benzene-aws packages/benzene-aws
COPY packages/benzene-kafka packages/benzene-kafka
RUN pip install --no-cache-dir \
      ./packages/benzene-results ./packages/benzene-core ./packages/benzene-http \
      "./packages/benzene-aws[boto3]" "./packages/benzene-kafka[kafka]" "uvicorn>=0.30"

COPY examples/orders_domain orders_domain
COPY examples/http_orders http_orders
COPY examples/sqs_orders sqs_orders
COPY examples/kafka_orders kafka_orders
COPY examples/k8s_orders k8s_orders

ENV PORT=8080
EXPOSE 8080
CMD ["python", "-m", "k8s_orders.app"]
