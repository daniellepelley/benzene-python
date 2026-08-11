# Build context is the repo root - see Api.Dockerfile's comment on the local-packages install.
#   docker build -f examples/k8s_orders/SqsWorker.Dockerfile -t orders-sqs-worker:local .
FROM python:3.12-slim
WORKDIR /app

COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-aws packages/benzene-aws
RUN pip install --no-cache-dir "./packages/benzene-results" "./packages/benzene-core" \
      "./packages/benzene-aws[boto3]"

COPY examples/orders_domain orders_domain
COPY examples/sqs_orders sqs_orders

CMD ["python", "-m", "sqs_orders.host"]
