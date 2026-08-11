# Build context is the repo root (the example imports the local packages/, matching this monorepo's
# pre-PyPI-publish state - once the benzene-* packages are published this collapses to
# `pip install benzene-http uvicorn`).
#   docker build -f examples/k8s_orders/Api.Dockerfile -t orders-api:local .
FROM python:3.12-slim
WORKDIR /app

COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-http packages/benzene-http
RUN pip install --no-cache-dir \
      ./packages/benzene-results ./packages/benzene-core ./packages/benzene-http "uvicorn>=0.30"

COPY examples/orders_domain orders_domain
COPY examples/http_orders http_orders

ENV PORT=8080
EXPOSE 8080
CMD ["uvicorn", "http_orders.main:app", "--host", "0.0.0.0", "--port", "8080"]
