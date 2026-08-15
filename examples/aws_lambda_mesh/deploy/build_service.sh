#!/usr/bin/env bash
# Build the service Lambda deployment zip — one shared artifact for all six domains (orders/payments/
# shipping/inventory/notifications/analytics); Terraform sets SERVICE_NAME (+ per-producer outbound
# target env vars) per function, all pointing the same zip's handler at service.main.handler.
# Run from the repo root:  examples/aws_lambda_mesh/deploy/build_service.sh
# Produces: examples/aws_lambda_mesh/deploy/build/service.zip (Terraform's var.service_zip default)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
example="$(cd "$here/.." && pwd)"
root="$(cd "$example/../.." && pwd)"
build="$example/deploy/build"
staging="$build/service-pkg"

rm -rf "$staging" "$build/service.zip"
mkdir -p "$staging"

# The pure-Python Benzene layers a service Lambda needs (no compiled deps -> portable zip), plus boto3
# for the real SQS/SNS/EventBridge outbound clients.
pip install --quiet --target "$staging" \
  "$root/packages/benzene-results" \
  "$root/packages/benzene-core" \
  "$root/packages/benzene-http" \
  "$root/packages/benzene-aws" \
  "$root/packages/benzene-mesh" \
  boto3

# The example package itself (service/ + its __init__.py); leave mesh/, tests/, deploy/ out — the
# service Lambda's handler only ever imports aws_lambda_mesh.service.main.
mkdir -p "$staging/aws_lambda_mesh"
cp "$example/__init__.py" "$staging/aws_lambda_mesh/__init__.py"
cp -r "$example/service" "$staging/aws_lambda_mesh/service"
rm -rf "$staging/aws_lambda_mesh/service/__pycache__"

( cd "$staging" && zip -qr "$build/service.zip" . -x '*.pyc' '*/__pycache__/*' )
echo "built $build/service.zip"
