#!/usr/bin/env bash
# Build the service Function App deployment zip — one shared artifact for all six domains
# (orders/payments/shipping/inventory/notifications/analytics); Terraform sets SERVICE_NAME (+ per-
# producer outbound target env vars) per app, all pointing the same zip at function_app.py.
#
# Unlike examples/aws_lambda_mesh's Lambda zips, no dependency vendoring happens here: the Function
# App's SCM_DO_BUILD_DURING_DEPLOYMENT=true app setting (deploy/main.tf) makes Oryx `pip install` from
# requirements.txt during zip deploy, so this just stages the source + host.json + requirements.txt at
# the zip root (function_app.py must sit at the deployment root for Azure Functions' v2 Python model to
# discover it).
#
# Run from the repo root:  examples/azure_functions_mesh/deploy/build_service.sh
# Produces: examples/azure_functions_mesh/deploy/build/service.zip (Terraform's var.service_zip default)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
example="$(cd "$here/.." && pwd)"
build="$example/deploy/build"
staging="$build/service-pkg"

rm -rf "$staging" "$build/service.zip"
mkdir -p "$staging"

cp "$example"/service/*.py "$staging/"
cp "$example/service/host.json" "$staging/host.json"
cp "$example/service/requirements.txt" "$staging/requirements.txt"
rm -f "$staging"/__pycache__ 2>/dev/null || true

( cd "$staging" && zip -qr "$build/service.zip" . -x '*.pyc' '*/__pycache__/*' )
echo "built $build/service.zip"
