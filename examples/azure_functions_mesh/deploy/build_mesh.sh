#!/usr/bin/env bash
# Build the mesh Function App deployment zip (discovery + interrogation + Blob catalog publisher).
# Same Oryx-remote-build approach as build_service.sh — no dependency vendoring, just source staging.
#
# Run from the repo root:  examples/azure_functions_mesh/deploy/build_mesh.sh
# Produces: examples/azure_functions_mesh/deploy/build/mesh.zip (Terraform's var.mesh_zip default)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
example="$(cd "$here/.." && pwd)"
build="$example/deploy/build"
staging="$build/mesh-pkg"

rm -rf "$staging" "$build/mesh.zip"
mkdir -p "$staging"

cp "$example"/mesh/*.py "$staging/"
cp "$example/mesh/host.json" "$staging/host.json"
cp "$example/mesh/requirements.txt" "$staging/requirements.txt"
rm -f "$staging"/__pycache__ 2>/dev/null || true

( cd "$staging" && zip -qr "$build/mesh.zip" . -x '*.pyc' '*/__pycache__/*' )
echo "built $build/mesh.zip"
