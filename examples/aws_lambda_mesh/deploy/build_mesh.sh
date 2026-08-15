#!/usr/bin/env bash
# Build the mesh Lambda deployment zip (discovery + interrogation + S3 catalog publisher).
# Run from the repo root:  examples/aws_lambda_mesh/deploy/build_mesh.sh
# Produces: examples/aws_lambda_mesh/deploy/build/mesh.zip (Terraform's var.mesh_zip default)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
example="$(cd "$here/.." && pwd)"
root="$(cd "$example/../.." && pwd)"
build="$example/deploy/build"
staging="$build/mesh-pkg"

rm -rf "$staging" "$build/mesh.zip"
mkdir -p "$staging"

pip install --quiet --target "$staging" \
  "$root/packages/benzene-results" \
  "$root/packages/benzene-core" \
  "$root/packages/benzene-http" \
  "$root/packages/benzene-aws" \
  "$root/packages/benzene-mesh" \
  "$root/packages/benzene-mesh-fleet" \
  boto3

# Only mesh/ is needed for this handler (service/ is the six domain Lambdas' code, not the mesh's).
mkdir -p "$staging/aws_lambda_mesh"
cp "$example/__init__.py" "$staging/aws_lambda_mesh/__init__.py"
cp -r "$example/mesh" "$staging/aws_lambda_mesh/mesh"
rm -rf "$staging/aws_lambda_mesh/mesh/__pycache__"

( cd "$staging" && zip -qr "$build/mesh.zip" . -x '*.pyc' '*/__pycache__/*' )
echo "built $build/mesh.zip"
