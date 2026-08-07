#!/usr/bin/env bash
# Build the fleet Lambda deployment zip (one artifact serving every fleet node; the env selects which).
# Run from the repo root:  deploy/mesh/fleet/build.sh
# Produces: deploy/mesh/build/fleet.zip  (Terraform's var.fleet_zip default)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
build="$root/deploy/mesh/build"
staging="$build/fleet-pkg"

rm -rf "$staging" "$build/fleet.zip"
mkdir -p "$staging"

# The pure-Python Benzene layers the fleet service needs (no compiled deps → portable zip).
pip install --quiet --target "$staging" \
  "$root/packages/benzene-results" \
  "$root/packages/benzene-core" \
  "$root/packages/benzene-http" \
  "$root/packages/benzene-aws" \
  "$root/packages/benzene-mesh"

# The env-driven service (handler = fleet.service.handler).
cp -r "$root/deploy/mesh/fleet" "$staging/fleet"
rm -rf "$staging/fleet/tests" "$staging/fleet/build.sh"

( cd "$staging" && zip -qr "$build/fleet.zip" . -x '*.pyc' '*/__pycache__/*' )
echo "built $build/fleet.zip"
