# The Mesh Host container image (collector + aggregator + UI), run on App Runner (or Fargate).
#
# Build from the REPO ROOT so the workspace packages are in the build context:
#
#     docker build -f deploy/aws/host/Dockerfile -t benzene-mesh-host .
#
# Installs the benzene-* layers the host needs — benzene-mesh with its [host] (benzene-http) and [aws]
# (boto3) extras — plus uvicorn (the ASGI server), then runs the env-driven host_app:app on :8080.
FROM python:3.12-slim

WORKDIR /app

# The workspace packages (installed from source — not published to PyPI). Installing benzene-http and
# boto3 explicitly satisfies benzene-mesh's [host] + [aws] extras from local dists (no PyPI fetch).
COPY packages/benzene-results /build/benzene-results
COPY packages/benzene-core    /build/benzene-core
COPY packages/benzene-http    /build/benzene-http
COPY packages/benzene-mesh    /build/benzene-mesh
RUN pip install --no-cache-dir \
        /build/benzene-results /build/benzene-core /build/benzene-http /build/benzene-mesh \
        "boto3>=1.26" "uvicorn>=0.30" \
    && rm -rf /build

# The host ASGI app + the single canonical mesh UI (copied straight from its one home — never forked;
# it lands at /app/mesh-ui.html and is copied into out_dir at startup, same-origin with the artifacts).
COPY deploy/aws/host/host_app.py /app/host_app.py
COPY examples/mesh_fleet/mesh-ui.html /app/mesh-ui.html

ENV BENZENE_MESH_UI_HTML=/app/mesh-ui.html \
    BENZENE_MESH_OUT_DIR=/tmp/mesh-artifacts \
    PYTHONUNBUFFERED=1

EXPOSE 8080
# One instance only (the collector catalog is in-process) — App Runner is pinned min=max=1.
CMD ["uvicorn", "host_app:app", "--host", "0.0.0.0", "--port", "8080"]
