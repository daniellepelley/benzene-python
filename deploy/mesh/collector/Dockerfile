# The Benzene-Python Mesh Host container.
#
# Build from the REPO ROOT (the context needs packages/ and deploy/):
#   docker build -f deploy/mesh/collector/Dockerfile -t benzene-mesh-host .
#
# Pre-publish, the Benzene layers install from the workspace source; once they are on PyPI, replace the
# source COPY + install with `pip install benzene-mesh benzene-http` and just COPY the collector.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PORT=8080

# The layers the mesh host needs (results <- core <- http/mesh), installed from workspace source.
COPY packages/benzene-results /src/benzene-results
COPY packages/benzene-core /src/benzene-core
COPY packages/benzene-http /src/benzene-http
COPY packages/benzene-mesh /src/benzene-mesh
COPY deploy/mesh/collector/requirements.txt /src/requirements.txt
RUN pip install --no-cache-dir \
      /src/benzene-results /src/benzene-core /src/benzene-http /src/benzene-mesh \
 && pip install --no-cache-dir -r /src/requirements.txt

# The host app (imported as the `collector` package: python -m collector.main).
COPY deploy/mesh/collector /app/collector

EXPOSE 8080
# The fleet to poll is supplied at run time via MESH_SERVICES (inline JSON) or a mounted MESH_CONFIG file.
CMD ["python", "-m", "collector.main"]
