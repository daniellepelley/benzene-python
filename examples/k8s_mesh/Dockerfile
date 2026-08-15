# The k8s_mesh domain-SERVICE image — one image, deployed three times (orders/payments/shipping,
# selected at runtime by MESH_SERVICE). Build from the REPO ROOT (the context needs packages/ and
# examples/):
#   docker build -f examples/k8s_mesh/Dockerfile -t benzene-k8smesh-service:local .
#
# Pre-publish, the Benzene layers install from workspace source; once they are on PyPI, replace the
# source COPY + install with `pip install benzene-http benzene-mesh` and just COPY examples/k8s_mesh.
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PORT=8080

COPY packages/benzene-results packages/benzene-results
COPY packages/benzene-core packages/benzene-core
COPY packages/benzene-http packages/benzene-http
COPY packages/benzene-mesh packages/benzene-mesh
COPY examples/k8s_mesh/requirements.txt requirements.txt
RUN pip install --no-cache-dir \
      ./packages/benzene-results ./packages/benzene-core ./packages/benzene-http ./packages/benzene-mesh \
 && pip install --no-cache-dir -r requirements.txt

COPY examples/k8s_mesh k8s_mesh

EXPOSE 8080
# MESH_SERVICE (orders/payments/shipping), DOWNSTREAM_MSG_URL, and MESH_COLLECTOR_ENVELOPE_URL are
# supplied at run time (see k8s/services.yaml).
CMD ["python", "-m", "k8s_mesh.service.main"]
