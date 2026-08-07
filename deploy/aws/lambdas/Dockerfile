# Lambda container image shared by all three mesh services (orders / payments / shipping).
#
# One image, three functions: each Lambda overrides image_config.command to select its entrypoint
# (orders_handler.handler / payments_handler.handler / shipping_handler.handler). Build from the REPO
# ROOT so the workspace packages + the shared domain are in the build context:
#
#     docker build -f deploy/aws/lambdas/Dockerfile -t benzene-mesh-lambda .
#
# Bundles the benzene-* layers this host needs (results, core, http, aws, mesh) + boto3, then copies the
# shared domain (mesh_fleet) and the flat handler modules into the Lambda task root.
FROM public.ecr.aws/lambda/python:3.12

# 1. The workspace packages (installed from source — they are not published to PyPI). benzene-aws is the
#    Lambda host binding; benzene-mesh gives the service its MeshFeedSender + trace middleware. boto3 is
#    the AWS SDK (the runtime already ships it, but pin it so the image is self-contained).
COPY packages/benzene-results /build/benzene-results
COPY packages/benzene-core    /build/benzene-core
COPY packages/benzene-http    /build/benzene-http
COPY packages/benzene-aws     /build/benzene-aws
COPY packages/benzene-mesh    /build/benzene-mesh
RUN pip install --no-cache-dir \
        /build/benzene-results /build/benzene-core /build/benzene-http \
        /build/benzene-aws /build/benzene-mesh \
        "boto3>=1.26" \
    && rm -rf /build

# 2. The shared domain (imported as `mesh_fleet.domain`) + the flat handler/service modules, into the
#    Lambda task root so `service` and `mesh_fleet` are importable and `<svc>_handler.handler` resolves.
COPY examples/mesh_fleet ${LAMBDA_TASK_ROOT}/mesh_fleet
COPY deploy/aws/lambdas/service.py ${LAMBDA_TASK_ROOT}/service.py
COPY deploy/aws/lambdas/orders_handler.py ${LAMBDA_TASK_ROOT}/orders_handler.py
COPY deploy/aws/lambdas/payments_handler.py ${LAMBDA_TASK_ROOT}/payments_handler.py
COPY deploy/aws/lambdas/shipping_handler.py ${LAMBDA_TASK_ROOT}/shipping_handler.py

# Default entrypoint; Terraform overrides it per function via image_config.command.
CMD ["orders_handler.handler"]
