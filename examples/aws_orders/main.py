"""AWS Lambda entry point (the handler you point your Lambda function at).

One function handles API Gateway, SQS, and SNS events — the binding selects the inner handling by
event shape. Configure the handler as ``main.handler`` and set ``BENZENE_SNS_TOPIC_ARN``.
"""

from __future__ import annotations

from benzene.aws import to_lambda_handler

from .host import build_aws_orders_app

handler = to_lambda_handler(build_aws_orders_app())
