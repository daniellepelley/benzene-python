"""AWS Lambda host for the shared order domain (API Gateway + SQS + SNS + egress)."""

from __future__ import annotations

from .host import build_aws_orders_app

__all__ = ["build_aws_orders_app"]
