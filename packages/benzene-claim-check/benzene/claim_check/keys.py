"""The object-key layout the two cloud stores share.

Kept in one place so an S3 bucket and a Blob container laid out by different services still look the
same to an operator, and so a deployment that overrides the layout overrides it once.
"""

from __future__ import annotations

import datetime
import uuid

#: The default key prefix for both cloud stores. A prefix (rather than the bucket/container root) is
#: what makes a lifecycle rule and an IAM policy expressible against claim-check objects alone.
DEFAULT_PREFIX = "claim-checks/"


def default_key() -> str:
    """``2026/09/07/1a2b…`` — a date path, then a random key.

    The date segment costs nothing and makes the store legible by eye: an operator can see at a
    glance how far back objects go, and whether the retention rule is actually firing. The random
    tail is what makes a key unguessable and collision-free; nothing about a reference is derived
    from the payload, so a store never needs to know what a payload means.
    """
    today = datetime.datetime.now(tz=datetime.timezone.utc)
    return f"{today:%Y/%m/%d}/{uuid.uuid4().hex}"
