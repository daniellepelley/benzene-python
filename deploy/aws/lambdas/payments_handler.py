"""AWS Lambda entrypoint for the **payments** mesh service.

The Lambda container image is shared by all three services; each function's
``image_config.command`` points at this module's ``handler`` (i.e. ``payments_handler.handler``).
Handler modules are copied flat into the image's task root, so this is an absolute import of the
sibling :mod:`service` module. See :mod:`service` for the env-driven wiring.
"""

from __future__ import annotations

from service import make_handler

handler = make_handler("payments")
