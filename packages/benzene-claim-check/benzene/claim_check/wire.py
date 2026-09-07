"""The frozen cross-port wire surface: one header name and one placeholder shape.

Everything in this module is a **cross-language contract**, not a Python decision. A payload a
Python service offloads must be hydratable by a .NET consumer and vice versa, so both names below
are taken verbatim from the .NET port and are quoted here with their source so a future reader can
see they were copied rather than invented:

``Benzene.ClaimCheck/ClaimCheckHeaders.cs``::

    public const string ClaimCheck = "benzene-claim-check";

``Benzene.ClaimCheck/ClaimCheckPlaceholder.cs``::

    public string _benzeneClaimCheck { get; set; }

**Where the canonical specification stands on each of them.** The header *is* specified:
``docs/specification/wire-contracts.md`` §2 lists ``benzene-claim-check`` as a **Tier C (add-on)**
header — only meaningful when the application wired this optional middleware — and §2.1 gives its
value shape (an opaque URI-form reference ``scheme://location/key``), the fail-loud rule, the
store-boundary rule, and the ban on delete-on-consume. This package implements that section; it does
not extend it.

The **placeholder body is not specified**. §2.1 says, in as many words, that the body of an offloaded
message is *unspecified* — a consumer "MUST NOT interpret it directly, and MUST treat the header as
authoritative". ``{"_benzeneClaimCheck": "<ref>"}`` is therefore a .NET *convention* that this port
adopts deliberately, so that the bytes on the wire are identical between the two ports and a human
reading a raw queue message sees the same thing whichever port sent it. That is **Python matching
.NET ahead of the spec**, and it should be proposed upstream as a SHOULD (a recommended placeholder
body) rather than left as a shared-but-unwritten habit in two ports. Until then, nothing in this
package — and nothing in .NET's — *reads* the placeholder: the header is authoritative on both
sides, which is exactly what keeps the unwritten part harmless.

The leading underscore is load-bearing and is .NET's own reasoning: an underscore has no upper or
lower case, so the key round-trips identically through any serializer's naming policy (camelCase,
PascalCase, or none). It matches the ``_benzeneHeaders`` embedded-header key's rationale in
``wire-contracts.md``. Python's :func:`benzene.core.encode_body` writes application ``dict`` keys
verbatim, so the placeholder must be built as a plain dict and never passed through a field-name
camel-caser.
"""

from __future__ import annotations

#: The reserved default header carrying a claim-check reference (.NET ``ClaimCheckHeaders.ClaimCheck``,
#: ``wire-contracts.md`` §2, Tier C). A default, not a hard-coded literal: both middleware halves take
#: a ``header=`` override, and the spec's rule applies — an override is a deployment agreement and
#: MUST be applied to the sending and the receiving side alike.
CLAIM_CHECK_HEADER = "benzene-claim-check"

#: The single key in an offloaded message's placeholder body (.NET ``ClaimCheckPlaceholder``).
#: Verbatim, never camel-cased. Frozen: see this module's docstring.
PLACEHOLDER_KEY = "_benzeneClaimCheck"


def claim_check_placeholder(reference: str) -> dict[str, str]:
    """The tiny body that replaces an offloaded payload on the wire.

    Present for a human reading a raw queue message, and for a non-Benzene consumer that at least
    learns *why* the body it expected is not there. The header is authoritative: a consumer with the
    add-on wired replaces this body wholesale before deserialization and never inspects it.
    """
    return {PLACEHOLDER_KEY: reference}
