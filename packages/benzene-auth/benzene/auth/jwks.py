"""JWKS fetch, OIDC discovery, and **key-rotation-aware** JWT validation (mirrors ``Benzene.Auth.OAuth2``).

:class:`~benzene.auth.JwtValidator` verifies against a *configured static key*, which breaks the
moment the identity provider rotates its signing key. Real IdPs (Auth0, Cognito, Entra, Okta,
Keycloak) publish a **JWKS** document and rotate the keys in it, so this module resolves the
verification key *per token*, by the ``kid`` in the token header, from a cached JWKS document —
refetching when a ``kid`` is unknown, which is what makes a rotation a non-event instead of an
outage. :class:`JwksValidator` is a drop-in ``validate`` for
:func:`~benzene.auth.bearer_token_interception` (async, like the core's health checks await either
shape), and everything it touches the network with is injectable, so tests need no socket:

* ``fetch`` — a :data:`JwksFetch` (``url -> document text``); the default uses ``urllib`` on a worker
  thread, exactly like :mod:`benzene.mesh.probe`'s HTTP seam.
* ``clock`` — a :data:`Clock` (``() -> float``), the same monotonic-clock seam
  ``benzene.resilience`` and ``benzene.cache`` use, so TTLs are crossed by arithmetic, not sleeps.
* ``decode`` — a :data:`JwksDecoder` (``(token, jwk, algorithm) -> claims``); the default imports
  PyJWT lazily, so the ``[jwks]`` extra (PyJWT with its crypto backend) is only needed to actually
  verify a signature.

**The refresh policy is the security-relevant part.** An unknown ``kid`` is exactly what a key
rotation looks like — and also exactly what an attacker sending random ``kid`` values looks like. A
validator that refetches on every miss (what a naive ``PyJWKClient`` loop does) lets an unauthenticated
caller drive one outbound request per token, turning this service into an amplifier pointed at its own
IdP. So every fetch — scheduled or forced — passes one gate:

* **A floor between attempts** (:data:`DEFAULT_MIN_REFRESH_INTERVAL`, 300s). At most **one** outbound
  fetch per document per floor, no matter how many unknown ``kid``\\ s arrive. Failures count against
  it too, so an IdP that is down is not hammered either.
* **Single-flight.** Concurrent misses coalesce behind one :class:`asyncio.Lock`; eight simultaneous
  requests for a just-rotated key cause one fetch, not eight.
* **Last known good.** A failed refresh keeps the keys already fetched — an IdP outage must not
  invalidate every token in flight.
* **A TTL** (:data:`DEFAULT_CACHE_TTL`, 3600s) after which the document is refreshed on the next use.

The cost is bounded latency on rotation, not correctness: a token signed with a brand-new ``kid`` may
be rejected for up to the floor. That is the deliberate trade (IdPs publish a new key before signing
with it), and it is tunable per validator.

**Algorithms are never taken from the key or the token.** The configured allowlist is the only source
of truth: the header's ``alg`` must be in it *before* any key is resolved, a JWKS entry that declares
an ``alg`` is only usable for that exact algorithm, and the decoder is handed one already-allowlisted
algorithm — never a list the JWKS or the token got to widen. That is what closes RFC 8725 §3.1
algorithm confusion (the attack where a service's own RSA *public* key is replayed as an HMAC secret).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import time
import urllib.request
from collections.abc import Awaitable, Callable
from typing import Any

from .bearer import DEFAULT_PRINCIPAL_CLAIM
from .principal import Principal

#: An async document fetch — ``fetch(url) -> body text``. Injectable so tests (and an in-process fake
#: IdP) need no network; the default is :func:`stdlib_fetch`.
JwksFetch = Callable[[str], Awaitable[str]]

#: A monotonic clock — the same ``() -> float`` seam ``benzene.cache`` and ``benzene.resilience`` use.
Clock = Callable[[], float]

#: A JWKS-aware decode step — ``decode(token, jwk, algorithm) -> claims`` — raising on any token it
#: rejects. ``algorithm`` is *always* one the caller allowlisted; a decoder must never widen it.
JwksDecoder = Callable[[str, "dict[str, Any]", str], "dict[str, Any]"]

#: How long a fetched JWKS/discovery document is used before a refresh is attempted (seconds).
DEFAULT_CACHE_TTL = 3600.0

#: The floor between *outbound fetch attempts* for one document (seconds) — the bound that stops an
#: attacker spraying unknown ``kid``\\ s from driving unbounded requests at the identity provider.
DEFAULT_MIN_REFRESH_INTERVAL = 300.0

#: The OIDC discovery path appended to an issuer (OpenID Connect Discovery 1.0 §4).
OPENID_CONFIGURATION_PATH = "/.well-known/openid-configuration"

#: Cap on a fetched metadata document, so a hostile endpoint cannot stream an unbounded body at us.
MAX_DOCUMENT_BYTES = 1_048_576


def openid_configuration_url(issuer: str) -> str:
    """The OIDC discovery URL for ``issuer`` (``<issuer>/.well-known/openid-configuration``)."""
    return f"{issuer.rstrip('/')}{OPENID_CONFIGURATION_PATH}"


def stdlib_fetch(*, timeout: float = 5.0) -> JwksFetch:
    """A zero-dependency :data:`JwksFetch` using ``urllib`` on a worker thread (bounded body read)."""

    async def fetch(url: str) -> str:
        def _do() -> str:
            request = urllib.request.Request(  # noqa: S310 - scheme is checked by _check_https
                url, headers={"accept": "application/json"}
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                return str(response.read(MAX_DOCUMENT_BYTES).decode("utf-8"))

        return await asyncio.to_thread(_do)

    return fetch


def unverified_header(token: str) -> dict[str, Any] | None:
    """Decode a JWT's header segment *without* verifying anything, or ``None`` if it is not a JWT.

    Only the ``alg``/``kid`` routing hints are read from it, and both are checked against configured
    policy before any key is trusted — nothing here is evidence of anything.
    """
    segment = token.partition(".")[0]
    if not segment:
        return None
    try:
        raw = base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
        header = json.loads(raw)
    except (ValueError, binascii.Error):
        return None
    return header if isinstance(header, dict) else None


def _checked_algorithms(algorithms: tuple[str, ...]) -> tuple[str, ...]:
    """The allowlist, refusing the two shapes that would hand algorithm choice to the token."""
    if not algorithms:
        raise ValueError(
            "algorithms must name at least one signing algorithm — an empty allowlist would "
            "trust whatever 'alg' a token claims (RFC 8725 §3.1 algorithm confusion)."
        )
    unsecured = [alg for alg in algorithms if alg.lower() == "none"]
    if unsecured:
        raise ValueError(
            "algorithms must not contain 'none': an unsecured JWT carries no signature at all, so "
            "allowlisting it would accept any token anyone cares to write (RFC 8725 §3.2)."
        )
    return algorithms


def _check_https(url: str, *, required: bool, what: str) -> None:
    """Fail fast at wire-up when metadata would be fetched over plain HTTP (a MITM key swap)."""
    if required and not url.lower().startswith("https://"):
        raise ValueError(
            f"{what} must use https ({url!r} does not): the JWKS is what establishes which keys are "
            "trusted, so fetching it over plain HTTP lets a man-in-the-middle substitute a signing "
            "key. Pass require_https=False only for a local test/dev endpoint."
        )


class _CachedDocument:
    """One cached remote JSON document: a TTL, a floor between fetch attempts, and single-flight.

    The floor applies to *every* attempt — scheduled or forced by an unknown ``kid``, successful or
    failed — which is what bounds attacker-driven outbound traffic to one request per interval. On a
    failed fetch the previously fetched document is kept (last known good), so an IdP outage degrades
    to "keys stop rotating", never to "every token is rejected".
    """

    def __init__(
        self,
        *,
        fetch: JwksFetch,
        clock: Clock,
        ttl: float,
        min_refresh_interval: float,
    ) -> None:
        self._fetch = fetch
        self._clock = clock
        self._ttl = ttl
        self._floor = min_refresh_interval
        self._document: dict[str, Any] | None = None
        self._fetched_at = 0.0
        self._last_attempt: float | None = None
        self._lock = asyncio.Lock()

    @property
    def document(self) -> dict[str, Any] | None:
        """The last successfully fetched document, or ``None`` before any fetch succeeded."""
        return self._document

    def is_fresh(self) -> bool:
        """Whether the cached document is present and still inside its TTL."""
        return self._document is not None and (self._clock() - self._fetched_at) < self._ttl

    def _may_attempt(self) -> bool:
        return self._last_attempt is None or (self._clock() - self._last_attempt) >= self._floor

    async def get(self, url: str) -> dict[str, Any] | None:
        """The cached document, refreshing first when it is missing or past its TTL."""
        if not self.is_fresh():
            await self.refresh(url)
        return self._document

    async def refresh(self, url: str) -> dict[str, Any] | None:
        """Refetch ``url`` if the floor allows, coalescing concurrent callers into a single fetch."""
        if not self._may_attempt() and not self._lock.locked():
            return self._document  # rate-limited, with no fetch in flight worth waiting for
        # Single-flight: a fetch already running is the one to wait on — answering from the cache
        # here would hand a concurrent caller a stale (or empty) document the fetch is about to fill.
        async with self._lock:
            if not self._may_attempt():
                return self._document  # another coroutine fetched while we waited on the lock
            self._last_attempt = self._clock()  # a failed attempt spends the budget too
            try:
                parsed = json.loads(await self._fetch(url))
            except Exception:
                return self._document  # keep last known good; the floor bounds the retry rate
            if not isinstance(parsed, dict):
                return self._document
            self._document = parsed
            self._fetched_at = self._clock()
            return parsed


class JwksClient:
    """Resolves a token's signing key by ``kid`` from a cached, rotation-aware JWKS document.

    Configure it with an ``issuer`` (its JWKS URI is resolved by OIDC discovery and cached) or an
    explicit ``jwks_uri`` (for providers publishing only a bare JWKS); ``jwks_uri`` wins when both are
    given. ``algorithms`` is the caller's allowlist — a key is only ever offered for an algorithm in
    it, and a JWKS entry declaring an ``alg`` of its own is usable for that algorithm alone.

    Caching and the refresh floor are described in this module's docstring: at most one outbound fetch
    per document per ``min_refresh_interval``, single-flighted, keeping the last known good document
    when a fetch fails.
    """

    def __init__(
        self,
        *,
        issuer: str | None = None,
        jwks_uri: str | None = None,
        algorithms: tuple[str, ...] = ("RS256",),
        fetch: JwksFetch | None = None,
        clock: Clock = time.monotonic,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        min_refresh_interval: float = DEFAULT_MIN_REFRESH_INTERVAL,
        require_https: bool = True,
    ) -> None:
        if not issuer and not jwks_uri:
            raise ValueError(
                "JwksClient needs an issuer (OIDC discovery resolves its jwks_uri) or an explicit "
                "jwks_uri — with neither there is nothing to fetch signing keys from."
            )
        self._algorithms = _checked_algorithms(tuple(algorithms))
        if issuer:
            _check_https(issuer, required=require_https, what="issuer")
        if jwks_uri:
            _check_https(jwks_uri, required=require_https, what="jwks_uri")

        self._issuer = issuer
        self._configured_jwks_uri = jwks_uri
        self._require_https = require_https
        fetcher = fetch if fetch is not None else stdlib_fetch()
        cache_args: dict[str, Any] = {
            "fetch": fetcher,
            "clock": clock,
            "ttl": cache_ttl,
            "min_refresh_interval": min_refresh_interval,
        }
        self._jwks = _CachedDocument(**cache_args)
        self._discovery = _CachedDocument(**cache_args) if jwks_uri is None else None

    async def jwks_uri(self) -> str | None:
        """The JWKS URI — configured, or discovered from the issuer's metadata and cached."""
        discovery, issuer = self._discovery, self._issuer
        if discovery is None or issuer is None:  # an explicit jwks_uri — nothing to discover
            return self._configured_jwks_uri
        document = await discovery.get(openid_configuration_url(issuer))
        if document is None:
            return None
        # OIDC Discovery §4.3: the issuer in the metadata MUST match the one asked for. Skipping this
        # would let a redirected/substituted discovery document point us at someone else's keys.
        if document.get("issuer") != issuer:
            return None
        uri = document.get("jwks_uri")
        if not isinstance(uri, str):
            return None
        if self._require_https and not uri.lower().startswith("https://"):
            return None
        return uri

    async def key_for(self, *, kid: str | None, alg: str) -> dict[str, Any] | None:
        """The JWK to verify a token signed with ``kid``/``alg``, or ``None`` if none is trusted.

        A miss triggers **at most one** refetch, and only when the refresh floor allows it — an
        unknown ``kid`` is a rotation *or* an attacker, and this cannot tell the two apart, so it
        rate-limits both.
        """
        if alg not in self._algorithms:
            return None  # defence in depth — JwksValidator checks the header before we get here
        uri = await self.jwks_uri()
        if uri is None:
            return None
        await self._jwks.get(uri)  # a first fetch, or a scheduled one once the TTL has passed
        key = self._match(kid, alg)
        if key is not None:
            return key
        await self._jwks.refresh(uri)  # unknown kid: one floored, single-flighted refetch
        return self._match(kid, alg)

    def _match(self, kid: str | None, alg: str) -> dict[str, Any] | None:
        document = self._jwks.document
        keys = document.get("keys") if document is not None else None
        if not isinstance(keys, list):
            return None
        candidates = [
            key
            for key in keys
            if isinstance(key, dict)
            and self._is_usable(key, alg)
            and (kid is None or key.get("kid") == kid)
        ]
        if kid is None:
            # No kid to route on: only an unambiguous key set can be used, never a guess among many.
            return candidates[0] if len(candidates) == 1 else None
        return candidates[0] if candidates else None

    def _is_usable(self, jwk: dict[str, Any], alg: str) -> bool:
        """Whether ``jwk`` may verify an ``alg`` signature — a key never names its own algorithm.

        ``alg`` has already been checked against the configured allowlist. A JWKS entry that declares
        a *different* ``alg`` is rejected outright rather than borrowed: an entry advertising
        ``HS256`` (say, an RSA public key republished as an ``oct`` secret) must not become usable
        just because the service allowlists ``RS256`` and a token asks for it.
        """
        use = jwk.get("use")
        if use is not None and use != "sig":
            return False
        declared = jwk.get("alg")
        if declared is not None and declared != alg:
            return False
        key_ops = jwk.get("key_ops")
        return not (isinstance(key_ops, list) and "verify" not in key_ops)


class JwksValidator:
    """A bearer ``validate`` that verifies a JWT against an IdP's **rotating** JWKS keys.

    The rotation-aware counterpart to :class:`~benzene.auth.JwtValidator`: instead of one configured
    key, the verification key is resolved per token by the header's ``kid`` from a cached JWKS
    document (see :class:`JwksClient` for the caching and the bounded refresh policy). Point it at an
    ``issuer`` (OIDC discovery finds the JWKS URI) or an explicit ``jwks_uri``.

    ``algorithms`` is the allowlist, and it is the *only* source of algorithm truth — the header's
    ``alg`` must be in it before any key is fetched, and the decoder is handed exactly that one
    algorithm. ``audience``/``issuer`` are the usual constraints (the issuer doubles as the discovery
    root). Any token it rejects — bad signature, expired, wrong audience/issuer, unknown ``kid``,
    non-allowlisted ``alg`` — yields ``None``, never an exception, so it drops straight into
    ``unauthorized``; a missing PyJWT stays an :class:`ImportError`, because that is a deployment
    fault rather than a token outcome.

    :meth:`validate` is **async** (the bearer middleware awaits either shape). That makes it a
    validator for :func:`~benzene.auth.bearer_token_interception`, not for the synchronous API Gateway
    authorizer when it is invoked from inside a running event loop.
    """

    def __init__(
        self,
        *,
        issuer: str | None = None,
        jwks_uri: str | None = None,
        algorithms: tuple[str, ...] = ("RS256",),
        audience: str | None = None,
        principal_claim: str = DEFAULT_PRINCIPAL_CLAIM,
        leeway: float = 0.0,
        fetch: JwksFetch | None = None,
        clock: Clock = time.monotonic,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        min_refresh_interval: float = DEFAULT_MIN_REFRESH_INTERVAL,
        require_https: bool = True,
        decode: JwksDecoder | None = None,
        client: JwksClient | None = None,
    ) -> None:
        self._algorithms = _checked_algorithms(tuple(algorithms))
        if client is None:
            client = JwksClient(
                issuer=issuer,
                jwks_uri=jwks_uri,
                algorithms=self._algorithms,
                fetch=fetch,
                clock=clock,
                cache_ttl=cache_ttl,
                min_refresh_interval=min_refresh_interval,
                require_https=require_https,
            )
        self._client = client
        self._audience = audience
        self._issuer = issuer
        self._principal_claim = principal_claim
        self._leeway = leeway
        self._decode = decode

    def __call__(self, token: str) -> Awaitable[Principal | None]:
        return self.validate(token)

    @property
    def client(self) -> JwksClient:
        """The key client — share one across validators to share its cache and refresh budget."""
        return self._client

    async def validate(self, token: str) -> Principal | None:
        """Verify ``token`` against the IdP's current keys, or return ``None`` for any bad token."""
        header = unverified_header(token)
        if header is None:
            return None
        alg = header.get("alg")
        # The allowlist is checked first, on the routing hint alone: a token naming an algorithm this
        # service does not accept never reaches key resolution, so it cannot even cost us a fetch.
        if not isinstance(alg, str) or alg not in self._algorithms:
            return None
        kid = header.get("kid")
        if kid is not None and not isinstance(kid, str):
            return None

        jwk = await self._client.key_for(kid=kid, alg=alg)
        if jwk is None:
            return None  # unknown/untrusted kid — a rotation not yet visible, or a forged header

        decoder = self._decode if self._decode is not None else self._decode_with_pyjwt
        try:
            claims = decoder(token, jwk, alg)
        except ImportError:
            raise  # a missing PyJWT is a deployment error, not a token outcome — surface it
        except Exception:
            return None
        if not isinstance(claims, dict):
            return None
        return Principal(str(claims.get(self._principal_claim, "")), dict(claims))

    def _decode_with_pyjwt(self, token: str, jwk: dict[str, Any], algorithm: str) -> dict[str, Any]:
        """Verify with PyJWT, imported lazily so the package works without the ``[jwt]`` extra.

        ``algorithm`` is the one already-allowlisted algorithm, and it is passed to PyJWT as the whole
        of ``algorithms`` — the JWKS entry never gets to name the algorithm its key is used with.
        """
        try:
            import jwt  # optional [jwt] extra — lazy so the package works without PyJWT
            from jwt.algorithms import get_default_algorithms
        except ImportError as exc:
            raise ImportError(
                "JwksValidator requires PyJWT — install it with 'pip install benzene-auth[jwks]' "
                "(PyJWT plus the crypto backend an asymmetric JWKS needs; 'benzene-auth[jwt]' is "
                "PyJWT alone), or inject your own decode callable (JwksValidator(decode=...))."
            ) from exc

        implementation = get_default_algorithms().get(algorithm)
        if implementation is None:
            raise ImportError(
                f"PyJWT cannot verify {algorithm!r} — asymmetric algorithms need its crypto backend: "
                "install 'pip install benzene-auth[jwks]' (the [jwt] extra is PyJWT alone), or inject "
                "your own decode callable (JwksValidator(decode=...))."
            )

        key = implementation.from_jwk(json.dumps(jwk))
        kwargs: dict[str, Any] = {"algorithms": [algorithm], "leeway": self._leeway}
        if self._audience is not None:
            kwargs["audience"] = self._audience
        if self._issuer is not None:
            kwargs["issuer"] = self._issuer
        decoded: dict[str, Any] = jwt.decode(token, key, **kwargs)
        return decoded
