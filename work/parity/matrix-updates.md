# Capability-matrix updates owed by this branch

`docs/capability-matrix.md` is the port's most honest document: it states what is present, what is
*deliberately* absent, and what is simply unbuilt. Shipping a capability without moving the matrix in
the same change turns it into a false one. This file collects, verbatim from the implementing agents,
what each landed capability requires — input for the `capability-scribe` pass (task #57).

**Nobody edits the matrix ad hoc.** The scribe owns it, using this as source.

## Idempotency row (`d3c190b`)

1. **Column 2 (what the port provides)** — after "in-memory store (single-process)" add
   `RedisIdempotencyStore` (`SET NX`) and `DynamoDbIdempotencyStore` (conditional put, TTL on
   `expiresAt`), behind optional `[redis]` / `[dynamodb]` extras.
2. **Column 4 (how to solve the rest)** — the current text tells the reader to *implement*
   `IdempotencyStore` over a store with an atomic conditional write. That is now shipped: it should
   say *use* those stores, while keeping "design handlers to be naturally idempotent".
3. **Column 3 (deliberately not) — LEAVE THE FRAMING INTACT.** Cross-instance dedupe is still not a
   framework guarantee: a shared store relocates the race, it does not remove it. That sentence is
   repeated verbatim in the module docstrings, `docs/reference/resilience.md` and the package README,
   and shipping the stores does not make it less true.

## AuthN / AuthZ row (`2f25646`)

- **Column 2**, replacing "**Partial.**": Basic auth (`basic.py`); bearer middleware + `JwtValidator`
  over PyJWT for a **configured static key** (HMAC or RSA/EC public key) with algorithm/audience/
  issuer constraints; **plus `JwksValidator` — key-rotation-aware validation against an IdP's JWKS,
  with OIDC discovery of the `jwks_uri` from the issuer's `/.well-known/openid-configuration`, both
  cached (injectable clock and fetcher). An unknown `kid` triggers at most one refetch per bounded
  refresh floor (300s), single-flighted, keeping last-known-good keys on fetch failure, so no caller
  can drive unbounded outbound requests; the algorithm allowlist stays the only source of algorithm
  truth (RFC 8725 §3.1) and metadata must be HTTPS** (`jwks.py`); API Gateway custom-authorizer
  adapter (`authorizer.py`).
- **Column 3**: drop "Not implemented: JWKS fetch / OIDC discovery…". A truthful replacement is:
  unbounded refresh on every unknown `kid` (the naive `PyJWKClient` loop) — deliberately
  rate-limited, trading ≤300s of rotation latency for not being an amplifier aimed at your IdP; and
  scope/policy evaluation, which stays your handler's `Result.forbidden`.
- **Column 4**: `validate` is still a plain callable — supply your own (sync or async) for an IdP
  that fits neither shape, or share one `JwksClient` across validators.

## Also stale, outside the matrix

- `docs/index.md:38` still describes `benzene.auth` as "Basic auth, JWT/OAuth2 bearer, and an API
  Gateway custom-authorizer adapter" — JWKS/OIDC belongs there too.

## Still owed when the remaining capabilities land

- **Outbox row** — currently "**Not implemented.** … (nothing deliberate here; this is unbuilt, not
  declined)". Must become a description of what shipped, *and* must state plainly what an outbox does
  not buy: it converts a lost send into a delayed at-least-once send; it does not make delivery
  exactly-once, and the consumer still needs the Idempotency row.
- **Oversized payloads (claim check)** and **Schema registry / wire codecs** rows — same treatment if
  and when those land; until then they stay exactly as they are.
