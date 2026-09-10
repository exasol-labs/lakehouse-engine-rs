# Feature: Scan-Spec Credential Reference

Replaces the storage credentials the adapter embedded in the scan-driving SQL with a REFERENCE to the Exasol CONNECTION: the scan spec carries the connection NAME, and the scan UDF resolves credentials at execution time through `ctx.connection()`. Closes #135 (credential visible in `EXPLAIN VIRTUAL`). A vended credential that cannot be referenced travels only inside an AES-GCM-sealed envelope (closing #378); vending without key material is refused at plan time.

## Background

* **The pushdown response carries exactly one field.** `EXPLAIN VIRTUAL` returns it verbatim — the leak surface. Profiling/audit `SQL_TEXT` carry only the user's literal statement.
* **The scoped credential claim.** A CONNECTION-supplied credential MUST NOT appear in any returned SQL. A VENDED credential appears ONLY inside the sealed envelope, never in plaintext.
* **The sealed envelope's threat model is deliberately bounded.** Defeats plaintext reads, not offline cryptanalysis. See `docs/security.md`.
* **`ctx.connection()` is engine-local, not a catalog round-trip.** The wire wrapper exposes no secret accessor (compile failure, not an empty redaction set). The required grant is a BREAKING deployment change.

## Scenarios

### Scenario: The scan spec references the CONNECTION by name

* *GIVEN* a virtual schema with static storage credentials (not vended)
* *THEN* the common scan-spec argument carries the `CATALOG_CONNECTION` name and `ALLOW_HTTP`, no other storage field; the SQL contains no credential value in any encoding
* *AND* store addressing is re-derived by the UDF from the same CONNECTION read; the reference is carried ONCE in the shard-invariant argument; a join carries one reference PER SIDE

### Scenario: One pure function selects the wire variant and gates the sealed envelope

* *GIVEN* the resolved connection config and the effective storage backend
* *THEN* ONE function returns REFERENCE (not vended), SEALED (vended + key material present), or refusal error (vended + no key material); every storage-block site calls it; it never returns plaintext `Inline`
* *AND* key-material gate: exactly ONE predicate reports TRUE iff at least one of `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, `sas_token` is non-empty; `access_key` alone does not satisfy it
* *AND* refusal names both remedies (add auth or disable vending); no pushdown SQL returned, no credential in the error
* *AND* a vended JOIN seals each side independently (two envelopes, one key)

### Scenario: The scan UDF resolves the referenced CONNECTION

* *GIVEN* a scan invocation with a storage reference or sealed envelope
* *THEN* the UDF calls `ctx.connection()`, deserializes into a STORAGE-ONLY projection (nine fields), and derives the backend through the SAME selector the adapter uses
* *AND* sealed envelope: derives the key from password BYTES, opens via AEAD; failure produces a named error with no plaintext; unresolvable CONNECTION produces a named error, no inline fallback

### Scenario: Error redaction reads from resolved credentials, not the wire spec

* *GIVEN* a scan invocation whose storage resolves to a credential-bearing backend
* *THEN* value-based redaction takes its secret set from RESOLVED backends; the wire wrapper exposes no secret accessor (compile-time); a join's secret set is the UNION of both sides

### Scenario: The generated SQL is asserted credential-free at every builder path

* *GIVEN* every `RequestShape` variant crossed with join/top-N/COUNT(DISTINCT) sub-paths
* *THEN* under BOTH vending settings: no sentinel credential in the returned SQL STRING; the connection NAME is positively asserted present (static) or the envelope unseals correctly (vended)
* *AND* the eighteen credential-bearing golden fixtures carry the reference encoding; the six `empty_*` fixtures stay byte-identical

### Scenario: The scan script requires a script-scoped grant; rotation is observed per shard

* *GIVEN* a deployment with the adapter grant already in place
* *THEN* the scan script additionally needs `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <owner>`; a missing grant fails with a named error; no inline fallback
* *AND* each shard reads the CONNECTION value current at ITS resolution time; a sealed envelope under a rotated password fails AEAD with no plaintext
