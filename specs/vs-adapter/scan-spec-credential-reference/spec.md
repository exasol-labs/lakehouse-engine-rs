# Feature: Scan-Spec Credential Reference

Replaces the storage credentials the adapter embedded in the scan-driving SQL with a REFERENCE to the Exasol CONNECTION: the scan spec carries the connection NAME, and the scan UDF resolves credentials at execution time through `ctx.connection()`. Closes #135 (credential visible in `EXPLAIN VIRTUAL`). A vended credential that cannot be referenced travels only inside an AES-GCM-sealed envelope (closing #378); vending without key material is refused at plan time.

## Background

* **The pushdown response carries exactly one field.** No bind parameter, no second field.
* **`EXPLAIN VIRTUAL` returns that string verbatim.** Profiling/audit `SQL_TEXT` carry only the user's literal statement (verified live on 2025.2.1). `EXPLAIN VIRTUAL` and pushdown-path errors are the leak surfaces.
* **The scoped credential claim.** A CONNECTION-supplied credential MUST NOT appear in any returned SQL. A VENDED credential appears ONLY inside the sealed envelope, never in plaintext.
* **The sealed envelope's threat model is deliberately bounded.** Defeats plaintext reads, not offline cryptanalysis. See `docs/security.md`.
* **`ctx.connection()` is engine-local, not a catalog round-trip.** The resolve-once rule is satisfied.
* **The wire wrapper exposes no secret accessor.** Compile failure, not an empty redaction set.
* **The required grant is a BREAKING deployment change.** See `docs/security.md`.

## Scenarios

### Scenario: The scan spec references the CONNECTION by name

* *GIVEN* a virtual schema with static storage credentials (not vended)
* *WHEN* the adapter builds scan-driving SQL for any pushdown shape
* *THEN* the common scan-spec argument carries the `CATALOG_CONNECTION` name and `ALLOW_HTTP`, no other storage field; the SQL contains no credential value in any encoding
* *AND* store addressing (`endpoint`, `region`, etc.) is re-derived by the UDF from the same CONNECTION read
* *AND* the reference is carried ONCE in the shard-invariant argument; a join carries one reference PER SIDE

### Scenario: One predicate gates the sealed envelope

* *GIVEN* the CONNECTION password at the sealing-key derivation step
* *THEN* exactly ONE predicate, beside the derivation, reports TRUE iff at least one secret field is non-empty (`token`, `client_secret`, `secret_key`, `session_token`, `account_key`, `sas_token`); `access_key` alone does not satisfy it
* *AND* a test asserts: absent for no fields, present per non-empty field, absent per empty field

### Scenario: One pure function selects the wire variant

* *GIVEN* the resolved connection config and the effective storage backend
* *THEN* ONE function returns REFERENCE (not vended), SEALED (vended + key), or refusal error (vended + no key); every storage-block site calls it
* *AND* it never returns plaintext `Inline`; the refusal contains no credential value
* *AND* a vended JOIN seals each side independently (two envelopes, one key)

### Scenario: The scan UDF resolves the referenced CONNECTION

* *GIVEN* a scan invocation with a storage reference or sealed envelope
* *THEN* the UDF calls `ctx.connection()`, deserializes into a STORAGE-ONLY projection (nine fields, no catalog-auth fields), and derives the backend through the SAME selector the adapter uses
* *AND* for a sealed envelope: derives the key from password BYTES, opens via AEAD; failure produces a named error with no plaintext
* *AND* resolution happens ONCE per invocation, both join sides resolved in one step
* *AND* an unresolvable CONNECTION returns a named error; no inline fallback, no panic

### Scenario: Error redaction reads from resolved credentials, not the wire spec

* *GIVEN* a scan invocation whose storage resolves to a credential-bearing backend
* *THEN* value-based redaction takes its secret set from RESOLVED backends, not the wire spec
* *AND* the wire wrapper exposes no secret accessor (compile-time enforcement)
* *AND* a join's secret set is the UNION of both sides
* *AND* raw-scan and partial-aggregate paths each have a test asserting resolved credentials are stripped

### Scenario: A vended credential travels only inside a sealed envelope

* *GIVEN* a virtual schema with `use_vended_credentials` true and key material present
* *THEN* the common spec carries the backend SEALED (AES-256-GCM, HKDF-SHA256, fresh nonce, `base64(nonce || ciphertext)`) beside the connection NAME; no vended value in plaintext
* *AND* the sealed path requires the SAME script-scoped grant as the reference path

### Scenario: Vending without key material is refused at plan time

* *GIVEN* `use_vended_credentials` true and no non-empty secret field
* *THEN* the variant-selection function returns an error naming both remedies (add auth or disable vending); no pushdown SQL returned, no credential in the error

### Scenario: The generated SQL is asserted credential-free at every builder path

* *GIVEN* every `RequestShape` variant crossed with join/top-N/COUNT(DISTINCT) sub-paths
* *THEN* under BOTH vending settings: no sentinel credential in the returned SQL STRING; the connection NAME is positively asserted present (static) or the envelope unseals correctly (vended)
* *AND* the eighteen credential-bearing golden fixtures carry the reference encoding; the six `empty_*` fixtures stay byte-identical

### Scenario: The scan script requires its own script-scoped grant

* The scan script needs `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <owner>`. A missing grant fails with a named error; no inline fallback. See `docs/security.md`.

### Scenario: A rotated CONNECTION is observed per shard

* Each shard reads the CONNECTION value current at ITS resolution time; a sealed envelope under a rotated password fails AEAD with no plaintext.
