# Feature: Scan-Spec Credential Reference

Replaces the storage credentials the adapter embedded in the scan-driving SQL with a REFERENCE to the Exasol CONNECTION: the scan spec carries the connection NAME, and the scan UDF resolves credentials at execution time through `ctx.connection()`. Closes #135 (credential visible in `EXPLAIN VIRTUAL`). A storage credential the CONNECTION does not state, such as a vended credential or an assumed AWS role's session credential, cannot be referenced and travels only inside an AES-GCM-sealed envelope (closing #378); vending without key material is refused at plan time.

## Background

* **The pushdown response carries exactly one field.** `EXPLAIN VIRTUAL` returns it verbatim — the leak surface. Profiling/audit `SQL_TEXT` carry only the user's literal statement.
* **The scoped credential claim.** A CONNECTION-supplied credential MUST NOT appear in any returned SQL. A VENDED credential or an assumed role's session credential appears ONLY inside the sealed envelope, never in plaintext.
* **The sealed envelope's threat model is deliberately bounded.** Defeats plaintext reads, not offline cryptanalysis. See `docs/security.md`.
* **`ctx.connection()` is engine-local, not a catalog round-trip.** The wire wrapper exposes no secret accessor (compile failure, not an empty redaction set). The required grant is a BREAKING deployment change.

## Scenarios

### Scenario: The scan spec references the CONNECTION by name

* *GIVEN* a virtual schema whose CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`connection/connection-credentials-assume-role`), so its storage credentials are the CONNECTION's own static credentials
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the common scan-spec argument SHALL carry the `CATALOG_CONNECTION` name and `ALLOW_HTTP` and no other storage field, and the SQL MUST NOT contain any credential value in any encoding
* *AND* the UDF SHALL re-derive store addressing from the same CONNECTION read, the reference SHALL be carried ONCE in the shard-invariant argument, and a join SHALL carry one reference PER SIDE

### Scenario: One pure function selects the wire variant and gates the sealed envelope

* *GIVEN* the resolved connection config and the effective storage backend
* *WHEN* any storage-block site builds the scan-spec storage block
* *THEN* ONE function SHALL return REFERENCE when the CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`connection/connection-credentials-assume-role`), SEALED when it does either and key material is present, or a refusal error when it sets `use_vended_credentials` and no key material is present; every storage-block site SHALL call it, and it MUST NOT return plaintext `Inline`
* *AND* the key-material gate SHALL be exactly ONE predicate reporting TRUE iff at least one of `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, `sas_token` is non-empty; `access_key` alone MUST NOT satisfy it; a CONNECTION that names a role always satisfies it, because it requires `secret_key`
* *AND* the refusal SHALL name both remedies (add auth or disable vending), SHALL return no pushdown SQL, and MUST NOT contain any credential
* *AND* a vended or assumed-role JOIN SHALL seal each side independently (two envelopes, one key)

### Scenario: The scan UDF resolves the referenced CONNECTION

* *GIVEN* a scan invocation with a storage reference or sealed envelope
* *WHEN* the scan UDF starts
* *THEN* the UDF SHALL call `ctx.connection()`, deserialize into a STORAGE-ONLY projection (nine fields), and derive the backend through the SAME selector the adapter uses
* *AND* for a sealed envelope it SHALL derive the key from password BYTES and open via AEAD
* *AND* a failed open SHALL produce a named error with no plaintext, and an unresolvable CONNECTION SHALL produce a named error with no inline fallback

### Scenario: Error redaction reads from resolved credentials, not the wire spec

* *GIVEN* a scan invocation whose storage resolves to a credential-bearing backend
* *WHEN* the scan builds an error message
* *THEN* value-based redaction SHALL take its secret set from RESOLVED backends, and a join's secret set SHALL be the UNION of both sides
* *AND* the wire wrapper MUST NOT expose a secret accessor (enforced at compile time)

### Scenario: The generated SQL is asserted credential-free at every builder path

* *GIVEN* every `RequestShape` variant crossed with join/top-N/COUNT(DISTINCT) sub-paths
* *WHEN* the adapter generates the pushdown SQL under BOTH vending settings
* *THEN* the returned SQL STRING MUST contain no sentinel credential
* *AND* the connection NAME SHALL be positively asserted present (static) or the envelope SHALL unseal correctly (vended)
* *AND* the eighteen credential-bearing golden fixtures SHALL carry the reference encoding, and the six `empty_*` fixtures SHALL stay byte-identical

### Scenario: The scan script requires a script-scoped grant; rotation is observed per shard

* *GIVEN* a deployment with the adapter grant already in place
* *WHEN* a scan runs
* *THEN* the scan script SHALL additionally need `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <owner>`, and a missing grant SHALL fail with a named error and no inline fallback
* *AND* that grant SHALL be checked against the virtual schema's OWNER, not the querying user, so a user holding only `CREATE SESSION` and `SELECT` on the virtual schema, and no privilege on the CONNECTION, SHALL receive the owner's rows
* *AND* when the owner's grant is missing, the error a reader receives SHALL name the CONNECTION, the scan script, and the virtual schema's owner as the grantee, and MUST NOT contain any credential value
* *AND* each shard SHALL read the CONNECTION value current at ITS resolution time, and a sealed envelope under a rotated password SHALL fail AEAD with no plaintext

### Scenario: A virtual schema reader can read the pushdown plan but cannot execute it

* *GIVEN* a virtual schema whose owner holds `EXECUTE` on the adapter, scan, and distributor scripts and the script-scoped CONNECTION grant
* *AND* a reader that holds only `CREATE SESSION` and `SELECT` on the virtual schema, and holds no `EXECUTE` privilege on any of the three scripts and no `EXECUTE ANY SCRIPT` privilege, neither directly nor through a role
* *WHEN* the reader takes the scan-driving statement that `EXPLAIN VIRTUAL` returns for a query over the virtual schema and submits it verbatim as its own SQL
* *THEN* Exasol SHALL reject the statement with an insufficient-privilege error on the script call, and that error MUST NOT contain any CONNECTION credential value
* *AND* the reader's ordinary query over the virtual schema SHALL still return the owner's rows, because the denial applies only to a direct script call and not to the scan the virtual schema runs on the reader's behalf
