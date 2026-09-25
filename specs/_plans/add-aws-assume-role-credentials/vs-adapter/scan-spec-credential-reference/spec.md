<!-- DELTA:CHANGED -->
# Feature: Scan-Spec Credential Reference

Replaces the storage credentials the adapter embedded in the scan-driving SQL with a REFERENCE to the Exasol CONNECTION: the scan spec carries the connection NAME, and the scan UDF resolves credentials at execution time through `ctx.connection()`. Closes #135 (credential visible in `EXPLAIN VIRTUAL`). A storage credential the CONNECTION does not state, such as a vended credential or an assumed AWS role's session credential, cannot be referenced and travels only inside an AES-GCM-sealed envelope (closing #378); vending without key material is refused at plan time.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* **The pushdown response carries exactly one field.** `EXPLAIN VIRTUAL` returns it verbatim — the leak surface. Profiling/audit `SQL_TEXT` carry only the user's literal statement.
* **The scoped credential claim.** A CONNECTION-supplied credential MUST NOT appear in any returned SQL. A VENDED credential or an assumed role's session credential appears ONLY inside the sealed envelope, never in plaintext.
* **The sealed envelope's threat model is deliberately bounded.** Defeats plaintext reads, not offline cryptanalysis. See `docs/security.md`.
* **`ctx.connection()` is engine-local, not a catalog round-trip.** The wire wrapper exposes no secret accessor (compile failure, not an empty redaction set). The required grant is a BREAKING deployment change.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The scan spec references the CONNECTION by name

* *GIVEN* a virtual schema whose CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`vs-adapter/connection-credentials-assume-role`), so its storage credentials are the CONNECTION's own static credentials
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the common scan-spec argument SHALL carry the `CATALOG_CONNECTION` name and `ALLOW_HTTP` and no other storage field, and the SQL MUST NOT contain any credential value in any encoding
* *AND* the UDF SHALL re-derive store addressing from the same CONNECTION read, the reference SHALL be carried ONCE in the shard-invariant argument, and a join SHALL carry one reference PER SIDE
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One pure function selects the wire variant and gates the sealed envelope

* *GIVEN* the resolved connection config and the effective storage backend
* *WHEN* any storage-block site builds the scan-spec storage block
* *THEN* ONE function SHALL return REFERENCE when the CONNECTION neither sets `use_vended_credentials` nor names `aws_assume_role_arn` (`vs-adapter/connection-credentials-assume-role`), SEALED when it does either and key material is present, or a refusal error when it sets `use_vended_credentials` and no key material is present; every storage-block site SHALL call it, and it MUST NOT return plaintext `Inline`
* *AND* the key-material gate SHALL be exactly ONE predicate reporting TRUE iff at least one of `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, `sas_token` is non-empty; `access_key` alone MUST NOT satisfy it; a CONNECTION that names a role always satisfies it, because it requires `secret_key`
* *AND* the refusal SHALL name both remedies (add auth or disable vending), SHALL return no pushdown SQL, and MUST NOT contain any credential
* *AND* a vended or assumed-role JOIN SHALL seal each side independently (two envelopes, one key)
<!-- /DELTA:CHANGED -->
