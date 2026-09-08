# Feature: Scan-Spec Credential Reference

Replaces the storage credentials the adapter embedded verbatim in the scan-driving SQL with a REFERENCE to the Exasol CONNECTION that supplies them: the scan spec carries the connection NAME, and the scan UDF resolves the credentials at execution time through `ctx.connection()`. Closes issue #135 — a user holding only `SELECT` on the virtual schema could read a standing `access_key` and `secret_key` out of `EXPLAIN VIRTUAL` output and out of any error raised on the pushdown path. Verified live that profiling and audit `SQL_TEXT` do NOT carry it — both record only the user's own literal statement, never the VS-rewritten pushdown SQL. A credential the planning layer resolves from the `loadTable` response rather than from the CONNECTION cannot be referenced this way; it travels in the SQL ONLY inside an AES-GCM-sealed envelope whose key both sides derive from that same CONNECTION — closing issue #378 — and a vended query whose CONNECTION password carries no secret material at all is refused at plan time.

## Background

This feature is the home for four facts the other deltas of this change cite rather than restate.

* **The pushdown response carries exactly one field.** `PushDownResponse` serializes `{"type": "pushdown", "sql": <string>}` and nothing else. There is no bind parameter and no second field.
* **`EXPLAIN VIRTUAL` returns that string verbatim with no redaction.** Profiling and audit `SQL_TEXT` carry only the user's own literal statement, never the VS-rewritten pushdown SQL (verified live on Exasol 2025.2.1). `EXPLAIN VIRTUAL` and pushdown-path error text are the real leak surfaces.
* **The scoped credential claim.** A CONNECTION-supplied storage credential MUST NOT appear in any returned SQL string. A VENDED storage credential appears there ONLY inside the AES-GCM-sealed envelope this feature specifies — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — never in plaintext. No credential of either kind appears in an error message.
* **The sealed envelope's threat model is deliberately bounded.** It defeats a plaintext read of the pushdown SQL, not offline cryptanalysis against a low-entropy password. Acceptable because vended values are short-lived and prefix-scoped, and the key material is what `ACCESS ON CONNECTION` already reveals. The gate tests non-emptiness of a secret field, not entropy; vending without key material is refused at plan time. See `docs/security.md` for the full privilege model and threat analysis.
* **`ctx.connection()` is NOT the forbidden catalog round-trip.** It is one engine-local metadata request answered from the database's own catalog. The resolve-once rule (`specs/mission.md`) is satisfied.
* **The wire wrapper exposes no secret accessor.** A site left reading the unresolved wire value fails to compile rather than yielding an empty redaction set.
* **The required grant is a BREAKING deployment change.** See `docs/security.md` for the privilege model, grant semantics, and rotation guidance.

## Scenarios

### Scenario: The scan spec references the CONNECTION by name instead of carrying its credentials

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false, supplying either static S3 storage fields or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter builds the scan-driving SQL for any pushdown request shape
* *THEN* the shard-invariant common scan-spec argument SHALL carry the value of the `CATALOG_CONNECTION` virtual-schema property as the storage reference, together with the resolved `ALLOW_HTTP` value, and SHALL carry no other field
* *AND* the generated SQL MUST NOT contain the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, or `sas_token` value, in any encoding
* *AND* the store `endpoint`, `region`, `path_style`, and ADLS `account_name` MUST NOT appear in the generated SQL for a referenced credential either, and SHALL be re-derived by the UDF from the same CONNECTION, so the addressing and the secret cannot come from two different reads
* *AND* the reference SHALL be carried ONCE in the shard-invariant common argument and MUST NOT be repeated per shard, because it is invariant across the fan-out
* *AND* a join spec SHALL carry a storage reference PER SIDE, so the fact side's and the dimension side's references travel independently even when both name the same CONNECTION

### Scenario: One named predicate decides whether the sealed envelope's guarantee can hold

* *GIVEN* the CONNECTION password parsed into its credential fields, at the connection-resolve step that populates the sealing key
* *WHEN* the adapter decides whether a sealing key exists for this request
* *THEN* exactly ONE named predicate function SHALL make that decision, declared BESIDE the key derivation it gates and called from exactly ONE site, so the criterion and the derivation cannot drift apart and the refusal error can cite the criterion it failed
* *AND* the predicate SHALL report TRUE iff at least ONE of the password's secret-bearing fields is NON-EMPTY — `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, or `sas_token` — and a non-empty `access_key` alone SHALL NOT satisfy it, because an AWS access key id is an identifier rather than a secret on its own
* *AND* the predicate MUST NOT be narrowed to the catalog-auth fields alone: a CONNECTION carrying a non-empty static `secret_key` under a no-auth catalog — the shape `deploy/scripts/install.sh`'s own next-step template emits — carries genuine key material and MUST NOT be refused as if it carried none
* *AND* the test the predicate applies SHALL be stated as NON-EMPTINESS rather than entropy, so the envelope's bound reads as resting on the operator's own secret strength as well as on this feature's threat model
* *AND* a test SHALL assert the sealing key is ABSENT for a password carrying none of the six fields, PRESENT for each of the six carried non-empty in turn, and ABSENT again for each of the six carried but empty

### Scenario: One pure function selects the wire variant for every builder path

* *GIVEN* the resolved connection configuration for a pushdown request — its `ConnectionCreds`, its `CATALOG_CONNECTION` name, the resolved `ALLOW_HTTP` value, and its sealing key (derived once at connection-resolve time from the raw password, present iff the predicate scenario above reports key material) — and the effective storage backend the format reader resolved for one side
* *WHEN* the adapter populates a scan spec's storage block, on the single-table path or on either side of a join
* *THEN* exactly ONE pure function SHALL make the choice, taking those five inputs and returning the wire wrapper or a refusal error, and every site that populates a storage block SHALL call it rather than construct a variant itself
* *AND* that function SHALL return the REFERENCE variant when `use_vended_credentials` is false; the SEALED variant — the effective backend serialized and sealed under the sealing key, beside the connection name — when it is true and the key exists; and the named refusal `UdfError` when it is true and no key exists, so the variant can never disagree with the resolver that produced its payload and the vending-without-key-material combination is unreachable past this function
* *AND* the function MUST NOT return the plaintext `Inline` variant on any input, and the refusal error MUST NOT contain any credential value — the vended credential resolved before the refusal never leaves the adapter's memory
* *AND* a vended JOIN SHALL seal each side's effective backend independently — two envelopes, two fresh nonces, one key — so the per-side storage contract of this feature holds on the sealed path exactly as on the reference path
* *AND* the format readers' own vended/static split SHALL be UNCHANGED and MUST NOT return the wrapper, because each reader uses the concrete backend immediately for its own plan-time manifest or log read
* *AND* the guarantee test SHALL drive every builder path with a spec template produced by THAT function from credentials carrying sentinel values, under both settings of `use_vended_credentials`, so the assertion observes the SELECTION rather than its own fixture

### Scenario: The scan UDF resolves the referenced CONNECTION without contacting the catalog

* *GIVEN* a scan UDF invocation whose shard-invariant common spec argument carries a storage reference naming a CONNECTION, or a sealed envelope naming one
* *WHEN* the UDF prepares to read its assigned files
* *THEN* the UDF SHALL call `ctx.connection()` with the referenced name and, for a REFERENCE, SHALL deserialize the returned `password` into a STORAGE-ONLY projection declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token`
* *AND* for a SEALED envelope the UDF SHALL derive the sealing key from the returned password BYTES without parsing them, open the envelope, and deserialize the plaintext into the storage backend that was sealed; an envelope that fails base64 decoding or AEAD authentication — a rotated password is the expected cause — SHALL produce a `UdfError` naming the connection and the failed unseal operation, MUST NOT fall back to any inline or partial credential, and MUST NOT echo the payload, the password, or any plaintext
* *AND* the UDF MUST NOT construct any value carrying a catalog-authentication field, so `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` are structurally excluded from what the UDF parses rather than parsed and discarded, and a source-level probe SHALL assert that the projection's own declaration names no field with any of those five spellings
* *AND* the UDF SHALL derive the storage backend from that projection and the carried `ALLOW_HTTP` value through the SAME single backend selector the adapter reaches, so a referenced and an inline backend for one CONNECTION are field-for-field equal
* *AND* the UDF SHALL perform this resolution EXACTLY ONCE per invocation, before it builds any object store, and SHALL resolve BOTH sides of a join spec in that one step
* *AND* when the referenced CONNECTION cannot be resolved — it does not exist, or the invoking user's grants do not reach it through this script — the UDF SHALL return a `UdfError` naming the connection name and the missing access, MUST NOT fall back to any inline credential, and MUST NOT panic, because a panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part

### Scenario: Error redaction reads its secret set from the resolved credentials, not from the wire spec

* *GIVEN* a scan invocation whose storage reference resolves to a backend holding a credential, on a single-table spec or on either side of a join spec
* *WHEN* the UDF raises any error while building an object store, running a raw scan, running a partial aggregate, reading a data file, reading a positional-delete file, or applying a deletion vector
* *THEN* the value-based redaction applied to that message SHALL take its secret set from the RESOLVED storage backends and MUST NOT take it from the scan spec's own storage block, at EVERY site that builds such a set
* *AND* the wire wrapper MUST NOT expose any method returning secret values or a credential payload, so a site left reading the unresolved wire value fails to COMPILE rather than yielding an empty set
* *AND* the secret set for a join spec SHALL be the UNION of both sides' resolved secrets, so a message raised while either side's credential is in scope is redacted for both
* *AND* the raw-scan and partial-aggregate error paths SHALL each be covered by a test asserting that a resolved credential value is stripped from an error raised on that path, because those two paths read the fact-side set directly and are where an empty set would go unnoticed
* *AND* no resolved credential value SHALL appear in any error message the UDF returns

### Scenario: A vended credential travels only inside a sealed envelope keyed by the referenced CONNECTION

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true and whose password carries key material under the predicate above — a non-empty `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, or `sas_token`
* *WHEN* the adapter builds the scan-driving SQL after resolving the effective scan storage from the `loadTable` or Unity temporary-credentials response
* *THEN* the common scan-spec argument SHALL carry that resolved backend SEALED — AES-256-GCM under the HKDF-SHA256 key derived from the CONNECTION password bytes, a fresh random 96-bit nonce per encryption, encoded as `base64(nonce ‖ ciphertext)` beside the connection NAME — and no vended credential value SHALL appear in plaintext anywhere in the generated SQL, closing issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378)
* *AND* the guarantee SHALL be presented as BOUNDED wherever it is described — it defeats a plaintext read of the SQL surfaces, not offline cryptanalysis against a low-entropy password — with this feature's Background as the one home of the bound and its justification
* *AND* the adapter MUST NOT emit a bare reference variant under vending, because no CONNECTION name identifies a credential the catalog vended for one table, and MUST NOT emit a plaintext inline backend on any production path
* *AND* the scan UDF SHALL call `ctx.connection()` for the sealed variant exactly as for the reference variant — the sealed path is therefore subject to the SAME script-scoped grant requirement, including for vended-only deployments
* *AND* when the CONNECTION password carries no secret material at all, the adapter SHALL refuse at plan time under this feature's refusal scenario rather than seal under a guessable key or fall back to plaintext

### Scenario: A vended query whose CONNECTION password carries no secret material is refused at plan time

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true and whose password carries NO non-empty secret-bearing field — no `token`, no `client_secret`, no `secret_key`, no `session_token`, no `account_key`, and no `sas_token`; a no-auth catalog whose password holds only `{"warehouse":"…"}` is the canonical shape
* *WHEN* Exasol sends a `pushdown` request for any query shape
* *THEN* the adapter SHALL return a `UdfError` naming the vending-without-key-material combination and both remedies — configure catalog authentication (or supply the CONNECTION's own storage secret), or disable `use_vended_credentials` — and MUST NOT return any pushdown SQL
* *AND* the refusal SHALL be raised by the ONE pure variant-selection function, so no builder path can bypass it, and the error text MUST NOT contain any credential value, vended or CONNECTION-supplied
* *AND* the refusal MUST NOT be softened to a plaintext-inline fallback or to an envelope under a key derived from a password carrying no secret material, because both ship the exposure this feature closes under the appearance of having closed it
* *AND* the justification for refusing rather than supporting the combination — code-path-only orthogonality in the recorded spec, an E2E matrix that never exercises it, and the brokenness of a catalog vending real credentials to unauthenticated callers — SHALL be recorded in this feature's Background with its citations, so the refusal reads as a deliberate scoped decision rather than a silent gap

### Scenario: The generated SQL is asserted credential-free at every builder path

* *GIVEN* the single site that interpolates the serialized common scan spec into the scan-driving SQL, and the builder paths that reach it — every `RequestShape` variant crossed with the join, top-N, and `COUNT(DISTINCT)` sub-paths, cross-checked against the eighteen credential-bearing golden fixtures
* *WHEN* the adapter renders the SQL for a CONNECTION supplying static storage credentials whose values are distinctive test sentinels
* *THEN* a test SHALL assert, for every one of those builder paths and under BOTH settings of `use_vended_credentials`, that no sentinel credential value appears anywhere in the returned SQL string, and SHALL assert POSITIVELY — before any absence assertion — that the connection NAME is present for the static case and that the sealed envelope is present AND unseals to the sentinel backend for the vended case, so a path whose fixture seeded nothing, or a vended path regressing to plaintext or to an empty blob, fails instead of passing vacuously
* *AND* that assertion SHALL be made on the returned SQL STRING rather than on the scan-spec structure alone, because the structure is what a later change would keep valid while the rendered string regressed
* *AND* the eighteen credential-bearing golden pushdown-SQL fixtures SHALL be regenerated so that each contains the reference encoding and no credential value, while the six `empty_*` fixtures carry no `storage` value at all and SHALL stay byte-identical and be asserted unchanged
* *AND* the builder-path list SHALL be DERIVED and stated in this feature rather than asserted as a count, so a later reader can verify coverage against the fixture names instead of trusting a number
* *AND* the assertion SHALL be that no credential VALUE appears in PLAINTEXT — a vended credential is still present as AES-GCM ciphertext under issue #378's sealed envelope, which is why the sealed fixtures cannot be byte-stable goldens: the nonce is fresh per encryption, so the vended encoding is covered by the selection-driven test alone and all eighteen regenerated golden fixtures render the REFERENCE variant

### Scenario: The scan script requires its own script-scoped connection access grant

* *GIVEN* a deployment whose adapter script already holds access to the catalog CONNECTION, which `vs-adapter/connection-credentials` requires for the adapter to resolve it at all
* *WHEN* an operator installs or upgrades to a build carrying this feature
* *THEN* the deployment SHALL additionally require that the SCAN script reach the same CONNECTION, granted as `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <owner-or-role>` — VENDED deployments included, because the sealed path resolves the CONNECTION for its envelope key
* *AND* that grant SHALL be held by the VIRTUAL SCHEMA's OWNER — once per (connection, script, owner), a deployment-time grant rather than one per reader — because Exasol evaluates `ACCESS ON CONNECTION ... FOR SCRIPT` against the OWNER of the virtual schema being queried when the script is reached through VS-rewritten pushdown SQL; the check reads the SESSION user's own grant only for a DIRECTLY invoked script call, which is not how a virtual-schema query reaches the scan
* *AND* the ADAPTER script SHALL hold the equivalent grant on the same CONNECTION — `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_ADAPTER TO <owner-or-role>`, held by the same owner, required at `CREATE VIRTUAL SCHEMA` time AND on every later query — which is a PRE-EXISTING requirement this feature does not introduce, so this feature adds exactly ONE of the two grants and its breaking-change count reads as one
* *AND* the installer's next-step template SHALL emit BOTH grants BETWEEN the `CREATE CONNECTION` and the `CREATE VIRTUAL SCHEMA` statements it already emits, because the adapter resolves the CONNECTION *while* the virtual schema is being created, so a template printing the grants after `CREATE VIRTUAL SCHEMA` cannot be run top-to-bottom by a non-DBA owner; and it SHALL warn that re-provisioning EITHER the CONNECTION or a SCRIPT drops both grants and requires re-issuing them
* *AND* the script-scoped form SHALL be the form documented and emitted, and the blanket `ACCESS ANY CONNECTION` system privilege MUST NOT be, because the script-scoped grant is what stops a user reading the credential through a script of their own — while USE of the credential through this script is delegated to the grantee, as this feature's Background states
* *AND* a deployment missing that grant SHALL fail with the scan-time error named above, and MUST NOT silently read through an inline credential
* *AND* a user who holds `SELECT` on the virtual schema but holds neither `ACCESS ON CONNECTION` for that connection nor `ACCESS ANY CONNECTION` SHALL be able to run the query and MUST NOT be able to recover any credential value from `EXPLAIN VIRTUAL` output or from any error the query returns — profiling and audit `SQL_TEXT` are not asserted here because they were verified live to never carry the rewritten pushdown SQL in the first place (see Background), so an absence assertion against them would pass vacuously on a vulnerable build

### Scenario: A CONNECTION rotated while a query is in flight is observed per shard

* *GIVEN* a query whose adapter has returned its pushdown SQL and whose shard invocations have not all completed
* *AND* an `ALTER CONNECTION` that replaces the referenced CONNECTION's password, its address, or its storage addressing between the adapter's read and a shard's read
* *WHEN* the remaining shards resolve the reference
* *THEN* each shard SHALL read whichever value the CONNECTION holds at the moment of ITS OWN resolution, so one query MAY read some shards with the pre-rotation values and others with the post-rotation values — for the store `endpoint` and `region` as much as for the secret, because both are re-derived from the same read
* *AND* the engine MUST NOT pin either the credential or the addressing for the query's lifetime, because pinning them means carrying them in the generated SQL, which is the exposure this feature closes
* *AND* a shard whose resolved credential or addressing the storage provider rejects SHALL fail with the storage backend's own access error carrying no credential value, rather than retrying with another value
* *AND* a shard of a VENDED query whose CONNECTION password was rotated after the adapter sealed the envelope SHALL fail the AEAD open with the named unseal error carrying no plaintext — it structurally cannot read stale or mixed credentials — so a vended-path rotation is safe only when no vended query is in flight
* *AND* the operator-facing consequence — that a rotation is safe for in-flight queries only while the storage provider accepts BOTH the old and the new secret, and that an addressing change should be made when no query is in flight — SHALL be stated in the operator documentation, because the engine holds no state with which to make it safe on its own
