# Decisions: fix-connection-credential-exposure

## ADR: Reference the CONNECTION by name; the scan UDF resolves it

**ID:** reference-connection-by-name-scan-udf-resolves-it
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
`EXPLAIN VIRTUAL` returns the adapter's pushdown SQL with no redaction, and the adapter serialized the resolved `StorageBackend` — including `access_key`/`secret_key` — straight into that SQL. The Exasol Virtual Schema contract permits exactly one pushdown response field (`{"type":"pushdown","sql":<string>}`), so a redacted placeholder, a schema property, an adapter note, and a bind parameter are all unavailable: `EXPLAIN VIRTUAL` would still show the real value, and properties/adapter notes are documented readable by any user with schema access.

### Decision
Carry the `CATALOG_CONNECTION` property value and the resolved `ALLOW_HTTP` flag in the scan spec's storage block instead of the credential. The scan UDF calls `ctx.connection(name)` and applies the same derivation the adapter applies, resolving the credential itself rather than receiving it on the wire.

### Options Considered
Render a placeholder for `EXPLAIN VIRTUAL` while executing the real string (rejected — one `sql` string serves both); carry the credential in a schema property or adapter note (rejected — readable by any user with schema access); bind parameters (rejected — no such field exists); re-request credentials from the catalog per shard (rejected — violates the resolve-once rule and multiplies catalog calls by up to 300); mint a per-query CONNECTION over connect-back (rejected — stateful DDL per query, leaves droppings on failure).

### Consequences
`ctx.connection()` is one engine-local metadata request, not the forbidden catalog round-trip. This is the only remedy the Exasol pushdown contract admits, and matches the fix `exasol-virtual-schema` 4.0.0 used for the same class of defect (issue #24).

## ADR: Close the static credential path in this plan; defer the vended residual as issue #378

**ID:** close-static-credential-path-defer-vended-residual-as-378
**Plan:** fix-connection-credential-exposure
**Status:** Superseded by seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material

### Context
A vended credential comes from the `loadTable`/Unity response and has no CONNECTION name to reference, so the reference design alone could not close it. Encrypting it under a key derived from the CONNECTION was considered, but at this point in the plan its guarantee looked unconditionally weaker than the static fix's: a no-auth catalog password of `{"warehouse":"…"}` would yield a near-guessable key, and shipping a conditional guarantee alongside an unconditional one would blur what the release promises.

### Decision
With `use_vended_credentials` false, the wire carries a reference and no credential. With it true, the resolved backend is still carried inline and the vended credential still appears in the SQL, tracked as the open exception issue #378.

### Options Considered
Encrypt the storage block under a key derived from the resolved CONNECTION (the eventual direction, adopted later — see the seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material entry); refuse to plan a vended query until closed (rejected — breaks every working vended deployment); say nothing about the vended path (rejected — a silent gap).

### Consequences
The static fix is the strict prerequisite of any later envelope design, since the envelope's key derivation needs the same `ctx.connection()` read this decision builds — so landing the static fix first composes and forecloses nothing. Superseded on the vended half only after a round-2 discussion with the user reopened the deferral; the static half of this decision stands unchanged.

## ADR: No fallback when the script-scoped connection grant is absent

**ID:** no-fallback-when-script-scoped-connection-grant-absent
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
A deployment upgrading to the reference-based design needs a new `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` before the scan UDF can resolve its credential. A fallback to an inline credential when that grant is missing would keep every unupgraded deployment vulnerable, since no operator is forced to notice.

### Decision
A deployment missing the grant fails at scan time with an error naming the connection and the missing access. No code path reads an inline credential when the reference cannot be resolved.

### Options Considered
Fall back to an inline credential with a logged warning (rejected); keep the inline form behind a virtual-schema property for one release (rejected).

### Consequences
`exasol-virtual-schema` 4.0.0 took the same position on removing its old variant ("intentionally not supported anymore to tighten security"). A hard failure is the only outcome an operator cannot silently ignore.

## ADR: Resolve scan storage once per invocation; the redaction secret set follows the resolved value

**ID:** resolve-scan-storage-once-per-invocation-secret-set-follows-resolved-value
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
`CommonScanSpec::all_secret_values` fed value-based redaction from the unresolved spec. Once the spec carries a connection reference instead of a credential, that method has nothing to yield — so redaction would quietly go empty exactly where it matters, with every existing test still green.

### Decision
One `resolve_scan_storage` call at the top of `run_scan` resolves both join sides into a `ResolvedScanStorage`, which owns `all_secret_values()`. `CommonScanSpec::all_secret_values` is deleted.

### Options Considered
Resolve lazily at each store-construction site (rejected — reads one CONNECTION twice per join invocation, and leaves the secret set undefined between reads); keep the secret set on the unresolved spec (rejected — the silent-empty failure mode above).

### Consequences
The secret set now follows the credential rather than the spec, closing the one path where the fix could silently weaken error-path redaction while leaving tests green.

## ADR: The CONNECTION-to-backend derivation gains a storage-only projection; nothing moves out of the adapter module

**ID:** storage-only-connection-creds-projection-stays-in-adapter-module
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An earlier direction moved `parse_creds` and `storage_block` into `lakehouse-catalog` so the scan UDF could call them directly. That reverses `catalog-crate-structure`'s recorded prohibition on the catalog crate naming the CONNECTION delivery mechanism, and running the full `parse_creds` inside the UDF would materialize catalog-auth fields (`token`, `client_secret`) on every shard invocation, outside the storage-only redaction set.

### Decision
`read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` all stay in `lakehouse_engine::adapter::connection`. `lakehouse-catalog` gains a `StorageCreds` projection (nine storage fields only), `StorageCreds::from_json`, `StorageCreds::backend`, and one `From<&ConnectionCreds>` conversion. `parse_creds` reads its nine storage spellings through `StorageCreds::from_json`; the scan UDF calls `from_json` then `backend` and never constructs a `ConnectionCreds`.

### Options Considered
Move both functions into `lakehouse-catalog` (rejected — reverses the recorded prohibition and exposes catalog-auth fields inside the UDF); duplicate the derivation scan-side (rejected — one decision, two homes free to drift); have the scan module call into `adapter::connection` directly (rejected — points business logic at a delivery mechanism).

### Consequences
What crosses the crate boundary is a credential TYPE, not a delivery-mechanism-aware function — the same boundary `StaticStoreAddress` already sits on. `storage_block` becomes a one-line projection-and-delegate rather than the rule's home.

## ADR: Seal the vended storage block under a key derived from the CONNECTION; refuse vending when the password carries no secret material

**ID:** seal-vended-storage-block-hkdf-aes-gcm-refuse-when-no-key-material
**Plan:** fix-connection-credential-exposure
**Status:** Accepted
**Supersedes:** close-static-credential-path-defer-vended-residual-as-378

### Context
A vended credential comes from the `loadTable`/Unity response and has no CONNECTION name to reference, so the reference design alone cannot close issue #378. An earlier decision deferred the vended residual, reasoning that an encryption envelope's guarantee would be conditional on the CONNECTION password's entropy — a no-auth catalog password of `{"warehouse":"…"}` yields a near-guessable key. The round-2 discussion with the user reopened this: the reference design's `ctx.connection()` read already supplies key material, so the prerequisite objection no longer holds.

### Decision
Under `use_vended_credentials`, the resolved `StorageBackend` is serialized as before and then sealed with AES-256-GCM under a 32-byte key derived via HKDF-SHA256 from the CONNECTION password bytes (empty salt, fixed info string), with a fresh random 96-bit nonce per encryption. The scan UDF resolves the same CONNECTION, derives the identical key, and opens the envelope. The gate criterion tests the CONNECTION password's own secret content — non-empty `token`, `client_secret`, `secret_key`, `session_token`, `account_key`, or `sas_token` — not the catalog-auth mode (a non-empty `access_key` alone does not satisfy it). When no field is non-empty and vending is enabled, planning is refused with a named error stating the combination and the remedies.

### Options Considered
Keep the prior deferral, vended values inline in plaintext under a tracked issue (rejected — leaves #378 open with no closing mechanism); encrypt unconditionally, no-auth included (rejected — ships a false guarantee for a guessable key); refuse all vended queries (rejected — breaks every working vended deployment); a second dedicated high-entropy CONNECTION as key material (rejected — adds an operator-facing provisioning step and a second grant for marginal gain over passwords that already carry secrets).

### Consequences
The guarantee is deliberately bounded: it defeats a plaintext read of `EXPLAIN VIRTUAL` output or pushdown-path error text, not offline cryptanalysis of the ciphertext. Acceptable because vended values are short-lived and prefix-scoped, and the key material is exactly what `ACCESS ON CONNECTION` already reveals. This is the first direct cryptographic dependency the `.so` carries (`hkdf` is new to the tree; `aes-gcm` and `sha2` were already present transitively), the scan-script grant now binds vended deployments too, and a mid-query CONNECTION rotation fails in-flight vended shards with a named error rather than reading stale credentials.

## ADR: The scan UDF reads a storage-only credential projection; no catalog-auth field is constructible inside it

**ID:** scan-udf-reads-storage-only-creds-projection-no-catalog-auth-fields
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early implementation ran the moved `parse_creds` over the whole CONNECTION password inside the UDF. `parse_creds` populates all seventeen `ConnectionCreds` fields, so `token`, `client_secret`, `client_id`, `oauth2_server_uri`, and `scope` would have materialized on every shard invocation — up to 300 per query — outside the storage-only redaction set, falsifying `connection-credentials-catalog-auth`'s recorded guarantee that those fields never cross the UDF boundary.

### Decision
The scan side deserializes a storage-only projection, `StorageCreds`, declaring exactly nine fields (`endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, `sas_token`) and never constructs a `ConnectionCreds`. A source-level probe asserts the type's own declaration names no catalog-auth field.

### Options Considered
Run the existing `parse_creds` (populating all seventeen fields) inside the UDF and rely on discipline not to read the catalog-auth fields (rejected — the exact defect this ADR closes); duplicate a hand-picked field subset ad hoc at each call site (rejected — no single declaration to probe against).

### Consequences
The exclusion of catalog-auth fields from the UDF becomes structural (enforced by the type's own field list) rather than a discipline six call sites must honour independently.

## ADR: Every absence assertion in this change's verification needs a positive control

**ID:** positive-control-required-for-every-absence-assertion
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
Each test carrying the credential-absence guarantee was an absence assertion with no positive control — satisfied equally by "the secret is genuinely absent" and by "the surface being checked is empty for an unrelated reason." The sharpest instance: a claim that a profiling view's `SQL_TEXT` must not contain a credential value was checked against a view that, unflushed, returned zero rows for the querying user in the planning environment.

### Decision
Every absence assertion this change introduces is paired with a positive control proving its surface is populated before asserting the credential absent from it. The profiling case was settled live: with `ALTER SESSION SET PROFILE = 'ON'`, a marked query, and a DBA-issued `FLUSH STATISTICS`, the least-privilege user reads back its own profiling rows, each carrying the full `SQL_TEXT`. Live verification later (see the recorder checklist for this plan) established that profiling and audit `SQL_TEXT` never carry the VS-rewritten pushdown SQL in the first place — they record only the user's own literal statement — so the profiling positive control was removed, not merely satisfied, once the surface was confirmed unable to distinguish the two states.

### Options Considered
Demote the profiling clause to a weaker, achievable claim (rejected initially — the determinism question was settled live instead, then the clause was corrected once profiling was shown structurally incapable of carrying the pushdown SQL).

### Consequences
The principle — pair every absence assertion with a positive control — remains applied wherever the surface genuinely can distinguish a fixed build from a vulnerable one (the rendered SQL string, the golden fixtures). An assertion against a surface that structurally cannot carry the thing being checked for is corrected instead of defended with a positive control it cannot supply.

## ADR: The installer grants scan-script connection access to a deployment-scoped role, not to PUBLIC or a bare per-user placeholder

**ID:** installer-grants-connection-access-to-deployment-scoped-role-not-public
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
The scan UDF's new script-scoped connection grant needs a repeatable installer story. Four grantee shapes were checked live against Exasol 2025.2.1: `GRANT ACCESS ANY CONNECTION` (a blanket privilege), `GRANT ... FOR SCRIPT ... TO PUBLIC`, a bare per-user placeholder repeated per user, and a dedicated role.

### Decision
The installer template creates one schema-qualified role, `LAKEHOUSE_ENGINE_ROLE_<schema>`, grants `ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_SCAN` to that role once, and grants the role to the installing user. A further user gets access via a plain `GRANT <role> TO <user>`.

### Options Considered
`GRANT ACCESS ANY CONNECTION` (rejected — live-verified it lets the grantee's own arbitrary script resolve ANY connection, reopening the leak); `... TO PUBLIC` (rejected — script-scoping holds, but it grants every current and future instance user with no way to scope down later); a bare per-user placeholder repeated per user (rejected — every additional user needs the operator to re-run connection-grant syntax they likely have not memorized).

### Consequences
Onboarding a further user is one standard `GRANT <role> TO <user>` line. The cost: `CREATE ROLE` has no `IF NOT EXISTS` form, so the template and `docs/security.md` must tell the operator to check `EXA_ALL_ROLES` before re-running it on an existing deployment.

## ADR: parse_creds and storage_block stay in the adapter module; nothing moves to the catalog crate for this change

**ID:** parse-creds-storage-block-stay-in-adapter-module-not-moved
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early design moved `parse_creds` and `storage_block` into `lakehouse-catalog` so the scan side could call them directly. `catalog-crate-structure`'s recorded spec states verbatim that those six functions "SHALL stay in `lakehouse_engine::adapter::connection`, because they interpret the Exasol CONNECTION object and the catalog crate MUST NOT name that delivery mechanism" — a substantive constraint, not a formal one, since `parse_creds` reads the full CONNECTION password schema including its catalog-auth half.

### Decision
Nothing moves. All six functions keep their adapter-module home. `lakehouse-catalog` gains only the `StorageCreds` projection, `StorageCreds::backend` (the one selection rule), and one `From<&ConnectionCreds>` conversion, mirroring the existing `From<&ConnectionCreds> for StaticStoreAddress`.

### Options Considered
The original move, superseding `catalog-crate-structure:68` with a delta (rejected — reverses a coherent recorded decision and runs the full credential parse inside the UDF); duplicate the derivation scan-side (rejected — one decision, two homes free to drift); have the scan module call into `adapter::connection` (rejected — points business logic at a delivery mechanism).

### Consequences
`catalog-crate-structure:68` is satisfied rather than superseded and needs no delta. What crosses the boundary is a credential type, keeping exactly one home for the derivation.

## ADR: The delta set covers every recorded feature this change falsifies, not only the features whose own behaviour changes

**ID:** delta-set-widened-to-cover-every-falsified-credential-claim
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early plan recorded six deltas while roughly twenty recorded features kept asserting the opposite of the shipped behaviour: unscoped "credentials MUST NOT appear in any returned SQL" prohibitions across thirteen `vs-adapter/pushdown-planning*` features plus `unity-catalog-vended-credentials` and three E2E/Azure harnesses, plus wire-encoding and byte-identity clauses across several scan-execution and join features with no `storage` carve-out.

### Decision
The feature list grew from 6 to 40 deltas. Every recorded claim this change falsifies gets its own delta. Thirteen carry substantive normative changes; eleven are one-scenario scoping corrections to sibling `pushdown-planning-*` features, each stating its own scoped claim and citing `scan-spec-credential-reference` rather than restating it (a single shared statement was not available, since `speq plan validate` rejects a delta with no `## Scenarios` section).

### Options Considered
Record only the features whose own behaviour materially changes, and add one global note elsewhere (rejected — leaves ~20 recorded features asserting the opposite of shipped behaviour on a security feature, and `speq audit` would report drift).

### Consequences
Every `DELTA:CHANGED` scenario was reproduced verbatim from the recorded spec with only the named clauses altered, then machine-diffed against the recorded text to confirm the change count — keeping the merge mechanical rather than a rewrite.

## ADR: Seven redaction feed sites read the scan-spec storage value, not two — ScanStorage exposes no secret accessor

**ID:** seven-redaction-feed-sites-scanstorage-exposes-no-secret-accessor
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early task named two feed sites for value-based redaction. The natural way to make a third, undiscovered site compile — a `secret_values()` method on the wire wrapper that returns empty for the reference variant — would keep every existing test green while silently disarming redaction on the raw-scan and partial-aggregate error paths, which no verification step covered.

### Decision
Verified the site list independently: seven sites feed value-based redaction, not two — two read the union off the spec, three read the fact side off the spec, and two already take a `&StorageBackend` parameter from their callers. `ScanStorage` exposes no `secret_values()` method and no payload accessor, so a site left reading the unresolved wire value fails to compile rather than silently redacting nothing.

### Options Considered
Add a `secret_values()` method to the wire wrapper that returns empty for the reference variant (rejected — compiles everywhere while silently disarming redaction, the exact failure mode this decision exists to prevent).

### Consequences
A compile-time guard replaces a discipline six modules would otherwise have to honour independently. Tests were added covering the raw-scan and partial-aggregate error paths this gap had left uncovered.

## ADR: The credential-absence guarantee is asserted through the production selection function, not a hand-built fixture

**ID:** assert-credential-guarantee-via-production-selection-function
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early test built its own scan-spec template with `ScanStorage::Connection` directly and then asserted no credential sentinel appeared in it — asserting that its own fixture holds no sentinel, never exercising the production selection logic. The committed golden fixtures had the same defect: they were rendered from a test-only sample-storage helper, so regenerating them reflected the fixture helper, not the adapter.

### Decision
The variant choice is extracted into one pure function, `scan_storage_for`, and every builder-path test drives its assertions with a template that function produces from a `ConnectionCreds` carrying sentinel values, under both settings of `use_vended_credentials`. The builder-path list is derived from the code (every `RequestShape` variant crossed with join/top-N/`COUNT(DISTINCT)` sub-paths) rather than hand-counted, cross-checked against the eighteen credential-bearing golden fixture names.

### Options Considered
Assert on the `ScanSpec` structure alone (rejected — stays green while the rendered string regresses); assert only at the single chokepoint `build_fan_out_inner` (rejected — misses a later builder path that bypasses it).

### Consequences
A regression where a builder path emits `Inline` for a static CONNECTION becomes visible to the test suite; under the earlier fixture-only design it was invisible to both the unit test and the golden fixtures.

## ADR: A recorder checklist file replaces an unenforceable orchestration step for correcting recorded Background prose

**ID:** recorder-checklist-file-replaces-unenforceable-pipeline-step
**Plan:** fix-connection-credential-exposure
**Status:** Accepted

### Context
An early plan draft claimed the `speq-implement-pr` pipeline would run an extra verification step between `/speq:implement` and `/speq:record` to catch stale credential-exposure claims left in recorded `## Background` prose. That pipeline is fixed and cannot add a step, and `/speq:spec-merge` itself only ever acts on `## Scenarios` — it has no mechanism for correcting a recorded Background bullet or a feature description, so a `DELTA:CHANGED` reproduction cannot strike a false Background bullet; it can only place a correct scenario beside a surviving false one.

### Decision
Task 8.1 writes `specs/_plans/fix-connection-credential-exposure/recorder-checklist.md`, an exact `file:line`, strike-sentence, and replacement per entry for every recorded Background bullet or description this change falsifies. `/speq:record` must execute it against the recorded library as part of merging this plan, in addition to the normal scenario-delta merges.

### Options Considered
Convert every affected location to a `DELTA:CHANGED` scenario reproduction (rejected — not available for `## Background` bullets and feature descriptions, which the merge mechanism cannot touch); rely on an orchestration step the fixed pipeline cannot accommodate (rejected — the claim was checked and found false).

### Consequences
Twelve recorded features carried the exact falsified sentence "Credentials MUST NOT appear in any returned SQL(...)"; all are struck by this checklist's SET 1/SET 2 entries. A manual post-record grep sweep (`plan.md` § Post-Record Verification) confirms zero survivors, since no automated gate can verify the checklist ran.
