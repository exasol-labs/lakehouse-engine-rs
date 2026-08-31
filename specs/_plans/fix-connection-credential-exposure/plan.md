# Plan: fix-connection-credential-exposure

> **Status:** blocked — see open-questions.md

## Summary

Replace the storage credentials the adapter embeds verbatim in the scan-driving SQL with a reference to the Exasol CONNECTION that supplies them, resolved by the scan UDF through `ctx.connection()`. Closes issue #135 for CONNECTION-supplied standing credentials — a user holding only `SELECT` on a virtual schema can today read a standing `access_key` and `secret_key` out of `EXPLAIN VIRTUAL` output, out of `EXA_USER_PROFILE_LAST_DAY.SQL_TEXT`, and out of any error raised on the pushdown path; the vended residual is tracked as issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378).

## Design

### Context

`crates/lakehouse-engine/src/adapter/pushdown/support.rs:441` serializes the resolved `StorageBackend` into a SQL string literal:

```rust
let common_literal = sql_string_literal(&spec_template.to_common_json());
```

`to_common_json` is plain `serde_json` — no encoding, no compression, no obfuscation anywhere on the path. The committed golden fixture `testdata/dispatch_golden/single_group_row_scan.sql` therefore contains:

```
"storage":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin",…}}
```

That string is what the adapter returns as the pushdown response's single `sql` field, and Exasol echoes it verbatim. `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:128-133` records the leak as a `ponytail:` debt marker and names the two candidate fixes; this plan takes the first of them.

**The contract permits exactly one remedy.** The relevant findings, each with its source:

| Question | Finding |
|---|---|
| Does `EXPLAIN VIRTUAL` redact? | No. Returns `PUSHDOWN_ID`, `PUSHDOWN_INVOLVED_TABLES`, `PUSHDOWN_SQL`; prerequisite is "Privileges similar to the executed query". Exasol's own `exasol-virtual-schema` issue #24 shows a password echoed verbatim. |
| Can the adapter hand Exasol a value out of band? | No. `virtual-schema-common-java`'s `PushDownResponse` has ONE field; `ResponseJsonConverter` emits `{"type":"pushdown","sql":<string>}` and nothing else. No bind parameter, no placeholder, no second field. |
| Could a property or adapter note carry it? | No. `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` and `EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES` are documented "All users have access to the table" — worse than the SQL, not better. |
| Is `EXPLAIN VIRTUAL` the only surface? | No. Verified live on 2025.2.1: a user holding neither `ACCESS ANY CONNECTION` nor `SELECT ANY DICTIONARY` read its own profiled statement's full `SQL_TEXT` out of `EXA_USER_PROFILE_LAST_DAY`. `EXA_DBA_AUDIT_SQL.SQL_TEXT` holds it under `SELECT ANY DICTIONARY` with auditing enabled by default. |
| Can a UDF read a CONNECTION by name? | Yes, and it is the documented remedy. `hide_access_keys_passwords.htm`: a hardcoded secret is "visible in the audit tables, profiling tables, and in the `EXA_ALL_SCRIPTS` table"; carry the name and read it with `get_connection`. The SLC protocol request is `PB_IMPORT_CONNECTION_INFORMATION` (`zmqcontainer.proto`), language-agnostic; `exasol-udf-sdk` 0.23.0 exposes it as `UdfContext::connection` (`context.rs:217`) on the same trait the scan UDF receives. |
| Can the grant be narrowed to one script, and does it work with a RUNTIME-argument name? | Yes to both, verified live end to end on 2025.2.1 — see task 1.3a. A user holding only `CREATE SESSION`, `EXECUTE ON SCRIPT`, and `GRANT ACCESS ON CONNECTION c FOR SCRIPT s.p TO u` resolved the connection from inside the script with the name arriving as a VARCHAR argument; after the revoke the same call failed with `insufficient privileges for using connection c in script p`, SQL state `22001`. |
| Precedent for the change? | `exasol-virtual-schema` 4.0.0 closed issue #24 the same way — inline connection string replaced by a named `EXA_CONNECTION` — recording "The old variant is intentionally not supported anymore to tighten security." |

Two consequences follow. First, **redaction-at-render is not a fix**: there is one pushdown string, Exasol executes exactly the string it is given, so a placeholder for `EXPLAIN VIRTUAL` would still ship the real value into profiling and audit. Second, **a vended credential cannot be referenced at all**: it comes from the `loadTable` response, the UDF may not re-request it, and no name identifies it. That residual is tracked as issue #378.

**Sequencing.** Issue #135 was deferred behind the Azure ADLS work, whose static-credentials slice was to rework redaction into a per-variant `secret_values()` seam. That seam has landed (`crates/lakehouse-catalog/src/storage.rs:106-121`), `abfss://` support is in, and the named prerequisite #267 is merged, so the deferral condition is discharged and this plan lands on that seam.

- **Goals** — no CONNECTION-supplied storage credential in any generated SQL, on any builder path; the guarantee enforced by a test on the rendered string, driven through the production selection function rather than through a hand-built fixture; the error-path redaction no weaker after the change than before, at every one of its seven feed sites; no catalog-authentication field constructible inside the scan UDF; the vended residual named and cited; the four adjacent same-class `Debug`/redaction exposures closed in the same change; every recorded feature whose credential claim this change falsifies given its own delta.
- **Non-Goals** — closing the vended residual (#378: needs a cryptographic envelope and its own design); adding a cryptography dependency to the `.so`; changing any credential SELECTION rule (SigV4 gate, vending gate, credential-source selection, longest-`prefix` match, CONNECTION-wins addressing, scheme-driven backend, plaintext consent gates); changing the scan-driving SQL's shape, the shard fan-out, or the per-shard file encoding; re-running CONNECTION acceptance validation inside the UDF; moving any function that interprets the Exasol CONNECTION object out of the adapter module; supporting a deployment that declines the new grant.

### Decision

Put the connection NAME where the credential used to be. Resolve it once, in the UDF, before any store is built, through a storage-only projection that cannot hold a catalog secret.

#### Architecture

```
 PLAN TIME (adapter, once per query)              SCAN TIME (UDF, once per shard invocation)

 ctx.connection(CATALOG_CONNECTION)
        │
        ├── parse_creds  (STAYS in adapter/connection.rs)
        │        │
        │        ├── catalog auth ──────────────► never crosses the UDF boundary
        │        │
        │        └── StorageCreds (9 storage fields)
        │                     │
        │                 .backend(allow_http)     ← the ONE selection rule
        │                     │
        │              effective_storage (plan-time manifest / log reads)
        │
        └── scan_storage_for(creds, name, allow_http, effective)   ← ONE pure function
                                │
        ┌───────────────────────┴────────────────────────┐
        │ !use_vended_credentials                        │ use_vended_credentials
        ▼                                                ▼
 ScanStorage::Connection {                       ScanStorage::Inline(StorageBackend)
   name, allow_http }                                    │  vended credential on the wire (#378)
        │                                                │
   NO credential, NO addressing on the wire              │
        │                                                │
        └──────────────► common blob ◄───────────────────┘
                              │
                    sql_string_literal  →  {"type":"pushdown","sql":…}
                              │
                              ▼
                                          resolve_scan_storage(spec, ctx)   ← ONE site
                                            Connection{name,allow_http}
                                              → ctx.connection(name)
                                              → StorageCreds::from_json(password)
                                              → .backend(allow_http)
                                            Inline(b) → b
                                                     │
                                                     ▼
                                          ResolvedScanStorage { primary, join }
                                            .all_secret_values()  ← the ONLY secret set
                                                     │
                                          threaded to all 7 redaction feed sites
                                                     │
                                                     ▼
                                          existing per-side store registration
                                          (backend-dispatching, unchanged)
```

The wire gains one wrapper. The catalog crate gains one storage-credential projection type and the one backend selector over it. The scan gains one resolution step and one owner of the redaction secret set. Everything downstream of `ResolvedScanStorage` receives the same `StorageBackend` it receives today.

#### Key interfaces

| Item | Home | Visibility |
|---|---|---|
| `enum ScanStorage { Connection { name: String, allow_http: bool }, Inline(StorageBackend) }` — externally tagged, never `untagged`, and exposing NO `secret_values()` or payload accessor | `crates/lakehouse-engine/src/scan/spec.rs` | `pub` (wire type) |
| `CommonScanSpec.storage: ScanStorage`, `JoinSpec.storage: ScanStorage` | `scan/spec.rs:1167`, `:618` | `pub` (field type change) |
| `struct StorageCreds` declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, `sas_token`; `StorageCreds::from_json(&serde_json::Value) -> StorageCreds` (the ONE reader of those nine key spellings, applying the existing non-empty normalization and `path_style` default of true); `StorageCreds::backend(&self, allow_http: bool) -> StorageBackend` (the ONE selection rule, body moved from `storage_block`); `impl From<&ConnectionCreds> for StorageCreds` | `crates/lakehouse-catalog/src/creds.rs` | `pub`, re-exported at crate root |
| `parse_creds`, `storage_block`, `read_connection`, `validate_creds`, `catalog_block`, `REQUIRED_KEY` | `crates/lakehouse-engine/src/adapter/connection.rs` | UNMOVED — `catalog-crate-structure:68` pins them here. `parse_creds` reads its nine storage fields through `StorageCreds::from_json`; `storage_block` becomes `StorageCreds::from(creds).backend(allow_http)` |
| `scan_storage_for(creds: &ConnectionCreds, connection_name: &str, allow_http: bool, effective: &StorageBackend) -> ScanStorage` — the ONE pure variant-selection function, called by all three spec-storage population sites | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `pub(super)` |
| `struct ResolvedScanStorage { primary: StorageBackend, join: Option<StorageBackend> }` with `all_secret_values(&self) -> Vec<&str>` — the ONLY type in the scan path exposing a secret set | `crates/lakehouse-engine/src/scan/storage_ref.rs` (new) | `pub(crate)` |
| `resolve_scan_storage(&CommonScanSpec, &dyn UdfContext) -> Result<ResolvedScanStorage, UdfError>` — the ONLY constructor of `ResolvedScanStorage` | `scan/storage_ref.rs` | `pub(crate)` |
| `ResolvedConnectionConfig.connection_name: String` | `crates/lakehouse-engine/src/adapter/mod.rs:182-188` | `pub(crate)` (one added field) |
| `CommonScanSpec::all_secret_values` | `scan/spec.rs:1232-1238` | DELETED — see § Dead Code Removal |

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| Indirection by name | `ScanStorage::Connection` | The only channel the Exasol pushdown contract leaves open, and the one Exasol documents and used to fix its own leak |
| One pure function owns the variant choice | `scan_storage_for` | A choice made at three sites is a choice three sites can get wrong, and a test driving a hand-built template asserts on its own fixture rather than on the selection |
| Storage-only projection at the boundary | `StorageCreds` | A type with no catalog-auth field cannot carry one into the UDF; the guarantee is structural rather than a discipline six modules must honour |
| Resolve-once-at-the-boundary | `resolve_scan_storage` at the top of `run_scan` | Both join sides resolved in one step, so the redaction set is defined before any store exists and one CONNECTION is read once per invocation |
| Single owner of the secret set, and no way to read the wrong one | `ResolvedScanStorage::all_secret_values`; no `secret_values()` on `ScanStorage` | The set must follow the credential, and a wrapper accessor returning empty would compile at all five spec-reading sites and silently disarm redaction |
| Guard on the type, not the site | redacting `Debug` on `StorageProps`, `StorageBackend`, `ConnectionCreds` | A `{:?}` added later cannot be caught by a test that does not exist yet |
| Positive control on every absence assertion | § Verification | An assertion that a secret is absent is satisfied by an empty surface; each one first proves its surface is populated |
| Assert on the rendered string | § Verification, all builder paths | A structural assertion stays green while the rendered SQL regresses |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Reference the CONNECTION by name; UDF resolves it | Render a placeholder for `EXPLAIN VIRTUAL`; carry the credential in a property or adapter note; bind parameters | Placeholder rendering fixes only the demo surface — one `sql` string is both explained and executed, so profiling and audit still get the real value. Properties and adapter notes are documented readable by all users with access to the virtual schema. Bind parameters do not exist in the API: `PushDownResponse` has one field. |
| The scan derives from a storage-only projection; `parse_creds`/`storage_block` do NOT move | Move both into `lakehouse-catalog` and run `parse_creds` in the UDF; duplicate the derivation scan-side | `catalog-crate-structure:68` records that those six functions "SHALL stay in `lakehouse_engine::adapter::connection`, because they interpret the Exasol CONNECTION object and the catalog crate MUST NOT name that delivery mechanism". Moving them reverses a recorded decision AND runs `parse_creds` — which populates all seventeen `ConnectionCreds` fields — inside the UDF, materializing `token` and `client_secret` on up to 300 shard invocations outside the storage-only redaction set. Publishing a nine-field projection plus the one selector satisfies the recorded clause and makes the catalog-auth exclusion structural. |
| No addressing on the wire for a referenced credential | Carry `endpoint`, `region`, `path_style`, `account_name` on the `Connection` variant beside the name | Two sources for one backend is how the "field-for-field EQUAL" guarantee breaks. Re-deriving everything from the one CONNECTION read keeps exactly one source; the cost is that an `ALTER CONNECTION` changing addressing mid-query is observable, which is specified rather than hidden. |
| Vended credentials stay inline; residual tracked as #378 | Encrypt the storage block under a key derived from the CONNECTION password (HKDF + AEAD); refuse to plan a vended query | The static fix is the strict PREREQUISITE of the envelope (which needs `ctx.connection()` for its key), so landing it first composes rather than forks. The envelope's guarantee is CONDITIONAL on the CONNECTION password's entropy — a no-auth OSS catalog password of `{"warehouse":"…"}` yields a near-guessable key — and shipping a conditional guarantee beside an unconditional one blurs what the release promises. Refusing to plan breaks every working vended deployment. |
| No fallback to inline credentials when the grant is absent | Fall back and warn | A fallback keeps the leak reachable for every deployment that never adds the grant, which is every existing one. A clear scan-time error is the only outcome that cannot be ignored. |
| UDF runs the derivation, not the acceptance validation | Re-run `validate_creds` in the UDF | `validate_creds` is parameterized by `CatalogKind` and answers a plan-time question already answered for this query. Threading `CatalogKind` onto the wire buys no new decision, and it would report a CONNECTION edited mid-query as a validation error instead of the storage access error it is. |
| One wrapper enum on the wire, tagged, with no secret accessor | Optional credential fields on `StorageProps`; a sibling `connection_name` field; a `secret_values()` on the wrapper returning empty for the reference variant | Optional fields make "credential absent because referenced" and "credential absent because empty" the same state. A sibling field admits the contradictory combination of a name AND a credential. A wrapper `secret_values()` would make all five spec-reading redaction sites compile while returning nothing — the exact silent disarm this plan exists to avoid. |
| The variant choice is one pure function called at three sites | Choose it inside each format reader; choose it at the single-table site only | The readers use the concrete backend immediately for their own manifest and log reads, so a wrapper there breaks plan-time reading. Choosing it only on the single-table path leaves the broadcast-join and N-scan-join paths emitting the static credential inline, and no test would see it. |
| The wire type stays the spec's field; no parallel `ResolvedScanSpec` | A `ResolvedScanSpec` owning a `ScanSpec` whose storage is already resolved, taken by every downstream function | The invariant a `ResolvedScanSpec` would carry structurally is already enforced at compile time by the wrapper exposing no secret or payload accessor, and the two sites that take a `&StorageBackend` parameter cannot read the wire value at all. A parallel spec type would touch the 28 files that construct `CommonScanSpec` for an invariant already unrepresentable. Recorded as considered, not dismissed. |
| Rotation guidance ships as `docs/security.md` | A spec feature; a section in `docs/install.md` | The engine holds no state and re-resolves every query, so rotation is an operator procedure with no behaviour to specify — except the one mid-query consequence this plan introduces, which IS specified. Its own page rather than a subsection because it also carries the grant model and the delegation residual. |
| Four adjacent `Debug`/redaction exposures fixed here | Defer them | All four are the same CWE class on the error path. Shipping "fixed credential exposure" while `{:?}` on `StorageProps` still prints a secret key would be inaccurate. |
| Every recorded feature whose credential claim this falsifies gets its own delta | Record six deltas and leave the rest; add one global note | Roughly twenty recorded features assert "credentials MUST NOT appear in any returned SQL", four assert a `storage`-inclusive byte-identity gate, and one asserts a per-side redaction set. Recording six deltas would leave the library asserting the opposite of the shipped behaviour on a security feature, and `speq audit` would report drift. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/scan-spec-credential-reference | NEW | `vs-adapter/scan-spec-credential-reference/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/connection-credentials-catalog-auth | CHANGED | `vs-adapter/connection-credentials-catalog-auth/spec.md` |
| vs-adapter/catalog-crate-public-surface-extensions | CHANGED | `vs-adapter/catalog-crate-public-surface-extensions/spec.md` |
| vs-adapter/storage-backend-enum | CHANGED | `vs-adapter/storage-backend-enum/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | CHANGED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |
| vs-adapter/unity-catalog-vended-credentials | CHANGED | `vs-adapter/unity-catalog-vended-credentials/spec.md` |
| vs-adapter/delta-table-planning | CHANGED | `vs-adapter/delta-table-planning/spec.md` |
| vs-adapter/pushdown-module-dedup-consolidation | CHANGED | `vs-adapter/pushdown-module-dedup-consolidation/spec.md` |
| vs-adapter/pushdown-agg-sql-consolidation | CHANGED | `vs-adapter/pushdown-agg-sql-consolidation/spec.md` |
| vs-adapter/pushdown-format-neutral-resolution | CHANGED | `vs-adapter/pushdown-format-neutral-resolution/spec.md` |
| vs-adapter/pushdown-planning-file-encoding | CHANGED | `vs-adapter/pushdown-planning-file-encoding/spec.md` |
| vs-adapter/pushdown-planning-topn | CHANGED | `vs-adapter/pushdown-planning-topn/spec.md` |
| vs-adapter/pushdown-planning-join-fallback | CHANGED | `vs-adapter/pushdown-planning-join-fallback/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback | CHANGED | `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback/spec.md` |
| vs-adapter/pushdown-planning-count-distinct | CHANGED | `vs-adapter/pushdown-planning-count-distinct/spec.md` |
| vs-adapter/pushdown-planning-order-by-capability | CHANGED | `vs-adapter/pushdown-planning-order-by-capability/spec.md` |
| vs-adapter/pushdown-planning-aggregate-extensions | CHANGED | `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` |
| vs-adapter/pushdown-planning-expression-aggregate | CHANGED | `vs-adapter/pushdown-planning-expression-aggregate/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| vs-adapter/pushdown-planning-empty-result | CHANGED | `vs-adapter/pushdown-planning-empty-result/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/scan-execution-join | CHANGED | `datafusion-scan/scan-execution-join/spec.md` |
| datafusion-scan/scan-module-structure | CHANGED | `datafusion-scan/scan-module-structure/spec.md` |
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| e2e-harness/e2e-harness | CHANGED | `e2e-harness/e2e-harness/spec.md` |
| e2e-harness/lakekeeper-e2e-harness | CHANGED | `e2e-harness/lakekeeper-e2e-harness/spec.md` |
| e2e-harness/unity-catalog-e2e-harness | CHANGED | `e2e-harness/unity-catalog-e2e-harness/spec.md` |
| azure-e2e/azure-e2e-harness | CHANGED | `azure-e2e/azure-e2e-harness/spec.md` |

Twenty-nine of the thirty-five deltas exist because the change falsifies a recorded claim rather than because the feature's own behaviour changes. Eleven of those are one-bullet scoping corrections to sibling `pushdown-planning-*` features, four are `storage`-value carve-outs in byte-identity gates, and four correct vended-credential prohibitions that were never true for the returned SQL.

## Impact

**Breaking — every user or role that queries a virtual schema must hold the scan script's grant.** Add `GRANT ACCESS ON CONNECTION <connection> FOR SCRIPT <schema>.LAKEHOUSE_SCAN TO <user-or-role>`. This is per grantee, not once per deployment: every user or role that queries the virtual schema needs it, or their queries start failing at scan time. No `ACCESS ON CONNECTION` grant is emitted or documented anywhere in this repository today — `deploy/scripts/install.sh`, `docs/install.md`, `docs/catalogs.md`, the E2E harness, and the recorded spec library contain none — so this is a new statement operators have no existing line to copy, not an addition beside one they already run. Whether the ADAPTER script also needs its own grant for a non-owning user is settled by task 1.3b. A deployment that upgrades without the grant plans successfully and fails at scan time with an error naming the connection and the missing access. There is deliberately no fallback. The installer's next-step template emits the statement, and `docs/security.md` carries the grant model.

**Breaking — `CREATE OR REPLACE CONNECTION` now silently revokes access.** Verified live on Exasol 2025.2.1: `CREATE OR REPLACE CONNECTION` DROPS a `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` grant, while `ALTER CONNECTION ... IDENTIFIED BY` PRESERVES it. `deploy/scripts/install.sh`'s next-step template and `crates/lakehouse-engine/tests/common/stack.rs:370` both provision with `CREATE OR REPLACE CONNECTION`, so re-running the repo's own documented provisioning after this change breaks every scan until the grants are re-issued. The installer template carries a warning line, the harness issues its grant after the connection, and the rotation runbook recommends `ALTER`.

**Fixed — a `SELECT`-only user can no longer recover standing storage credentials from a query plan.** With vending disabled, the CONNECTION's `access_key`, `secret_key`, `session_token`, `account_key`, and `sas_token` no longer appear in the generated SQL, so they no longer appear in `EXPLAIN VIRTUAL` output, in `EXA_USER_PROFILE_LAST_DAY.SQL_TEXT`, in `EXA_DBA_AUDIT_SQL.SQL_TEXT`, or in an error raised on the pushdown path. A user holding `ACCESS ON CONNECTION` gains nothing from the change: Exasol documents that grant as including "passwords/tokens", so such a user could already read the credential.

**Not fixed, and named — a vended credential still appears in the generated SQL.** Under `use_vended_credentials` the credential comes from the `loadTable` or Unity temporary-credentials response, the UDF may not re-request it, and no name identifies it. Tracked as issue #378 and cited in seven specs. The residual is materially narrower than what closes: a vended credential expires and is scoped to the prefix the catalog vended it for, where a CONNECTION `secret_key` is long-lived and account-wide. Issue #135 as filed names "the `access_key` and `secret_key` from the catalog CONNECTION" and quotes an `AKIA...` key — the standing-credential case this plan closes — so the closure is against the case the issue reports. Task 1.5 establishes whether the reporting deployment had vending enabled and records the answer in the verification report, so the issue is not closed against a configuration it does not describe.

**Not contained, and named — the script-scoped grant delegates USE of the credential.** The connection name, the table root, and the file list all arrive as runtime arguments the caller controls, so a user who can execute `LAKEHOUSE_SCAN` and holds the grant can point the UDF at an arbitrary path inside that storage account. The grant stops the grantee READING the credential through a script of their own, not USING it through this one. `EXECUTE` on `LAKEHOUSE_SCAN` therefore becomes security-relevant, and the CONNECTION's credential should be scoped to the warehouse prefix. This is not a regression — today the same user can read the credential outright — but it is the boundary the fix leaves, and `docs/security.md` states it.

**Behavioural — a CONNECTION rotated mid-query is now observable per shard.** Previously a query's credentials were fixed when the adapter returned its SQL. Now each shard resolves the CONNECTION itself, so an `ALTER CONNECTION` landing between the adapter's read and a shard's read can leave one query reading some shards with the old values and others with the new — for the store `endpoint` and `region` as much as for the secret, because both are re-derived from the same read. The engine cannot close the window; a rotation is safe for in-flight queries only while the storage provider accepts both secrets. `docs/security.md` § Rotation carries the procedure.

**Wire-format change.** The `storage` key of the shard-invariant common argument becomes a tagged wrapper. The `.so`, SLC, and adapter deploy together and the scan script DDL is recreated per deployment, so no in-flight spec crosses a version boundary and no compatibility path is kept. Eighteen of the twenty-four committed golden pushdown-SQL fixtures are regenerated; the six `empty_*` fixtures carry no `storage` value and stay byte-identical.

**Unchanged.** Every credential selection rule; the scan-driving SQL's shape; the shard fan-out and its cap; the per-shard file encoding; projection, filter, LIMIT, aggregate, and join pushdown; the memory pool, batch size, and Parquet pruning flags; the CONNECTION password schema; the location of every function that interprets the Exasol CONNECTION object. No existing CONNECTION needs editing.

## Dependencies

None added; no dependency version changes. `exasol-udf-sdk` 0.23.0 already exposes `UdfContext::connection`, used today by the adapter at `crates/lakehouse-engine/src/adapter/connection.rs:57`.

Ordering:

- **Task 1.3a is the gate and it has already PASSED**, live against Exasol 2025.2.1 during plan revision, through a throwaway Lua probe script that needs no engine change (see the task for the exact statements and results). It is retained in the plan as a re-runnable probe so an implementer reproduces the result on the target container before starting, but it no longer blocks on unbuilt code and no fix task waits on new information.
- **Task 1.3b is the residual that genuinely needs the built artefact** — resolution from inside the real `LAKEHOUSE_SCAN` when reached through VS-rewritten pushdown SQL rather than through a directly-invoked script. It sits at the head of Group E, immediately before task 7.3. A 1.3b failure REVERTS tasks 3.x and 4.x and re-opens the plan; it is not worked around.
- Group A → Group B → Group C → Group D → Group E otherwise as § Parallelization states.

## Migration

| Current | New |
|---------|-----|
| No `ACCESS ON CONNECTION` grant is emitted or documented anywhere in this repository | `GRANT ACCESS ON CONNECTION c FOR SCRIPT s.LAKEHOUSE_SCAN TO u`, per user or role that queries the virtual schema |
| `CREATE OR REPLACE CONNECTION` is the documented provisioning form and drops no grant that exists | Still the provisioning form, but it now DROPS the scan-script grants; re-issue them after every replacement, or rotate with `ALTER CONNECTION` |
| `"storage":{"s3":{…"access_key":"AKIA…","secret_key":"…"…}}` on the wire | `"storage":{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":false}}` (vending disabled) |
| `"storage":{"s3":{…}}` on the wire, vended | `"storage":{"inline":{"s3":{…}}}` (vending enabled; #378) |
| `ResolvedConnectionConfig { catalog_uri, storage, creds, allow_http, catalog_kind }` | plus `connection_name` |
| `storage_block` owns the CONNECTION-to-backend selection rule | `StorageCreds::backend` owns it; `storage_block` keeps its adapter-module home and becomes a projection-and-delegate |
| `CommonScanSpec::all_secret_values()`, plus four sites reading `spec.common.storage.secret_values()` | `ResolvedScanStorage::all_secret_values()` at every feed site; no secret accessor on the wire type |
| Credentials fixed for a query at pushdown time | Re-resolved per shard invocation; a mid-query rotation is observable |

## Implementation Tasks

1. **Reproduce, and record the live contract findings.**
   1.1 Reproduce issue #135 live against the Docker stack: provision the VS, create a user holding only `SELECT` on it, run `EXPLAIN VIRTUAL`, and record that `PUSHDOWN_SQL` contains the seeded `access_key` and `secret_key`. Capture the plan text into the verification report. `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs` already prints the generated SQL and is the fastest route to the "before" artifact. [expert]
   1.2 Establish live which other surfaces carry the pushdown SQL. The mechanism is settled: on 2025.2.1, `ALTER SESSION SET PROFILE = 'ON'` plus a DBA-issued `FLUSH STATISTICS` makes a least-privilege user's own statement appear in `EXA_USER_PROFILE_LAST_DAY` with full `SQL_TEXT` — three rows, one per execution-graph part — while without the flush the same query returns zero rows. Reproduce that with the seeded credential values, and repeat against `EXA_DBA_AUDIT_SQL.SQL_TEXT` as `sys`. Record whether the REWRITTEN statement appears as its own row or only the user's original `SELECT`, because that scopes what the fix can claim. [expert]
   1.3a **GATE — ALREADY PASSED during plan revision; re-run to confirm on the target container.** Verified live on Exasol 2025.2.1 with a throwaway Lua probe needing no engine change: create a schema, a CONNECTION whose password is a JSON credential document, and `CREATE OR REPLACE LUA SCALAR SCRIPT <schema>.CONN_PROBE (cname VARCHAR(200)) RETURNS VARCHAR(2000)` whose `run` calls `exa.get_connection(ctx.cname)` and returns the address and the password LENGTH (never the password). Create a user holding only `CREATE SESSION`, `EXECUTE ON SCRIPT <schema>.CONN_PROBE`, and `GRANT ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.CONN_PROBE TO <u>`. Result: `SELECT <schema>.CONN_PROBE('<c>')` as that user returned `OK addr=… user=… pwlen=142`, so the script-scoped grant DOES cover a connection name arriving as a runtime VARCHAR argument. After `REVOKE ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.CONN_PROBE FROM <u>` the same call failed with `insufficient privileges for using connection <c> in script CONN_PROBE`, SQL state `22001`, naming the connection and the script and carrying no credential value. The same user was refused `EXA_DBA_CONNECTIONS` with SQL state `42500`. Drop the probe schema, connection, and user afterwards. [expert]
   1.3b **Residual gate, at the head of Group E.** Verify that `ctx.connection(name)` succeeds inside the real `LAKEHOUSE_SCAN` when the UDF is reached through VS-rewritten pushdown SQL rather than through a directly-invoked script, for a user holding only the script-scoped grant, and fails after the revoke. Record the error text and SQL state the Rust `UdfError` carries. A failure here REVERTS tasks 3.x and 4.x rather than being worked around. [expert]
   1.4 Record live that `CREATE OR REPLACE CONNECTION` DROPS an `ACCESS ON CONNECTION ... FOR SCRIPT` grant while `ALTER CONNECTION ... IDENTIFIED BY` PRESERVES it — both confirmed on 2025.2.1 during plan revision. Re-run on the target container and record the result in the verification report, because tasks 7.1 and 7.2 depend on it.
   1.5 Establish from the reporter's configuration whether `use_vended_credentials` was set in the deployment issue #135 describes, and absent that configuration, run the vended E2E path and record that the reported symptom survives there. Record the answer in the verification report, so #135 is closed against the case it actually reports and #378 carries the rest.

2. **Give the CONNECTION-to-backend derivation a storage-only projection, without moving anything out of the adapter.**
   2.1 Add `StorageCreds` to `crates/lakehouse-catalog/src/creds.rs` declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, `sas_token`, with `StorageCreds::from_json` applying the same non-empty normalization and `path_style` default of true that `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:310-339`) applies today, and `StorageCreds::backend(&self, allow_http: bool) -> StorageBackend` carrying `storage_block`'s body (`:360-382`) verbatim. Add `impl From<&ConnectionCreds> for StorageCreds`, mirroring the existing `From<&ConnectionCreds> for StaticStoreAddress` (`storage.rs:288-295`). Re-export all three at the crate root. [expert]
   2.2 Repoint `parse_creds` to read its nine storage fields through `StorageCreds::from_json`, and reduce `storage_block` to `StorageCreds::from(creds).backend(allow_http)`. Both functions KEEP their home in `crates/lakehouse-engine/src/adapter/connection.rs` — `specs/vs-adapter/catalog-crate-structure/spec.md:68` pins them there and this plan does not move them. [expert]
   2.3 Add the source-level probe to `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, mirroring `static_store_address_field_declarations()` / `assert_static_store_address_declares_no_credential_field()` (`:545-594`): assert from `StorageCreds`' own declaration that it names no field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `warehouse`, `use_sigv4`, or `use_vended_credentials`, and add `StorageCreds`, `StorageCreds::from_json`, and `StorageCreds::backend` to the probe's enumerated `pub` set.
   2.4 Add `crates/lakehouse-catalog/src/creds_tests.rs` cases asserting that `StorageCreds::from_json(json).backend(h)` equals `storage_block(&parse_creds(json), h)` field-for-field, over a password carrying every storage field, one carrying empty strings for each, and one omitting each — and that `from_json` over a password carrying `client_secret` yields no value equal to it. Leave the existing `storage_block` and `validate_creds` assertions in `crates/lakehouse-engine/src/adapter/connection_tests.rs` unedited; they are the characterization gate for 2.2.

3. **Add the wire wrapper and emit it from the adapter.**
   3.1 Add `enum ScanStorage` to `crates/lakehouse-engine/src/scan/spec.rs`, externally tagged with lowercase variant keys `connection` and `inline`, exposing NO `secret_values()` and no payload accessor, and change `CommonScanSpec.storage` (`:1167`) and `JoinSpec.storage` (`:618`) to it. Add a decode-side test pinning that neither enum is `untagged` and that a payload naming a variant it does not match is rejected. Update the 28 files that construct `CommonScanSpec` — including `CommonScanSpec::Default` and the 15 `tests/scan_*.rs` binaries plus `micro_bench.rs` — to wrap their storage in `Inline`. [expert]
   3.2 Add `connection_name` to `ResolvedConnectionConfig` (`adapter/mod.rs:182-188`) and assign it in `resolve_connection_config` (`:203-220`) from `PROP_CATALOG_CONNECTION`.
   3.3a Add `scan_storage_for(creds, connection_name, allow_http, effective) -> ScanStorage` to `crates/lakehouse-engine/src/adapter/pushdown/support.rs` as the ONE pure variant-selection function, and call it on the single-table path: change `build_dispatch_sql`'s `storage: &StorageBackend` parameter (`crates/lakehouse-engine/src/adapter/pushdown/mod.rs:349`) to carry the selected `ScanStorage`, chosen once in `handle_pushdown` immediately after `resolver.resolve(...)` (`:248-249`) from `conn.creds.use_vended_credentials` and `conn.connection_name`, and assign it at the spec-template site (`:379`). `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs:102` and `format/delta_format_reader.rs:92-121` are UNCHANGED: each reader uses its `effective_storage` immediately for `.secret_values()`, `.file_io()`, or `read_delta_log`, so a wrapper there breaks plan-time manifest and log reading. [expert]
   3.3b Call the same function per side on the join paths: `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:603` (fact side, in `join_fan_out_scan_spec`) and `:762` (dimension side, in the `JoinSpec` literal of `build_broadcast_join_sql`), each from its own `ResolvedJoinSide::effective_storage` (`joins/planning.rs:219`) plus the one resolved connection config both sides share. Split from 3.3a so the join sides cannot be dropped — patching only `mod.rs` leaves the broadcast-join and N-scan-join paths emitting the static credential inline. [expert]
   3.4 Update `test_support::sample_storage` (`crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs:369-378`) and add a sibling helper producing the reference variant, so `dispatch_golden_tests.rs` renders the reference variant for the static case and the inline variant for the vended case. Regenerate ONLY the 18 credential-bearing fixtures under `testdata/dispatch_golden/` — `filterless_broadcast_join`, `filterless_n_scan_join`, `filterless_single_table`, `group_by_fallback`, `grouped_aggregate`, `grouped_all_agg_kinds`, `lone_count_distinct`, `multi_count_distinct_decline`, `nested_aggregate_decline`, `rendering_broadcast_join`, `rendering_n_scan_join`, `rendering_single_table`, `single_group_all_agg_kinds`, `single_group_row_scan`, `single_group_scalar_over_aggregate`, `single_group_scalar_over_aggregate_dedup`, `single_group_scalar_over_aggregate_interleaved`, `single_group_scalar_over_variance`. Assert each regenerated fixture contains `"connection":{"name":` and no sentinel credential value; assert the six `empty_*` fixtures are BYTE-IDENTICAL and unchanged. Correct the golden module's stale header, which still says "Ten committed fixtures … five non-empty and five empty".

4. **Resolve the reference in the scan UDF.**
   4.1 Add `crates/lakehouse-engine/src/scan/storage_ref.rs` with `ResolvedScanStorage` and `resolve_scan_storage` as its only constructor, resolving both sides in one step: `Connection` calls `ctx.connection(name)` then `StorageCreds::from_json(&password).backend(allow_http)`; `Inline` returns its payload. Never construct a `ConnectionCreds` here. Return `UdfError` naming the connection name and the missing access on failure — never a panic, never a fallback. [expert]
   4.2 Call `resolve_scan_storage` once in `run_scan` (`crates/lakehouse-engine/src/scan/mod.rs:227`, straight after `read_scan_spec`) and thread the resolved pair through the intermediate signatures it must cross — `run_scan_one` (`scan/mod.rs:253`), `run_scan_dispatch` (`:270`), `build_session_context` (`scan/object_store.rs:38`), and the `pub` host-test entry `run_raw_scan_with_session` (`scan/raw_scan.rs:48`, pinned by name in `crates/lakehouse-engine/src/scan_surface_probe_tests.rs:22`) — down to `scan/raw_scan.rs`, `scan/partial_agg.rs`, `scan/join_scan.rs`, `scan/positional_deletes.rs`, and `scan/deletion_vectors.rs`, in place of the spec's storage blocks. [expert]
   4.3 Move the redaction secret set onto `ResolvedScanStorage::all_secret_values`, delete `CommonScanSpec::all_secret_values` (`spec.rs:1232-1238`), and repoint ALL SEVEN feed sites: the two that read the union — `object_store.rs:66`, `join_scan.rs:48` — the three that read the fact side off the spec — `partial_agg.rs:70`, `partial_agg.rs:125`, `raw_scan.rs:54` — and the two already taking a `&StorageBackend` parameter, which now receive the resolved backend from their callers — `raw_scan.rs:224` (`register_file_list`), `positional_deletes.rs:629` (`PositionalDeleteScanTable::new`). `ScanStorage` MUST NOT expose a `secret_values()` method or any payload accessor, so a site left reading the unresolved wire value fails to COMPILE rather than yielding an empty set. [expert]
   4.4 Add unit tests for `resolve_scan_storage`: inline passthrough; connection resolution via a stub `UdfContext`; a join spec resolving two sides; an unresolvable connection erroring without fallback; the derived backend equalling `storage_block`'s output for the same password; and a `Connection`-variant spec yielding a NON-empty secret set after resolution.
   4.5 Add tests asserting that a resolved credential value is stripped from an error raised on the RAW-SCAN path and from an error raised on the PARTIAL-AGGREGATE path. Those two read the fact-side set directly, no recorded scenario covered either, and they are where a disarmed set would go unnoticed. [expert]

5. **Close the four adjacent same-class exposures.**
   5.1 Replace the derived `Debug` on `StorageProps` (`crates/lakehouse-catalog/src/creds.rs:181-182`) and on `StorageBackend` (`storage.rs:80`) with manual impls redacting `access_key`, `secret_key`, and `session_token` while keeping `endpoint`, `region`, `allow_http`, and `path_style` visible, mirroring `AdlsCred`'s existing impl; add `access_key` to the redacted set of `ConnectionCreds`' manual `Debug` (`creds.rs:61-92`). Give `StorageCreds` the same redacting manual `Debug`.
   5.2 Add the exact literal `"sas":` to `redact_credentials`' label list (`crates/lakehouse-catalog/src/redaction.rs:31-59`) — the serialized `AdlsCred::Sas` wire key. Do NOT add a bare `sas` pattern: the matcher redacts from a matched label to the next delimiter and would destroy unrelated text. A non-JSON rendering such as `Debug`'s `Sas("…")` is covered by task 5.1's manual impl, not by the label pass.
   5.3 Repoint `crates/lakehouse-catalog/src/auth.rs:93` and `:160` at `redact_error_text`, replacing the inverted composition that `redaction.rs:91-97` documents as broken for SAS tokens.
   5.4 Add `redaction_tests.rs` and `creds_tests.rs` cases for each: a `{:?}` of each type over a populated credential; a serialized SAS carrying the `"sas":` key redacted whole; a text containing the bare word `sas` in unrelated prose left INTACT; and the auth-site ordering case.

6. **Guard the guarantee.**
   6.1 Add `no_connection_credential_reaches_the_generated_sql` to `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`. Derive the builder-path list rather than asserting a count: every `RequestShape` variant (`RowScan`, `SingleGroupAgg`, `Grouped`, `GroupByWrapper`) crossed with the join, top-N, and `COUNT(DISTINCT)` sub-paths, plus the `scalar_over_agg.rs` shapes, `group_by_fallback`, `multi_count_distinct_decline`, and `nested_aggregate_decline` — cross-checked against the 18 credential-bearing fixture names. Drive each path through its own entry point, with the spec template produced by `scan_storage_for` from a `ConnectionCreds` carrying sentinel values, under BOTH settings of `use_vended_credentials`, so the assertion observes the SELECTION rather than its own fixture. Assert POSITIVELY that the connection NAME appears in the returned SQL for the static case and that the sentinel appears for the vended case, then assert the sentinel is absent for the static case — so a path whose fixture seeded nothing fails instead of passing vacuously. [expert]
   6.2 Extend `crates/lakehouse-engine/src/scan_surface_probe_tests.rs` to assert that `ScanStorage` is the declared type of both `storage` fields and that `ScanStorage` declares no `secret_values` method, so a later change reverting either field to a bare `StorageBackend`, or adding the accessor back, fails a test rather than silently restoring the leak.

7. **Deployment, harness, and documentation.**
   7.1 Emit the scan-script grant in `deploy/scripts/install.sh`'s `print_next_step_template` (`:1425-1446`) beside the `CREATE CONNECTION` and `CREATE VIRTUAL SCHEMA` statements, add a warning line that re-running the `CREATE OR REPLACE CONNECTION` DROPS the grant and requires re-issuing it, and add both assertions to `deploy/scripts/tests/install.test.sh`.
   7.2 Issue `GRANT ACCESS ON CONNECTION <c> FOR SCRIPT <schema>.LAKEHOUSE_SCAN` from the shared harness definition in `crates/lakehouse-engine/tests/common/e2e_harness.rs`, AFTER the `CREATE OR REPLACE CONNECTION` in `tests/common/stack.rs:370`, because a connection replacement drops the grant. Every E2E binary then provisions it rather than passing because its caller is `sys`.
   7.3 Add `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`: provision the least-privilege user; assert the query returns the DBA-equivalent rows; assert POSITIVELY that `EXPLAIN VIRTUAL`'s `PUSHDOWN_SQL` is non-empty and names the CONNECTION, then that it contains neither seeded credential value; enable session profiling, run the query carrying a distinctive SQL comment, `FLUSH STATISTICS` from the DBA connection, assert POSITIVELY that at least one `EXA_USER_PROFILE_LAST_DAY` row matches that comment, then that no matching row's `SQL_TEXT` contains either value; then revoke the script-scoped grant and assert the query fails with the named error carrying no credential value. Add the binary to the `test-e2e` make target, which enumerates its binaries explicitly. [expert]
   7.4 Add `docs/security.md` covering the privilege model (which grant each script needs and why the script-scoped form rather than `ACCESS ANY CONNECTION`); that the grant is per grantee; that the script-scoped grant authorises the grantee to have `LAKEHOUSE_SCAN` USE the credential on any path they name, so `EXECUTE` on `LAKEHOUSE_SCAN` is security-relevant and the CONNECTION's credential should be scoped to the warehouse prefix; that `CREATE OR REPLACE CONNECTION` drops the grants; the leak surfaces and what a `SELECT`-only user can and cannot read; the #378 residual; and the rotation runbook of § Rotation Runbook below. Link it from `docs/index.md` and cross-reference it from `docs/install.md` and `docs/catalogs.md`.
   7.5 Delete the `ponytail:` debt marker at `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:128-133` and rewrite the surrounding doc comment to state that the storage credential is referenced by connection name, citing #378 for the vended residual.

## Rotation Runbook

Content for `docs/security.md` § Rotation, delivered as documentation because the engine holds no state to migrate. The one behavioural consequence is specified in `vs-adapter/scan-spec-credential-reference`, not here.

| Fact | Basis |
|---|---|
| Rotate in place with `ALTER CONNECTION <c> TO '<uri>' USER '<u>' IDENTIFIED BY '<json>'` | Verified live on Exasol 2025.2.1: the statement succeeded against a real CONNECTION and the `ACCESS ON CONNECTION ... FOR SCRIPT` grant SURVIVED — the least-privilege user resolved the rotated connection immediately afterwards. |
| Do NOT rotate with `CREATE OR REPLACE CONNECTION` | Verified live on the same container: the replacement DROPPED the script-scoped grant, and the same user's next call failed with `insufficient privileges for using connection <c> in script <p>`, SQL state `22001`. This is the form `deploy/scripts/install.sh` and `tests/common/stack.rs:370` use for PROVISIONING, so a re-provision requires re-issuing every grant. |
| No cache to invalidate, no restart, no redeployment | UDFs are stateless and every query re-resolves the CONNECTION. |
| The secret is not recoverable from the catalog afterwards | `EXA_DBA_CONNECTIONS` exposes `CONNECTION_NAME`, `CONNECTION_STRING`, `USER_NAME`, `PUBLIC_KEY`, `CREATED`, `CONNECTION_COMMENT` and no password column — verified live, and a least-privilege user is refused the view outright with SQL state `42500`. |
| Zero-downtime rotation requires the PROVIDER to accept both secrets | Exasol holds exactly one password per CONNECTION, so there is no overlap window on the Exasol side. Register the new secret at the identity provider or cloud account first, `ALTER` the CONNECTION second, revoke the old secret third. |
| In-flight queries may straddle the switch | Each scan shard resolves the CONNECTION itself, so both values must be valid for the duration of the longest running query. An `ALTER` that changes the store `endpoint` or `region` rather than the secret should be made when no query is in flight, because a later shard would read a different store. |
| A `client_secret` rotation is narrower than a storage-credential rotation | The catalog-auth secret never crosses the UDF boundary (`vs-adapter/connection-credentials-catalog-auth`), so only the adapter reads it, once per pushdown request. An already-minted bearer token stays valid for its own lifetime — that is the provider's contract, not the engine's. |

## Test Disposition

| Test | File | Disposition |
|---|---|---|
| The 18 credential-bearing golden pushdown-SQL fixtures | `src/adapter/pushdown/testdata/dispatch_golden/*.sql` | REGENERATED (task 3.4). Only the `storage` value changes; every other byte is unchanged. Each must then contain `"connection":{"name":` and no credential value. |
| The 6 `empty_*` golden fixtures | `src/adapter/pushdown/testdata/dispatch_golden/empty_*.sql` | UNCHANGED and asserted unchanged. They carry no `storage` value, so a diff in any of them is a regression, never an expected update — the contract the golden module's own header states. |
| `common_blob_wire_is_byte_stable` | `src/scan/spec_tests.rs` | AMENDED: the pinned bytes gain the wrapper. The byte-stability PROPERTY must not weaken. |
| The 15 `tests/scan_*.rs` integration binaries plus `micro_bench.rs` | `scan_two_arg.rs`, `scan_plan_shape.rs`, `scan_join_test.rs`, `scan_deletion_vectors.rs`, `scan_positional_deletes.rs`, `scan_column_binding.rs`, `scan_partition_values.rs`, `scan_batch_loop.rs`, `scan_telemetry.rs`, `scan_parquet_pruning.rs`, `scan_no_head_test.rs`, `scan_name_mapping.rs`, `scan_agg_projection_pruning.rs`, `scan_footer_refetch_observable.rs`, `micro_bench.rs` | MECHANICAL (task 3.1): spec construction gains the `Inline` wrapper. No assertion weakens. |
| `CommonScanSpec::Default` | `src/scan/spec.rs` | MECHANICAL: its placeholder S3 `StorageBackend` gains the `Inline` wrapper. |
| The four threaded signatures | `run_scan_one` (`scan/mod.rs:253`), `run_scan_dispatch` (`:270`), `build_session_context` (`scan/object_store.rs:38`), `run_raw_scan_with_session` (`scan/raw_scan.rs:48`) | MECHANICAL (task 4.2): each gains the resolved pair. `run_raw_scan_with_session` is pinned by name in `scan_surface_probe_tests.rs:22`, so its name must not change. |
| `parse_creds` / `storage_block` / `validate_creds` unit tests | `src/adapter/connection_tests.rs` | UNCHANGED assertions. Nothing moves out of the adapter module; these are the characterization gate for task 2.2. |
| Decode-side variant-key pins (`{"azure": …}`, S3-shaped-under-`adls`) | `lakehouse-catalog` serde tests | UNCHANGED assertions. The inner backend encoding is byte-identical; only what encloses it changes. |
| `pushdown_tests.rs:55-84` (catalog-auth keys absent, `VENDED_AK`/`VENDED_TOK` present) | `src/adapter/pushdown/pushdown_tests.rs` | UNCHANGED. Vended values still travel inline under #378, so both halves still hold. |
| `secret_values_are_the_wrapped_props_secret_values`, `adls_secret_values_are_the_one_credential_and_omit_an_empty_one` | `lakehouse-catalog/src/storage_tests.rs` | UNCHANGED. `StorageBackend::secret_values()` is not edited. |
| `catalog_auth_secrets_never_in_scan_spec_with_vending` | engine-side test module | UNCHANGED assertions, and now also covered structurally by `StorageCreds`' probe. |
| `object_store_tests.rs` cases reading `all_secret_values` | `src/scan/object_store_tests.rs` | MECHANICAL: read the set from `ResolvedScanStorage`. No assertion weakens. |
| `redact_error_text_removes_a_sas_token_whole_unlike_the_inverted_order` | `lakehouse-catalog/src/redaction_tests.rs` | UNCHANGED. It documents the ordering that tasks 5.2 and 5.3 make universal. |
| The fact/dimension storage swap-detection test | `src/adapter/pushdown/joins/sql_builders_tests.rs:2069-2123` | AMENDED to compare the two sides' `ScanStorage` values. It stays the only test that sees a side swap, because both join goldens use one `sample_storage` for both sides. |
| E2E redaction tests on the failure path (`cloud_e2e_test.rs:889`, `e2e_azure_test.rs:638-684`, `e2e_lakekeeper_test.rs:557-614`, `e2e_unity_test.rs:270-312`) | `tests/` | UNCHANGED assertions. They assert on error output, which this plan strengthens rather than changes. |
| All `exasol-e2e`, Lakekeeper, Unity, and Azure suites | `tests/` | UNCHANGED assertions, plus the harness grant of task 7.2. These are the characterization gate: a credential that stops reaching storage breaks them loudly. |
| `e2e_capture_pushdown.rs` | `tests/` | UNCHANGED code; used in task 1.1 to capture the "before" artifact and again after the fix for the "after". |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Method | `crates/lakehouse-engine/src/scan/spec.rs::CommonScanSpec::all_secret_values` | The spec no longer holds the secrets; replaced by `ResolvedScanStorage::all_secret_values` |
| Selection logic | `crates/lakehouse-engine/src/adapter/connection.rs::storage_block` body (`:361-381`) | Moved into `StorageCreds::backend`; the function keeps its home and becomes a projection-and-delegate |
| Field-reading logic | `crates/lakehouse-engine/src/adapter/connection.rs::parse_creds`, its nine storage-field reads | Moved into `StorageCreds::from_json`; the function keeps its home and its catalog-auth reads |
| Comment | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs:128-133` (`ponytail:` marker) | The tradeoff it accepted is the defect this plan fixes |
| Comment | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs:1-11` header | Says "Ten committed fixtures … five non-empty and five empty"; there are 24, 18 non-empty and 6 empty |
| Derived impl | `#[derive(Debug)]` on `StorageProps` and `StorageBackend` | Replaced by redacting manual impls |

`cargo clippy --all-targets` is this workspace's lint gate, so an item left without a caller surfaces there rather than lingering.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3a, 1.4, 1.5 |
| Group B | 2.1, 2.2, 2.3, 2.4 · 5.1, 5.2, 5.3, 5.4 · 7.1 · 7.4 |
| Group C | 3.1, 3.2, 3.3a, 3.3b, 3.4 |
| Group D | 4.1, 4.2, 4.3, 4.4, 4.5 |
| Group E | 1.3b → 6.1, 6.2 · 7.2, 7.3, 7.5 |

Sequential dependencies:
- Group A → Group B (the "before" artifacts and the live contract findings are recorded first)
- Group B → Group C (the wire variant's `Connection` arm is resolved by the projection Group B adds)
- Group C → Group D (the scan resolves the variant Group C emits)
- Group D → Group E (the guards and the E2E assertions need the final wire and scan behaviour)

Intra-group order:

- **Within Group A: 1.1 · 1.2 · 1.4 · 1.5 concurrent; 1.3a first.** 1.3a is a re-confirmation of an already-passed live probe, so it is quick and it is what the rest rests on. 1.1, 1.2, 1.4, and 1.5 are independent measurements.
- **Within Group B** the four strands are file-disjoint: task 2 touches `creds.rs`, `connection.rs`, and the surface probe; task 5 touches `creds.rs`, `storage.rs`, `redaction.rs`, and `auth.rs`; task 7.1 touches `install.sh`; task 7.4 touches `docs/`. Task 2 and task 5 both edit `creds.rs`, so run 2.1 → 2.2 → 2.3 → 2.4 before 5.1. Task 7.4 depends on Group A's recorded findings for its leak-surface section and on 1.4 for its rotation section.
- **Within Group C: 3.1 → 3.2 · 3.3a · 3.3b → 3.4.** 3.1 defines the type the rest use; 3.3a and 3.3b are file-disjoint (`mod.rs` versus `joins/sql_builders.rs`) once `scan_storage_for` exists, so 3.3a lands the function first; 3.4 regenerates fixtures from the finished emitter.
- **Within Group D: 4.1 → 4.2 → 4.3 → 4.4 · 4.5.** Strictly sequential through 4.3 — each repoints what the previous added, and 4.3's non-empty-secret-set assertion is only meaningful once 4.2 has threaded the resolved pair. 4.4 and 4.5 are then independent.
- **Within Group E: 1.3b FIRST, alone.** It is the residual gate and a failure reverts Groups C and D rather than producing a finding. Then 6.1 · 6.2 and 7.2 → 7.3 → 7.5 are independent strands; 7.2 must precede 7.3 because the new binary relies on the shared grant, and 7.5 is a comment deletion that belongs after the behaviour it described is gone.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| scan-spec-credential-reference: The scan spec references the CONNECTION by name instead of carrying its credentials | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `no_connection_credential_reaches_the_generated_sql`, `dispatch_golden_matches_committed_fixtures` |
| scan-spec-credential-reference: One pure function selects the wire variant for every builder path | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `scan_storage_for_selects_reference_when_vending_is_disabled_and_inline_when_enabled`, `no_connection_credential_reaches_the_generated_sql` |
| scan-spec-credential-reference: The scan UDF resolves the referenced CONNECTION without contacting the catalog | Unit | `crates/lakehouse-engine/src/scan/storage_ref_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `connection_reference_resolves_through_ctx_connection`, `resolved_backend_equals_the_adapter_storage_block`, `unresolvable_connection_errors_without_falling_back`, `storage_creds_declares_no_catalog_auth_field` |
| scan-spec-credential-reference: Error redaction reads its secret set from the resolved credentials, not from the wire spec | Unit | `crates/lakehouse-engine/src/scan/storage_ref_tests.rs`, `crates/lakehouse-engine/src/scan/object_store_tests.rs`, `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`, `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`, `crates/lakehouse-engine/src/scan_surface_probe_tests.rs` | `connection_variant_yields_a_non_empty_secret_set_after_resolution`, `join_secret_set_is_the_union_of_both_resolved_sides`, `raw_scan_error_is_redacted_against_the_resolved_credential`, `partial_agg_error_is_redacted_against_the_resolved_credential`, `scan_storage_declares_no_secret_values_method` |
| scan-spec-credential-reference: A vended credential still travels inline and the residual is a tracked exception | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `vended_storage_is_emitted_inline_and_static_storage_is_emitted_as_a_reference` |
| scan-spec-credential-reference: The generated SQL is asserted credential-free at every builder path | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `no_connection_credential_reaches_the_generated_sql`, `dispatch_golden_matches_committed_fixtures`, `empty_fixtures_are_byte_identical` |
| scan-spec-credential-reference: The scan script requires its own script-scoped connection access grant | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs`, `deploy/scripts/tests/install.test.sh` | `least_privilege_user_scans_and_reads_no_credential`, `revoking_the_scan_script_grant_fails_the_query`, `next_step_template_emits_the_scan_script_grant_and_the_replace_warning` |
| scan-spec-credential-reference: A CONNECTION rotated while a query is in flight is observed per shard | Unit | `crates/lakehouse-engine/src/scan/storage_ref_tests.rs` | `each_resolution_reads_the_connection_value_current_at_that_moment` |
| connection-credentials: Adapter reads catalog and storage credentials from a CONNECTION object | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolved_config_carries_the_catalog_connection_name` |
| connection-credentials: One storage-credential projection and one selector serve both readers | Unit | `crates/lakehouse-catalog/src/creds_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-engine/src/scan/storage_ref_tests.rs` | `storage_creds_backend_equals_storage_block_over_parse_creds`, `storage_creds_and_backend_are_reachable_from_the_crate_root`, `resolved_backend_equals_the_adapter_storage_block` |
| connection-credentials-catalog-auth: The scan UDF reads the same CONNECTION and cannot construct a catalog-auth field | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/src/creds_tests.rs` | `storage_creds_declares_no_catalog_auth_field`, `storage_creds_from_json_over_a_client_secret_yields_no_matching_value` |
| catalog-crate-public-surface-extensions: The storage-credential projection extends the crate's public surface through an explicit reviewed edit | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `storage_creds_and_backend_are_reachable_from_the_crate_root`, `storage_creds_declares_no_catalog_auth_field`, `storage_creds_fields_are_not_public` |
| storage-backend-enum: The scan-spec wire carries the backend as a tagged variant | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs`, `crates/lakehouse-engine/src/scan_surface_probe_tests.rs` | `scan_storage_is_externally_tagged_and_rejects_a_mismatched_payload`, `common_blob_wire_is_byte_stable`, `both_storage_fields_are_declared_as_scan_storage`, `scan_storage_declares_no_secret_values_method` |
| storage-backend-enum: No storage credential type prints its payload through Debug | Unit | `crates/lakehouse-catalog/src/creds_tests.rs`, `crates/lakehouse-catalog/src/storage_tests.rs`, `crates/lakehouse-catalog/src/redaction_tests.rs` | `debug_redacts_every_storage_credential_field`, `debug_redacts_the_connection_access_key`, `redact_credentials_covers_the_serialized_sas_wire_key`, `redact_credentials_leaves_the_bare_word_sas_intact`, `auth_error_sites_apply_the_value_pass_first` |
| pushdown-planning-cloud-credentials: all six scenarios (unsigned path; static credentials; vended sole source; vended on bearer-token path; vended on OAuth2 path; vended Azure SAS; one concept-level call) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `vended_storage_is_emitted_inline_and_static_storage_is_emitted_as_a_reference`, `vended_creds_are_the_sole_storage_source_across_all_auth_modes`, `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`, `resolve_vended_storage_selects_credential_source_once_for_all_six_values`, plus existing vended coverage UNCHANGED where only the SQL-visibility clause changed |
| pushdown-planning: The common scan-spec literal carries a credential reference, not a credential | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `no_connection_credential_reaches_the_generated_sql` |
| pushdown-planning-join: Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | the amended fact/dimension swap-detection test, `no_connection_credential_reaches_the_generated_sql` |
| rest-catalog-oauth-auth: Catalog auth props are never placed in any scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | existing catalog-auth-absent assertions UNCHANGED, `storage_creds_declares_no_catalog_auth_field` |
| unity-catalog-vended-credentials: An S3 vended response terminates in an S3 storage backend; An ADLS vended response terminates in an ADLS storage backend | Unit | `crates/lakehouse-catalog/src/vended_tests.rs` | existing coverage UNCHANGED — only the returned-SQL visibility clause changed |
| delta-table-planning: Delta planning resolves its storage credential through the table's own catalog | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | existing reader coverage UNCHANGED, `vended_storage_is_emitted_inline_and_static_storage_is_emitted_as_a_reference` |
| pushdown-module-dedup-consolidation: The dispatcher builds each fan-out spec from one shared shard-invariant base | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `dispatch_golden_matches_committed_fixtures` |
| pushdown-agg-sql-consolidation: The aggregate byte-identity gate carves out the storage value and nothing else | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `dispatch_golden_matches_committed_fixtures`, `empty_fixtures_are_byte_identical`, the six existing merge assertions UNCHANGED |
| pushdown-format-neutral-resolution: Iceberg pushdown output is byte-identical across the rewiring | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `dispatch_golden_matches_committed_fixtures` |
| scan-execution-memory-and-credentials: Scan reads data files with credentials referenced or carried in the scan spec | Unit + Integration (E2E) | `crates/lakehouse-engine/src/scan/storage_ref_tests.rs`, `crates/lakehouse-engine/src/scan/object_store_tests.rs`, `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `connection_reference_resolves_through_ctx_connection`, `join_spec_resolves_two_sides_in_one_step`, `least_privilege_user_scans_and_reads_no_credential` |
| scan-execution-memory-and-credentials: Every redaction secret set in the scan path is built from the resolved backends | Unit | `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`, `crates/lakehouse-engine/src/scan/partial_agg_tests.rs`, `crates/lakehouse-engine/src/scan/object_store_tests.rs`, `crates/lakehouse-engine/src/scan_surface_probe_tests.rs` | `raw_scan_error_is_redacted_against_the_resolved_credential`, `partial_agg_error_is_redacted_against_the_resolved_credential`, `connection_variant_yields_a_non_empty_secret_set_after_resolution`, `scan_storage_declares_no_secret_values_method` |
| scan-execution-memory-and-credentials: Positional-delete files are read with the same resolved credentials | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_positional_deletes_test.rs` | existing coverage, UNCHANGED — same store, now from a resolved backend |
| scan-execution: Scan registers only its assigned files and returns matching rows | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | existing coverage, MECHANICALLY amended for the `Inline` wrapper |
| scan-execution-spec-reconstitution: Consolidating the shard-invariant fields preserves the two-argument wire | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `common_blob_wire_is_byte_stable`, `scan_storage_is_externally_tagged_and_rejects_a_mismatched_payload` |
| scan-execution-join: Scan reconstitutes a join scan spec carrying two file lists; Scan registers both tables and executes the inner equi-join | Unit + Integration | `crates/lakehouse-engine/src/scan/spec_tests.rs`, `crates/lakehouse-engine/src/scan/storage_ref_tests.rs`, `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_storage_is_a_required_key`, `join_spec_resolves_two_sides_in_one_step`, `join_secret_set_is_the_union_of_both_resolved_sides`, existing join coverage |
| scan-module-structure: Behavior is unchanged across the refactor | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `dispatch_golden_matches_committed_fixtures` |
| work-unit-sharding: The shard-invariant literal carries one credential reference for the whole fan-out | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `crates/lakehouse-engine/src/scan/spec_tests.rs` | `no_connection_credential_reaches_the_generated_sql`, `per_shard_files_argument_carries_no_storage_value` |
| The eleven sibling scoping deltas (`pushdown-planning-file-encoding`, `-topn`, `-join-fallback`, `-grouped-agg`, `-grouped-agg-wrapper-fallback`, `-count-distinct`, `-order-by-capability`, `-aggregate-extensions`, `-expression-aggregate`, `-capability-extensions`): each one scenario asserting its own shape's SQL carries a reference, not a credential | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `no_connection_credential_reaches_the_generated_sql` — its derived builder-path list covers every one of these shapes, under both settings of `use_vended_credentials` |
| pushdown-planning-empty-result: An empty-result plan emits no storage block at all | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `empty_fixtures_are_byte_identical` |
| e2e-harness: Every E2E binary provisions the scan path from one shared harness definition | Integration (E2E) | `crates/lakehouse-engine/tests/common/e2e_harness.rs` driving every `exasol-e2e` binary | the full `make test-e2e` suite; a missing or connection-dropped grant fails every binary |
| e2e-harness: A least-privilege user queries the virtual schema and recovers no credential from the plan | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `least_privilege_user_scans_and_reads_no_credential`, `revoking_the_scan_script_grant_fails_the_query` |
| lakekeeper-e2e-harness: End-to-end scan over a vended-credential warehouse; A two-table broadcast join over a vended-credential warehouse | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | existing coverage, UNCHANGED assertions plus the harness grant |
| unity-catalog-e2e-harness: The Unity Catalog E2E suite leaks no credential value | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | existing coverage, UNCHANGED assertions plus the harness grant |
| azure-e2e-harness: End-to-end scan over the vended-credential ADLS warehouse returns correct rows | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | existing coverage, UNCHANGED assertions plus the harness grant |

`resolve_scan_storage`, `scan_storage_for`, the wire encoding, `StorageCreds::backend`, and the redaction primitives are pure computation over strings and typed values, so unit tests in the sibling `_tests.rs` files are the correct form. The credential-exposure claim itself is only observable through Exasol's own plan and profiling surfaces, so its home is an E2E binary against the Docker stack — a unit test cannot see `EXPLAIN VIRTUAL`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Reproduce issue #135 before the fix (task 1.1) | `CAPTURE_SQL=1 cargo test --features exasol-e2e --test e2e_capture_pushdown -- --nocapture` | The printed pushdown SQL contains `"access_key":"minioadmin","secret_key":"minioadmin"` — the "before" artifact |
| Same command after the fix | `CAPTURE_SQL=1 cargo test --features exasol-e2e --test e2e_capture_pushdown -- --nocapture` | The printed SQL contains `"storage":{"connection":{"name":…}}` and neither credential value |
| Gate 1.3a: script-scoped connection resolution with a runtime-argument name, no engine change | `exapump sql --dsn "exasol://sys:exasol@127.0.0.1:8563?validateservercertificate=0" -` fed the probe DDL, then `exapump sql --dsn "exasol://<u>:<pw>@127.0.0.1:8563?validateservercertificate=0" "SELECT <schema>.CONN_PROBE('<c>')"` | `OK addr=… user=… pwlen=<n>` while the grant is held; after the revoke, `insufficient privileges for using connection <c> in script CONN_PROBE`, SQL state `22001` |
| Gate 1.4: connection replacement versus in-place rotation | as above, re-running the least-privilege probe after `CREATE OR REPLACE CONNECTION` and again after `ALTER CONNECTION … IDENTIFIED BY` | the replacement makes the probe FAIL with SQL state `22001`; the `ALTER` leaves it succeeding with the rotated address |
| Gate 1.3b: resolution from the real scan UDF through rewritten pushdown SQL | `cargo test --features exasol-e2e --test e2e_credential_exposure_test -- --nocapture --test-threads=1` | The least-privilege user's query returns the seeded rows; after the revoke, it fails with the connection-access error and no credential value |
| Profiling surface is populated before it is asserted empty (task 1.2) | `ALTER SESSION SET PROFILE = 'ON'`, the marked query, `FLUSH STATISTICS` as `sys`, then `SELECT PART_NAME, SQL_TEXT FROM EXA_USER_PROFILE_LAST_DAY WHERE SQL_TEXT LIKE '%<marker>%'` as the least-privilege user | Rows exist, one per execution-graph part, each carrying the full `SQL_TEXT`; without the flush the same query returns zero rows |
| Adapter + scan + catalog units | `cargo test` | 0 failures; the 18 regenerated golden fixtures match and none contains a credential value; the 6 `empty_*` fixtures are byte-identical |
| Full E2E suite (characterization gate) | `make test-e2e` | 0 failures; every binary provisions the scan-script grant after its connection and still reads its rows |
| Vended path still reads (residual #378) | `make test-e2e-lakekeeper` | 0 failures; the vended-warehouse scan returns the seeded rows with the credential still inline |
| Installer template carries the grant and the warning | `bash deploy/scripts/tests/install.test.sh` | 0 failures; the next-step template output contains `GRANT ACCESS ON CONNECTION` … `FOR SCRIPT` … `LAKEHOUSE_SCAN` and the `CREATE OR REPLACE` warning line |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Test (E2E, vended) | `make test-e2e-lakekeeper` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
