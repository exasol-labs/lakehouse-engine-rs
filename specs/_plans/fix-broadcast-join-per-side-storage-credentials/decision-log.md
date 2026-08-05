# Decision Log: fix-broadcast-join-per-side-storage-credentials

## Interview

**Q:** Reproducing this needs two tables in ONE warehouse (one VS) that resolve to credentials which genuinely cannot read each other's files. How should the plan get there?
**A:** Investigate-first task, with a declared fallback. The first plan task empirically determines whether Lakekeeper's vended `loadTable` narrows the STS session policy to the table prefix and whether MinIO honours it. If yes, a two-table vended broadcast-join E2E reproduces the bug with no new infrastructure.

**Q:** If the investigation shows no MinIO/Lakekeeper fixture can make the bug fail as a read error, what is the accepted proof?
**A:** Escalate back to me. Do NOT pick a fallback proof strategy yourself. If the investigation establishes that no CI-runnable fixture can make this fail as a genuine read error, the plan must say so and escalate the question of an acceptable substitute proof, rather than committing to unit-level-only proof or new stub-catalog scaffolding.

**Q:** When should the prefix-routing store be used?
**A:** Always, whenever a join block is present. One code path. Even when both sides' credentials are identical, both sides get their own inner store behind the router. The common same-warehouse case changes shape but not behaviour.

**Q:** A requested path may match neither side's table root (Iceberg metadata, manifests, or delete files can sit outside the data prefix). What should the router do?
**A:** Fail with a clear error. An unroutable path is treated as a planning defect and surfaced, rather than silently using a credential that may not be scoped for it. The error message must name the unroutable path and the roots that were tried. Accepted risk: if any real access legitimately falls outside both table roots, this converts a currently-working read into an error — so the spec must enumerate which access paths the scan actually issues (data files, positional-delete files for BOTH sides, anything else) and confirm each falls under one of the two roots.

**Correction made during the interview:** an earlier framing proposed a two-WAREHOUSE fixture. That is untestable — two warehouses mean two virtual schemas, two adapters, and Exasol never hands either adapter a join to push down. The fixture must be two tables in ONE warehouse.

**Refinement on what "divergent credentials" must mean:** the two sides' credential VALUES already differ today, because `resolve_vended_storage` runs per side and each side gets its own STS session with a distinct key/secret/token. That value divergence is enough to test the CARRIAGE fix — that the fan-out spec carries each side's own storage. It is NOT enough to reproduce the DEFECT: if both sessions grant the same whole-bucket access, reading the dimension through the fact's credential simply succeeds. A failing repro needs credentials whose SCOPE differs, so the fact's credential is genuinely DENIED on the dimension's prefix. The investigate-first task's question is therefore "does the credential SCOPE diverge per table?", not "do the credential VALUES differ?".

## Design Decisions

### [1] Route by path inside one registered store per bucket, not by a custom object-store registry

- **Decision:** Register one `PrefixRoutingObjectStore` per bucket, holding one inner store per join side, each built from that side's own storage backend. Every `ObjectStore` trait call carrying a `Path` routes to the owning side's inner store.
- **Alternatives:** (a) A custom DataFusion `ObjectStoreRegistry` keyed with userinfo — rejected because the registry never receives a path (verified: `get_url_key` keys on `scheme://host[:port]` at `datafusion-execution-54.1.0/src/object_store.rs:266-274`, and `ObjectStoreUrl::parse` rejects any URL with a path at `:58-72`), so it cannot separate two S3 tables in one bucket, which is the Databricks-normal case. `object_store` 0.13.2 does ship a userinfo-retaining, prefix-matching registry (`registry.rs:220-223`) that DataFusion 54.1.0 does not use — noted as the only route that would also subsume the ADLS-container guard, and rejected as a strictly larger change against a pluggable DataFusion seam for a case both existing guards already refuse loudly. (b) Refuse credential-divergent joins — rejected: forfeits broadcast for the common Databricks case. (c) Fall back to the N-scan renderer — rejected for the same reason.
- **Rationale:** Object-store trait methods DO receive the full path, so the store layer is the only place the two sides can be told apart. One bucket therefore keeps exactly one registered store, satisfying DataFusion, while two credentials live behind it.
- **Promotes to ADR:** yes

### [2] Route on each side's enumerated file paths first, table root only as a fallback

- **Decision:** The router's per-side routing table is the SET of that side's own object-store paths — every data file and every one of its positional-delete files — matched exactly first. Only a path no side enumerated falls through to a longest-table-root-prefix match. Anything matching neither is an error.
- **Alternatives:** Longest table-root prefix match ONLY, as originally prescribed in the planning brief — rejected on Iceberg-spec grounds.
- **Rationale:** The Apache Iceberg table spec explicitly permits a table's files outside its `location`. The NORMATIVE support is the Appendix E → Version 4 rule "Absolute paths must be used for files that do not share a common prefix with the table location", which alone carries the conclusion. Two normative definitions agree: `location` is a writer target ("This is used by writers to determine where to store data files, manifest files, and table metadata files") and `data_file.file_path` is "Full URI for the file with FS scheme" with no containment clause. `write.data.path` ("If … an absolute path, it is used directly as the base for new data files") is CORROBORATION only — it sits in Appendix F, which is titled *Implementation Notes* and is explicitly non-normative ("This section covers topics not required by the specification but recommendations for systems implementing the Iceberg specification"); an earlier draft of this entry and of the `scan-execution-join` delta mislabelled it normative. Root-only routing would therefore misroute a spec-legal table. Because the scan discovers no files, each side's spec names every path it will request, so exact membership is complete for every access the scan issues. This keeps the interviewed "fail on an unroutable path" decision while removing the Iceberg deviation it would otherwise have carried.
- **Rule 2's reachability, recorded rather than glossed:** the root-prefix fallback is UNREACHABLE through the current scan on a well-formed spec. Verification (`plan.md` § Iceberg table-spec compliance ¶1) shows every access carries a path the side's spec enumerates exactly — including the schema-inference `list(Some(prefix))`, whose prefix is the first data file's own path on the `head`-returns-`NotFound` trigger. The one syntactic route to rule 2 is a `files` entry whose path ends in `/`, which makes `is_collection()` list a directory — itself a malformed spec. Rule 2 is therefore retained as the interviewed prescription's fallback and is labelled as such; it is NOT cited as evidence that the enumeration is complete. This is the resolution of the completeness-vs-fallback contradiction the round-1 review flagged: the completeness claim is scoped to rule 1, and rule 2 is honestly named unreachable rather than quietly implied to be load-bearing.
- **Promotes to ADR:** yes

### [3] Layer the router OUTSIDE the spec-sized store, one size index per side

- **Decision:** `Router[ SpecSized(inner_fact, sizes_fact), SpecSized(inner_dim, sizes_dim) ]`. The whole-spec size index is replaced by one index per side.
- **Alternatives:** `SpecSized( Router[ inner_fact, inner_dim ], sizes_whole_spec )` — one sized layer outside a router, keeping the existing whole-spec index.
- **Rationale:** Routing must happen before the sized-`head` shortcut, or an unroutable `head` would be answered from the index and the routing failure would surface later, at the range read, as a credential-shaped access denial instead of the plan defect it is. Per-side indexes also restore information hiding: the index was whole-spec ONLY because one store had to answer both sides' `head` calls, which is the premise this change removes, so one side's store can no longer satisfy the other side's metadata lookup.
- **Promotes to ADR:** yes

### [4] `JoinSpec::storage` is a REQUIRED wire field with no serde default

- **Decision:** Add `pub storage: StorageBackend` as the last field of `JoinSpec`, with neither `#[serde(default)]` nor `skip_serializing_if`.
- **Alternatives:** `Option<StorageBackend>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`, matching the neighbouring fields' convention, falling back to the whole-spec backend when absent.
- **Rationale:** The neighbouring optional fields are optional in MEANING; this one must not be. A default would let a storage-less join block deserialize into one that silently reuses the fact-side backend — the exact collapse being removed. Requiring the field makes "every join block names its own dimension backend" a compile-time property at all seven construction sites instead of a rule to audit. `datafusion-scan/scan-execution-spec-reconstitution` already records that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)", so a required field costs no compatibility.
- **Promotes to ADR:** no

### [5] `validate_sides_share_one_store` STAYS; prefix routing does not subsume it

- **Decision:** Keep the scan-time guard that refuses a spec whose sides collapse onto one DataFusion registry key while needing different stores. Rewrite only its doc comment.
- **Alternatives:** Delete it as subsumed by the new router.
- **Rationale:** Verified, not assumed. DataFusion's `get_url_key` slices `[Position::BeforeHost..Position::AfterPort]`, dropping userinfo — its own test asserts `s3://username:password@host:123` keys as `s3://host:123` (`datafusion-execution-54.1.0/src/object_store.rs:330-332`). On `abfss://` that userinfo IS the container. An Azure store built from a container-qualified URL is container-scoped, and the `object_store::Path` its trait methods receive is container-RELATIVE, so two tables in two containers of one storage account can produce IDENTICAL paths. Path routing cannot distinguish them, so the guard is the only safe reading of such a spec.
- **Promotes to ADR:** yes

### [6] `validate_sides_share_one_backend` keeps its variant + ADLS-account scope, for a new reason

- **Decision:** Keep the plan-time comparison scoped to the backend variant and, for ADLS, `account_name`. Remove the "(tracked separately as `#294`)" justification and replace it.
- **Alternatives:** Widen to full backend equality; or delete the guard.
- **Rationale:** The old justification was that a credential difference was a deferred defect. It is now SERVED, so widening would reject exactly what this plan fixes. The narrow scope survives on a different footing: a variant difference is unserveable (an `AmazonS3Builder` cannot address an `abfss://` URI) and an ADLS account difference is unserveable per decision [5]. This supersedes ADR 053's "Guard on full backend equality (including per-prefix vended credentials) — ✗ Rejected … the pre-existing per-prefix credential collapse is named as a separate, pre-existing defect".
- **Supersedes:** A join whose sides resolve to different storage backends is rejected at plan time (ADR from plan `change-vended-storage-resolution-scheme-driven`) — only its per-prefix-collapse consequence, not its guard.
- **Promotes to ADR:** yes

### [7] The connection budget applies per inner store, so a join can hold 2N warm sockets

- **Decision:** Each side's inner store receives the full `s3_max_connections` budget N. A two-side join therefore holds up to 2N warm idle connections per host.
- **Alternatives:** Divide the budget across the sides present (`N / side_count`, clamped to ≥1) so the per-host total stays N.
- **Rationale:** Dividing would silently halve fact-side fetch parallelism on every broadcast join, making a tuning knob's effective value depend on whether the query happens to be a join — a data-dependent performance regression. What doubles is `pool_max_idle_per_host`, which bounds warm reusable sockets rather than in-flight requests (`object_store` 0.13.2 exposes no hard concurrency ceiling), and a different-bucket join already held 2N before this change.
- **Promotes to ADR:** no

### [8] The dimension side's table registration receives its own storage, closing a redaction defect found during planning

- **Decision:** `register_join_tables` passes `&join.storage` to the dimension side's `register_file_list`, not `&spec.common.storage`.
- **Alternatives:** Leave it, since the store is already routed and the argument is only used for `secret_values()`.
- **Rationale:** `PositionalDeleteScanTable` keeps that backend's `secret_values()` as its redaction set, so today a dimension-side read error is redacted against the FACT side's secrets — the dimension credential could survive into a surfaced message. This is the same root cause as issue #294 (one whole-spec storage value serving both sides) and is fixed in the same change rather than left as a second silent gap.
- **Promotes to ADR:** no

### [9] The routing store is a new submodule, not an addition to `scan/object_store.rs`

- **Decision:** Add `crates/lakehouse-engine/src/scan/store_router.rs` with its own `#[cfg(test)] mod tests`.
- **Alternatives:** Add the decorator beside `SpecSizedObjectStore` in `scan/object_store.rs`.
- **Rationale:** `object_store.rs` is already 1,265 lines, and the open file-size guardrail (issue #129) counts against growing it. The router owns exactly one decision — which side owns a path — which is a different decision from `object_store.rs`'s "how is a store for this backend built and registered". `datafusion-scan/scan-module-structure` records that the submodule list is a plan-level design decision rather than a normative contract, so no spec amendment is needed; its per-submodule test rule is satisfied by the new module's own test module.
- **Promotes to ADR:** no

### [10] The multi-bucket-per-side refusal is named as a tracked exception, not fixed here

- **Decision:** Name `validate_uniform_object_store_files`' refusal of a side whose own files span more than one bucket as a deliberate, accurately-scoped, LOUD deviation from the Iceberg table spec, file it as a GitHub issue during implementation, and cite that number inline in the `datafusion-scan/scan-execution-join` Background.
- **Alternatives:** (a) Fix it in this plan by registering one store per bucket per SIDE — rejected as a strictly larger change orthogonal to the credential collapse, requiring a per-side multi-store model rather than a per-join one. (b) Leave it unstated — rejected: the project rule forbids a silent Iceberg-spec gap in any plan touching scanning or pushdown.
- **Rationale:** The Iceberg v4 rule "Absolute paths must be used for files that do not share a common prefix with the table location" makes cross-bucket files spec-legal, so refusing them is a real deviation. It predates this plan on both the single-table and the join path, produces a clear error rather than a wrong read, and this plan is the first to quote the spec section that exposes it — which is why it is named here rather than in a future plan.
- **Promotes to ADR:** no

### [11] The reproduction gate is a hard stop, not a fallback ladder

- **Decision:** Task 1.3 stops the plan if the investigation shows the two vended credentials do not diverge in SCOPE, and escalates the substitute-proof question to the user rather than choosing one.
- **Alternatives:** Pre-declare a fallback — unit-level-only proof of carriage, or a stub catalog vending two prefix-scoped credentials.
- **Rationale:** The interview decided this explicitly. A passing join test over two whole-bucket credentials would evidence nothing about the defect: reading the dimension through the fact's credential simply succeeds, so the test would be green both before and after the fix. Silently substituting the carriage assertion for the defect reproduction would ship a fix whose proof cannot fail.
- **Promotes to ADR:** yes

### [12] The two lakekeeper scenarios are CONTINGENT on the task 1.2 gate's outcome

- **Decision:** `e2e-harness/lakekeeper-e2e-harness`'s two DELTA:NEW scenarios, the two `e2e_lakekeeper_test.rs` rows of `plan.md` § Verification § Scenario Coverage, and the first § Manual Testing row are all written for the DENIED outcome. They are contingent artifacts, not settled requirements. If task 1.2 observes an ALLOWED cross-table read, those four artifacts return to `/speq:plan` revision rather than standing as recorded requirements the suite can only ever fail.
- **Alternatives:** (a) Author the scenarios conditionally, hedging both outcomes — rejected: a scenario that describes two mutually exclusive outcomes is not verifiable, and RFC 2119 obligations cannot be written against an unresolved empirical question. (b) Defer authoring them until after 1.2 runs — rejected: the plan must state the intended coverage for review, and the gate's job is to confirm it, not to discover it.
- **Rationale:** The plan concedes the gate may fail: the harness's vended MinIO user has a BUCKET-scoped IAM policy, and whether Lakekeeper additionally sends an inline STS session policy narrowing it to the table prefix is unverified. Recording the contingency keeps a failed gate from silently merging a permanently-unsatisfiable normative requirement into the library, which is the specific risk the round-1 review raised. Task 1.3 names the four artifacts explicitly so the revision has no discovery step.
- **Promotes to ADR:** no

### [13] A live Databricks vended broadcast-join E2E is deferred

- **Decision:** Ship no live Databricks two-table vended broadcast-join E2E in this plan. The Lakekeeper `sts-enabled` warehouse stands in for it. Named explicitly in `plan.md` § Non-Goals rather than left absent.
- **Alternatives:** (a) Add the live Databricks E2E now — rejected: no Databricks catalog is reachable from CI, so the test would be permanently env-gated and unrun, following the same constraint that already gates `cloud_e2e_test.rs`. (b) Leave it unmentioned — rejected: Databricks-managed Iceberg is the MOTIVATING system for this fix (§ Context: "Databricks vends a credential scoped to the table it loaded"), so its absence from the proof set is a scope boundary a reader must be told about, not an oversight to discover.
- **Rationale:** The user's verification list ended with "Ideally add a live Databricks two-table vended broadcast join E2E." "Ideally" makes it deferrable, not undocumented. What stands in for it: Lakekeeper's `sts-enabled` warehouse vends per-table credentials under the same Iceberg REST `StorageCredential.prefix` contract Databricks uses, so the mechanism under test is identical even though the vendor is not. The residual risk is Databricks-specific vending behaviour that Lakekeeper does not reproduce — accepted, and recorded here so a future Databricks failure is triaged against a known gap rather than a surprise.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The scan's object-store access set was enumerated wrongly in both directions

- **Finding:** `scan-execution-join` Background bullet 5 and `plan.md` § Iceberg table-spec compliance ¶1 asserted "one sized `head` per data file … No other path is requested". Both halves were false, and the interview had made the fail-on-unroutable decision conditional on exactly this enumeration being accurate. The production path issues ZERO per-data-file heads (`ObjectMeta` is synthesized from spec sizes, `scan/positional_deletes.rs:663-665`); exactly ONE head exists, on the first file only, in the schema-inference branch (`scan/raw_scan.rs:204-217`). And `ObjectStore::list` IS reachable: `list_prefixed_files` falls through to `store.list(Some(&full_prefix))` on `NotFound` and skips the head entirely when the path ends with `/` (`datafusion-datasource-54.1.0/src/url.rs:266-294`, `:397-398`), which task 3.2 left unspecified by ruling only on `list(None)`.
- **Direction change:** Both passages now state the VERIFIED access set, re-derived against the tree rather than recalled; `plan.md` carries it as an evidence table with a routing column per access. Task 3.2 gains an explicit rule that `list`/`list_with_delimiter` with `Some(prefix)` route by the same two-step rule as any other path, and task 3.3 gains tests for `list(Some(exact_file_path))` and `list(Some(table_root))`. The `scan-execution-join` routing scenario gains a normative clause that a prefixed listing routes by that same rule. No design change was needed — both reachable list prefixes were already routable — but the plan no longer certifies a completeness property it had not established, and no longer records a false statement about the engine's head behaviour.
- **Promotes to ADR:** no

### [plan-review] A failed reproduction gate had no route for the artifacts that assume it passes

- **Finding:** the plan concedes the gate may fail (the harness's vended MinIO user has a BUCKET-scoped IAM policy, and the inline-STS-session-policy question is unverified), yet all seven deltas and both verification tables were authored as if it passes. `lakekeeper-e2e-harness/spec.md` normatively records a DENIED cross-table read as the load-bearing property. On an ALLOWED read that scenario becomes a permanently-recorded requirement the suite can only ever fail, and task 1.3 offered no route beyond "record and escalate".
- **Direction change:** task 1.3 now states that an ALLOWED cross-table read returns the plan to `/speq:plan` revision, and names the four contingent artifacts in a table so the revision has no discovery step: both `lakekeeper-e2e-harness` DELTA:NEW scenarios, the two `e2e_lakekeeper_test.rs` § Scenario Coverage rows, and the first § Manual Testing row. Decision `[12]` records the contingency.
- **Promotes to ADR:** no

### [plan-review] Three passages gave three different answers on whether escalation unblocks the fix work

- **Finding:** the one question deciding whether 21 of 22 tasks run had three conflicting answers. The § Implementation Tasks preamble said tasks 2 onward wait until the gate is established "or the plan has escalated" — escalating unblocks. Task 1.3 said "STOP" — escalating blocks. § Parallelization said "No fix work starts before the gate resolves" — undefined whether escalation resolves it.
- **Direction change:** one rule now stated identically in all three places — an ALLOWED cross-table read HALTS every task in groups B through F pending the user's answer on substitute proof, and escalating alone does not unblock them. "or the plan has escalated" is deleted from the preamble; § Parallelization now says the gate is passed only by a DENIED read, never by having asked the question.
- **Promotes to ADR:** no

### [plan-review] The prescribed `build_side_store` signature could not satisfy the prescribed union redaction

- **Finding:** § Migration fixed the signature as `build_side_store(&ScanSide, budget)` and § Patterns defines `ScanSide` as holding ONE side's backend, while task 4.2 required union redaction over every side's `secret_values()` inside that same function. A function holding one side's backend structurally cannot redact against the other side's secrets. Two deltas make union redaction normative, so an implementer following § Migration would ship fact-only redaction and reintroduce a variant of the leak decision `[8]` exists to close.
- **Direction change:** the redaction set is now passed explicitly — `build_side_store(&ScanSide, budget, all_secrets: &[String])`. Task 4.1 computes the union of every present side's `secret_values()` once in `build_session_context` and passes it to each call; task 4.2 points the S3 and ADLS redaction at that parameter rather than at the side's own backend. Both the § Migration row and task 4.2 state that ownership of the union sits with the caller because only it knows every side.
- **Promotes to ADR:** no

### [plan-review] The plan's central property — per-side credential provenance — had no falsifiable test

- **Finding:** the one thing issue #294 is about (the dimension store built from `join.storage`, not `common.storage`) was asserted nowhere host-side. Its mapped test, task 5.2's `each_join_side_reads_through_its_own_store`, hand-builds the router with a failing stub — proving routing, which task 3.3 already proves, and proving nothing about which backend each inner store came from. Task 5.1's assertions are store counts and index contents, also silent on provenance. The only end-to-end proof sat behind a gate the plan expects to fail. Task 5.2 additionally could not be built as described: `build_session_context` cannot register a `file://` store (its S3 arm requires `store_url.host_str()`), DataFusion pre-registers an unwrapped `LocalFileSystem` under `file://`, and `scan_join_test.rs` deliberately injects its own session — so a `file://` test exercises no production wiring.
- **Direction change:** new task 5.6 asserts provenance THROUGH `build_session_context` itself — a same-bucket join spec whose two `StorageBackend::S3` values differ only in `endpoint`, each endpoint bound to its own loopback listener (the `file_resolution.rs` loopback-fake precedent), asserting each side's request reaches that side's listener and failing if either store carries the other's configuration. The assertion is on the endpoint reached, not on `Display`, because `AmazonS3`'s `Display` prints only the bucket (`object_store-0.13.2/src/aws/mod.rs:88-92`) and both sides share one bucket by construction. Task 5.2 is restated as a router-level test only, with its "falsifiable gate" claim and its three infeasibility reasons both recorded. The § Scenario Coverage row now names both tests and labels which is the gate. § Parallelization sequences 5.6 after 5.1, since both edit `object_store.rs`'s test module.
- **Promotes to ADR:** no

### [plan-review] The round-1 fix certified a zero-head regression test that does not exist

- **Finding:** the evidence table added for round-1's access-set blocker attributed row 1 ("**Zero** `head` per data file, production path") to `tests/scan_no_head_test.rs` (`scan_uses_spec_size_and_issues_no_head`, `scan_issues_no_head_for_delete_files`), and `scan-execution-join` Background bullet 5 recorded the same as "a property regression-tested by `tests/scan_no_head_test.rs`". Neither test covers that path and the first asserts the opposite of row 1's "no store call is made": the suite's only spec builder, `raw_spec` (`tests/scan_no_head_test.rs:261-272`), closes with `..Default::default()` and `CommonScanSpec::logical_schema` (`scan/spec.rs:703`) defaults empty via that struct's `Default` impl (`:798`), so all three tests run row 2's schema-inference branch, and `scan_uses_spec_size_and_issues_no_head` (`:504`) asserts `discover_counts.forwarded_to_inner() >= 1` (`:526-530`) and `spec_counts.served_from_spec() >= 1` (`:550-554`). The suite proves a `head` is answered from the size index instead of the network — not that none is issued. Two smaller inaccuracies rode along: row 3 cited `positional_deletes.rs:670` as a general footer read when it sits inside the `!deletes.is_empty()` guard (`:667-669`), and `ensure_positional_delete` ends at `:247`, not `:246`.
- **Direction change:** the false attribution is deleted from the delta, so it never reaches the permanent library. Row 1's Evidence is now code-inspection only — `positional_deletes.rs:663-665` via `object_meta_for` (`:215-225`), plus `raw_scan.rs:214` being the one `infer_schema` call site in `crates/…/src` and skipped on this path — and states outright that **no test currently guards the production path's zero-head property**. All three `scan_no_head_test.rs` names moved to row 2, which now records what they actually prove: this branch's single `head` is answered from that side's own size index by `SpecSizedObjectStore::get_opts`' `options.head` short-circuit (`scan/object_store.rs:350-366`), which is why the listing fallback's `NotFound` trigger cannot fire for a spec-indexed file. Row 3 now scopes `:670` to delete-carrying data files and the Deletion Vectors row cites `:232-247`. No routing conclusion changed — the access set and both rules still hold — but the plan no longer certifies a guard that does not exist. The same false clause was also deleted from the round-1 finding entry above, which had recorded it.
- **Promotes to ADR:** no

### [plan-review] Five named tests, including the only proof of union redaction, had no producing task

- **Finding:** five of the fourteen tests § Verification names existed in no task. The worst was `unreadable_join_file_error_redacts_both_sides_credentials`, the sole mapped proof of the union-redaction requirement two deltas make normative ("from EITHER side's storage backend"; "every side's `secret_values()`, not the fact side's alone"). The nearest existing test, `join_unreadable_file_errors_without_secrets` (`tests/scan_join_test.rs:477`), builds both sides from one `storage()` helper (`:166`) and asserts one literal (`:507`), so it stays green under fact-only redaction — round-1's interface fix made union redaction possible without making it falsifiable. Also unproduced: `join_registers_each_side_against_its_own_backend` (nearest existing `join_executes_inner_equi`, `:283`); `each_side_store_gets_the_full_connection_budget`, absent from task 5.1's assertions while the existing `build_s3_store_applies_spec_connection_budget` (`:682`) is whole-spec; `broadcast_carries_each_sides_own_storage`, where task 5.4 named only the three goldens; and `adls_sides_on_two_accounts_are_rejected`, a name no test in the tree carries.
- **Direction change:** new tasks 5.7 and 5.8 produce the two `scan_join_test.rs` tests — 5.7 renames the redaction test, adds a `dim_storage()` helper with a second secret literal to the shared `join_spec` helper (`:192`) so the whole file runs credential-divergent, and asserts both literals absent; 5.8 renames `join_executes_inner_equi` and keeps its row assertions as the characterization half. Task 5.7 also states what its assertions do NOT prove: on a local-file session neither literal can appear in the message, so they guard a future regression and cover both sides' redaction sets rather than positively demonstrating a redaction. Task 5.1 gains `each_side_store_gets_the_full_connection_budget` with the budget read from `spec.common.s3_max_connections` and asserted undivided per side, plus the note that a built `AmazonS3` does not expose its pool config back. Task 5.4 gains `broadcast_carries_each_sides_own_storage` as a NEW test that parses the emitted blob and pins which side each backend came from, which a golden cannot. § Test Disposition gains four rows (one REPLACE, three RESTATE) and § Parallelization sequences 5.7 → 5.8 → 5.2 as one file.
- **ADLS name reconciled toward the tree:** § Verification now names the real `adls_sides_on_different_storage_accounts_are_rejected` (`joins/planning.rs:920`) instead of the invented short form. The KEEP disposition is the correct one: no task in this plan renames that test, its assertions do not change, and only the guard doc above it is rewritten (task 5.5) — so a rename would be churn with no producing task.
- **Promotes to ADR:** no

### Task 1.4 reproduction — UNEXPECTED PASS, halted per instructions

**Status:** Sub-step (a), the promotion of the join helpers to `crates/lakehouse-engine/tests/common/e2e_harness.rs` (made `pub`), is done and builds clean under `exasol-e2e`, `lakekeeper-e2e`, and `azure-e2e`. Sub-step (b), the new test `lakekeeper_vended_broadcast_join_result_correct` in `e2e_lakekeeper_test.rs`, is written and builds clean, but **it PASSED against the live Docker stack** where the task requires it to FAIL. Per the task's own instruction ("If it unexpectedly passes, stop and report that clearly rather than trying to fix it"), work stopped here. Task 1.4 is NOT marked complete; tasks.md still reads `[~]`.

**What was run:**
```
cargo test --test e2e_lakekeeper_test --features lakekeeper-e2e \
  lakekeeper_vended_broadcast_join_result_correct -- --nocapture --test-threads=1
```
Result: `test lakekeeper_vended_broadcast_join_result_correct ... ok` — all four assertions passed
(`has_broadcast_join_block`, `!has_two_scan_wrapper`, 6 rows, `actual == expected`). A broadcast
join genuinely was chosen (the two structural assertions on `EXPLAIN VIRTUAL` output passed), and
the row-level read of `dim_customer`'s file — embedded via the join block and read with
`fact_orders`' registered store — succeeded and returned correct data.

**Why this is a genuine surprise, not a weak reproduction attempt:**
- Task 1.2/1.3's own gate test (`lakekeeper_vended_credentials_are_scoped_per_table`), re-run
  immediately before this one against the same live stack, still confirms per-table SCOPE
  divergence: `fact_orders` and `dim_customer` resolve to different STS sessions
  (`same vended access key: false`), and a raw cross-table `GetObject` — `fact_orders`' credential
  against `dim_customer`'s own data file — is DENIED with a `403 AccessDenied` from MinIO.
- Both tables live in the SAME bucket (`s3://warehouse/...`), different key prefixes
  (`.../019fd0f2-d9a2.../` vs `.../019fd0f2-d94d.../`).
- Current source (`crates/lakehouse-engine/src/scan/object_store.rs::register_side_store` /
  `build_session_context`, unchanged by this plan so far) registers exactly ONE `AmazonS3` store
  per distinct bucket-scoped `store_url`; since both sides share one bucket, the dimension side's
  registration call is a no-op (`Ok(None)`, key already held) and the fact side's ALREADY-registered
  store — built from `spec.common.storage`, which `build_broadcast_join_sql`'s own doc comment
  states is "the fact side's effective storage" — serves both sides' reads.
  `SpecSizedObjectStore` only intercepts `head`; real data `get`s fall through to that same inner
  `AmazonS3` client unchanged (`object_store.rs:296-311`).
- So the production scan path, as currently wired, should issue the exact cross-credential
  `GetObject` the raw probe proved is denied — yet it read `dim_customer`'s file successfully.

**Not investigated further, per the task's stop instruction:** whether the STS session
`register_side_store` actually receives for the fact side in the full VS→UDF pushdown path differs
in scope from the one `probe_vended_credential`'s standalone `CatalogSession::resolve` +
`load_table_any_auth` call obtains (e.g. a broader grant, a caching/reuse behavior in Lakekeeper's
vended-credential issuance, or a session-token TTL/refresh difference) — any of these would explain
a wider-than-expected fact-side credential without contradicting the isolated probe. Confirming this
would need `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` debug tracing inside the live UDF (viable here
per the project's live-debugging notes: this is a single-leg join, which stays stable under debug
tracing, unlike multi-leg joins) or comparing the two credential-resolution call sites directly.
Deferred to the user/orchestrator to decide the next step rather than chased further under this task.

**Files touched for sub-step (a) (kept, not reverted):** the twelve helpers listed in the task
(`ORDERDATE_LOWER_BOUND` through `expected_join_rows_with_fact_where`) moved from
`e2e_join_test.rs` to `e2e_harness.rs` as `pub` items; `e2e_join_test.rs` lost only those
definitions (plus its now-unused `E2E_DIM_TABLE`/`E2E_FACT_TABLE` import) and is otherwise
unchanged; `e2e_lakekeeper_test.rs` gained the six new names in its import list plus the new test
function. All three feature-gated builds (`exasol-e2e`, `lakekeeper-e2e`, `azure-e2e`) and clippy
(`-D warnings`) are clean; `cargo fmt --all -- --check` is clean.

### [plan-review] Tasks 1.1 and 1.4 invited duplication of harness code that already exists

- **Finding:** round 1 raised an `[EFFORT_MISESTIMATION]` advisory that tasks 1.1 and 1.4 understate their work, and it was deferred. Re-checked against the tree, the effort estimate was right and the WORDING was the defect: both tasks described the work in terms that lead an implementer to write new code beside existing reusable code. Task 1.1 said to "add `seed_star_schema_with_auth(...)` alongside `seed_star_schema` (`:1053`)" and that "the ONLY difference is calling `build_seed_catalog_with_auth`" — but `seed_star_schema` is not parameterized on auth, so following that sentence literally produces a sibling function holding a duplicate of its ~62-line body, forking the project's only broadcast-join fixture. Task 1.4 named helpers that live private to `e2e_join_test.rs` (`#![cfg(feature = "exasol-e2e")]`, `:25`) without saying how a `lakekeeper-e2e` binary reaches them, leaving "copy them across" as the path of least resistance. Task 1.4 also justified its ground-truth choice by calling `expected_join_rows` (`:192`) "unfiltered", which is false — it delegates to `expected_join_rows_with_fact_where` with the module's fixed `ORDERDATE_LOWER_BOUND`.
- **Direction change:** both tasks now name the existing precedent they follow. Task 1.1 is restated as the `seed_events_table` (`:503-506`) / `seed_events_table_with_auth` (`:520-526`) split applied verbatim — rename the existing body to `seed_star_schema_with_auth`, change one argument at `:1054`, leave `seed_star_schema` as a delegating wrapper so `seed.rs:131` is untouched — with "the body MUST NOT be duplicated" stated outright and the net edit quantified (one rename, one changed argument, a three-line wrapper, one call site). Task 1.4 gains an explicit sub-step (a) that PROMOTES the helpers to `tests/common/e2e_harness.rs` as a move, listing the full transitive closure (the brief's eight names plus `has_n_scan_wrapper`, `columns_to_sorted_pairs`, `value_to_string`, and `expected_join_rows`, each reached through a named call edge) and recording that the promotion needs no new feature gate and no new dead-code allowance: `e2e_harness` and `seed` are already gated across `exasol-e2e` + `lakekeeper-e2e` + `azure-e2e` (`tests/common/mod.rs:20-25`, `:39-44`) and `mod.rs:13-16` already carries a module-wide `#![allow(dead_code)]` for the used-by-one-binary case, with `explain_virtual_sql` (`e2e_harness.rs:283`) as the standing precedent. Sub-step (b) now calls the promoted `join_query(VS_VENDED)` instead of re-typing the SQL, since `vs_fact_table` / `vs_dim_table` are parameterized on the VS name and render the specified query exactly. Three mechanics the advisory had not surfaced are recorded so they cannot become rediscovery work: `e2e_join_test.rs` needs no import edit (it already glob-imports the module, `:28`); `e2e_harness.rs` needs `use std::collections::HashMap;` and `use super::seed::{E2E_DIM_TABLE, E2E_FACT_TABLE};`; and the promoted `vs_fact_table` / `vs_dim_table` collide by name with the zero-arg locals in `e2e_scan_test.rs` (`:116`, `:111`) and `e2e_capability_test.rs` (`:91`, `:87`), which shadow the glob import, so neither file is touched. The false "unfiltered" claim is replaced with the real distinction: `expected_join_rows` hardcodes the join suite's bound, so this suite passes its own bound through `expected_join_rows_with_fact_where`. The expected row count stays 6 — round 2 verified it independently. § Test Disposition's `e2e_join_test.rs` row changes from "KEEP unedited" to "KEEP. Every assertion unchanged", since the file now loses the promoted helper definitions.
- **Resolves:** round-1's deferred `[EFFORT_MISESTIMATION]` advisory — by pointing both tasks at existing reusable code, not by re-estimating. The work is small BECAUSE the reuse path exists; the tasks now say so.
- **Promotes to ADR:** no

### Task 1.4 follow-up investigation — root cause found, but the prescribed test cannot observe it as written; ESCALATING

**Status: HALTED, pending user direction.** This is a genuine escalation, not a routine finding — it touches a load-bearing claim in `plan.md` itself (see below) and the interview's own instruction: "If the investigation shows no MinIO/Lakekeeper fixture can make the bug fail as a read error... escalate back to me. Do NOT pick a fallback proof strategy yourself." No production code under `crates/*/src` was changed by this investigation (`git diff` on those paths is empty); all diagnostic instrumentation was reverted.

**The defect is real and was fully reproduced end-to-end**, inside the Exasol container, through the production object-store path, against the live vended MinIO endpoint:

```
scan failed: assigned data could not be read: Parquet error: Failed to fetch metadata for file
lakehouse_vended/<dim_customer-uuid>/data/dim_customer-00000-….parquet:
Object Store error: The operation lacked the necessary privileges … GET
http://minio:9000/warehouse/lakehouse_vended/<dim_customer-uuid>/data/dim_customer-….parquet
- Server returned non-2xx status code: 403 Forbidden
```

using: `SELECT C_NAME, O_ORDERDATE FROM LK_VENDED_LAKEHOUSE.FACT_ORDERS JOIN LK_VENDED_LAKEHOUSE.DIM_CUSTOMER ON O_CUSTKEY = C_CUSTKEY WHERE O_ORDERDATE >= DATE '2024-01-05'` (table-alias-free, executed without a `resultSetMaxRows` result-set attribute).

**But task 1.4's test as specified — `join_query`/`fetch_join_rows` through the shared harness — passed instead of failing**, for two compounding, independent reasons discovered by instrumented live debugging (UDF debug tracing, temporary `eprintln!`s in `register_side_store`/`build_session_context`/`handle_pushdown`/`run_scan`, all since reverted):

1. **The shared test harness (`tests/common/exasol_ws.rs`) sends `"attributes": {"resultSetMaxRows": 10000}` on every `execute()` call.** Exasol turns that into a `limit` on the pushdown request. `join_requires_exasol_postprocessing` (`crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`) routes any limit-carrying join to the safe two-scan-per-leg fallback — `join: None`, each leg keeps its own `effective_storage` — never entering the broadcast/shared-store path at all. `EXPLAIN VIRTUAL` (what `has_broadcast_join_block` inspects) wraps the query differently and carries no `limit`, so it shows a broadcast plan even though the actual `fetch_join_rows` execution takes the unaccelerated fallback. Verified: with `register_join_tables` unconditionally erroring, `e2e_join_test.rs::e2e_broadcast_join_result_correct` still passes.
2. **A second, separate, pre-existing defect**, unrelated to credentials: with the limit removed, the broadcast path IS entered but fails immediately with a `DataFusion` schema error (`No field named "O"."O_CUSTKEY"`) whenever the join query uses Exasol table aliases. `build_join_sql` (`crates/lakehouse-engine/src/scan/join_scan.rs`) wraps each side in an unaliased sub-SELECT (`fact_scan`/`dim_scan`), so an alias-qualified `join.condition`/`common.filter` (which is what every existing helper — `join_query`, `expected_join_rows_with_fact_where` — renders) cannot resolve. Reproducing the credential defect required an alias-FREE query, which is not what the promoted helpers produce.

**Why this is load-bearing, not a narrow test-authoring bug:**

- **`resultSetMaxRows` is set unconditionally by the shared harness**, so ALL 18 `e2e_join_test.rs` tests — which `plan.md` § Test Disposition calls "the characterization gate that the always-route decision changes shape but not behaviour" — currently execute the two-scan fallback at row-fetch time, never the broadcast/router path this plan changes. That gate is empirically vacuous for exactly the thing it is cited as covering. This plan's own confidence claim ("no previously-returned result set changes," "this suite is the characterization gate") rests on a suite that isn't exercising the code being changed.
- **The alias-qualification defect (finding 2) is itself a real, separate, currently-shipping bug** in the broadcast path (`build_join_sql`'s unaliased sub-SELECT vs. alias-qualified `join.condition`/`common.filter`), discovered only as a side effect of debugging this one. It is out of this plan's stated scope (§ Non-Goals doesn't mention it; § Consequences doesn't name it), yet it means the broadcast path may not be reachable via aliased SQL at all today — which, combined with finding 1, suggests the broadcast-join feature may rarely if ever engage via a typical client (most drivers/tools set a max-rows-style fetch attribute).

**Open questions for the user, not decided here:**

1. Should task 1.4's reproduction be redesigned to use an alias-free query and a fetch path that doesn't set `resultSetMaxRows` (e.g. CTAS, or a new low-level exec helper), so the fix work in groups B–F can proceed against a genuinely failing repro? Or does discovering that the existing 18-test "characterization gate" is vacuous, plus a second unrelated defect, warrant returning to `/speq:plan` revision first?
2. Should the alias-qualification defect be filed as its own tracked GitHub issue (parallel to task 6.1's multi-bucket issue) and left out of this plan's scope, or does it need to be folded in — since without a fix or workaround, no aliased broadcast-join query (which is most realistic SQL) ever reaches the code this plan changes, undermining the plan's Impact claim that "a broadcast join against a catalog that scopes vended credentials per table starts working"?
3. Should `e2e_join_test.rs`'s vacuous-at-execution-time gate be repaired (so it actually exercises the broadcast path's row-fetch, not just its `EXPLAIN VIRTUAL` shape) as part of this plan's task 5.4/Test Disposition work, given the plan explicitly relies on that suite's behavior being unchanged?

Groups B–F remain HALTED pending the user's answer, per the interview's standing instruction that escalating does not itself authorize picking a substitute proof strategy.
- **Promotes to ADR:** no

## Round 2: Alias-Qualification Fix and Gate Repair

Resolves the three open questions above. The user answered all three directly (this round's brief) rather than through a live interview; recorded here as the interview record for this round.

### [14] The alias-qualification defect is folded into this plan, not tracked separately

- **Decision:** The alias-qualification defect (open question 2) is fixed in this same plan, as new tasks 1.5-1.7 (Group G), rather than filed as an out-of-scope tracked issue like task 6.1's multi-bucket exception.
- **Alternatives:** File it as its own tracked GitHub issue, parallel to task 6.1's pattern, and leave it out of this plan's scope — the option open question 2 posed alongside folding it in.
- **Rationale:** This is not scope creep for its own sake — it is folded in because leaving it untracked would leave this plan's OWN claims unverifiable. Two of this plan's own artifacts depend on it: the Impact section's claim that "a broadcast join... starts working" is false for aliased SQL (the overwhelming majority of real client queries) without this fix, and task 1.4's reproduction — the plan's own verification credibility for issue #294 — cannot reach the credential defect it targets without it, because the alias defect fails first, at the DataFusion planning step, for every query the existing harness helpers produce. A tracked-separately issue would leave both problems unresolved: the Impact claim would need a caveat it should not need, and the reproduction gate would stay unable to pass on its own terms. Task 1.5 still files a GitHub issue for it (mirroring task 6.1's practice of tracking a discovered defect), but as a FIXED defect referenced in the implementing commit, not a deferred exception.
- **Promotes to ADR:** no

### [15] Strip Exasol's native `tableAlias` in `render_broadcast_join`, render everything bare (Design 2)

- **Decision:** `render_broadcast_join` strips `tableAlias` from the join condition, the WHERE filter, and the whole `pushdown_req` passed to `extract_join_projection`, using the existing `strip_table_alias` helper, immediately after the disjoint-schema guard passes. `build_join_sql` is unchanged.
- **Alternatives:** (a) Thread each side's native alias through the wire — new `JoinSpec` fields, `build_join_sql` aliases its derived sub-SELECTs to match. (b) Recover the alias by parsing it out of the already-rendered condition string inside `build_join_sql`, at scan time. (c) Leave `render_broadcast_join` as-is and declare an aliased join ineligible for broadcast, extending the existing decline path.
- **Rationale:** Verified against the tree, not assumed. `build_side_fan_out_sql` — the N-scan fallback's own per-side scan renderer — already strips `tableAlias` before rendering its side-local filter, with the comment "the fan-out is a single-table scan whose relation exposes bare uppercase column names, so an alias-qualified reference would not resolve." `build_join_sql`'s derived sub-SELECTs (`SELECT {cols} FROM {registered_table_name}`, no `AS alias`) have the IDENTICAL shape, so the SAME fix applies for the SAME reason — this is not a bespoke solution, it is the existing, working pattern applied where it was missing. `disjoint_schema_guard` (checked immediately before, in the same function) already proves the two sides share no column name, which is exactly the precondition that makes bare-name resolution unambiguous — the same precondition `build_side_fan_out_sql` already relies on. Alternative (a) is correct in principle but needs a home for the FACT side's alias too (`JoinSpec` only describes the dimension side today, coupling into the credential fix's own wire-format work in tasks 2.x), asymmetric-alias handling (one side aliased, one not — legal SQL), and live-Exasol verification of whether Exasol ever omits `tableAlias` or sends it equal to the table name — all solvable, but for a problem bare rendering already discharges for free. Alternative (b) is rejected outright: string-surgery on an opaque, already-rendered SQL fragment to recover structured information the JSON form already carried cleanly is fragile, and still cannot tell which alias belongs to which side without wire metadata, converging back to (a)'s coupling problem while being harder to get right. Alternative (c) would silently forfeit broadcast for the common case rather than fix the defect, and contradicts this plan's own Impact claim.
- **Supersedes:** `render_broadcast_join`'s own doc comment ("Rendering is side-agnostic: the translator emits bare column names") and `vs-adapter/pushdown-planning-join`'s permanent-spec Background ("Broadcast rendering is side-agnostic bare-name") both asserted this as CURRENT fact. Both were false before this fix — this decision makes them true rather than adding a new requirement, the same pattern the delta already uses for its own `#215` bullet.
- **Promotes to ADR:** yes

### [16] Task 1.4 is redesigned around the alias fix; `e2e_join_test.rs`'s vacuous gate is repaired for exactly the 4 tests that claim broadcast execution

- **Decision:** Task 1.4(b) as originally written (open question 1) is superseded, not retried: new tasks 1.8 (a scoped `ExaConn::unbounded_result_sets()` opt-out) and 1.9 (the re-run, now gated on the alias fix landing first) replace it. Once the alias fix lands, 1.9 uses the EXISTING aliased `join_query`/`fetch_join_rows` helpers unchanged — no alias-free query variant, no new fetch path. Separately (open question 3), `e2e_join_test.rs`'s false "characterization gate" claim is corrected, and the opt-out is applied to exactly 4 of its 18 tests: `e2e_broadcast_join_result_correct`, `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters`, `e2e_join_decimal_stringification_matches_native_at_both_surfaces`, and `e2e_above_threshold_result_matches_broadcast`.
- **Alternatives for task 1.4:** (a) An alias-free query plus a new low-level exec helper that bypasses `resultSetMaxRows` — the option open question 1 posed. (b) A global change to `exasol_ws.rs`'s `execute`/`try_execute` defaults.
- **Rationale for task 1.4:** Once the alias fix (decision [15]) lands, an alias-free query is no longer needed — the existing aliased helpers already render correctly, and inventing a second, alias-free query variant just to prove a fix that no longer requires one would be needless duplication with its own drift risk (the two variants could diverge over time). The opt-out is a per-connection field with a builder method, not a global default change (alternative (b)), because a global change would risk silently altering row-fetch behavior for the many EXISTING passing tests that were never in scope for this investigation — the brief's own instruction was a SCOPED opt-out "used only by the new Lakekeeper test."
- **Alternatives for the `e2e_join_test.rs` repair:** (a) Surgical — apply the opt-out to only the ONE test literally named in this round's brief, `e2e_broadcast_join_result_correct`. (b) Broad — apply the opt-out to all 18 tests. (c) Leave the file untouched and only correct its Test Disposition row's prose.
- **Rationale for the repair scope:** Reading every one of the 18 tests' bodies (not just names) found the vacuousness is not isolated to the one test the brief named as a candidate: `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` asserts `has_broadcast_join_block` via a SEPARATE `EXPLAIN VIRTUAL` call (unaffected by the row-limit) but then fetches its actual rows on the SAME limited connection, so its "stays broadcast... and filters" claim was validated only at plan time, never at row-fetch time; `e2e_join_decimal_stringification_matches_native_at_both_surfaces`'s doc explicitly claims its `VS_NAME` iteration exercises "the broadcast plan," which was equally false; and `e2e_above_threshold_result_matches_broadcast` labels one of its two fetches "broadcast" while it was actually comparing fallback against fallback, silently weakening its value as a broadcast-vs-fallback equivalence check. Alternative (a) would leave 3 of these 4 false claims uncorrected. Alternative (b) was rejected: the OTHER 14 tests each name a DIFFERENT thing they are proving — an aggregate forces the fallback, a 3-or-4-table join cannot broadcast, an above-threshold side forces the fallback, a declined type falls back — and none of those claims is about broadcast execution, so bypassing the limit for them would change nothing they assert while adding rewrite churn with no verification value. Alternative (c) was rejected because it would leave the suite exercising less than its own now-corrected documentation claims, which is the same category of drift this round exists to close.
- **Promotes to ADR:** no

## Round 3: Advisory Polish

Resolves all 6 ADVISORY findings from `review/round-3.md` (0 blockers that round). Applied as a direct
polish pass on top of the already-validated Round 2 revision, per the orchestrator's brief; no Round 2
decision is reopened.

### [plan-review] Impact's confident claim outlived its own Non-Goals hedge

- **Finding:** § Impact stated, unqualified, that a broadcast join "starts working... including when the
  query aliases the joined tables." § Non-Goals disclosed, three paragraphs later, that whether a real
  client driver's own default fetch-size attribute reproduces the same suppression this plan's test
  harness had is unstudied. A reader who stops at Impact never learns the plan's own risk assessment
  leaves that open.
- **Direction change:** § Impact now states, immediately after the aliasing sentence, that this remains
  unstudied and cross-references the Non-Goals bullet that carries the detail.
- **Promotes to ADR:** no

### [plan-review] Task 1.9's pass criterion was a judgment call, not an exact check

- **Finding:** task 1.9 named `403 Forbidden` only parenthetically, with no statement that a failure text
  lacking it — a connection-level error or a timeout, for instance — must not be waved through as the
  expected credential denial.
- **Direction change:** task 1.9 now states the literal substring `403 Forbidden` as the concrete pass
  criterion and states explicitly that any other failure text must be escalated, not treated as a pass.
- **Promotes to ADR:** no

### [plan-review] `pushdown-planning-alias-stripping` was absent from § Features

- **Finding:** task 1.6 adds a third caller to `strip_table_alias`, but the already-recorded feature
  owning that helper appeared in neither the CHANGED nor the checked-not-amended list in § Features.
- **Direction change:** added to the checked-not-amended list. Verified its scenarios are scoped to the
  single-table (non-join) pushdown path and never claim the caller list is exhaustive, so the third
  caller does not falsify anything the recorded scenarios assert.
- **Promotes to ADR:** no

### [plan-review] Task 1.7's citation drifted by one line at both ends

- **Finding:** task 1.7 cited `render_broadcast_join_preserves_native_table_alias_unchanged` at
  `:1584-1607`.
- **Direction change:** verified directly against the tree — the doc comment starts at `:1585` and the
  closing brace is at `:1608` — and corrected the citation to `:1585-1608`.
- **Promotes to ADR:** no

### [plan-review] tasks.md flattened the three sub-groups plan.md's own § Parallelization claims

- **Finding:** § Parallelization distinguishes Group G, Group A′, and a non-gating repair line for
  tasks 1.5-1.10, but tasks.md presented all six as one flat, implicitly sequential list.
- **Direction change:** tasks.md's "Phase 1 (Round 2)" section is split into the same three sub-groups,
  stating 1.8's parallel-with-Group-G option and 1.10's independence inline.
- **Promotes to ADR:** no

### [plan-review] § Non-Goals had grown to an 8-exclusion single sentence for a third consecutive round

- **Finding:** round-1 and round-2 both flagged the Non-Goals run-on sentence and deferred the fix; this
  round's own edit added an eighth exclusion, making it longer still.
- **Direction change:** converted to a bulleted list, one exclusion per bullet, giving the
  LIMIT-forces-fallback exclusion its own bullet — the anchor the Impact cross-reference above now
  points to.
- **Promotes to ADR:** no

## Task 1.9 — GATE re-run: PASSED

With tasks 1.6-1.8 landed (alias stripping in `render_broadcast_join`, `unbounded_result_sets()` wired
into `lakekeeper_vended_broadcast_join_result_correct`), re-ran the gate against the live Docker stack:

```
cargo test --features lakekeeper-e2e --test e2e_lakekeeper_test \
  lakekeeper_vended_broadcast_join_result_correct -- --nocapture --test-threads=1
```

Result: `FAILED. 0 passed; 1 failed` — exit 101, as required. The captured failure text contains the
literal substring `403 Forbidden` (credential-redacted; no key/secret/token value appears in the text,
only path, bucket, request-id, and host-id):

```
Error: {"exception":{"sqlCode":"22002","text":"VM error: F-UDF-CL-RUST-9001: UDF error: UDF run
returned error code 1: scan failed: assigned data could not be read: Parquet error: Parquet error:
Failed to fetch metadata for file lakehouse_vended/<warehouse-uuid>/data/dim_customer-00000-<hash>.parquet:
Object Store error: The operation lacked the necessary privileges to complete for path
lakehouse_vended/<warehouse-uuid>/data/dim_customer-00000-<hash>.parquet: Error performing GET
http://minio:9000/warehouse/lakehouse_vended/<warehouse-uuid>/data/dim_customer-00000-<hash>.parquet
in 2.546921ms - Server returned non-2xx status code: 403 Forbidden: <Error><Code>AccessDenied</Code>
<Message>Access Denied.</Message>...</Error>"},"status":"error"}
```

This is the exact credential-denial shape task 1.9 requires — a DataFusion Parquet-metadata-fetch error
reaching MinIO's `403 Forbidden`, not a connection-level error, a timeout, or a DataFusion alias/schema
error. The gate PASSES: Groups B-F are unblocked.
- **Promotes to ADR:** no

## Task 7.1 — Reproduction re-run: PASSES with the fix in place

With Groups B-F landed (per-side `JoinSpec.storage`, `PrefixRoutingObjectStore`, per-side session
construction, redaction union, and the full test suite), re-ran the exact same reproduction:

```
cargo test --features lakekeeper-e2e --test e2e_lakekeeper_test \
  lakekeeper_vended_broadcast_join_result_correct -- --nocapture --test-threads=1
```

Result: `test result: ok. 1 passed; 0 failed` (19.29s). The broadcast join over the vended-credential
warehouse now reads the dimension side through its OWN vended credential instead of the fact side's,
and returns the correct 6-row result. This closes issue #294 end-to-end against the live Docker stack —
not just at the unit/integration level.
- **Promotes to ADR:** no
