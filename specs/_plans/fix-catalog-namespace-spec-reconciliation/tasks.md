# Tasks: fix-catalog-namespace-spec-reconciliation

## Phase 2: Implementation (Group A — premise)
- [x] 1.1 Confirm live against Docker Exasol that `NAMESPACE` is a legal VS property name on both CREATE and ALTER...SET DDL paths; record captures in decision-log.md. HALT if rejected.

## Phase 2: Implementation (Group B — production rename)
- [x] 2.1 Rename `PROP_ICEBERG_NAMESPACE`→`PROP_NAMESPACE` (literal `"NAMESPACE"`) in `crates/lakehouse-engine/src/adapter/mod.rs`; rename local `iceberg_namespace`→`namespace`; replace the two comment lines.
- [x] 2.2 Change error message in `crates/lakehouse-catalog/src/namespace.rs:59` to `invalid namespace '{}': {}`.

## Phase 2: Implementation (Group C — test rename)
- [x] 3.1 Update `adapter_tests.rs` (JSON keys, assert_eq!, doc comment, PROP_* refs) and `unity_schema_tests.rs` (one JSON key); ADD no-alias test.
- [x] 3.2 Update E2E DDL/doc-comment sites: `e2e_unity_test.rs`, `cloud_e2e_test.rs`, `e2e_scan_test.rs`, `e2e_refresh_test.rs`, `tests/common/e2e_harness.rs`.
- [x] 3.3 Rename env var in `tests/tpch_loader.rs` (env::var call + module doc comment), keep `tpch` default.

## Phase 2: Implementation (Group D — bench/deploy/docs env var rename)
- [x] 4.1 Rename env var across `bench/run.sh` (careful: local `NAMESPACE` var collision, delete resulting self-assignment), `bench/batch_size_aggcheck.sh`, `bench/batch_size_sweep.sh`, `bench/emit_s3conn_sweep.sh`, `bench/.env.example`, `bench/README.md`. Do not rename `BENCH_DELETE_NAMESPACE`.
- [x] 4.2 Rename in `deploy/scripts/install.sh` and `deploy/scripts/secrets.sh`.
- [x] 4.3 Rename in `README.md`, `docs/install.md`, `docs/catalogs.md`, `docs/tuning.md`, `docs/benchmark.md`.

## Phase 2: Implementation (Group E — permanent spec prose, direct edits) [expert]
- [x] 5.1 `vs-adapter/create-virtual-schema`: rename the one `ICEBERG_NAMESPACE` Background mention (not the unrelated "Iceberg namespace" fixture bullet).
- [x] 5.2 `vs-adapter/unity-catalog-create-virtual-schema`: rename Background property mention; delete discharged #324 deferral clause.
- [x] 5.3 `datafusion-scan/scan-execution`, `scan-execution-spec-reconstitution`, `parallelism/work-unit-sharding`, `vs-adapter/pushdown-planning-file-encoding`: replace "the Iceberg table root" + sibling neutral-field mentions per plan §5.3; leave genuinely-Iceberg clauses unedited. [expert]
- [x] 5.4 `vs-adapter/pushdown-planning-file-resolution`: replace feature-description paragraph verbatim from delta file; neutralize Background bullet on resolve-once orchestration. [expert]
- [x] 5.5 `vs-adapter/pushdown-planning-join` and `datafusion-scan/scan-execution-join`: neutralize Iceberg-specific Background bullets/description line; keep Iceberg-spec-grounded clauses (Appendix E, #304) unedited. [expert]
- [x] 5.6 `vs-adapter/pushdown-planning`: rename two "Iceberg table root" mentions; ADD Background bullet on per-format pruning-predicate ownership.
- [x] 5.7 `vs-adapter/pushdown-planning-empty-result`: replace description-line phrase "When Iceberg-level file pruning"→"When plan-time file pruning"; neutralize Background bullet.

## Phase 2: Implementation (Group F — mission.md + CLAUDE.md)
- [x] 6.1 `specs/mission.md` Core Capability 7: reword to name two Databricks access routes (Iceberg REST vs native Unity Catalog) without documenting their differing correctness dependencies.
- [x] 6.2 `specs/mission.md`: correct remaining single-format claims (Core Capability 2, 3, 6, Tech Stack Lakehouse row, Project Structure crate comments, sibling-projects note, Architecture data-flow line, External Dependencies table). Coordinate wording with 7.1.
- [x] 7.1 `CLAUDE.md` Build section: update both crate descriptions (Iceberg+Delta / Iceberg REST+Unity Catalog). Coordinate wording with 6.2.
- [x] 7.2 `CLAUDE.md`: extend "Iceberg specification compliance" section to also cover the Delta Lake protocol; retitle section.

## Phase 2: Implementation (Group G — coverage gap + doc-comment fix)
- [x] 8.1 Add missing unit test in `crates/lakehouse-engine/src/scan/spec_tests.rs` for delete-file relative/absolute path resolution (3 clauses).
- [x] 8.2 Fix stale doc comment at `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs:269` ("Iceberg-manifest byte sizes" → neutral per-file metadata size).

## Phase 2: Implementation (Group H — completeness guards, run last)
- [x] 8.3 Run and record the three completeness guards from plan §8.3 (grep counts, "Iceberg table root" grep, `git diff --stat specs/_recorded specs/_decision` empty). Guards 1 and 3 pass exactly. Guard 1 has 3 additional deliberate hits in `adapter_tests.rs`'s plan-mandated no-alias test (quotes the old literal by design). Guard 2 has 8 additional hits — `datafusion-scan/scan-execution/spec.md:89`, `datafusion-scan/scan-execution-spec-reconstitution/spec.md:88`, `datafusion-scan/scan-execution-file-metadata/spec.md:84` and `:111`, `parallelism/work-unit-sharding/spec.md:90`, `vs-adapter/pushdown-planning-file-encoding/spec.md:27` and `:35`, and `vs-adapter/pushdown-planning-file-resolution/spec.md:120` — all inside `## Scenarios` clauses with a staged `DELTA:CHANGED` replacement in this plan's delta files, resolved at `/speq:record` time — same pending-merge pattern Guard 1 explicitly carves out for `ICEBERG_NAMESPACE`. Both are plan-wording gaps, not implementation defects; noted in the verification report.

## Phase 4: Review Fixes
- [x] 4.1 `specs/datafusion-scan/scan-execution/spec.md` line 26: keep the neutral antecedent but re-scope the consequent so the projection binding key is attributed per format (Iceberg field-id with physical-name fallback; Delta `columnMapping.id` / `delta.columnMapping.physicalName` where column mapping is configured, physical-name binding where it is not), keeping the no-logical-schema fallback clause and lines 45-48 unchanged, and not contradicting `datafusion-scan/scan-execution-field-id-projection`. [expert]
- [x] 4.2 `crates/lakehouse-engine/src/scan/spec_tests.rs`: replace the hand-composed calls in `reconstruct_delete_file_entry_resolves_like_a_data_file_entry` with assertions that drive a real scan-side delete-resolution site (relative-join plus absolute-passthrough), moving the test to the seam owner's sibling `_tests.rs` if unreachable from this module, and confirm mutation-sensitivity against `scan/store_router.rs`'s `reconstruct_abs_uri` call. [expert]
- [x] 4.3 `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`: complete task 8.2 across the six remaining Iceberg-manifest-sizing sites (lines 196-198, 208, 224, 280, 309, 316) — neutral wording per finding.
- [x] 4.4 `crates/lakehouse-engine/src/adapter/mod.rs` line 93: replace "its Iceberg-manifest byte size" with "its total resolved file byte size" in the `PROP_JOIN_BROADCAST_MAX_BYTES` doc comment.
- [x] 4.5 `crates/lakehouse-engine/src/adapter/mod.rs`: replace the Iceberg-only attribution of `TABLE_MAP` at lines 115, 401, 516, 620, 623, 651 with neutral wording ("catalog identifier" / "the scanned table"), leaving lines 320-321 and 570 unedited.
- [x] 4.6 Run `cargo fmt --all` to fix the two red `cargo fmt --all -- --check` diffs (`adapter_tests.rs` double blank line, `mod.rs` method-chain collapse) and re-verify clean.
- [x] 4.7 `crates/lakehouse-engine/src/adapter/adapter_tests.rs` lines 197-199: rewrite the doc comment's last sentence on `create_virtual_schema_rejects_old_namespace_alias_without_replacement` to state that reintroducing an alias makes the request succeed, tripping `expect_err` and failing the test.
- [x] 4.8 `crates/lakehouse-engine/src/adapter/adapter_tests.rs` lines 183-190 and 214-221: replace the two-part `contains(PROP_NAMESPACE)` + `contains("is required")` assertions with a single assertion on `format!("property '{PROP_NAMESPACE}' is required")`.
- [x] 4.9 `crates/lakehouse-engine/src/scan/spec_tests.rs` lines 77-80: already resolved by the prior expert-fix pass, which deleted the whole test — no action needed.
- [x] 4.10 `specs/mission.md` line 173: rewrite the Databricks External-Dependencies Failure Impact cell so both Databricks routes are stated to fail together, with the non-Databricks Iceberg REST and Unity Catalog dependencies above unaffected.
- [x] 4.11 `specs/mission.md` lines 53, 147, 172: retitle Core Capability 7 and widen its first sentence to name both catalog kinds and both table formats; neutralize the line-147 data-flow block; replace "Snapshot discovery" at line 172 with the Delta term.
- [x] 4.12 `CLAUDE.md`: replace the Delta protocol section's bare repo-path citation with the full PROTOCOL.md URL and extend the Exasol-target-type carve-out to cover a Delta-driven deviation.
- [x] 4.13 `specs/_plans/fix-catalog-namespace-spec-reconciliation/tasks.md` line 40 (task 8.3): correct "5 additional hits" to "8 additional hits" and list the eight `file:line` locations.
- [x] 4.14 `deploy/scripts/tests/install.test.sh` line 1013: add an `assert_contains` for the renamed `NAMESPACE          = ` DDL template line and an `assert_not_contains` for `ICEBERG_NAMESPACE`.

## Phase 3: Verification
- [x] V.1 Run automated checks: `cargo build`, `cargo test`, `cargo clippy --all-targets`, `cargo fmt` (check), `speq feature validate`. All green; plus `make test-e2e` and `make test-e2e-unity` (required by the plan's own Checklist and this implement-pr run's Test+Record gate) both green after provisioning the missing one-shot `spark-iceberg-fixtures` job.
- [x] V.2 Scenario coverage audit against plan's Scenario Coverage table. All scenarios have a passing test; one test-location correction (delete-file-path scenario moved to `store_router_tests.rs`) reconciled in plan.md and verification-report.md.
- [x] V.3 Manual testing steps (Docker-dependent DDL probes) — reuse task 1.1's captures where applicable. All four DDL rows pass; the bench `NAMESPACE=tpch` row proved the env-var rename via the generated DDL but hit an unrelated pre-existing BucketFS `.so`/SLC fingerprint mismatch — documented in verification-report.md, not a rename regression.
- [x] V.4 Generate verification-report.md.
