# Code Review Findings: fix-catalog-namespace-spec-reconciliation

## Summary
- Files reviewed: 38
- Total findings: 14 (standard: 12, expert: 2)

Gates run for evidence: `cargo test --workspace` → 0 failures (1129 lib + all integration targets; both
new tests execute and pass). `cargo clippy --all-targets` → clean. `speq feature validate` → 0 errors.
`git diff --stat specs/_recorded specs/_decision` → empty. Plan guard 1 (`ICEBERG_NAMESPACE` sweep) → passes
exactly as the plan enumerates: 6 in-`## Scenarios` occurrences across the 4 named specs, 3 intentional
literals in `adapter_tests.rs`, 12 frozen in `specs/_recorded/`, zero under `crates/`, `bench/`, `deploy/`,
`docs/`, `README.md`. `cargo fmt --check` → RED (see finding 6).

The rename itself is mechanically complete and correct, including the two `bench/run.sh` edits the plan
called out by hand. The findings below are concentrated in two places: production doc comments carrying the
same false claim task 8.2 was created to delete, and one spec clause whose neutralization went too far.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs

#### [OUTDATED_COMMENT] Task 8.2 removed one copy of the Iceberg-manifest sizing claim and left six
- Location: lines 196-198, 208, 224, 280, 309, 316
- Issue: task 8.2 corrected only the `JoinSides` doc comment at line 269. The identical false claim survives
  six times in the same file, including on `select_broadcast_sides` — the very symbol the plan's task 8.2 and
  its Dead Code Removal table name. Line 309 reads "The SMALLER side by total Iceberg-manifest bytes is the
  dimension"; line 316 reads "unit-testable without a live Iceberg catalog"; line 280 reads "**Self-join**
  (both sides the same Iceberg table)"; lines 196-198, 208 and 224 describe the neutral metric as
  `file_size_in_bytes` (an Iceberg manifest field). Evidence: the code sums the neutral field —
  `planning.rs:250-252` is `files.iter().fold(0u64, |acc, entry| acc.saturating_add(entry.size))` over
  `FileEntry::size` (`crates/lakehouse-engine/src/scan/spec.rs:903`), and the Delta reader populates that
  field from the `add` action's `size`, not from `file_size_in_bytes`. Every one of these six contradicts the
  `vs-adapter/pushdown-planning-join` Background bullet this plan just corrected to read "the sum of the
  per-file sizes that side's format reader resolved".
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`, complete task 8.2 across all six
  remaining sites. Line 309: replace "total Iceberg-manifest bytes" with "total resolved file bytes". Line
  316: replace "without a live Iceberg catalog" with "without a live catalog". Line 280: replace "the same
  Iceberg table" with "the same table". Lines 196-198, 208 and 224: replace each `file_size_in_bytes`
  reference with `FileEntry::size`, naming the Iceberg manifest `file_size_in_bytes` and the Delta `add`
  action `size` as the two format readers' sources rather than as the rule. Keep lines 214-216, 222 and 227
  unedited — `schema.name-mapping.default`, `partition_columns` and `refused_columns` are genuine per-format
  distinctions that already name Delta correctly.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [OUTDATED_COMMENT] The broadcast-threshold constant carries the same Iceberg-manifest sizing claim
- Location: line 93
- Issue: the comment above `PROP_JOIN_BROADCAST_MAX_BYTES` reads "the smaller side of a two-table inner
  equi-join is broadcast … when its Iceberg-manifest byte size is at or below this threshold". This is a
  third copy of the claim task 8.2 exists to delete, in a file this diff already edits, and it contradicts
  the `vs-adapter/pushdown-planning-join` Background bullet corrected by task 5.5.
- Fix: In `crates/lakehouse-engine/src/adapter/mod.rs` line 93, replace "its Iceberg-manifest byte size" with
  "its total resolved file byte size". Leave the rest of the comment unchanged.

#### [OUTDATED_COMMENT] TABLE_MAP is documented as Iceberg-only but carries Unity Catalog entries too
- Location: lines 115, 401, 516, 620, 623, 651
- Issue: six comments and doc comments describe TABLE_MAP and the identifier it resolves as Iceberg-only —
  "Exasol-name → Iceberg-identifier map" (115), "Derive the scanned Iceberg table" (401), "Iceberg
  identifier" (516, 620, 623, 651). TABLE_MAP is populated for both catalog kinds: `unity_schema_tests.rs`
  asserts on TABLE_MAP contents for a `CATALOG_KIND = 'UNITY_CATALOG'` `createVirtualSchema` response (lines
  418, 453, 461-465, 492). These comments now mis-describe a code path both catalog kinds reach, in the same
  class as the finding above.
- Fix: In `crates/lakehouse-engine/src/adapter/mod.rs`, replace the Iceberg-only attribution at lines 115,
  401, 516, 620, 623 and 651 with the neutral term — "catalog identifier" / "the scanned table" — so the
  comments describe the map both catalog kinds populate. Leave lines 320-321 (`NotLoadableIcebergTable`) and
  line 570 (`CatalogKind::IcebergRest`) unedited: those name a genuinely Iceberg-specific variant.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [SUPPRESSED_WARNING] The `cargo fmt` verification gate is red
- Location: line 193 (and `crates/lakehouse-engine/src/adapter/mod.rs` line 238)
- Issue: `cargo fmt --all -- --check` reports two diffs, so the plan's Checklist row "Format: `cargo fmt` →
  No changes" does not hold. (1) `adapter_tests.rs:193` has a double blank line between
  `set_properties_null_unset_required_property_errors_not_panic` and the new test; rustfmt's default
  `blank_lines_upper_bound` is 1. (2) `mod.rs:238` — renaming `iceberg_namespace` to `namespace` shortened the
  line, so rustfmt now wants `let configured_ns: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();`
  on one line instead of the four-line chain left in place. Both are direct consequences of this diff.
- Fix: Run `cargo fmt --all` and commit the result, then re-run `cargo fmt --all -- --check` and confirm it
  reports no diff before recording the Checklist row as passing.

#### [OUTDATED_COMMENT] The new test's doc comment states the opposite of what the test does
- Location: lines 197-199
- Issue: the doc comment on `create_virtual_schema_rejects_old_namespace_alias_without_replacement` claims
  "if an alias for the renamed property is ever reintroduced, this request would instead succeed and this
  test would silently stop catching the regression." The test calls `dispatch(...).expect_err(...)`, so a
  succeeding request panics and the test fails loudly — it catches exactly that regression rather than
  silently missing it. The comment inverts the test's own failure mode and would mislead the next reader into
  thinking the no-alias contract is unguarded.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, rewrite the last sentence of the doc comment
  at lines 197-199 to state the real mechanism: reintroducing an alias makes the request succeed, which trips
  the `expect_err` and fails this test.

#### [ASSERTION_FREE_TEST] The property-name assertion cannot distinguish `NAMESPACE` from `ICEBERG_NAMESPACE`
- Location: lines 184 and 215
- Issue: both tests assert `err.to_string().contains(PROP_NAMESPACE)` where `PROP_NAMESPACE` is `"NAMESPACE"`.
  The string `"ICEBERG_NAMESPACE"` contains `"NAMESPACE"` as a substring, so an error message naming the OLD
  property satisfies the assertion. This makes the guarantee the plan's Verification section claims —
  "`set_properties_null_unset_required_property_errors_not_panic` asserts the required-property error names
  the property, so it fails if the rename is partial" — false: a partial rename that left the error text
  saying `ICEBERG_NAMESPACE` would pass. Line 215 has the same hole while sitting in a test whose request
  deliberately supplies an `ICEBERG_NAMESPACE` key, which is precisely the case the substring cannot
  discriminate.
- Fix: In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, replace the two-part
  `contains(PROP_NAMESPACE)` + `contains("is required")` assertions at lines 183-190 and 214-221 with a
  single assertion on the whole expected message, `format!("property '{PROP_NAMESPACE}' is required")`, so an
  error naming the old property fails the test.

### crates/lakehouse-engine/src/scan/spec_tests.rs

#### [DUPLICATE_TEST] The new test's third assertion cannot fail and is already covered
- Location: lines 77-80
- Issue: `reconstruct_abs_uri(absolute_delete.object_store_path().unwrap(), "")` re-asserts the case the
  assertion immediately above it already covers. `reconstruct_abs_uri` (`scan/spec.rs:1460-1462`) returns
  early on `entry_path.contains("://")` and never reads `table_root`, so this assertion is provably
  unfailable unless the preceding one fails first. The identical empty-root passthrough is also already
  asserted at `spec_tests.rs:13-17` in `reconstruct_absolute_entry_passes_through`. Meanwhile the plan's
  actual third clause — "an empty table root leaving every delete-file entry absolute" — has no discriminating
  assertion.
- Fix: In `crates/lakehouse-engine/src/scan/spec_tests.rs`, delete the redundant assertion at lines 77-80 and
  update the doc comment at lines 47-51 so it no longer claims an empty-table-root clause the test does not
  discriminate.

### specs/mission.md

#### [OUTDATED_COMMENT] The Databricks External-Dependencies row now contradicts itself
- Location: line 173
- Issue: the row reads `| Databricks (Iceberg REST or Unity Catalog) | Databricks-managed table access via
  either catalog kind | Databricks queries fail; the other catalog kind's path is unaffected |`. Widening the
  Service cell to cover BOTH catalog kinds broke the Failure Impact cell: if Databricks is unavailable, its
  Iceberg-REST route and its Unity Catalog route both fail, so there is no surviving "other catalog kind's
  path". The pre-edit text ("Iceberg path unaffected") was true because it referred to the separate
  non-Databricks Iceberg-REST dependency in the row above. The neutralization turned a true statement false.
- Fix: In `specs/mission.md` line 173, rewrite the Failure Impact cell to state that both Databricks routes
  fail together and that the non-Databricks Iceberg REST and Unity Catalog dependencies in the rows above are
  unaffected.

#### [OUTDATED_COMMENT] Three single-format claims survive task 6.2's acceptance criterion
- Location: lines 53, 147, 172
- Issue: task 6.2's acceptance requires "every Core Capability, the Tech Stack, and the External Dependencies
  table name both catalog kinds and both table formats". Three sites still do not. (1) Line 53: Core
  Capability 7's title is still "**Iceberg + Databricks access**" and its first sentence still enumerates only
  "Apache Iceberg tables and Databricks-managed tables", although the shipped Unity Catalog path is exercised
  against an OSS Unity Catalog (`specs/e2e-harness/unity-catalog-e2e-harness/spec.md:61`, fixture
  `unity.delta_e2e` at `http://unitycatalog:8080`), which no Core Capability names. (2) Line 147: the
  Architecture data-flow block reads `→ Iceberg / Databricks Parquet files` two lines below the line 144 the
  same task just neutralized to `resolve snapshot + file list ONCE per query (format-neutral: Iceberg or
  Delta)` — one code block now mixes both vocabularies. (3) Line 172: the Unity Catalog row's Purpose uses
  Iceberg's "Snapshot discovery" vocabulary for Delta, which has versions and commits.
- Fix: In `specs/mission.md`, retitle Core Capability 7 at line 53 and widen its first sentence to name both
  catalog kinds and both table formats, keeping task 6.1's fence — do NOT add differing-correctness-dependency
  text, issue #11/#12 citations, Delta-deletion-vector discussion, or a comparison table. Neutralize line 147
  to name the table formats' data files rather than "Iceberg / Databricks". Replace "Snapshot discovery" at
  line 172 with the Delta term (table version / log replay).

### CLAUDE.md

#### [OUTDATED_COMMENT] The Delta protocol rule cites a repo path where task 7.2 required the URL
- Location: the "Iceberg and Delta Lake specification compliance" section
- Issue: task 7.2 required the Delta half to cite
  `https://github.com/delta-io/delta/blob/master/PROTOCOL.md` and to follow the `§ <Section>` convention. Only
  the convention landed; the citation is a bare repo path (`delta-io/delta`, `PROTOCOL.md`, `master`) while the
  Iceberg half one paragraph above carries a resolvable URL (`https://iceberg.apache.org/spec/`). A planner
  told to quote a normative section has no link to follow. Separately, the Iceberg half's Exasol-target-type
  carve-out ("A deviation driven by an Exasol target-type limitation … is not a gap") is not mirrored on the
  Delta side, although Delta hits the same Exasol type ceiling (`vs-adapter/delta-type-mapping`).
- Fix: In `CLAUDE.md`, replace the Delta half's bare repo-path citation with the full URL
  `https://github.com/delta-io/delta/blob/master/PROTOCOL.md`, keeping the `§ <Section>` convention, and extend
  the Exasol-target-type carve-out sentence to cover a Delta-driven deviation as well.

### specs/_plans/fix-catalog-namespace-spec-reconciliation/tasks.md

#### [OUTDATED_COMMENT] Guard 2's recorded hit count does not match the command output
- Location: line 40 (task 8.3)
- Issue: task 8.3 records "Guard 2 has 5 additional hits". The command it names returns **8** permanent-spec
  hits: `datafusion-scan/scan-execution/spec.md:89`, `datafusion-scan/scan-execution-spec-reconstitution/spec.md:88`,
  `datafusion-scan/scan-execution-file-metadata/spec.md:84` and `:111`, `parallelism/work-unit-sharding/spec.md:90`,
  `vs-adapter/pushdown-planning-file-encoding/spec.md:27` and `:35`, and
  `vs-adapter/pushdown-planning-file-resolution/spec.md:120`. The task's substantive conclusion is correct and
  I independently confirmed it — every one of the 8 sits after its file's `## Scenarios` heading and is
  delta-carried, so this is a plan-wording gap, not a missed rename — but the count is wrong, and task V.4
  will carry it into the verification report as recorded evidence.
- Fix: In `specs/_plans/fix-catalog-namespace-spec-reconciliation/tasks.md` line 40, correct "5 additional
  hits" to "8 additional hits" and list the eight `file:line` locations, so the verification report records the
  guard's real output.

### deploy/scripts/install.sh

#### [MISSING_BOUNDARY_TEST] The renamed operator-facing DDL template has no assertion
- Location: line 1193
- Issue: `emit "  NAMESPACE          = '<namespace>'"` is the operator-facing `CREATE VIRTUAL SCHEMA` template
  and was renamed with no test guarding it. `deploy/scripts/tests/install.test.sh` exercises this template in
  four places (lines 1013, 1118, 1570, 1655) but asserts only the substring `CREATE VIRTUAL SCHEMA`; nothing
  asserts the property names it emits. A property rename is exactly the regression class such an assertion
  catches, and it went in unguarded.
- Fix: In `deploy/scripts/tests/install.test.sh`, extend the template assertion at line 1013 with an
  `assert_contains` for `NAMESPACE          = ` and an `assert_not_contains` for `ICEBERG_NAMESPACE`, so the
  emitted DDL template's property names are pinned.

## Expert fixes

### specs/datafusion-scan/scan-execution/spec.md

#### [OUTDATED_COMMENT] Neutralizing the field-id bullet's antecedent made a normative clause false for Delta
- Location: line 26
- Issue: the Background bullet was changed from "When the scan spec carries a logical **Iceberg** schema,
  column projection binds by Iceberg field-id (with a physical-name fallback)…" to "When the scan spec carries
  a logical schema, column projection binds by Iceberg field-id (with a physical-name fallback)…". The
  antecedent was widened to every format while the consequent still names Iceberg field-id as the binding key,
  so the clause now asserts Iceberg-field-id binding for every Delta scan spec. That is false in the common
  Delta case: a Delta-produced `LogicalField` binds by `columnMapping.id` or
  `delta.columnMapping.physicalName`, and with no column mapping configured — the Delta default — by neither
  (`crates/lakehouse-engine/src/scan/spec.rs:411-446`,
  `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs:329-333`). This also violates the
  plan's own § Requirements ("A clause naming … field ids … MUST NOT be neutralized") and task 5.3 ("leave
  every genuinely Iceberg clause of `scan-execution` — field-id binding … — unedited"). Note the plan is
  self-contradictory here: task 5.3 also orders the rename of `scan-execution`'s "the logical Iceberg schema",
  and this bullet is its only Background home — the conflict was resolved the wrong way, by widening the rule
  instead of scoping the mechanism. This is the one finding whose failure mode is a normative spec clause that
  a future planner will read as licence to assume field-id binding on a Delta table.
- Fix: In `specs/datafusion-scan/scan-execution/spec.md` line 26, keep the neutral antecedent ("When the scan
  spec carries a logical schema") but re-scope the CONSEQUENT so the binding key is attributed per format:
  state that projection binds by the logical field's identity as the format reader resolved it — Iceberg
  field-id with a physical-name fallback on the Iceberg side, Delta `columnMapping.id` /
  `delta.columnMapping.physicalName` where column mapping is configured and physical-name binding where it is
  not — and keep the existing no-logical-schema fallback clause unchanged. Verify against
  `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` before writing the Delta half, and
  confirm the result does not contradict the owning feature
  `datafusion-scan/scan-execution-field-id-projection`. Do not touch lines 45-48 (INT96 tolerance,
  Iceberg-spec grounding, microsecond-truncation trade-off) — those are correctly Iceberg-scoped.

### crates/lakehouse-engine/src/scan/spec_tests.rs

#### [IMPLEMENTATION_COUPLED_TEST] The new delete-file test re-implements the rule instead of exercising it
- Location: lines 52-81
- Issue: task 8.1 was created because "the scan-side resolution is unasserted" — `shard_paths_tests.rs` covers
  the plan side and `object_store_tests.rs` covers only the rejection side. The test added does not close that
  gap. Its body calls `reconstruct_abs_uri(relative_delete.object_store_path().unwrap(), root)` — the test
  itself performs the composition, then asserts that `reconstruct_abs_uri` behaves as
  `reconstruct_absolute_entry_passes_through` and `reconstruct_relative_entry_normalizes_single_separator`
  (lines 3-45) already prove. No production delete-resolution site is invoked. The three sites that actually
  apply the rule to delete files are `scan/store_router.rs:94` (via `store_path` → `reconstruct_abs_uri`),
  `scan/object_store.rs:561-562`, and `scan/positional_deletes.rs:745`. Any of them could stop joining
  delete paths onto the table root and this test would still pass — a green test over wrong behavior, which is
  the exact condition the scenario "Delete-file relative and absolute paths resolve like data-file paths"
  exists to prevent.
- Fix: In `crates/lakehouse-engine/src/scan/spec_tests.rs`, replace the hand-composed calls in
  `reconstruct_delete_file_entry_resolves_like_a_data_file_entry` with assertions that drive a real scan-side
  delete-resolution site, so the composition is verified rather than reproduced. Prefer constructing a
  `ScanSide` whose single `FileEntry` carries a relative `DeleteMechanism::IcebergPositionalDelete` and
  asserting the resolved owned-path set produced by `scan/store_router.rs`'s ownership construction contains
  the delete path joined onto the table root, plus the absolute-passthrough case; if that seam is not
  reachable from this module, move the test to the sibling `_tests.rs` file of the module that owns the seam
  rather than keeping a helper-only assertion here. Confirm the new test is mutation-sensitive by temporarily
  removing the `reconstruct_abs_uri` call at `scan/store_router.rs:94` and observing the test fail, then
  restore it.
