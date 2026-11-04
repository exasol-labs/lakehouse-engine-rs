# Code Review Findings: add-export-into-script-entry-points

## Summary
- Files reviewed: 25
- Total findings: 6 (standard: 6, expert: 0)
- Evidence: `cargo test --workspace --lib export::` passes 17 of 17 tests.

## Standard fixes

### crates/lakehouse-engine/src/export/sql.rs

#### [OUTDATED_COMMENT] The group-key comment claims an error ordering that the measurement disproves
- Location: lines 38-39, in `WriterSelect::render`
- Issue: The comment says the per-row key is used "even at G = 1, so the guard error precedes Exasol's own type error". Decision [2] in `decision-log.md` measured the opposite. The guard races Exasol's type check: a one-row table with five unsupported columns gave the guard error in 6 of 10 runs and Exasol's error in 4 of 10. The same decision rejects a literal key because it "gives no better ordering" than the per-row key, so the ordering is not the reason for the key. The second sentence of the comment, about the top-level key, is accurate.
- Fix: In `crates/lakehouse-engine/src/export/sql.rs` `WriterSelect::render`, delete the sentence "A per-row key even at G = 1, so the guard error precedes Exasol's own type error." Keep the comment as the single line `// The key stays top-level: an export SELECT over a subquery of DUAL fails.`

### crates/lakehouse-engine/src/export/mod.rs

#### [INFORMATION_LEAKAGE] The missing-grant error restates the installer's role name and assumes the user holds it
- Location: lines 87-92, in `require_connection_access`; `crates/lakehouse-engine/src/export/export_tests.rs` line 162
- Issue: The error ends with "or to the role LAKEHOUSE_EXPORT_ROLE_<schema> that <user> holds". The role name is a choice of the installer template (`export_role` in `print_next_step_template`, `deploy/scripts/install.sh:1353`). The Rust message restates it, and no test ties the two copies together. The spec scenario "A missing script-scoped CONNECTION grant fails with a named error" requires only the CONNECTION, `LAKEHOUSE_EXPORT`, and the user. The message also states that the user holds the role, which is false for a user granted the privileges directly, such as `NO_ACCESS_EXPORTER` in `crates/lakehouse-e2e/tests/e2e_local/export.rs`. The second `ctx.script_schema()` call at line 91 exists only to build the role name.
- Fix: In `crates/lakehouse-engine/src/export/mod.rs` `require_connection_access`, change the last clause of the format string from `or to the role LAKEHOUSE_EXPORT_ROLE_{} that {user} holds` to `or to a role that {user} holds`, and remove the `ctx.script_schema()` format argument after the string. In `crates/lakehouse-engine/src/export/export_tests.rs` `hook_without_script_grant_names_connection_script_user_and_grant`, remove the `"LAKEHOUSE_EXPORT_ROLE_LHVS"` entry from the fragment list. Run `cargo test --workspace --lib export::`.

### crates/lakehouse-engine/src/export/options.rs

#### [SIDE_EFFECT] `writer_layout` also validates TABLE_NAME
- Location: line 99, in `WithOptions::writer_layout`; line 33, in `ExportOptions::from_spec`
- Issue: `writer_layout` computes the writer layout, and its name announces nothing else. Its first statement is `check_table_name(self.table_name)?`, so a missing or malformed TABLE_NAME fails inside the writer-layout query. A reader of `ExportOptions::from_spec` sees no TABLE_NAME check at all.
- Fix: In `crates/lakehouse-engine/src/export/options.rs`, remove the `check_table_name(self.table_name)?;` line from `WithOptions::writer_layout`. In `ExportOptions::from_spec`, replace `let writers = WithOptions::collect(&spec.parameters)?.writer_layout()?;` with three statements: `let with_options = WithOptions::collect(&spec.parameters)?;`, `check_table_name(with_options.table_name)?;`, and `let writers = with_options.writer_layout()?;`. The check order stays the same, so `options_tests.rs` needs no change. Run `cargo test --workspace --lib export::options`.

### deploy/scripts/install.sh

#### [OUTDATED_COMMENT] The printed export block tells the operator that SYS needs the export role
- Location: line 1409, in `print_next_step_template`
- Issue: The template prints "one role serves every exporting user, SYS included". SYS needs no export grant. The same template prints "SYS holds every CONNECTION implicitly; skip the grants." at line 1376. The local E2E suite exports as `sys` (`exa_conn`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs:131-133`) with no export role or grant, and those exports pass. The spec requires the block to be printed for a SYS installing user. It does not require the claim that SYS exporters need the role.
- Fix: In `deploy/scripts/install.sh` `print_next_step_template`, change the line `emit "-- Export (EXPORT ... INTO SCRIPT $schema.LAKEHOUSE_EXPORT): one role serves every exporting user, SYS included."` to `emit "-- Export (EXPORT ... INTO SCRIPT $schema.LAKEHOUSE_EXPORT): one role serves every exporting user. SYS needs no grant."`. Run `make test-install`.

### crates/lakehouse-e2e/tests/e2e_local/export.rs

#### [MAGIC_NUMBER] The no-access user's grants depend on the position of the ACCESS grant
- Location: lines 110-123, in `exporters`
- Issue: `for privilege in &privileges[..3]` grants `NO_ACCESS_EXPORTER` every privilege except the ACCESS grant. This works only because the ACCESS grant is the last of the four array entries. Reordering the array silently changes which privilege the user lacks, and `export_missing_script_grant_fails_named` would then test a different missing grant.
- Fix: In `crates/lakehouse-e2e/tests/e2e_local/export.rs` `exporters`, take the `ACCESS ON CONNECTION {conn} FOR SCRIPT {script}` entry out of `privileges` into its own binding `let access_grant = format!("ACCESS ON CONNECTION {conn} FOR SCRIPT {script}");`. Grant each entry of the three-entry `privileges` array to both `role` and `NO_ACCESS_EXPORTER`. Grant `access_grant` to `role` only. Remove the `[..3]` slice.

### crates/lakehouse-e2e/tests/cloud_e2e_test.rs

#### [INFORMATION_LEAKAGE] The writer-count measurement and the integer parser are copied into the cloud suite
- Location: `cloud_e2e_test.rs` lines 1202-1238 (`cloud_int`, `cloud_profiled_export`); `crates/lakehouse-e2e/tests/e2e_local/export.rs` lines 157-174 (`profiled_export`)
- Issue: Two test binaries carry the same protocol for reading a writer count: turn PROFILE on, run the export and read `rowCount`, turn PROFILE off, run `FLUSH STATISTICS`, query `OUT_ROWS` of the one `GROUP BY` part of `EXA_USER_PROFILE_LAST_DAY`, and destructure exactly one row. `docs/export.md` states the same protocol a third time. A change to the generated SELECT's shape, such as a different profile part name, needs the same edit in both helpers. `cloud_int` is a line-for-line copy of `parse_int` (`crates/lakehouse-e2e/tests/common/e2e_harness.rs:455`). The cloud binary cannot import it today, because `e2e_harness` is not compiled under the `cloud-e2e` feature. `common/exasol_ws.rs` is compiled under both features.
- Fix: Move `parse_int` from `crates/lakehouse-e2e/tests/common/e2e_harness.rs` into `crates/lakehouse-e2e/tests/common/exasol_ws.rs` unchanged, and re-export it from `e2e_harness.rs` with `pub use super::exasol_ws::parse_int;`, next to the existing `pub use super::exasol_ws::error_text;`. In `exasol_ws.rs`, add `pub fn profiled_export_counts(conn: &mut ExaConn, export_sql: &str) -> (i64, i64)`, with a one-line doc comment stating that it returns the exported row count and the writer count of one export run in a fresh session. Give it the body that both helpers share: PROFILE on, execute and read `rowCount`, PROFILE off, `FLUSH STATISTICS`, the `OUT_ROWS` query, and the one-part destructure with `parse_int`. Reduce `profiled_export` in `e2e_local/export.rs` to `profiled_export_counts(&mut exa_conn(), &export_sql(source, options))`. Reduce `cloud_profiled_export` in `cloud_e2e_test.rs` to building its `ExaConn` and SQL and calling `profiled_export_counts`. Delete `cloud_int`. Compile both binaries: `cargo test --workspace --features lakehouse-e2e/exasol-e2e --test e2e_local --no-run` and `cargo test --workspace --features lakehouse-e2e/cloud-e2e --test cloud_e2e_test --no-run`.

## Expert fixes
[none]
