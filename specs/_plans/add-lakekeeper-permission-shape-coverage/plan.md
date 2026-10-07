# Plan: add-lakekeeper-permission-shape-coverage

## Summary

This plan proves end to end, with two real Exasol users, that the Lakekeeper permission check decides every pushdown shape and every side of a join, asserts the listing limitation and the privilege boundary live, guards the single table-read path with a structural test, and documents setup and the trust model in `docs/permissions.md` (#416). It changes no adapter code, because the bypass audit finds no path that reads a table without the check.

## Context

- #415 (recorded plan `006-add-lakekeeper-permission-check`) runs the check in `TableScanResolver::for_request`, and the accepted ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata` makes that point the only table-read path.
- Existing proof per shape is seam-level, against a Lakekeeper stand-in. The only live proof is one row scan by three users (`permission_check_grants_decide_each_mapped_users_query`).
- #416 asks to authorize every referenced table in every shape, to audit four named sites for bypasses, to assert the listing limitation, to run an E2E with two users who hold different grants, and to document setup, the mapping, the trust model, and the privilege boundary.
- The bypass audit is recorded in decision-log.md [2]. No site reads a table outside the resolver, so the plan has no fix task.
- The #414 environment (`make test-e2e-lakekeeper`: Lakekeeper with OpenFGA, Keycloak, SeaweedFS) already holds the permission virtual schema `LK_PERM_LAKEHOUSE` and the users `LK_PERM_ALLOWED` and `LK_PERM_DENIED`. Its `lakehouse_authz` warehouse holds one data table, `events`, so a join needs the star schema seeded there.
- The trust model from #416: the engine enforces and the catalog advises. Storage credentials stay the service account's, an adapter defect means unrestricted access, and enforcement rests on the Exasol privilege boundary (#402, ADR `plan-visibility-execution-privilege-split`).
- Plan 006 recorded facts that operators need and no document states yet: the CONNECTION identity's `manage_grants` also permits writing grants, `USER_MAPPING` holds up to 100,000 characters on Exasol 2025.1.16, and each query adds one batch-check with a 30-second deadline. No page in `docs/` mentions `PERMISSION_CHECK`.
- The plan changes no architecture, so it carries no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-permission-check | CHANGED | `specs/_plans/add-lakekeeper-permission-shape-coverage/vs-adapter/lakekeeper-permission-check/spec.md` |

## Impact

- Query behavior does not change. Every statement in the spec delta describes behavior that #415 shipped; this plan proves it live and states it.
- Operators get `docs/permissions.md`: setup, `USER_MAPPING`, what the check covers, the trust model and its limits, the privilege boundary, and the listing limitation.
- `make test-e2e-lakekeeper` gains four tests. The `lakehouse_authz` warehouse gains the tables `fact_orders` and `dim_customer`, and the mapped principals' grants change: `oidc~lk.allowed@lakehouse.test` reads `events`, `fact_orders`, and `dim_customer`, and `oidc~lk.denied@lakehouse.test` reads `authz_alpha` and `dim_customer`.
- `cargo test` gains a structural test that fails when production code reads a table outside `TableScanResolver::resolve`, or when a pushdown shape module performs async I/O.
- Breaking: none.

## Dependencies

- #415 is merged to main (PR #457). The plan branch is `feat/add-lakekeeper-permission-shape-coverage`, off main.
- The implementing commit references `Closes #416`.

## Implementation Tasks

### Group A: Live certification in the Lakekeeper E2E suite

- [ ] 1.1 Extend the authz fixture in `crates/lakehouse-engine/tests/common/lakekeeper_authz.rs`. In `provision_authz_fixture`, seed the star schema into `WAREHOUSE_AUTHZ` with `seed_star_schema_with_auth`, using the token pattern of `ensure_seeded_events_table`, and record the `table-uuid` of `fact_orders` and `dim_customer` (namespace `E2E_NAMESPACE`) in `table_ids`. Add `Scope::Table(E2E_FACT_TABLE)` and `Scope::Table(E2E_DIM_TABLE)` to `ALL_SCOPES`. Set `MAPPED_ALLOWED` to `select` on `events`, `fact_orders`, and `dim_customer`, and `MAPPED_DENIED` to `select` on `authz_alpha` and `dim_customer`. In `lakekeeper_authz_tests.rs`, add a unit test that every fixture table has its table scope in `ALL_SCOPES`, so a stale grant on any fixture table is reconciled.
- [ ] 1.2 In `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`, extend `permission_setup` with a result oracle: the check-off virtual schema `LK_PERM_UNCHECKED` with CONNECTION `LK_PERM_UNCHECKED_CREDS`, operator credentials, the `lakehouse_authz` warehouse, `E2E_NAMESPACE`, and no `PERMISSION_CHECK`. Only SYS queries it. Drop it with the other permission objects at the start of setup. Add local helpers: sorted rows of a query as string tuples; a denial assertion that takes the user, the principal, the unreadable qualified identifiers that must appear, and the readable ones that must not, and runs `assert_no_secret_or_token`; and shape markers over `explain_virtual_sql` text. The markers: a row scan has `LAKEHOUSE_SCAN` and none of `"aggregates"`, `group_keys`, `"order_by":`, `LHS_T0`; a single-group aggregate has `"aggregates"` and no `group_keys`; a grouped aggregate has `group_keys` and `PARTIAL_`; a COUNT(DISTINCT) fan-out has `COUNT(DISTINCT` and no `LHS_T0`; a top-N has `"order_by":`; the qualified wrapper has `LHS_T0`; an empty result has `FROM DUAL` and no `LAKEHOUSE_SCAN`; joins use `has_broadcast_join_block` and `has_n_scan_wrapper` from `common/e2e_harness.rs`. Keep the markers in this file, so no other suite changes.
- [ ] 1.3 Write `permission_check_decides_every_single_table_shape_per_user` with `/// Scenario: Every single-table pushdown shape returns rows only to a user whose principal holds the grant`. Run each query over `LK_PERM_LAKEHOUSE.EVENTS`, and the same text over `LK_PERM_UNCHECKED.EVENTS` as SYS for the oracle:
  - row scan: `SELECT id, name, score FROM <vs>.EVENTS WHERE score > 15`
  - single-group aggregate: `SELECT COUNT(*), SUM(score), MIN(id) FROM <vs>.EVENTS`
  - grouped aggregate: `SELECT event_date, COUNT(*) FROM <vs>.EVENTS GROUP BY event_date`
  - COUNT(DISTINCT): `SELECT COUNT(DISTINCT name) FROM <vs>.EVENTS`
  - top-N: `SELECT id, score FROM <vs>.EVENTS ORDER BY score DESC, id LIMIT 3`
  - qualified wrapper: `SELECT COUNT(DISTINCT name), COUNT(DISTINCT event_date) FROM <vs>.EVENTS`
  - grouped query through the wrapper: `SELECT event_date FROM <vs>.EVENTS GROUP BY event_date ORDER BY SUM(score)`
  - empty result: `SELECT id FROM <vs>.EVENTS WHERE id > 1000`

  For each query, assert that the `LK_PERM_ALLOWED` user's `EXPLAIN VIRTUAL` shows the shape's marker; when a query takes another shape, change the query, not the marker. Assert that the allowed user's sorted rows equal the oracle's. Assert that `LK_PERM_DENIED` gets the denial naming `LK_PERM_DENIED`, `MAPPED_DENIED`, and `e2e_lakehouse.events`.
- [ ] 1.4 Write `permission_check_names_each_unreadable_table_of_a_join` with `/// Scenario: Each table of a join is checked and named on its own`. Joins, over `LK_PERM_LAKEHOUSE` and over `LK_PERM_UNCHECKED` for the oracle:
  - unreadable on the left: `join_query(<vs>)` (`fact_orders` left, `dim_customer` right)
  - unreadable on the right: `SELECT c.C_NAME, o.O_ORDERDATE FROM <vs>.DIM_CUSTOMER c JOIN <vs>.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY`
  - unreadable on both sides: `SELECT e.name, o.O_ORDERDATE FROM <vs>.EVENTS e JOIN <vs>.FACT_ORDERS o ON e.id = o.O_ORDERKEY`
  - unaccelerated wrapper: `events` joined to `fact_orders` on `e.id = o.O_ORDERKEY` and to `dim_customer` on `o.O_CUSTKEY = c.C_CUSTKEY`

  Assert from the allowed user's `EXPLAIN VIRTUAL` that the left and right joins carry `has_broadcast_join_block` and the three-table join carries `has_n_scan_wrapper(_, 3)`; when a join takes another path, change the query. Assert that the allowed user's sorted rows equal the oracle's for every join. Assert that the denied user's denial names `e2e_lakehouse.fact_orders` and not `dim_customer` for the left and right joins, and names `e2e_lakehouse.events` and `e2e_lakehouse.fact_orders` and not `dim_customer` for the both-sides and three-table joins.
- [ ] 1.5 Write `permission_check_lists_unreadable_tables_and_refuses_their_queries` with `/// Scenario: A table that the user cannot read stays listed and is refused at query time`. As `LK_PERM_DENIED`, read `SYS.EXA_ALL_TABLES` and `SYS.EXA_ALL_COLUMNS` for `LK_PERM_LAKEHOUSE`, and assert that `EVENTS` and its columns `ID`, `NAME`, `SCORE`, `EVENT_DATE`, and `EVENT_TS` appear. Query `EVENTS` and assert the denial. Then run `ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE REFRESH` as SYS and repeat both checks as `LK_PERM_DENIED`.
- [ ] 1.6 Write `permission_check_denied_user_can_neither_explain_nor_run_the_plan` with `/// Scenario: A user without the grant can neither obtain nor run the table's plan`. As in `the_reader_cannot_execute_the_pushdown_plan_it_captured` (`tests/e2e_credential_exposure_test.rs`), assert from `EXA_DBA_OBJ_PRIVS`, `EXA_DBA_SYS_PRIVS`, and `EXA_DBA_ROLE_PRIVS` that neither `LK_PERM_ALLOWED` nor `LK_PERM_DENIED` holds `EXECUTE` on the adapter, scan, or distributor script, `EXECUTE ANY SCRIPT`, or a role. Capture the allowed user's `isolated_pushdown_statement` for `SELECT id, name FROM LK_PERM_LAKEHOUSE.EVENTS WHERE score > 15`. As `LK_PERM_DENIED`, assert that `EXPLAIN VIRTUAL` of the same query gets the denial, and that `try_execute` of the captured statement fails with `insufficient privileges for calling script` and passes `assert_no_secret_or_token`.
- [ ] 1.7 Start the Lakekeeper stack yourself (see the `test-e2e-lakekeeper` comment in the `Makefile`) and run `make test-e2e-lakekeeper`. Every test passes and none is ignored, the recorded `permission_check_grants_decide_each_mapped_users_query` and `authz_fixture_provisioning_*` tests included under the new grants.

### Group B: Structural guard of the table-read path

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs`, write the self-test `chokepoint_scanner_flags_planted_table_reads` first, then a pure scanner function over `(path relative to src/, file text)` pairs that returns one `path:line: call` entry per violation. It skips lines whose trimmed text starts with `//`. Rule one: a table-read call outside its owner. `format_reader(`, `.resolve_scan(`, `.load_table(`, and `CatalogSession::resolve(` belong to `adapter/pushdown/scan_resolution.rs`; `load_table_any_auth(`, `read_iceberg_metadata_file(`, `.temporary_table_credentials(`, and `.partitions(` belong under `adapter/pushdown/format/`; `.list_tables(` belongs to `adapter/mod.rs`. Rule two: `async fn`, `.await`, or `block_on(` in a file under `adapter/pushdown/` other than `scan_resolution.rs`, `mod.rs`, `joins/mod.rs`, `joins/planning.rs`, and `format/`. The self-test asserts that planted `format_reader(` in `adapter/pushdown/topn.rs`, `.load_table(` in `adapter/mod.rs`, `load_table_any_auth(` in `adapter/pushdown/grouped_agg.rs`, and `.await` in `adapter/pushdown/joins/sql_builders.rs` are each reported, and that a `//` comment naming a call, the definition `fn load_table(`, and each call in its owner file are not.
- [ ] 2.2 Write `no_production_code_reads_a_table_outside_the_scan_resolver` with `/// Scenario: One batch-check per query decides every table before any table is read`. It walks `concat!(env!("CARGO_MANIFEST_DIR"), "/src")` recursively with `std::fs`, skips files matching `[_-]tests.rs`, runs the scanner, and asserts no violation. It also asserts that the walk read `adapter/pushdown/scan_resolution.rs`, so a wrong root cannot pass with no files. The failure message lists every violation and says to route the read through `TableScanResolver::resolve`, or, for async code that reads no table, to add the file to the permitted list with a reason.

### Group C: Documentation

- [ ] 3.1 Write `docs/permissions.md` with the breadcrumb header of the other pages (`[lakehouse-engine](../README.md) › [Docs](index.md) › Permissions`). Sections, in order:
  1. Summary: the check refuses a query over a table that the querying user's Lakekeeper principal may not read. The engine enforces and the catalog advises. The check is stronger than no check and weaker than enforcement inside the catalog.
  2. Requirements: the Iceberg REST catalog kind, a catalog URI that ends in `/catalog` (a path-rewriting gateway is not supported), and a Lakekeeper server with an authorization backend such as OpenFGA. Every other catalog kind gets `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind`.
  3. Setup: grant the CONNECTION's identity `manage_grants` on the warehouse or namespace, which includes `can_read_assignments`, with a management API example (`POST /management/v1/permissions/warehouse/<warehouse-id>/assignments`, body `{"writes":[{"type":"manage_grants","user":"oidc~<subject>"}]}`), and show the 403 `CannotInspectPermissions` refusal that appears without it. Give `CREATE VIRTUAL SCHEMA ... WITH PERMISSION_CHECK = 'LAKEKEEPER' USER_MAPPING = '...'` and `ALTER VIRTUAL SCHEMA ... SET` examples. State that Lakekeeper grants must name each mapped principal, or that `LAKEKEEPER__OPENID_SUBJECT_CLAIM` must name a claim whose value the mapping reproduces. State that a querying user needs only `CREATE SESSION` and `SELECT ON SCHEMA`.
  4. `USER_MAPPING`: the `user` variable, uppercase for an undelimited name; the trimmed output is the Lakekeeper user id, `oidc~<subject>` for an OIDC login. Examples from the recorded spec: lowercase plus a domain, a multi-line conditional with a fixed service-account id, a lookup table, and one principal shared by several BI accounts. The strict-name form `{% if user == user|upper %}...{% endif %}`. The refusals: no current user, a render failure, an empty id, or an id with whitespace or a control character. A compile error rejects create, refresh, and SET. The template author is trusted, and the adapter checks no principal's uniqueness or owner. The template holds up to 100,000 characters (measured on Exasol 2025.1.16).
  5. What is checked: every table of every pushdown shape, `EXPLAIN VIRTUAL` included. A join with any unreadable table is refused whole, and the denial names each unreadable table. A missing table looks denied. Include an example denial message. Each query sends one batch-check with a 30-second deadline, and a Lakekeeper or management API outage refuses every query of the virtual schema.
  6. Trust model and limits, each limit with its consequence: (1) storage credentials stay the CONNECTION's service account's, vended credentials included, and are not scoped per user; (2) an adapter defect means unrestricted access, not a refused query, because the engine is the only enforcement point; (3) enforcement rests on the Exasol privilege boundary. Also state that every Lakekeeper privilege that permits the check also permits writing grants, so the CONNECTION's client secret is a grant-administration credential.
  7. Privilege boundary: never grant `EXECUTE` on `LAKEHOUSE_SCAN` or `LAKEHOUSE_DISTRIBUTE_FILES`, or `EXECUTE ANY SCRIPT`, to a querying user. A user without the table grant gets the denial from `EXPLAIN VIRTUAL` and cannot run a plan obtained from another user. Link `security.md#plan-visibility-versus-plan-execution` and cite #402.
  8. Listing limitation: create and refresh list as the CONNECTION's identity, so every user who may query the virtual schema sees every table name, column name, and column type, and a query over an unreadable table is refused.
  9. Out of scope: row-level and column-level authorization, which need a policy engine; other catalog kinds; per-user storage credentials.

  Every behavior statement must match a passing test of this plan or plan 006, or a measurement recorded in plan 006's verification report. Write nothing that no test or measurement shows.
- [ ] 3.2 Link the page. In `docs/security.md`, add a section "Per-user table permissions" after "Plan visibility versus plan execution": two or three sentences stating that `PERMISSION_CHECK = 'LAKEKEEPER'` adds a per-user table check that rests on the plan-execution boundary above, with a link to `permissions.md`. In `docs/index.md`, add a "Permissions" row after "Security" in the Documentation table, and name the page in the "Deploying for the first time?" bullet.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Live certification | 1.1-1.7 | none | spec delta `vs-adapter/lakekeeper-permission-check`; decision-log [4]-[7]; `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`, `crates/lakehouse-engine/tests/common/lakekeeper_authz.rs`, `crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs`; read-only `crates/lakehouse-engine/tests/common/{e2e_harness.rs,seed.rs}`, `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` |
| B: Table-read guard | 2.1-2.2 | none | recorded scenario "One batch-check per query decides every table before any table is read" (`specs/vs-adapter/lakekeeper-permission-check/spec.md`); decision-log [2], [3]; `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` |
| C: Documentation | 3.1-3.2 | A (the page states what A's tests prove) | decision-log [7], [8]; `docs/permissions.md`, `docs/security.md`, `docs/index.md`; read-only `specs/_recorded/006-add-lakekeeper-permission-check/{plan.md,decision-log.md,verification-report.md}` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | none | The plan adds tests, fixtures, and documentation. The resolver's `TableAdmission` refusal stays as the runtime guard that the structural test complements. |

## Open Questions

- No GitHub issue tracks row-level and column-level authorization (`gh issue list` searched for "row-level", "column-level", "OPA", and "policy engine"). `docs/permissions.md` states that scope without an issue link. Cite the issue (#TBD) if one exists.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Every single-table pushdown shape returns rows only to a user whose principal holds the grant | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_decides_every_single_table_shape_per_user` |
| Each table of a join is checked and named on its own | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_names_each_unreadable_table_of_a_join` |
| A table that the user cannot read stays listed and is refused at query time | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_lists_unreadable_tables_and_refuses_their_queries` |
| A user without the grant can neither obtain nor run the table's plan | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_denied_user_can_neither_explain_nor_run_the_plan` |
| One batch-check per query decides every table before any table is read (recorded, unchanged; added test) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `no_production_code_reads_a_table_outside_the_scan_resolver` |
| Scanner self-test for the row above | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `chokepoint_scanner_flags_planted_table_reads` |
| Grants on the mapped principal decide a live Exasol user's query (recorded, unchanged; must pass under the new grants) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_grants_decide_each_mapped_users_query` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-permission-check | `make test-e2e-lakekeeper` | Every test passes, and none is reported ignored |
| lakekeeper-permission-check | `exapump sql "SELECT c.C_NAME FROM LK_PERM_LAKEHOUSE.FACT_ORDERS o JOIN LK_PERM_LAKEHOUSE.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY" -d "exasol://LK_PERM_DENIED:LkPermDenied2026x@localhost:28563?validateservercertificate=0"` | An error that names `LK_PERM_DENIED`, `oidc~lk.denied@lakehouse.test`, and `e2e_lakehouse.fact_orders`, and does not name `dim_customer` |
| lakekeeper-permission-check | The same command as `LK_PERM_ALLOWED:LkPermAllowed2026x` | 10 rows |
| lakekeeper-permission-check | `exapump sql "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = 'LK_PERM_LAKEHOUSE'" -d "exasol://LK_PERM_DENIED:LkPermDenied2026x@localhost:28563?validateservercertificate=0"` | `DIM_CUSTOMER`, `EVENTS`, and `FACT_ORDERS` |
| lakekeeper-permission-check | `exapump sql "EXPLAIN VIRTUAL SELECT id FROM LK_PERM_LAKEHOUSE.EVENTS" -d "exasol://LK_PERM_DENIED:LkPermDenied2026x@localhost:28563?validateservercertificate=0"` | The denial, and no pushdown SQL |
| Documentation | Open `docs/index.md` and `docs/security.md` and follow each new link | Each link opens `docs/permissions.md` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e-lakekeeper` | 0 failures, 0 ignored |
| Lint | `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| Lint (Lakekeeper E2E) | `cargo clippy --all-targets --features lakekeeper-e2e -- -D warnings` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Plan | `speq plan validate add-lakekeeper-permission-shape-coverage` | Pass |
