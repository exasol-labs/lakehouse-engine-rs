# Plan: add-lakekeeper-permission-shape-coverage

## Summary

This plan finishes #416 on top of main (PR #457): it proves live that the Lakekeeper permission check decides the pushdown shapes and join sides that main's tests leave out, asserts the listing limitation, and documents setup, the mapping, and the trust model. It changes no adapter code, because the bypass audit finds no path that reads a table without the check.

## Context

- #415 (PR #457, on main) runs the check in `TableScanResolver::for_request`, before any shape is dispatched. `TableScanResolver::resolve` refuses a table that the check did not cover (ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata`), and two unit tests in `scan_resolution_tests.rs` cover that refusal.
- #416 asks for five things: authorize every table in every shape, with each join side deciding separately; audit four named sites for bypasses; assert the listing limitation; run a two-user E2E on the #414 environment; document setup, the mapping, the trust model, and the privilege boundary.
- Main's Lakekeeper E2E suite (`crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`) already proves these cases live: the row scan, the single-group aggregate, `EXPLAIN VIRTUAL`, a view, a revoked grant, a describe-only grant, and inherited grants. It also proves a join where the user cannot read the right table, with join pushdown on and off.
- Main's suite leaves these cases unproven live: the grouped aggregate, COUNT(DISTINCT), top-N, the qualified fallback wrapper, the empty result, a join where the user cannot read the left table or either table, and the listing limitation. The wrapper is reached in two ways: through two COUNT(DISTINCT) items, and through a grouped query that the adapter cannot decompose.
- Main proves the privilege boundary live. `permission_check_refuses_explain_virtual_without_a_grant` shows that a user without the grant cannot obtain the plan. `the_reader_cannot_execute_the_pushdown_plan_it_captured` (`e2e_credential_exposure_test.rs`) shows that a reader cannot run the plan.
- Main's `lakehouse_authz` warehouse holds `events`, `fact_orders`, and `dim_customer`. The check-off virtual schema `LK_STATIC_LAKEHOUSE` holds the same seeded `events` rows.
- `docs/security.md` § "Lakekeeper permission check (#415)" states what the check covers, that the listing runs as the CONNECTION's identity, and the `EXECUTE` bypass. No page documents setup, `USER_MAPPING`, or the trust-model limits.
- Decision [2] records the bypass audit. The plan changes no architecture, so it carries no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| lakekeeper-permission-check | CHANGED | `specs/_plans/add-lakekeeper-permission-shape-coverage/vs-adapter/lakekeeper-permission-check/spec.md` |

## Impact

- Query behavior does not change. The spec delta states behavior that #415 shipped, and this plan proves it live.
- `make test-e2e-lakekeeper` gains two tests and extends the join test. The single-group aggregate test `permission_check_gates_an_aggregate_query` becomes one case of the new shape test. The suite adds no Exasol user, Lakekeeper principal, warehouse table, or virtual schema.
- The Lakekeeper section of `docs/security.md` gains setup, `USER_MAPPING`, coverage, the trust model and its limits, and what is out of scope. `docs/index.md` and `docs/catalogs.md` link to it.
- No production line changes, so the changed-line coverage gate has nothing to measure.
- Breaking: none.

## Dependencies

- #415 is merged to main (PR #457). The plan branch `feat/add-lakekeeper-permission-shape-coverage` is current with `origin/main`.
- The implementing commit references `Closes #416`.

## Implementation Tasks

### Group A: Live certification in the Lakekeeper E2E suite

All tasks edit `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` and reuse its users, virtual schemas, and helpers (`grant_only`, `select_table`, `assert_refused`, `qualified`, `events_query`, `enumerated_table_names`).

- [ ] 1.1 Extend `permission_check_refuses_a_join_unless_the_user_may_read_both_tables`. On both `VS_PERMISSION` (broadcast join) and `VS_PERMISSION_NO_JOIN` (unaccelerated join wrapper), `JOIN_ONE_USER` runs `join_query(vs)` under three grant sets, each set with `grant_only`:
  - `select` on `fact_orders` only: the denial names `qualified(E2E_DIM_TABLE)` and not `qualified(E2E_FACT_TABLE)`. Main already tests this case.
  - `select` on `dim_customer` only: the denial names `fact_orders` and not `dim_customer`.
  - no grant: the denial names both tables.

  Keep the `JOIN_BOTH_USER` assertions unchanged. The join text stays fixed, so the three cases deny each table of one request and do not depend on how Exasol orders the join sides.
- [ ] 1.2 Write `permission_check_decides_every_single_table_shape` with `/// Scenario: One batch-check per query decides every table before any table is read` and `/// Scenario: A denied table refuses the whole query`. Delete `permission_check_gates_an_aggregate_query`, whose query becomes the first case below. Give `ALLOWED_USER` `select` on `events` and `DENIED_USER` `select` on `authz_alpha` with `grant_only`. Drive one case table. Each case holds a query over `<vs>.EVENTS`, the markers that the allowed user's `EXPLAIN VIRTUAL` text must contain, and the markers it must not contain:
  - single-group aggregate: `SELECT COUNT(*), SUM(id) FROM <vs>.EVENTS`
  - grouped aggregate: `SELECT event_date, COUNT(*) FROM <vs>.EVENTS GROUP BY event_date`
  - lone COUNT(DISTINCT) fan-out: `SELECT COUNT(DISTINCT name) FROM <vs>.EVENTS`
  - top-N: `SELECT id, score FROM <vs>.EVENTS ORDER BY score DESC, id LIMIT 3`
  - qualified fallback wrapper: `SELECT COUNT(DISTINCT name), COUNT(DISTINCT event_date) FROM <vs>.EVENTS`
  - grouped query through the wrapper: `SELECT event_date FROM <vs>.EVENTS GROUP BY event_date ORDER BY SUM(score)`
  - empty result: `SELECT id FROM <vs>.EVENTS WHERE id > 1000`

  For each case, assert three things:
  - The allowed user's `explain_virtual_sql` over `VS_PERMISSION` matches the case's markers.
  - The allowed user's sorted rows over `VS_PERMISSION` equal SYS's sorted rows over `VS_STATIC`. The `VS_STATIC` rows are not empty, except in the empty-result case.
  - `assert_refused(DENIED_USER, <query over VS_PERMISSION>, &[&qualified(E2E_TABLE)], &[])` holds.

  Read each marker from the live `EXPLAIN VIRTUAL` text before you pin it. If a query takes another shape, change the query, not the marker. Candidate markers:
  - single-group aggregate: `"aggregates"`, without `group_keys`
  - grouped aggregate: `group_keys` and `PARTIAL_`
  - fan-out: `"distinct":true`, without `LHS_T0`
  - top-N: a non-empty `"order_by"`
  - both wrappers: `LHS_T0`
  - empty result: no `LAKEHOUSE_SCAN`

  Add a local `sorted_rows(conn, sql) -> Vec<Vec<String>>` on top of `value_to_string` from `common/e2e_harness.rs`.
- [ ] 1.3 Write `permission_check_lists_unreadable_tables_and_refuses_their_queries` with `/// Scenario: A table that the user cannot read stays listed and is refused at query time`. Give `DENIED_USER` `select` on `authz_alpha` only. Run the checks twice, first on `VS_PERMISSION` as `permission_setup` created it, then after SYS runs `ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE REFRESH`. As `DENIED_USER`, each run asserts three things:
  - `enumerated_table_names` lists `EVENTS`.
  - The user's `SYS.EXA_ALL_COLUMNS` rows for `LK_PERM_LAKEHOUSE.EVENTS` equal SYS's rows, and they are not empty.
  - `assert_refused` holds for `events_query(VS_PERMISSION)`.
- [ ] 1.4 Start the Lakekeeper stack yourself (see the comment above `test-e2e-lakekeeper` in the `Makefile`) and run `make test-e2e-lakekeeper`. Every test passes and none is ignored. Run `cargo clippy --all-targets --features lakekeeper-e2e -- -D warnings`.

Test budget: about 90 added and 15 removed lines of E2E test code, and no unit tests, because no production code changes.

### Group B: Documentation

- [ ] 2.1 Extend `docs/security.md` § "Lakekeeper permission check (#415)". Keep the heading, so existing anchors keep working, and keep its two paragraphs. Add these short subsections, about 50 lines in all:
  1. Setup. The check needs the Iceberg REST catalog kind, a catalog URI that ends in `/catalog` (a path-rewriting gateway is not supported), and a Lakekeeper server with an authorization backend such as OpenFGA. Grant the CONNECTION's identity `manage_grants` on the warehouse or the namespace. Without that grant, every query fails with 403 `CannotInspectPermissions`. The same grant lets the identity write grants, so the CONNECTION's client secret is a grant-administration credential. Give one `CREATE VIRTUAL SCHEMA ... WITH PERMISSION_CHECK = 'LAKEKEEPER' USER_MAPPING = '...'` example and one `ALTER VIRTUAL SCHEMA ... SET` example. Lakekeeper grants name each mapped principal. Alternatively, `LAKEKEEPER__OPENID_SUBJECT_CLAIM` names a claim whose value the mapping reproduces.
  2. `USER_MAPPING`. The `user` variable is uppercase for an undelimited name. The trimmed output is the Lakekeeper user id, which is `oidc~<subject>` for an OIDC login. Give two examples from the spec: `oidc~{{ user|lower|replace("_", ".") }}@corp.net`, and one principal shared by several BI accounts. A query is refused when the request names no user, when the template fails to render, or when the id is empty or holds a whitespace or control character. A template that does not compile rejects create, refresh, and SET. The template author is trusted: the adapter checks no principal's uniqueness or owner. A template holds up to 100,000 characters.
  3. Coverage. The check covers every table of every pushdown shape. A join with any unreadable table is refused whole, and the denial names each unreadable table and no readable one. A missing table looks denied. Show one denial message from the 1.1 run. Each query sends one batch-check with a 30-second deadline, so a Lakekeeper outage refuses every query of the virtual schema. The listing shows every user who may query the virtual schema every table name, column name, and column type.
  4. Trust model and limits. The engine enforces, and the catalog advises. The check is stronger than no check and weaker than enforcement inside the catalog. Each limit states its consequence:
     - Storage credentials stay the CONNECTION's, vended credentials included, and are not scoped per user.
     - An adapter defect means unrestricted access, not a refused query.
     - Enforcement rests on the Exasol privilege boundary. Link the existing bypass paragraph and § "Privilege model". Also state that the virtual schema's owner, and any user who holds `ALTER ANY VIRTUAL SCHEMA`, can set `PERMISSION_CHECK` off or change `USER_MAPPING`.
  5. Out of scope: row-level and column-level authorization, which need a policy engine; other catalog kinds; per-user storage credentials.

  Every statement must match evidence. Write nothing that no evidence shows. The accepted evidence is a passing test in the repository, a constant in the code, a measurement recorded in a plan, or a live `exapump` check. Evidence for the facts that no test of this plan covers:
  - the subject claim: the plan 005 probe (`specs/_recorded/005-test-lakekeeper-batch-check-fixtures-two-principals/plan.md`)
  - the 100,000 characters: the plan 006 verification report (Exasol 2025.1.16)
  - the 30-second deadline: `BATCH_CHECK_DEADLINE` in `crates/lakehouse-catalog/src/lakekeeper.rs`
  - storage credentials: `scan_storage_for` in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, which builds the scan's storage from the CONNECTION or from the vended credentials that the CONNECTION's identity received

  Before you write the `ALTER ANY VIRTUAL SCHEMA` sentence, use `exapump` to confirm that `LK_PERM_ALLOWED` cannot run `ALTER VIRTUAL SCHEMA LK_PERM_LAKEHOUSE SET PERMISSION_CHECK = ''`.
- [ ] 2.2 In `docs/index.md`, add the Lakekeeper per-user permission check to the Security row's description. In `docs/catalogs.md` § "Lakekeeper (OIDC via Keycloak + SeaweedFS)", add one sentence that links `security.md#lakekeeper-permission-check-415`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Live certification | 1.1-1.4 | none | spec delta `vs-adapter/lakekeeper-permission-check`; decision-log [1], [4]-[6]; `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`; read-only `crates/lakehouse-engine/tests/common/{e2e_harness.rs,lakekeeper_authz.rs}` |
| B: Documentation | 2.1-2.2 | A (the section quotes a denial from A's run and cites A's tests) | decision-log [7], [8]; `docs/security.md`, `docs/index.md`, `docs/catalogs.md`; read-only `specs/_recorded/005-test-lakekeeper-batch-check-fixtures-two-principals/plan.md`, `specs/_recorded/006-add-lakekeeper-permission-check/verification-report.md`, `crates/lakehouse-catalog/src/lakekeeper.rs` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`: `permission_check_gates_an_aggregate_query` | Its query becomes the single-group aggregate case of `permission_check_decides_every_single_table_shape` |

## Open Questions

- No GitHub issue tracks row-level and column-level authorization. `gh issue list` was searched for "row-level", "column-level", "OPA", and "policy engine". The docs state that scope without an issue link. Cite the issue (#TBD) if one exists.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A denied table refuses the whole query (CHANGED) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_refuses_a_join_unless_the_user_may_read_both_tables`, `permission_check_decides_every_single_table_shape` |
| A table that the user cannot read stays listed and is refused at query time (NEW) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_lists_unreadable_tables_and_refuses_their_queries` |
| One batch-check per query decides every table before any table is read (recorded, unchanged; new live test) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `permission_check_decides_every_single_table_shape` |

### Manual Testing

Run these after `make test-e2e-lakekeeper`, which leaves `LK_PERM_ALLOWED` with `select` on `events` and `LK_PERM_DENIED` with `select` on `authz_alpha`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| lakekeeper-permission-check | `make test-e2e-lakekeeper` | Every test passes, and none is reported ignored |
| lakekeeper-permission-check | `exapump sql "SELECT event_date, COUNT(*) FROM LK_PERM_LAKEHOUSE.EVENTS GROUP BY event_date" -d "exasol://LK_PERM_ALLOWED:LkPermAllowed2026x@localhost:28563?validateservercertificate=0"` | The same rows as the query over `LK_STATIC_LAKEHOUSE.EVENTS` as SYS |
| lakekeeper-permission-check | The same command as `LK_PERM_DENIED:LkPermDenied2026x` | An error that names `LK_PERM_DENIED`, `oidc~lk.denied@lakehouse.test`, and `e2e_lakehouse.events` |
| lakekeeper-permission-check | `exapump sql "SELECT c.C_NAME FROM LK_PERM_LAKEHOUSE.FACT_ORDERS o JOIN LK_PERM_LAKEHOUSE.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY" -d "exasol://LK_PERM_DENIED:LkPermDenied2026x@localhost:28563?validateservercertificate=0"` | An error that names `e2e_lakehouse.fact_orders` and `e2e_lakehouse.dim_customer` |
| lakekeeper-permission-check | `exapump sql "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = 'LK_PERM_LAKEHOUSE'" -d "exasol://LK_PERM_DENIED:LkPermDenied2026x@localhost:28563?validateservercertificate=0"` | `DIM_CUSTOMER`, `EVENTS`, and `FACT_ORDERS` |
| Documentation | Open `docs/index.md` and `docs/catalogs.md` and follow each new link | Each link opens the Lakekeeper section of `docs/security.md` |

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
