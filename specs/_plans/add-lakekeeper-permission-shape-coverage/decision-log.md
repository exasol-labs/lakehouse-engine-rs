# Decision Log: add-lakekeeper-permission-shape-coverage

## Interview

**Q:** How should this increment certify the shapes, given #415's recorded spec already says one batch-check covers every shape and a denied table refuses the whole query?
**A:** Live E2E per shape. Two-user E2E runs each shape (row scan, single agg, GROUP BY, COUNT(DISTINCT), TopN, join, both fallback wrappers, empty result) and asserts the allowed user gets rows and the denied user gets the denial. Narrowed by decision [1] to the shapes that main does not prove.

**Q:** The issue says each side of a pushed join decides separately, but #415 refuses the whole query if any table is denied. Which is intended?
**A:** Keep all-or-nothing. A join is refused if either side is denied, and the error names every denied table. Spec and test both sides individually (user denied on left only, right only, both). "Decides separately" means each side is individually checked and individually named, not partial results.

**Q:** How should the bypass audit land?
**A:** Audit plus guard test. Audit the four named sites, record findings in the plan and decision log, and add a structural test so any code path that loads a table without going through the resolver fails the build. Fix any real bypass found. The guard test is superseded by decision [3].

**Q:** Where should the documentation go?
**A:** A new `docs/permissions.md`, linked from `docs/security.md` and `docs/index.md`. One page: setup, `USER_MAPPING`, trust model and limits, privilege boundary (#402). Superseded by decision [8].

## Design Decisions

### [1] Certify live only the shapes and join sides that main does not prove

- **Decision:** The plan changes no adapter code. Live tests cover the grouped aggregate, the lone COUNT(DISTINCT) fan-out, top-N, the qualified fallback wrapper (reached through two COUNT(DISTINCT) items and through a grouped query that the adapter cannot decompose), the empty result, and the left-side and both-sides join denials. The single-group aggregate test from main becomes one case of the new shape test. The allowed user's `EXPLAIN VIRTUAL` text confirms each shape. The allowed user's rows must equal the rows of the same query over `LK_STATIC_LAKEHOUSE`, which runs without the check.
- **Alternatives:** A new shape scenario in the spec delta (rejected: the recorded scenarios "One batch-check per query decides every table before any table is read" and "A denied table refuses the whole query" already state that the check covers every shape, and the new test maps to them). Per-shape authorization code (rejected: it contradicts the accepted ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata`, and [2] finds no shape that needs it). Shape predicates in `common/e2e_harness.rs` (rejected: the shape test is their only caller, and the markers fit as data in its case table).
- **Rationale:** The check runs once, in `TableScanResolver::for_request`, before any shape is dispatched. What #416 still lacks is live evidence for each shape. `LK_STATIC_LAKEHOUSE` holds the same seeded `events` rows, so it answers each query for comparison without a new virtual schema.
- **Architecture:** no change: the plan adds tests and documentation only
- **Promotes to ADR:** no

### [2] The bypass audit finds no path that reads a table without the check

- **Decision:** No fix task. The audit read each site that #416 names and every table-reading call in `crates/lakehouse-engine/src`, with `grep` and Serena references, on main commit `ebbf0c2`:
  - `TableScanResolver` (`adapter/pushdown/scan_resolution.rs`) has private fields, so `for_request` is its only constructor. The Iceberg arm batch-checks every identifier before any `loadTable`. `resolve` admits the identifier through `TableAdmission` first. The resolver holds the only production calls of `format_reader`, `.resolve_scan`, `.load_table`, and `CatalogSession::resolve`.
  - `for_request` has two callers: `handle_pushdown` (`pushdown/mod.rs`, the one single-table identifier) and `plan_join` (`joins/mod.rs`, every join leaf). `resolve_one_join_side` (`joins/planning.rs`) reads a side only through the shared resolver.
  - `qualified_single_table_fallback_pushdown` (`joins/sql_builders.rs`) is synchronous and takes shards that were already resolved. Its only callers are five call sites in `build_dispatch_sql`, which is synchronous and runs only after `resolver.resolve` in `handle_pushdown`.
  - The grouped-aggregate wrapper fallback is the `RequestShape::GroupByWrapper` arm of `build_dispatch_sql`, which calls the same wrapper.
  - `empty_result_sql` (`pushdown/empty_result.rs`) is pure. It runs in `handle_pushdown` after the single table resolved and in `plan_join` after every side resolved. The check therefore refuses a denied user even when the result would be empty.
  - The top-N path is pure: `detect_topn` (`pushdown/topn.rs`) reads the request and the resolved logical schema. The broadcast-join top-N runs after resolution.
  - The format readers hold the remaining catalog and storage reads (`load_table_any_auth`, `read_iceberg_metadata_file`, Unity `temporary_table_credentials`, Glue `partitions`, and the Parquet directory listings). The resolver reaches them only through `resolve_scan`.
  - Outside the planner, `list_tables` runs only in `handle_create_virtual_schema` (`adapter/mod.rs`), shared by create, refresh, and setProperties. That listing runs as the CONNECTION's identity with no check by design (decision [7] of plan 006). The new listing scenario asserts this limitation. The direct-storage listing (`adapter/direct_storage.rs`) serves the same create path for a catalog kind on which the check is refused.
  - `format_reader`, `ScanSource`, and `FormatReader` are `pub` for the E2E harnesses. No adapter entry point reaches them except through the resolver.
  - The scan UDF reads storage with the plan's credentials and runs no check. Running a plan needs `EXECUTE ON SCRIPT`, which decision [7] covers.
- **Alternatives:** Narrow `format_reader` and `ScanSource` to `pub(crate)` (rejected: the E2E harnesses and `tests/pushdown_public_surface.rs` use them, and the counted public surface belongs to `vs-adapter/pushdown-module-structure`).
- **Rationale:** The interview required an audit against the code, not an assumption. Every named site is synchronous over resolved input, so none of them can read a table.
- **Promotes to ADR:** no

### [3] No structural guard of the table-read path

- **Decision:** The plan adds no source-scanning test.
- **Alternatives:** A unit test that scans production sources for table-reading calls outside their owner files (rejected, see Rationale). A `clippy.toml` `disallowed-methods` list (rejected: it lints test code too, which calls `format_reader` directly).
- **Rationale:** Main already refuses, at run time, a table that the batch-check did not cover. `TableScanResolver::resolve` admits a table only through `TableAdmission`, and two unit tests cover that refusal: `an_enforced_resolver_admits_only_the_tables_the_check_covered` and `an_enforced_check_admits_an_allowed_table_and_refuses_a_denied_one`. The accepted ADR states the consequence: a future bypass becomes a refusal instead of an unchecked read. This repository's test strategy excludes unit tests that read source files. Review round 1 also showed that such a scanner would match only nine call names and would exempt the dispatch files where a new shape is most likely added.
- **Promotes to ADR:** no

### [4] The tests reuse main's users, tables, and virtual schemas

- **Decision:** The plan adds no Exasol user, Lakekeeper principal, warehouse table, virtual schema, or fixture grant. `JOIN_ONE_USER` covers each join side by switching its grants with `grant_only`. `ALLOWED_USER` and `DENIED_USER` run the shape and listing tests. `LK_STATIC_LAKEHOUSE` answers the shape comparison.
- **Alternatives:** Seed the star schema and widen the mapped principals' fixture grants (rejected: main already seeds `fact_orders` and `dim_customer` into `lakehouse_authz` and reconciles their scopes). A check-off virtual schema over the authz warehouse (rejected: `LK_STATIC_LAKEHOUSE` already holds the same rows). New users for each join case (rejected: `grant_only` resets one user's grants per case).
- **Rationale:** Each test sets its users' grants with `grant_only` first, so the tests stay independent under `--test-threads=1`. The join text stays fixed while the grants change, so the three join cases deny each table of one request and do not depend on how Exasol orders the join sides.
- **Promotes to ADR:** no

### [5] A join stays all-or-nothing, and its denial names each unreadable table and no readable one

- **Decision:** A join with any unreadable table is refused whole. The denial names each unreadable table and no readable one. The spec delta adds the second part to the recorded scenario "A denied table refuses the whole query". `PermissionGate::verdict` (`adapter/permission.rs`) already lists only the denied tables, and main's join test asserts it for the right side only.
- **Alternatives:** Partial results from the readable side (rejected in the interview). A separate join scenario (rejected: the recorded denial scenario already covers a request over several tables, and one added clause states the per-side naming).
- **Rationale:** A partial inner join would return wrong rows. Naming each unreadable table tells the operator which grant is missing.
- **Promotes to ADR:** no

### [6] The listing limitation is asserted, not fixed

- **Decision:** A live scenario asserts that a user without a grant sees the table and its columns after create and after refresh, and is refused at query time. The recorded Background already names the limitation and stays unchanged.
- **Alternatives:** Filter the listing per user (rejected: create and refresh run once as the CONNECTION's identity, and Exasol shows one listing to every reader. #416 scopes this as a limitation to assert).
- **Rationale:** A tested limitation cannot change silently. The column listing exposes column names and types, which the documentation states.
- **Promotes to ADR:** no

### [7] The privilege boundary needs no new test

- **Decision:** The plan adds no privilege-boundary test. The documentation cites main's tests.
- **Alternatives:** A test in which the denied user submits the plan that `EXPLAIN VIRTUAL` returned to the allowed user (rejected: main already proves both halves, see Rationale).
- **Rationale:** `permission_check_refuses_explain_virtual_without_a_grant` proves that a user without the grant cannot obtain the plan. `the_reader_cannot_execute_the_pushdown_plan_it_captured` proves that a reader with only `CREATE SESSION` and `SELECT` on the virtual schema cannot run a captured plan. That second proof also holds with the check on, for two reasons. Exasol's script-execute privilege does not depend on virtual-schema properties. The recorded scenario "One batch-check per query decides every table before any table is read" makes an allowed plan byte-identical to the check-off plan. The users of the permission setup hold only `CREATE SESSION` and `SELECT ON SCHEMA`. Every scan reads storage with the CONNECTION's credentials, whoever queries, so the check rests on this boundary.
- **Promotes to ADR:** no

### [8] The documentation extends `docs/security.md` instead of adding a page

- **Decision:** The Lakekeeper section of `docs/security.md` gains setup, `USER_MAPPING`, coverage, the trust model and its limits, and what is out of scope. `docs/index.md` and `docs/catalogs.md` link to it. The plan adds no `docs/permissions.md`.
- **Alternatives:** A new `docs/permissions.md` (rejected: main's `docs/security.md` § "Lakekeeper permission check (#415)" already states the coverage, the listing behavior, and the `EXECUTE` bypass, so a new page would duplicate or move them and also need a new index row).
- **Rationale:** Extending the existing section is the smaller change and keeps one place for the check. Four facts that no test of this plan covers have this evidence:
  - the subject claim: the plan 005 probe
  - the 100,000-character template limit: plan 006's verification report (Exasol 2025.1.16)
  - the 30-second batch-check deadline: `BATCH_CHECK_DEADLINE` in `crates/lakehouse-catalog/src/lakekeeper.rs`
  - the shared storage credentials: `scan_storage_for` in `adapter/pushdown/support.rs`
- **Promotes to ADR:** no

### [9] No Iceberg table spec section governs this plan

- **Decision:** The plan changes no scan, pushdown rendering, schema, or type handling, so it quotes no Iceberg table spec or Delta protocol section and records no deviation.
- **Alternatives:** none
- **Rationale:** The check calls Lakekeeper's management API (`/management/v1/action/batch-check`), which is outside the Iceberg table spec and outside the Iceberg REST catalog's table operations. The pushdown SQL for an allowed query stays byte-identical to the check-off SQL (recorded scenario "One batch-check per query decides every table before any table is read").
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Storage-credential fact in the spec Background

- **Finding:** Background bullet 4 of the spec delta stated that every scan reads storage with the CONNECTION's credentials. No scenario step depends on that sentence. It is the reason for the privilege boundary, not observable behavior.
- **Direction change:** The spec delta changes no Background. It carries the recorded Background without a delta marker, only because `speq plan validate` requires the section. The fact lives in decision [7] and in the trust-model limits of task 2.1.
- **Promotes to ADR:** no

### [plan-review] Resync with main after PR #457

- **Finding:** PR #457 (#415) merged work that the plan still treated as new. Main seeds the star schema into `lakehouse_authz`. Main's Lakekeeper suite proves a right-side join denial with join pushdown on and off, a view, a revoked grant, a describe-only grant, inherited grants, the `EXPLAIN VIRTUAL` denial, and a single-group aggregate. Main's resolver refuses a table that the check did not cover, and `docs/security.md` documents the coverage, the listing behavior, and the `EXECUTE` bypass.
- **Direction change:** The plan now covers only the part of #416 that main lacks.
  - Removed: the authz fixture extension (old task 1.1); the check-off virtual schema `LK_PERM_UNCHECKED`; the privilege-boundary test and its scenario (old task 1.6, now decision [7]); the structural guard (old tasks 2.1 and 2.2, now decision [3]); `docs/permissions.md` (now decision [8]).
  - Removed from the spec delta: the Background change; the scenarios "Every single-table pushdown shape returns rows only to a user whose principal holds the grant" (main's recorded scenarios state it), "Each table of a join is checked and named on its own" (now one clause in the recorded denial scenario), and "A user without the grant can neither obtain nor run the table's plan".
  - Kept: the live shapes that main lacks (task 1.2), the left-side and both-sides join denials (task 1.1), the listing limitation (task 1.3), the bypass audit (decision [2]), and the documentation (tasks 2.1 and 2.2).
- **Promotes to ADR:** no

### [plan-review] Round-1 advisories after the resync

- **Finding:** Review round 1 raised ten advisories against the earlier plan.
- **Direction change:** Each advisory is now applied or no longer applies:
  - The guard's coverage and the `FIXTURE_TABLES` constant no longer apply, because the guard and the fixture task are removed.
  - Join side order: applied. Task 1.1 keeps one join text and changes only the grants.
  - Evidence attribution: applied. Task 2.1 and decision [8] name the evidence for the subject claim, the deadline, the template limit, and the shared storage credentials.
  - The ambiguous join WHEN step and the re-emitted Background bullet 6 no longer apply, because the delta drops the join scenario and changes no Background.
  - Docs privilege boundary: applied in part. Task 2.1 states that the owner and holders of `ALTER ANY VIRTUAL SCHEMA` can turn the check off, after a live `exapump` check. It links § "Privilege model" for the script-scoped CONNECTION grant. No test shows a bypass through `EXECUTE` on `LAKEHOUSE_ADAPTER`, so the docs do not name it.
  - Shape markers in the harness: not applied, see decision [1].
  - Line numbers in decision [2]: applied. The audit cites symbols at `ebbf0c2` instead.
  - The wrapper's name: applied. The plan uses "qualified fallback wrapper", the recorded name.
  - Test count in Impact, Summary length, and the size of task 1.2: applied.
- **Promotes to ADR:** no
