# Decision Log: add-lakekeeper-permission-shape-coverage

## Interview

**Q:** How should this increment certify the shapes, given #415's recorded spec already says one batch-check covers every shape and a denied table refuses the whole query?
**A:** Live E2E per shape. Two-user E2E runs each shape (row scan, single agg, GROUP BY, COUNT(DISTINCT), TopN, join, both fallback wrappers, empty result) and asserts the allowed user gets rows and the denied user gets the denial.

**Q:** The issue says each side of a pushed join decides separately, but #415 refuses the whole query if any table is denied. Which is intended?
**A:** Keep all-or-nothing. A join is refused if either side is denied, and the error names every denied table. Spec and test both sides individually (user denied on left only, right only, both). "Decides separately" means each side is individually checked and individually named, not partial results.

**Q:** How should the bypass audit land?
**A:** Audit plus guard test. Audit the four named sites, record findings in the plan and decision log, and add a structural test so any code path that loads a table without going through the resolver fails the build. Fix any real bypass found.

**Q:** Where should the documentation go?
**A:** A new `docs/permissions.md`, linked from `docs/security.md` and `docs/index.md`. One page: setup, `USER_MAPPING`, trust model and limits, privilege boundary (#402).

## Design Decisions

### [1] Certify the shapes live, with no production change

- **Decision:** The plan changes no adapter code. It certifies the existing check with four live scenarios in the Lakekeeper E2E suite: every single-table shape, every join case, the listing limitation, and the privilege boundary. A user with the grant must get the rows that the same query returns on a virtual schema without the check, and each shape is confirmed by that user's `EXPLAIN VIRTUAL` text.
- **Alternatives:** Per-shape authorization code (rejected: it contradicts the accepted ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata`, and the audit in [2] finds no shape that needs it). More seam-level tests against the Lakekeeper stand-in (rejected: the recorded scenario "One batch-check per query decides every table before any table is read" already covers every shape at that level, and the interview asked for live proof).
- **Rationale:** #415 already routes every shape through one check. What is missing is live evidence per shape and per join side. A virtual schema without the check over the same warehouse is a result oracle that needs no per-shape expected values.
- **Architecture:** no change: the plan adds tests, test fixtures, and documentation only
- **Promotes to ADR:** no

### [2] The bypass audit finds no path that reads a table without the check

- **Decision:** No fix task. The audit read each named site and every table-reading call in `crates/lakehouse-engine/src`, with Serena references and `grep`, on commit `7b297c6`:
  - `TableScanResolver` (`adapter/pushdown/scan_resolution.rs`) has private fields, so `for_request` (L78) is its only constructor. The Iceberg arm batch-checks every identifier (L98-102) before any `loadTable`. `resolve` (L156) admits the identifier first (L162). It holds the only production calls of `format_reader`, `.resolve_scan`, `.load_table`, and `CatalogSession::resolve`.
  - `for_request` has two callers: `handle_pushdown` (`pushdown/mod.rs:170`, the one single-table identifier) and `plan_join` (`joins/mod.rs:136`, every join leaf). `resolve_one_join_side` (`joins/planning.rs:221`) reads a side only through the shared resolver.
  - `qualified_single_table_fallback_pushdown` (`joins/sql_builders.rs:856`) is synchronous. It takes shards that were already resolved and is called only from `build_dispatch_sql` (`pushdown/mod.rs:306, 374, 421, 442, 462`). `build_dispatch_sql` is synchronous and runs only after `resolver.resolve` in `handle_pushdown`.
  - The grouped-aggregate wrapper fallback is the `RequestShape::GroupByWrapper` arm of `build_dispatch_sql`, which calls the same wrapper. The empty-result path renders the same shape without I/O.
  - `empty_result_sql` (`pushdown/empty_result.rs:18`) is pure. It runs at `pushdown/mod.rs:206` after the single table resolved and at `joins/mod.rs:177` after every side resolved. The check therefore refuses a denied user even when the result would be empty.
  - The top-N path is pure: `detect_topn` (`pushdown/topn.rs`) reads the request and the resolved logical schema. The broadcast-join top-N (`classify_join_window`, `build_broadcast_join_sql`) runs after resolution.
  - Inside `adapter/pushdown/`, only `handle_pushdown`, `plan_join`, `resolve_one_join_side`, the resolver, and the format readers are async. The catalog calls of the readers (`load_table_any_auth`, `read_iceberg_metadata_file`, Unity `temporary_table_credentials`, Glue `partitions`) all sit in `format/` and are reached only through `resolve_scan`.
  - Outside the planner, `list_tables` runs only in `handle_create_virtual_schema` (`adapter/mod.rs:236`), shared by create, refresh, and setProperties. That listing runs as the CONNECTION's identity with no check by design (decision [7] of plan 006), which is the listing limitation that scenario "A table that the user cannot read stays listed and is refused at query time" asserts. The pushdown dispatch (`adapter/mod.rs:110-124`) reads the CONNECTION and may assume an IAM role, but reads no table.
  - `format_reader`, `ScanSource`, and `FormatReader` are `pub` and re-exported from `pushdown/mod.rs:35-37`, and E2E harnesses call them directly. No adapter entry point reaches them except through the resolver. The `.so` exports only the UDF entry points.
  - The scan UDF reads storage with the plan's credentials and runs no check. Running a plan needs `EXECUTE ON SCRIPT` (ADR `plan-visibility-execution-privilege-split`), which scenario "A user without the grant can neither obtain nor run the table's plan" asserts in the permission setup.
- **Alternatives:** Narrow `format_reader` and `ScanSource` to `pub(crate)` (rejected: E2E harnesses and `tests/pushdown_public_surface.rs` use them, and the counted surface belongs to `vs-adapter/pushdown-module-structure`; the guard in [3] covers production callers instead).
- **Rationale:** The interview required an audit against the code, not an assumption. Every named site is synchronous over resolved input, so none can read a table.
- **Promotes to ADR:** no

### [3] A source-scanning test guards the single table-read path

- **Decision:** A unit test in `scan_resolution_tests.rs` reads every production `.rs` file of `crates/lakehouse-engine/src`, skipping `[_-]tests.rs` files and comment lines, and enforces two rules. First, each table-reading call has one permitted owner: `format_reader(`, `.resolve_scan(`, `.load_table(`, and `CatalogSession::resolve(` only in `adapter/pushdown/scan_resolution.rs`; `load_table_any_auth(`, `read_iceberg_metadata_file(`, `.temporary_table_credentials(`, and `.partitions(` only under `adapter/pushdown/format/`; `.list_tables(` only in `adapter/mod.rs`. Second, no file under `adapter/pushdown/` other than `scan_resolution.rs`, `mod.rs`, `joins/mod.rs`, `joins/planning.rs`, and `format/` contains `async fn`, `.await`, or `block_on(`. A pure scanner function, tested on planted violations, decides both rules.
- **Alternatives:** A type-level admission token required by `format_reader` (rejected: `format_reader` is `pub` and called by E2E harnesses, see [2]). A `clippy.toml` `disallowed-methods` list (rejected: it lints test code too, which calls `format_reader` directly, and it cannot express the no-I/O rule for shape modules). Seam tests per shape only (rejected: they cannot catch a shape added later).
- **Rationale:** The first rule catches a new caller of a known table read anywhere in the engine. The second catches a new kind of read in a shape module, because every catalog and storage read is async. A failure message names the file, the line, and the call, and says to route the read through `TableScanResolver::resolve`. `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` and the `include_str!` scan in `adapter/adapter_tests.rs` are precedent for source-scanning tests. The guard enforces the accepted ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata`, which `speq decision-log show` lists. A corollary of an accepted decision is not an ADR (`/speq:adr-rules` rule 3).
- **Consequences:** A legitimate new async helper in a shape module fails the test until its file joins the permitted list with a reason. The test implements the recorded step "the adapter SHALL NOT read any table that the batch-check did not cover, so a query shape added later cannot read an unchecked table" and needs no spec change.
- **Promotes to ADR:** no

### [4] The #414 environment gains the star schema, and one denied user covers every join case

- **Decision:** The authz fixture seeds `fact_orders` and `dim_customer` into the `lakehouse_authz` warehouse under `e2e_lakehouse`, next to `events`, and reconciles grants on both tables. `oidc~lk.allowed@lakehouse.test` reads all three tables. `oidc~lk.denied@lakehouse.test` reads `authz_alpha` and `dim_customer`. The existing users `LK_PERM_ALLOWED` and `LK_PERM_DENIED` run every new test. `fact_orders` joined to `dim_customer` is denied on the left, `dim_customer` joined to `fact_orders` on the right, and `events` joined to `fact_orders` on both sides. A check-off virtual schema over the same warehouse answers each query for comparison.
- **Alternatives:** New Exasol users per join case (rejected: more setup with no added coverage). A new E2E binary (rejected: it needs its own Makefile wiring and duplicates the permission setup).
- **Rationale:** `seed_star_schema_with_auth` and `create_and_append_files` are idempotent, and provisioning re-reconciles every grant, so the fixture stays repeatable. The denied user still holds no grant on `events`, so the recorded scenario "Grants on the mapped principal decide a live Exasol user's query" keeps its meaning.
- **Promotes to ADR:** no

### [5] A join stays all-or-nothing, and its denial names each unreadable table

- **Decision:** A join with any unreadable table is refused whole. The denial names each unreadable table and no readable one. The existing verdict in `PermissionGate::verdict` (`adapter/permission.rs:197`) already lists only the denied tables, without duplicates, so only the live scenario is new.
- **Alternatives:** Partial results from the readable side (rejected in the interview).
- **Rationale:** A partial join would return wrong rows for an inner join. Naming each unreadable table tells the operator which grant is missing.
- **Promotes to ADR:** no

### [6] The listing limitation is asserted, not fixed

- **Decision:** A live scenario asserts that a user without a grant sees the table and its columns after create and after refresh, and is refused at query time.
- **Alternatives:** Filter the listing per user (rejected: create and refresh run once for the owner, and Exasol shows one listing to every reader; #416 scopes this as a limitation to assert).
- **Rationale:** Stating the limitation as tested behavior keeps a later change from altering it silently. The column listing exposes column names and types, which the documentation states.
- **Promotes to ADR:** no

### [7] The privilege boundary is proven live in the permission setup

- **Decision:** A live scenario shows that a user without the grant gets the denial from `EXPLAIN VIRTUAL` and that Exasol rejects that user's submission of the plan `EXPLAIN VIRTUAL` returned to a user with the grant.
- **Alternatives:** Rely on the recorded scenario "A least-privilege reader cannot execute the pushdown plan it can read" (rejected: it runs against a virtual schema without the check, so it shows neither the `EXPLAIN VIRTUAL` denial nor a denied user holding another user's plan).
- **Rationale:** Trust-model limit 3 in the documentation rests on this boundary. AGENTS.md requires a live check for every claim about Exasol behavior, so the documentation cites a test, not an assumption. Every scan reads storage with the CONNECTION's credentials, whoever queries, so a user who may execute the scan and distributor scripts can read any table without the check.
- **Promotes to ADR:** no

### [8] `docs/permissions.md` holds setup, mapping, trust model, and limits

- **Decision:** One new page covers setup, `USER_MAPPING`, what the check covers, the trust model and its three limits, the privilege boundary, the listing limitation, and what is out of scope. `docs/security.md` and `docs/index.md` link to it.
- **Alternatives:** Extend `docs/security.md` (rejected in the interview: the page would mix the CONNECTION privilege model with Lakekeeper setup).
- **Rationale:** An operator who enables the check needs one page. Plan 006 recorded facts that this page must carry: the CONNECTION identity's `manage_grants` also permits writing grants, `USER_MAPPING` holds up to 100,000 characters on Exasol 2025.1.16, and each query adds one batch-check with a 30-second deadline.
- **Promotes to ADR:** no

### [9] No Iceberg table spec section governs this plan

- **Decision:** The plan changes no scan, pushdown rendering, schema, or type handling, so it quotes no Iceberg table spec or Delta protocol section and records no deviation.
- **Alternatives:** none
- **Rationale:** The check calls Lakekeeper's management API (`/management/v1/action/batch-check`), which is outside the Iceberg table spec and outside the Iceberg REST catalog's table operations. The pushdown SQL for an allowed query stays byte-identical to the check-off SQL (recorded scenario "One batch-check per query decides every table before any table is read").
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Storage-credential fact in the spec Background

- **Finding:** Background bullet 4 of the spec delta stated that every scan reads storage with the CONNECTION's credentials. No scenario step depends on that sentence. It is the reason for the privilege boundary, not observable behavior.
- **Direction change:** The sentence is removed from bullet 4, which now states only the privilege boundary. The fact lives in decision [7] Rationale and in task 3.1 item 6 of `docs/permissions.md`.
- **Promotes to ADR:** no
