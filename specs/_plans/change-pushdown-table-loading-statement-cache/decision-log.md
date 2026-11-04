# Decision Log: change-pushdown-table-loading-statement-cache

## Interview

- Q: Cache key: VS name only, name + session/statement id, or name + TTL? → A: VS name only, as in #583.
- Q: Why does direct storage have no load-table abstraction? → A: No kind has a uniform one; pushdown bypasses `CatalogClient`, and every format reader fuses load with pruning.
- Q: Is there a common core for catalog access today? → A: Partly: `CatalogClient` serves CREATE/REFRESH/SET PROPERTIES; pushdown runs a parallel stack (`TableScanResolver::for_request`, `RequestSession`, direct-storage `load_table` stub).
- Q: Goal of the refactor? → A: A clear abstraction: a core loads a table, caches it, then prunes through a common interface.
- Q: What is cached? → A: Only pre-pruning state; each reference prunes fresh with its own filter.
- Q: Pre-pruning state per source? → A: Iceberg: built `Table` (manifest `ObjectCache`); Delta: `SnapshotRef`; Unity Parquet and direct storage: full listing; Glue Hive: partition list (kept-prefix listings stay per reference).
- Q: Which `../PUSHDOWN_PLANNING_PERF.md` issues to fix? → A: Only those the new design solves with little effort: issue 7 (self-join plans twice) and issue 6 (Delta legs replay serially). All others out of scope.
- Q: Behavior change allowed? → A: None, except fixing issues 6 and 7, plus #583's statement-scoped reuse.
- Q: Fold the refactor into #583 or plan separately? → A: Fold into #583: one plan, one PR, refactor tasks first.
- Q: Cache lifetime? → A: One statement per virtual schema, as #583 specifies, with its three rule amendments.
- Q: Keep the direct-storage `CatalogClient::load_table` stub? → A: No. Remove `load_table` from `CatalogClient`; Unity and Glue keep theirs as inherent methods; delete the stub.
- Q: How does each kind load a table for scanning? → A: Each catalog kind's client implements one engine-side scan load that calls its format loaders directly; only the table's format tag is matched; delete `ScanSource`.
- Q: How many catalog-kind matches? → A: One, in `construct_catalog_client`. No special cases or duplicated load paths per kind.
- Q: Keep the checking principal beside the admitted tables? → A: No. One statement runs as one user, and the name-only key already accepts VM reuse; drop it.
- Q: Iceberg manifest cache capacity per kept table? → A: Keep iceberg-rust's default, 32 MB per table; state the memory cost in Impact.
- Q: Move the per-kind input checks (CONNECTION rules, Lakekeeper kind check, CREATE namespace rule) into the clients? → A: No. They stay where they are; the single kind match covers building clients and loading tables.
- Q: Task 3.4's Delta-overlap host test breaks `specs/testing.md` (chained modules, fake call order, timeout). Keep it? → A: Yes, as a scoped exception the plan records, with `join_legs_resolve_concurrently_in_leg_index_order` as precedent.
- Q: Request-count proofs read the proxy log, but an E2E test asserts only on what Exasol shows. How to allow them? → A: Add a rule to `specs/testing.md` § Correctness oracles: a request-count scenario is proved by counting request lines in the local forward proxy's log after a mark.
- Q: Pass the state map into `dispatch`, or keep a process static? → A: A process static in the adapter; tests that send pushdown requests share one lock, like `HOOK_LOCK` and `TELEMETRY_LOCK`. No function takes its state as a parameter today, and testing.md forbids production parameters that exist only for tests.
- Q: Revisit: keep the test lock? → A: No. Each test sets the map it uses, so no lock is needed, and a test that sends a virtual schema name can check what is kept. `adapter_call` passes the one static map into `dispatch`; tests pass their own.

## Design Decisions

### [1] Pushdown loads each table through the catalog client of the one catalog-kind match

- **Decision:** Every request type builds its catalog client at the one catalog-kind match. For pushdown, that client loads a table through one engine-side scan load, which calls the table format's loader directly, so only the loaded table's format tag is matched after the kind. `CatalogClient` has no single-table load.
- **Rationale:** Criteria 1 and 4 of `/speq:adr-rules` rule 2 apply: the decision spans vs-adapter, pushdown-planner, format-readers, and lakehouse-catalog, and it rejects a second per-kind session stack. ADR `one-per-request-scan-source-resolver` rejected `construct_catalog_client` because an abstract client hides the concrete session that the Unity Delta and Glue Hive loads need. Each client's scan load runs on its own concrete session, so that reason no longer holds. ADR digest: `one-per-request-scan-source-resolver` is superseded. `iceberg-rest-requests-run-on-the-catalog-session` still holds, because every Iceberg REST request runs on the client's one `CatalogSession`. `catalog-crate-boundary-at-access-not-file-planning` still holds, because the scan load and the format loaders stay in the engine. Rule 1 re-judgment: `yes` is required, because the entry supersedes an accepted ADR (rule 5).
- **Alternatives:** Keep the resolver's per-kind session enum and the scan-source match: two more kind matches beside `construct_catalog_client`. Keep `CatalogClient::load_table` beside the scan load: every client then has two table loads, and the direct-storage one always fails. Widen `CatalogClient::load_table` to return scan state: file-planning state moves into the catalog crate.
- **Consequences:**
  - A new catalog kind adds one client and one arm in `construct_catalog_client`, plus its CONNECTION rules, and no load path names the kind.
  - Each catalog kind and each table format hides its load behind one engine-side interface.
- **Supersedes:** one-per-request-scan-source-resolver
- **Architecture:** § Components, § Data Flow, § Constraints
- **Promotes to ADR:** yes

### [2] A pushdown table loads once and prunes once per reference

- **Decision:** Each table format resolves a pushdown table in two steps. The load depends on neither the filter nor the declaration and runs once per table. The prune runs once per table reference, with that reference's filter and declared schema.
- **Rationale:** Criteria 2 and 4 apply: the split binds every later table format, and it rejects whole-scan reuse. ADR digest: `format-reader-engine-owns-whole-resolution` is superseded, because a format no longer resolves its whole scan in one call. Rule 1 re-judgment: `yes` is required, because the entry supersedes an accepted ADR (rule 5).
- **Alternatives:** Reuse the whole resolved scan per identifier and filter: a self-join with a different filter per leg would load the table twice. Keep each table's unpruned file list and prune it in memory: the Delta kernel skips files on their statistics, and the scan's file entries keep no statistics.
- **Consequences:**
  - A Delta reference still reads the commit and checkpoint files in its own prune. Perf issue 4 owns that cost.
- **Supersedes:** format-reader-engine-owns-whole-resolution
- **Architecture:** § Components, § Data Flow
- **Promotes to ADR:** yes

### [3] The adapter keeps a virtual schema's pushdown state for one statement, keyed by the virtual schema name

- **Decision:** The adapter keeps a virtual schema's catalog client, its loaded tables, and its permission-checked tables in process-static state. The state is keyed by the virtual schema name alone and lives as long as the adapter VM.
- **Rationale:** Criterion 2 applies: the state binds later work and amends the stateless rule. Exasol runs one adapter VM per statement and virtual schema. The probe of #583 shows this for a UNION ALL, a CTE referenced twice, a join, an `EXISTS` subquery, two virtual schemas in one statement, and a re-run of one query. An E2E test checks the nested statements of one `EXECUTE SCRIPT`. The name key separates virtual schemas that share a VM. ADR digest: no accepted ADR covers adapter state. Rule 1 re-judgment: `yes`, because the exception binds `AGENTS.md`, `specs/mission.md`, and `specs/architecture.md`.
- **Alternatives:** Key the state by session id, statement id, or a TTL: the interview chose the name only, because the VM never outlives its statement.
- **Consequences:**
  - An Exasol version that reuses one adapter VM across statements would serve stale metadata, and the name key does not protect against that.
  - `AGENTS.md`, `specs/mission.md`, and `specs/architecture.md` state the statement-scoped exception to the stateless rule.
- **Architecture:** § Overview, § Components, § Constraints
- **Promotes to ADR:** yes

### [4] The entry point passes the one process-static state map into `dispatch`

- **Decision:** `adapter_call` passes the one process-static state map into `dispatch`. Each test that calls `dispatch` passes a map of its own.
- **Rationale:** Host unit tests run as parallel threads of one process and reuse virtual schema names, so a shared map would leak one test's CONNECTION into another. With its own map, a test needs no lock, and a test that sends a virtual schema name can check what the map kept. The parameter is not test-only: production passes the static map through it.
- **Alternatives:** `dispatch` reads the static itself and tests that send pushdown requests share one lock, as `HOOK_LOCK` and `TELEMETRY_LOCK` do: the tests then run one at a time, and a test cannot see or control the map it uses.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] CatalogClient keeps no table load, and each kind's client loads for pushdown

- **Finding:** `[SCOPE_REDUCTION]` BLOCKER: the plan kept `CatalogClient::load_table`, whose direct-storage implementation always fails, beside the new scan load, so every client carried two table loads. `[INFORMATION_COUPLING]`: each scan load built a kind-specific `ScanSource` only for a second match to read it.
- **Direction change:** `CatalogClient` loses `load_table`. Unity Catalog and Glue keep their single-table load as an inherent method, and the Iceberg REST and direct-storage implementations are deleted. Each client's scan load calls the format loaders directly and matches only the format tag, and `ScanSource` is deleted. The last three lines of the Interview record the user's criteria.
- **Promotes to ADR:** no
