# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* Every E2E scenario runs against a local Exasol Docker container over MinIO and MUST fail (never skip) when the stack is unavailable.
* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* The file-pruning E2E seeds a partitioned Iceberg table whose data files are distributed
  across partition values, so a partition-column predicate can prune whole files.
* See `e2e-harness/e2e-harness-grouped-order` for grouped-aggregate cases that deliberately
  place an aggregate before, between, or after the group keys in the `selectList` — the
  arrangement every case in this spec avoids.
* The provisioning helpers (SLC install, `.so` upload, script and Virtual Schema creation)
  are defined once in a shared `common/e2e_harness` module and reused by every E2E binary;
  per-binary variation is passed as explicit parameters.
* The harness sends Exasol's own `resultSetMaxRows` default (`0`, no limit) unless a call site declares a cap, and that uncapped default is kept so a plan-shape test is never silently perturbed by an undeclared row cap. A declared cap is NOT merely a result-delivery choice: on a real query execution it reaches the adapter as a `pushdownRequest` `limit`, for every statement shape measured. `EXPLAIN VIRTUAL` can never show this — it is a separate exchange from a real statement's pushdown request, so its echo cannot carry a limit only the real statement gained. Since issue #307 a pushed `limit` no longer disqualifies broadcast: a bare `LIMIT` and a bare-projected-column `ORDER BY` over a broadcast-eligible inner equi-join both stay on the broadcast path. Only the four surviving forcing conditions — an aggregate select item, a non-empty `GROUP BY`, `aggregationType = "group_by"`, or a non-null `HAVING` — plus a `limit` offset with no `orderBy` and an unrenderable or unprojected sort key still move a join onto the unaccelerated two-scan fallback (`vs-adapter/pushdown-planning-join`), with no `EXPLAIN VIRTUAL`-visible sign that it happened. The measured shape matrix, the per-shape adapter behavior, and the `EXPLAIN VIRTUAL` blind spot are recorded in `docs/debugging-pushdown.md`.
* The harness reads a result set to completion. A result set larger than one `fetch`
  response is retrieved across successive fetches, never truncated to the first response.

## Scenarios

### Scenario: End-to-end projection + filter + LIMIT query returns correct rows

* *GIVEN* the Docker stack is running with a seeded Iceberg table in the REST catalog over MinIO
* *AND* the Rust SLC and the `.so` are installed and the virtual schema is created
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns
* *AND* the returned values SHALL match the seeded source data

### Scenario: E2E suite fails when the stack is unavailable

* *GIVEN* the Exasol container is not reachable
* *WHEN* the `exasol-e2e` test suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL

* *GIVEN* an Exasol Docker container with the VS installed and a `parallelism_factor` VS property set
* *WHEN* an `EXPLAIN VIRTUAL` of a multi-shard scan query is executed
* *THEN* the EXPLAIN VIRTUAL output SHALL show a nested distributor subquery grouping on `shard_key` (not `IPROC()`) that drives `LAKEHOUSE_DISTRIBUTE_FILES`, wrapped by an outer ungrouped scalar `LAKEHOUSE_SCAN` invocation
* *AND* the outer scalar scan select SHALL NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: Harness provisions the scalar scan and the LUA distributor scripts

* *GIVEN* the E2E harness bootstrapping the lakehouse VS on the Exasol Docker container
* *WHEN* the harness creates the scan-path scripts
* *THEN* the harness SHALL create `LAKEHOUSE_SCAN` as a SCALAR SCRIPT (EMITS its dynamic output columns) referencing the uploaded `.so`
* *AND* the harness SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET SCRIPT that passes each shard's `files` VARCHAR through unchanged, referencing no `.so`
* *AND* an end-to-end projection/filter query over the installed scripts SHALL return results identical to the single-node DataFusion equivalent (grouped/nested-aggregate coverage lives in `e2e-harness/e2e-harness-grouped-agg`)
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: End-to-end filtered query over a partitioned table returns correct rows with file pruning

* *GIVEN* the Docker stack is running with a seeded **partitioned** Iceberg table in the REST catalog over MinIO, whose data files are distributed across partition values
* *AND* the lakehouse VS adapter and scan UDF are installed
* *WHEN* a `SELECT` with a `WHERE` predicate on the partition column (and a second predicate on a value column) is issued against the virtual schema
* *THEN* the returned rows SHALL exactly match the seeded source rows satisfying the predicate, and SHALL be identical to the same query run with Iceberg pruning unable to apply (predicate forced untranslatable)
* *AND* where the harness can observe it (Iceberg `plan_files` output during file resolution), the resolved file list SHALL contain fewer files than the unpruned snapshot file count
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: Every E2E binary provisions the scan path from one shared harness definition

* *GIVEN* the `exasol-e2e` test binaries under `crates/lakehouse-engine/tests`, each with its own `OnceLock`-guarded setup
* *AND* a single shared `common/e2e_harness` module defining the SLC install, the `.so` upload, the script creation, and the Virtual Schema creation
* *WHEN* any binary's setup provisions the lakehouse VS scan path
* *THEN* the binary SHALL install `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and the adapter script from that shared definition, so the script DDL is byte-identical across every binary
* *AND* the per-binary Virtual Schema properties that vary (VS name, Iceberg namespace, catalog CONNECTION name, `PARALLELISM_FACTOR`, `JOIN_BROADCAST_MAX_BYTES`) SHALL be supplied as explicit parameters rather than by re-declaring the provisioning logic
* *AND* an end-to-end query through any binary's Virtual Schema SHALL return results identical to the single-node DataFusion equivalent, and the affected tests MUST fail (not skip) when the Exasol Docker container or MinIO is unavailable

### Scenario: Harness statements carry no row cap the test did not declare

* *GIVEN* the E2E harness connected to the Exasol Docker container with the lakehouse VS installed, and no row cap declared at the call site
* *WHEN* a bare projection statement carrying no SQL `LIMIT` is issued against the virtual schema
* *THEN* the statement SHALL carry `resultSetMaxRows` `0` — Exasol's own documented "no limit" default
* *AND* the scan spec the adapter generates for that statement MUST NOT carry a `limit`
* *AND* the statement SHALL return every seeded row that satisfies it, never a truncated prefix
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: A declared row cap truncates the returned row count

* *GIVEN* the same harness and virtual schema, one connection declaring a row cap of `n` smaller than the seeded table's row count and one connection declaring no cap
* *WHEN* the identical bare projection statement carrying no SQL `LIMIT` is issued through each connection
* *THEN* the cap-declaring connection SHALL return exactly `n` rows
* *AND* the no-cap connection SHALL return the table's full row count
* *AND* this scenario SHALL NOT be read as a claim about the pushdown request either connection generates — `EXPLAIN VIRTUAL` cannot observe whether a real execution's request carries a `limit`, since it is a separate exchange from the real statement; see `docs/debugging-pushdown.md` for what is actually known about that request
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: Harness returns every row of a result set larger than one fetch response

* *GIVEN* the harness connected with no declared row cap, and a seeded table read under a `numBytes` fetch budget smaller than the bytes its result set occupies, so the result set cannot fit in one `fetch` response
* *WHEN* a test reads that table's rows through the harness result-reading helper
* *THEN* the helper SHALL issue successive `fetch` requests until the rows it has accumulated reach the count the result-set metadata reports in `numRows`
* *AND* the helper SHALL return exactly that row count
* *AND* the helper MUST NOT return a silently truncated column set — it SHALL fail loudly if a response returns zero rows while rows remain outstanding
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
