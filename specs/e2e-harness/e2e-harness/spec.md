# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* **This delta is issue #350.** It adds ONE scenario and ONE seed fixture: an Iceberg table carrying
  populated `list`, `struct`, and `map` columns, queried end to end so the JSON rendering
  `datafusion-scan/nested-json-rendering` specifies is proven against real Parquet data rather than
  against a hand-built Arrow array. No existing scenario changes.
* **No existing Iceberg seed helper writes a nested column.** Every `seed_*` function in
  `crates/lakehouse-engine/tests/common/seed.rs` declares primitive `NestedField`s only, which is why
  the gap this delta closes went untested: the pre-existing unit assertions for the JSON fallback used
  a ZERO-FIELD struct, a shape that sidesteps every field-wise code path.
* **The probe table needs the non-string-keyed map case, which is the one shape the JSON encoder
  cannot render without stringification.** The Iceberg spec permits any key type
  (https://iceberg.apache.org/spec/#nested-types), and `arrow-json`'s map encoder rejects every
  non-`Utf8` key outright, so a fixture without such a column would leave the map-key contract
  covered by unit tests alone.
* **Every requested shape IS writable, including `map<int, string>`, which refutes a stale comment in
  the seed module.** `crates/lakehouse-engine/tests/common/seed.rs` states that complex list/struct
  columns *"are not written here because iceberg-rust does not expose a struct/list writer"*. A live
  probe wrote `list<string>`, `list<int>`, `struct<street, city>`, `map<string, string>`,
  `map<int, string>`, and `list<struct<a: int>>` into one Iceberg Parquet file with iceberg-rust 0.10
  and parquet 58. That comment is corrected by this delta, not worked around.
* **The real obstacle is nested FIELD-ID REASSIGNMENT, and the existing seed helpers cannot absorb
  it.** `iceberg-rest-fixture` assigns fresh field ids on `create_table`, and
  `common::seed::overlay_iceberg_field_ids` repairs only TOP-LEVEL ids, matching them by name — nested
  ids keep the values the test authored. Feeding a batch built from the AUTHORED schema therefore fails
  with `DataInvalid => Field id 9 not found in struct array`. The fixture MUST build its Arrow batch
  from `iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())` AFTER
  `create_table` returns. `create_and_append_files` takes its batches up front and so cannot do this,
  which is why a nested-type seed needs its own create-then-write path rather than that helper.
* **The derived Arrow schema is already correct and carries a `PARQUET:field_id` on every nested
  field** — list elements, struct fields, and map key/value alike — which is what makes the nested
  field-id binding `datafusion-scan/nested-json-rendering` relies on implementable.
* **The new binary MUST be added to the `test-e2e` make target.** That target enumerates its test
  binaries explicitly, so a new E2E binary that is not listed never runs in the suite gate.
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

### Scenario: An Iceberg table's list, struct, and map columns return valid JSON end to end

* *GIVEN* a seeded Iceberg table carrying a primitive control column plus populated nested columns — a `list<string>`, a `list<int>`, a `struct<street: string, city: string>`, a `map<string, string>`, a `map<int, string>`, and a `list<struct<a: int>>` — each with at least one fully-populated row, one row whose nested value is NULL, and one row exercising an empty collection and a null member
* *WHEN* a query selects those columns through the virtual schema
* *THEN* `createVirtualSchema` SHALL declare every nested column as `VARCHAR(2000000)`, verifiable through `SYS.EXA_ALL_COLUMNS`
* *AND* the query SHALL SUCCEED for every nested column, so the recorded `sqlCode 22002` physical-to-logical cast failure for `struct` and `map` is gone
* *AND* every returned nested value SHALL parse as JSON and SHALL equal the exact document `datafusion-scan/nested-json-rendering` specifies for that value — a JSON array for each list, an object keyed by FIELD NAME for the struct, an object keyed by the STRINGIFIED key for both maps
* *AND* the `list<string>` value SHALL return `["hello","world"]` with QUOTED elements, so the recorded Arrow display text `[hello, world]` is gone
* *AND* a NULL nested value SHALL return SQL NULL rather than the text `null`, `{}`, or `[]`
* *AND* the `map<int, string>` value SHALL return its integer keys as JSON object names, proving the stringification path against real Parquet data
* *AND* a WHERE predicate, a GROUP BY key, an ORDER BY key, and `COUNT(DISTINCT)` over a nested column SHALL each return the rows an equivalent comparison over the rendered JSON string returns, so the column behaves as the `VARCHAR(2000000)` Exasol declared for it in every pushdown shape
* *AND* the WHERE case SHALL be written as a REGRESSION test with a discriminating fixture — a predicate matching ONE of several rows, plus a conjunction of a primitive predicate and a nested one — because a predicate over a `list` column today returns EVERY row (`datafusion-scan/nested-json-rendering`), so a fixture whose predicate matches every row would pass against the bug it exists to catch
* *AND* the new E2E binary SHALL be listed in the `test-e2e` make target, so the suite gate runs it
