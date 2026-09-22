# Feature: End-to-End Harness — Scan Correctness

End-to-end assertions that the lakehouse VS query path returns correct ROWS — projection, filter,
LIMIT, oversubscribed shard fan-out, partition pruning, nested JSON columns, and timestamp
precision — against a local Exasol Docker container, run through the same
`e2e-harness/e2e-harness` provisioning and shared harness definition. Split out of that feature
once its scenario count crossed this library's per-spec organization threshold; that sibling
feature keeps harness provisioning, the fail-fast contract, row-cap/fetch-paging mechanics, and
the Exasol-version CI gate.

## Background

* The nested-column E2E fixture seeds an Iceberg table carrying `list`, `struct`, and `map`
  columns (including a non-`Utf8`-keyed map) to prove JSON rendering against real Parquet
  data. The fixture builds its Arrow batch from the catalog-assigned schema AFTER
  `create_table`, because the REST catalog reassigns nested field ids and the existing
  `overlay_iceberg_field_ids` helper repairs only top-level ids.
* The file-pruning E2E seeds a partitioned Iceberg table whose data files are distributed
  across partition values, so a partition-column predicate can prune whole files.
* Timestamp precision E2E fixtures seed values that differ only below millisecond resolution,
  with every fourth fractional digit below 5 so truncation and round-to-nearest agree at
  millisecond resolution on the 8.x arm.
* Each standalone E2E fixture needs its OWN namespace and virtual schema (per the
  `e2e_non_ascii_identifier_test` precedent) and MUST be listed in the `test-e2e` make target.
* Expected precision MUST be derived from the LIVE session (not an environment variable) and
  MUST be an independent oracle (not a call into the production version parser).
* Two seeded tables span the type mix: `typed_distinct_probe` (long, decimal, double, string,
  date, timestamp, boolean) and `complex_probe` (list, struct, map declared `VARCHAR(2000000)`).
  The `ExaType::Int32` bin is reached via a projected `CAST(<col> AS DECIMAL(5,0))`, since no
  catalog-declared column in these fixtures bins to `Int32`.

## Scenarios

### Scenario: End-to-end projection + filter + LIMIT query returns correct rows

* *GIVEN* the Docker stack is running with a seeded Iceberg table in the REST catalog over MinIO
* *AND* the Rust SLC and the `.so` are installed and the virtual schema is created
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns
* *AND* the returned values SHALL match the seeded source data

### Scenario: Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL

* *GIVEN* an Exasol Docker container with the VS installed and a `parallelism_factor` VS property set
* *WHEN* an `EXPLAIN VIRTUAL` of a multi-shard scan query is executed
* *THEN* the EXPLAIN VIRTUAL output SHALL show a nested distributor subquery grouping on `shard_key` (not `IPROC()`) that drives `LAKEHOUSE_DISTRIBUTE_FILES`, wrapped by an outer ungrouped scalar `LAKEHOUSE_SCAN` invocation
* *AND* the outer scalar scan select SHALL NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end filtered query over a partitioned table returns correct rows with file pruning

* *GIVEN* the Docker stack is running with a seeded **partitioned** Iceberg table in the REST catalog over MinIO, whose data files are distributed across partition values
* *AND* the lakehouse VS adapter and scan UDF are installed
* *WHEN* a `SELECT` with a `WHERE` predicate on the partition column (and a second predicate on a value column) is issued against the virtual schema
* *THEN* the returned rows SHALL exactly match the seeded source rows satisfying the predicate, and SHALL be identical to the same query run with Iceberg pruning unable to apply (predicate forced untranslatable)
* *AND* where the harness can observe it (Iceberg `plan_files` output during file resolution), the resolved file list SHALL contain fewer files than the unpruned snapshot file count
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

### Scenario: Microsecond-distinct Iceberg timestamps round-trip at the declared precision

* *GIVEN* a live Exasol instance, MinIO, and an Iceberg REST catalog
* *AND* an Iceberg table seeded into its OWN namespace — invisible to every other suite's `createVirtualSchema` enumeration — carrying an `id` column, a `timestamp` column, and a `timestamptz` column, each timestamp column holding FOUR values that differ ONLY below millisecond resolution: `2024-01-01 00:00:00.000001`, `.000002`, `.123456`, and `.123457`, every one of whose fourth fractional digit is below 5 so truncation and round-to-nearest agree at millisecond resolution
* *AND* a virtual schema created over that namespace through a real `createVirtualSchema`
* *AND* the running engine's own version, read from the LIVE session and mapped to an expected precision by a test-owned table that MUST NOT call the production version rule it exists to check
* *WHEN* an Exasol user projects both timestamp columns and counts their distinct values through the virtual schema
* *THEN* `SYS.EXA_ALL_COLUMNS` SHALL report BOTH columns' `COLUMN_TYPE` as EXACTLY `TIMESTAMP(6)` when the expected precision is 6 and EXACTLY `TIMESTAMP` when it is 3, matched in full rather than by prefix — the prefix tolerance the recorded assertions use is precisely what made the truncation invisible
* *AND* at the microsecond precision the projected values SHALL render all SIX seeded fractional digits for every row of BOTH columns, and `COUNT(DISTINCT)` over each column SHALL return 4
* *AND* at the millisecond precision the projected values SHALL render the seeded millisecond prefix (`.000` for the first pair, `.123` for the second) for every row of BOTH columns, and `COUNT(DISTINCT)` over each column SHALL return 2 — the two microsecond-distinct values of each pair collapsing into one, which this scenario records as a named Exasol 8.x version limitation rather than a defect
* *AND* the `timestamptz` column's values SHALL be the same UTC instants as the `timestamp` column's, so the two columns' assertions differ ONLY in the Iceberg source type and never in an expected value — the zone-awareness trade-off `datafusion-scan/type-mapping-timestamp-precision` records is out of this scenario's scope
* *AND* the query SHALL be proven to reach the scan UDF rather than an unaccelerated fallback, so the asserted values are the ones the scan emitted rather than ones Exasol computed for itself
* *AND* the scenario SHALL FAIL, not skip, when no live Exasol instance is available, per this repo's E2E contract
* *AND* the new test binary SHALL be listed in the `test-e2e` make target, because that target enumerates its binaries explicitly and an unlisted binary never runs in the suite gate

### Scenario: A VS timestamp compared as a rendered string uses a precision-matched oracle

* *GIVEN* the recorded assertion that `UPPER(c_ts)` over the virtual table declines pushdown and matches an in-session native oracle — today `UPPER(CAST(TIMESTAMP '2024-01-01 00:00:00.100' AS TIMESTAMP))`, whose CAST target is the bare `TIMESTAMP` the VS column used to be declared as
* *WHEN* the virtual column is declared `TIMESTAMP(6)` and Exasol renders it with six fractional digits
* *THEN* the oracle's CAST target SHALL carry the SAME declared type the virtual column carries on the running engine, so the two sides of the comparison are rendered at one precision and the assertion keeps testing the declined-pushdown behavior it was written for rather than failing on a digit count
* *AND* the expected declared type SHALL come from the ONE shared helper this delta's round-trip scenario introduces, and MUST NOT be a second copy of the version-to-precision table
* *AND* every OTHER recorded timestamp assertion SHALL keep passing unchanged on both version arms, and the reason SHALL be that each is precision-insensitive rather than precision-correct: the declared-type checks in `create_vs_maps_iceberg_schema` and the Delta/Unity suites match by PREFIX, the INT96 far-future check prefix-matches at seconds resolution, and every `HOURS_BETWEEN`/`YEAR`/`SECOND(c_ts, 3)`/`COUNT(DISTINCT)` assertion reads a derived value no sub-millisecond digit reaches
* *AND* no recorded assertion SHALL be loosened to accommodate the new declaration — the one assertion that moves is the oracle's CAST target, and it moves to become MORE specific, not less

### Scenario: The scan returns correct values across the type mix with no spec-carried emit types

* *GIVEN* the seeded `typed_distinct_probe` and `complex_probe` Iceberg tables served through the virtual schema
* *AND* a query whose select list spans `long`, `decimal`, `double`, `string`, `date`, `timestamp`, and `boolean` columns, a projected `CAST(<col> AS DECIMAL(5,0))`, and the `list`, `struct`, and `map` columns the adapter declares `VARCHAR(2000000)`
* *WHEN* the query runs end to end against the local Exasol Docker container
* *THEN* `EXPLAIN VIRTUAL` SHALL show the generated `EMITS (...)` clause declaring one Exasol type per select-list item
* *AND* the generated scan spec JSON MUST NOT contain the key `emit_exa_types`, because the scan carries no second copy of that declaration
* *AND* the query SHALL return the seeded values for every column, with the nested columns returned as the JSON documents `datafusion-scan/nested-json-rendering` specifies
* *AND* the run SHALL exercise the `ExaType` variants `Int64`, `Int32`, `Numeric`, `Double`, `String`, `Date`, `Timestamp`, and `Boolean`, so the context-driven coercion is exercised for every variant the scan can meet on this fixture set rather than for a single column type
