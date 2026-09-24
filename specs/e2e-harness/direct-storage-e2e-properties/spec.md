# Feature: Direct-Storage E2E Discovery and Properties

Proves live, against Exasol and MinIO, that the direct-storage catalog kind discovers the right
tables and reads its virtual-schema properties as specified. Proves that the kind rejects a
malformed CONNECTION at `CREATE VIRTUAL SCHEMA` rather than at query time. Proves that the kind
pushes down the same operations every other catalog kind pushes down.

## Background

* This feature and `e2e-harness/direct-storage-e2e` share ONE test binary,
  `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, and ONE `OnceLock`-guarded setup. The
  split is by question, not by binary. That feature owns schema folding and row correctness. This
  one owns discovery, properties, validation, and pushdown parity.
* The fixture writer is the shared raw-Parquet helper that feature specifies. This feature adds
  fixture DIRECTORIES, never a second writer.
* Validation failures are asserted at `CREATE VIRTUAL SCHEMA`, because that is where the adapter
  reads and validates the CONNECTION. An error surfacing only at query time would mean an operator
  ships a broken virtual schema and learns about it from a user.
* Pushdown parity is a SMOKE test, not a second pushdown suite. The pushdown layer is format-neutral
  and catalog-kind-blind. The risk this scenario covers is that the direct-storage scan spec
  reaches that layer in a shape it cannot plan. The risk is not that projection or filtering is
  wrong.
* The GROUP BY and the two-table join cases are part of that smoke test because they are the two
  shapes this plan reshapes rather than reuses. The join path resolves its legs concurrently over
  one shared session and sizes each broadcast side from the seam's listing. Grouped aggregation
  is the one pushdown shape whose partial result is merged by Exasol rather than returned whole.
  Neither has live proof over a catalog-free kind otherwise.
* The live join assertion covers the returned rows and the pushdown request shape. That is all
  Exasol SQL exposes. The one-shared-store property and the concurrent-resolution property stay
  owned by their unit scenarios, because an E2E test asserting either would assert nothing.
* Apache Iceberg and Delta specification check: NOT implicated. Every fixture here is raw Parquet
  written by the suite's own writer. No scenario asserts Iceberg or Delta semantics.

## Scenarios

### Scenario: Only first-level directories holding a data file become virtual tables

* *GIVEN* a base path `s3://warehouse/direct_discovery/` holding a directory `orders/` with one Parquet file, a directory `deep/` whose only Parquet file is at `deep/y=2026/p.parquet`, a directory `empty/` whose only object is `_SUCCESS`, a directory `hidden_only/` whose only Parquet file is at `hidden_only/_staging/p.parquet`, and a loose object `notes.parquet` directly under the base path
* *AND* a virtual schema created over that base path with the direct-storage `CATALOG_KIND`
* *WHEN* an Exasol user reads the served table list from `SYS.EXA_ALL_TABLES`
* *THEN* the virtual schema SHALL serve EXACTLY the tables `ORDERS` and `DEEP`, so a directory is a table when and only when the shared listing rules find a data file under it
* *AND* `EMPTY` and `HIDDEN_ONLY` SHALL be ABSENT from the served tables, and `CREATE VIRTUAL SCHEMA` SHALL still succeed, so a directory with no visible data file is skipped rather than fatal
* *AND* no table SHALL be served for `notes.parquet`, so a loose data file under the base path names no table
* *AND* a `SELECT` over `DEEP` SHALL return that file's rows with its `y=2026` segment as the partition column `Y`, because `HIVE_PARTITIONING` defaults to TRUE
* *AND* the test MUST fail, not skip, when Exasol or MinIO is unavailable

### Scenario: NAMESPACE scopes discovery to a subtree of the CONNECTION address

* *GIVEN* the same CONNECTION address `s3://warehouse/direct_discovery/`, under which a further directory `sub/orders_eu/` holds one Parquet file
* *WHEN* one virtual schema is created with no `NAMESPACE` and a second is created with `NAMESPACE = 'sub'`
* *THEN* the first SHALL serve the base path's own first-level directories and MUST NOT serve `ORDERS_EU`, because `sub/` is a table whose files live below it rather than a namespace
* *AND* the second SHALL serve EXACTLY `ORDERS_EU`, so the property scopes discovery to the named subtree
* *AND* a `SELECT` through the second SHALL return the rows of `sub/orders_eu/`, so the table root is recomposed at pushdown from the CONNECTION address, the property, and the recorded directory name
* *AND* a virtual schema created with a `NAMESPACE` naming a path that holds no directory SHALL be created successfully with NO tables, matching the recorded empty-enumeration behaviour rather than failing

### Scenario: A CONNECTION the direct-storage kind cannot accept is rejected at create time

* *GIVEN* a set of CONNECTION objects over the same reachable MinIO endpoint, one carrying a `warehouse` field, one carrying a catalog `token`, one whose address uses the `abfs` scheme while the password carries S3 credentials, one whose address is empty, and one whose password carries Azure credentials while the address uses the `s3` scheme
* *WHEN* an Exasol user runs `CREATE VIRTUAL SCHEMA` with the direct-storage `CATALOG_KIND` against each
* *THEN* EVERY statement SHALL FAIL at `CREATE VIRTUAL SCHEMA`, not at query time, each with an error naming the offending field or the scheme mismatch
* *AND* the `abfs` error SHALL name `abfss` as the accepted spelling, so an operator who mistyped the scheme is told the fix
* *AND* the empty-address error SHALL name the storage base path rather than a catalog URI, so the message matches the kind the operator selected
* *AND* NO error message SHALL contain any credential value, asserted on the VALUES supplied rather than on field-name spellings
* *AND* an unparseable `MERGE_SCHEMA` value SHALL likewise fail at `CREATE VIRTUAL SCHEMA` rather than defaulting silently

### Scenario: Projection, filter, and LIMIT reach the direct-storage scan

* *GIVEN* the `events/` fixture directory at `s3://warehouse/direct/events/`, whose 64-bit integer column is named `EVENT_ID`
* *AND* a second fixture directory `s3://warehouse/direct/event_labels/` holding ONE Parquet file carrying exactly the columns `EVENT_ID`, a 64-bit integer, and `LABEL`, a string, whose `EVENT_ID` values are a subset of the `events/` values
* *AND* one virtual schema over `s3://warehouse/direct/` with the direct-storage `CATALOG_KIND`, serving both directories as the tables `EVENTS` and `EVENT_LABELS`
* *WHEN* an Exasol user runs a query selecting a column subset with a filter predicate and a `LIMIT`, and reads its `EXPLAIN VIRTUAL` output
* *THEN* the returned rows SHALL be exactly the rows satisfying the predicate, capped at the limit, projected to the selected columns
* *AND* the pushdown plan SHALL carry the projected column subset, the predicate, and the limit, so the direct-storage scan spec reaches the format-neutral pushdown layer in a shape that layer plans
* *AND* a single-group aggregate over the same table SHALL return the same answer the equivalent unpushed query returns, so partial aggregation reaches this kind unchanged
* *AND* a `GROUP BY` aggregate over the same table SHALL return the same answer the equivalent unpushed query returns, group for group, so the node-local partial aggregate and its Exasol-side merge reach this kind unchanged
* *AND* an INNER equi-join between `EVENTS` and `EVENT_LABELS` on `EVENT_ID` SHALL return EXACTLY the rows the equivalent unpushed join returns, so the join path plans and executes over a kind that reads footers at plan time
* *AND* the `EXPLAIN VIRTUAL` output for that join SHALL carry ONE pushdown request naming BOTH tables, so the two legs reach the adapter in one request rather than in two
* *AND* this scenario MUST NOT assert that both legs resolve through one shared admission-limited object store, because Exasol SQL exposes no such observation and the assertion would pass on any implementation. `vs-adapter/pushdown-format-neutral-resolution` § One catalog session per request serves every table the request resolves and `vs-adapter/direct-storage-table-discovery` § One admission-limited object store serves every table of one adapter call own that property
* *AND* `EXPLAIN VIRTUAL` SHALL name the CONNECTION and MUST NOT contain any credential value
