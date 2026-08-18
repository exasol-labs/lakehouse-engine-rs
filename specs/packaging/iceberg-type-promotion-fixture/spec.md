# Feature: Iceberg Type-Promotion E2E Fixture (Apache Spark)

Adds an Iceberg fixture to the end-to-end test stack whose columns are promoted mid-life — data
written at the source type, the schema evolved, then more data written at the target type — produced
by Apache Spark's Iceberg runtime, proving the read half of `vs-adapter/iceberg-type-promotion`
full-stack: the promotions this engine reads return correct rows across the promotion boundary. The
`date` promotion `vs-adapter/iceberg-type-promotion` refuses has no live fixture here — see the
Background note below — and is covered instead by unit tests over a synthetic `TableMetadata`
(`datafusion-scan/type-relaxation`'s sibling feature `vs-adapter/iceberg-type-promotion`, task 4.3).

## Background

* The fixture reuses the existing `spark-iceberg-fixtures` one-shot Compose job and the shared
  Iceberg REST catalog over MinIO every other E2E table uses — no new dependency, no new service.
* `run_fixtures.sh` invokes each `.sql` file by an EXPLICIT `spark-sql -f` line and does NOT glob the
  directory, so a fixture script that is added without its own invocation line is silently never run.
  The same is true of `make test-e2e`, whose `--test` list is explicit: a new E2E binary that is not
  added to it never executes, and CI runs that same target.
* No API in `iceberg` 0.10.0 can author this fixture. `UpdateSchemaAction` exposes `add_column` and
  `delete_column` only — there is no column-type update — so the promotion must come from Spark's
  `ALTER TABLE … ALTER COLUMN … TYPE`. Spark is chosen because it exercises a real Iceberg writer's
  promotion path rather than a hand-built schema commit, which is the point of proving the read
  against a table a production writer produced.
* **Only one table is authored here.** A `date` → `timestamp` fixture was planned as a second table,
  but Apache Iceberg Java never implements that promotion at any pinned or current version —
  `TypeUtil.isPromotionAllowed` (`api/…/types/TypeUtil.java`, identical across
  `apache-iceberg-1.10.1`, `apache-iceberg-1.11.0`, and `main`) switches on `INTEGER`, `FLOAT`,
  `DECIMAL` only, so `ALTER TABLE … ALTER COLUMN … TYPE` from `date` fails outright. A raw
  REST-metadata commit can force the schema history `vs-adapter/iceberg-type-promotion`'s refusal
  reads, but the resulting table is not readable by Iceberg Java's own Spark reader either
  (`TimeStampMicroVector cannot be cast to DateDayVector`) — no conforming writer or reader produces
  or reads this shape, so a hand-committed fixture would prove nothing a unit test over a synthetic
  `TableMetadata` does not already prove. The refusal's coverage is unit-only; see
  `vs-adapter/iceberg-type-promotion`, task 4.3, and decision [14] in this plan's decision log.
* **The table is format-version 2**, because `int` → `long`, `float` → `double`, and
  `decimal(P,S)` → `decimal(P',S)` are all valid at v1 and v2 per the Iceberg spec's promotion table,
  and using the version every other Iceberg fixture uses keeps the fixture about promotion rather than
  about format version.
* **The table MUST carry data written BEFORE the promotion.** A table promoted before any write, or
  one whose files were all rewritten afterwards, carries only target-type data files and target-width
  manifest bounds — it would pass the read test without ever exercising the cast. The pre-promotion
  write is therefore the load-bearing step, and a fixture-shape assertion MUST prove the committed
  pre-promotion data file's physical Parquet encoding is the SOURCE type, so a silent Iceberg-side
  rewrite makes the suite fail loudly instead of passing vacuously.
* **The pre-promotion rows must be non-trivial in the target range.** `int_long`'s pre-promotion rows
  carry values a 32-bit column can hold and the post-promotion rows carry a value only a 64-bit
  column can, so a scan that read the old file at the wrong width would return a wrong number rather
  than a coincidentally-equal one.
* Ground truth — the catalog and namespace (`rest_catalog.e2e_lakehouse`, matching
  `seed::E2E_NAMESPACE`), the table name, every column, its source and target type, and every
  inserted row — lives in the Rust test harness and MUST stay in lockstep with the Spark SQL script
  that produces it, exactly as `packaging/int96-timestamp-fixture` requires of its own script.
* The fixture step MUST fail, not skip, when Spark, the REST catalog, or MinIO is unavailable — the
  same fail-loud contract as every other fixture in this stack.

## Scenarios

### Scenario: Spark produces an Iceberg table whose readable promotions span the schema change

* *GIVEN* the E2E stack is running with the shared REST catalog over MinIO and an Apache Spark
  service with the Iceberg Spark runtime
* *WHEN* the fixture step creates a format-version-2 Iceberg table carrying an `int` column, a
  `float` column, and a `decimal(10,2)` column, inserts rows into it, promotes the three columns to
  `long`, `double`, and `decimal(20,2)` through `ALTER TABLE … ALTER COLUMN … TYPE`, and inserts
  further rows
* *THEN* the fixture SHALL commit at least one data file written BEFORE the promotion and at least
  one written after, so a scan of the table reads both physical layouts in one query
* *AND* a fixture-shape test SHALL assert directly from the committed pre-promotion data file that
  its three columns are physically `INT32`, `FLOAT`, and `INT64` carrying the `DECIMAL(10,2)`
  logical annotation (Iceberg encodes a decimal of precision ≤ 18 as a physical `INT64`), so an
  Iceberg-side rewrite that silently normalised them to the target types fails the suite rather than
  making the read test pass vacuously
* *AND* the fixture SHALL record the exact inserted rows and values, including a post-promotion
  `int_long` value outside the 32-bit range, so a test can assert the scan result rather than only
  its row count
* *AND* the promoted decimal SHALL widen PRECISION ONLY, keeping scale 2 on both sides, because the
  Iceberg spec permits `decimal(P,S)` → `decimal(P',S)` with `P' > P` and no scale change
* *AND* the fixture step SHALL fail, not skip, if the Spark service, the REST catalog, or MinIO is
  unavailable

### Scenario: The new fixture and its suite are wired into the paths that actually run

* *GIVEN* `run_fixtures.sh`, which invokes each fixture script by an explicit `spark-sql -f` line
  rather than by globbing, and `make test-e2e`, whose `--test` list names each E2E binary explicitly
* *WHEN* the fixture script and its E2E test binary are added
* *THEN* `run_fixtures.sh` SHALL gain its own invocation line for the new script, so the fixture is
  authored at stack bring-up in CI and locally rather than being present but never executed
* *AND* `make test-e2e` SHALL gain the new binary in its `--test` list, because CI's E2E job runs
  exactly that target and a binary missing from it is a suite that never runs
* *AND* the new script SHALL reuse the shared `SPARK_CONF` array verbatim, adding no package,
  catalog, or filesystem setting, because the only fixture that needs extra arguments is the INT96
  one and its reasons — a native Parquet write and an `add_files` import — do not apply here
