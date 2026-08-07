# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

<!-- DELTA:NEW -->
* The harness sends Exasol's own `resultSetMaxRows` default (`0`, no limit) unless a call
  site declares a cap. A declared cap is NOT merely a result-delivery choice: on a real query
  execution it reaches the adapter as a `pushdownRequest` `limit`, for every statement shape
  measured. `EXPLAIN VIRTUAL` can never show this — it is a separate exchange from a real
  query's pushdown request, so its echo cannot carry a limit only the real statement gained.
  This is exactly why the harness's default matters for join tests specifically: a
  broadcast-eligible inner equi-join disqualifies broadcast pushdown under ANY pushed limit
  (`join_requires_exasol_postprocessing`), so a capped connection silently moves a join test
  onto the unaccelerated two-scan fallback with no `EXPLAIN VIRTUAL`-visible sign that it
  happened. The measured shape matrix, the per-shape adapter behavior, and the
  `EXPLAIN VIRTUAL` blind spot are recorded in `docs/debugging-pushdown.md`.
* The harness reads a result set to completion. A result set larger than one `fetch`
  response is retrieved across successive fetches, never truncated to the first response.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
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
* *AND* this scenario makes no claim about the pushdown request either connection generates — `EXPLAIN VIRTUAL` cannot observe whether a real execution's request carries a `limit`, since it is a separate exchange from the real statement; see `docs/debugging-pushdown.md` for what is actually known about that request
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: Harness returns every row of a result set larger than one fetch response

* *GIVEN* the harness connected with no declared row cap, and a seeded table read under a `numBytes` fetch budget smaller than the bytes its result set occupies, so the result set cannot fit in one `fetch` response
* *WHEN* a test reads that table's rows through the harness result-reading helper
* *THEN* the helper SHALL issue successive `fetch` requests until the rows it has accumulated reach the count the result-set metadata reports in `numRows`
* *AND* the helper SHALL return exactly that row count
* *AND* the helper MUST NOT return a silently truncated column set — it SHALL fail loudly if a response returns zero rows while rows remain outstanding
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->
