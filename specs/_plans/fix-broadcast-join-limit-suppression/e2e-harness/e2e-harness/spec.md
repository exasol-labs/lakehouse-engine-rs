# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

<!-- DELTA:CHANGED -->
* This bullet SUPERSEDES the preceding Background bullet "The harness sends Exasol's own `resultSetMaxRows` default (`0`, no limit) unless a call site declares a cap. A declared cap is NOT merely a result-delivery choice: on a real query execution it reaches the adapter as a `pushdownRequest` `limit` …". The harness sends Exasol's own `resultSetMaxRows` default (`0`, no limit) unless a call site declares a cap, and that uncapped default is kept so a plan-shape test is never silently perturbed by an undeclared row cap. A declared cap is NOT merely a result-delivery choice: on a real query execution it reaches the adapter as a `pushdownRequest` `limit`, for every statement shape measured. `EXPLAIN VIRTUAL` can never show this — it is a separate exchange from a real query's pushdown request, so its echo cannot carry a limit only the real statement gained. Since issue #307 a pushed `limit` no longer disqualifies broadcast: a bare `LIMIT` and a bare-projected-column `ORDER BY` over a broadcast-eligible inner equi-join both stay on the broadcast path. Only the four surviving forcing conditions — an aggregate select item, a non-empty `GROUP BY`, `aggregationType = "group_by"`, or a non-null `HAVING` — plus a `limit` offset with no `orderBy` and an unrenderable or unprojected sort key still move a join onto the unaccelerated two-scan fallback (`vs-adapter/pushdown-planning-join`), with no `EXPLAIN VIRTUAL`-visible sign that it happened. The measured shape matrix, the per-shape adapter behavior, and the `EXPLAIN VIRTUAL` blind spot are recorded in `docs/debugging-pushdown.md`.
<!-- /DELTA:CHANGED -->

## Scenarios

The scenario below is reproduced verbatim and UNMARKED as required structural context — this delta changes only the Background bullet above and no scenario, so no `DELTA:*` marker applies and `/speq:record` leaves this scenario untouched.

### Scenario: A declared row cap truncates the returned row count

* *GIVEN* the same harness and virtual schema, one connection declaring a row cap of `n` smaller than the seeded table's row count and one connection declaring no cap
* *WHEN* the identical bare projection statement carrying no SQL `LIMIT` is issued through each connection
* *THEN* the cap-declaring connection SHALL return exactly `n` rows
* *AND* the no-cap connection SHALL return the table's full row count
* *AND* this scenario SHALL NOT be read as a claim about the pushdown request either connection generates — `EXPLAIN VIRTUAL` cannot observe whether a real execution's request carries a `limit`, since it is a separate exchange from the real statement; see `docs/debugging-pushdown.md` for what is actually known about that request
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
