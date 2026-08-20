# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* **The new binary MUST be added to the `test-e2e` make target.** That target enumerates its test
  binaries explicitly, so a new E2E binary that is not listed never runs in the suite gate.
* Every E2E scenario runs against a local Exasol Docker container over MinIO and MUST fail (never skip) when the stack is unavailable.
* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* See `e2e-harness/e2e-harness-grouped-order` for grouped-aggregate cases that deliberately
  place an aggregate before, between, or after the group keys in the `selectList` — the
  arrangement every case in this spec avoids.
* The provisioning helpers (SLC install, `.so` upload, script and Virtual Schema creation)
  are defined once in a shared `common/e2e_harness` module and reused by every E2E binary;
  per-binary variation is passed as explicit parameters.
* The harness sends Exasol's own `resultSetMaxRows` default (`0`, no limit) unless a call site declares a cap, and that uncapped default is kept so a plan-shape test is never silently perturbed by an undeclared row cap. A declared cap is NOT merely a result-delivery choice: on a real query execution it reaches the adapter as a `pushdownRequest` `limit`, for every statement shape measured. `EXPLAIN VIRTUAL` can never show this — it is a separate exchange from a real statement's pushdown request, so its echo cannot carry a limit only the real statement gained. Since issue #307 a pushed `limit` no longer disqualifies broadcast: a bare `LIMIT` and a bare-projected-column `ORDER BY` over a broadcast-eligible inner equi-join both stay on the broadcast path. Only the four surviving forcing conditions — an aggregate select item, a non-empty `GROUP BY`, `aggregationType = "group_by"`, or a non-null `HAVING` — plus a `limit` offset with no `orderBy` and an unrenderable or unprojected sort key still move a join onto the unaccelerated two-scan fallback (`vs-adapter/pushdown-planning-join`), with no `EXPLAIN VIRTUAL`-visible sign that it happened. The measured shape matrix, the per-shape adapter behavior, and the `EXPLAIN VIRTUAL` blind spot are recorded in `docs/debugging-pushdown.md`.
* The harness reads a result set to completion. A result set larger than one `fetch`
  response is retrieved across successive fetches, never truncated to the first response.
* **This delta is issue #359.** It adds THREE scenarios and amends no recorded clause. The first is a
  timestamp round-trip that asserts VALUE fidelity at the declared precision; the second gates the
  suite on both supported Exasol major versions; the third repairs the one existing assertion that
  compares a VS timestamp's RENDERED STRING against a native oracle. Every recorded scenario, seed
  fixture, and provisioning helper otherwise stays as recorded.
* **Split, issue #359/#350: the row-correctness scenarios moved to
  `e2e-harness/e2e-harness-scan-correctness`.** This feature's scenario count crossed this library's
  per-spec organization threshold once those scenarios landed; the projection/filter/LIMIT query, the
  oversubscribed shard fan-out, the partitioned file-pruning query, the nested-JSON-column query, the
  microsecond timestamp round-trip, and the rendered-string timestamp oracle now live in that sibling
  feature, which shares this feature's stack, provisioning, and fail-fast contract. This feature keeps
  harness provisioning, row-cap/fetch-paging mechanics, and the Exasol-version CI gate below.
* **`E2E` is a required status check on `main`'s ruleset, so the matrix MUST NOT rename it.** A
  matrixed job whose legs both carry new names leaves the ruleset waiting on a check that never
  reports; PRs then block until an admin edits the ruleset. Keeping one leg's name exactly `E2E`
  preserves the existing requirement, and the second leg's name is a NEW check an admin adds — the same
  operator step issue #336 already tracks for `e2e-azure`.
* **`upload-artifact@v7` rejects a name already used by another upload in the same run**, which the
  workflow already records for `e2e-azure`'s `exa-logs-azure`. Two matrix legs both uploading
  `exa-logs` on failure would fail the upload rather than the test, hiding the diagnostic the step
  exists to produce.
* **PR #358 already proved the whole suite passes on `8.29.13`** across the E2E, Lakekeeper, Unity, and
  Azure gates, so the 8.x leg is expected green from the start; the version gate is what keeps it green
  once the 2025.x declaration changes.

## Scenarios

### Scenario: E2E suite fails when the stack is unavailable

* *GIVEN* the Exasol container is not reachable
* *WHEN* the `exasol-e2e` test suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: Harness provisions the scalar scan and the LUA distributor scripts

* *GIVEN* the E2E harness bootstrapping the lakehouse VS on the Exasol Docker container
* *WHEN* the harness creates the scan-path scripts
* *THEN* the harness SHALL create `LAKEHOUSE_SCAN` as a SCALAR SCRIPT (EMITS its dynamic output columns) referencing the uploaded `.so`
* *AND* the harness SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET SCRIPT that passes each shard's `files` VARCHAR through unchanged, referencing no `.so`
* *AND* an end-to-end projection/filter query over the installed scripts SHALL return results identical to the single-node DataFusion equivalent (grouped/nested-aggregate coverage lives in `e2e-harness/e2e-harness-grouped-agg`)
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

### Scenario: The E2E suite gates on both supported Exasol major versions

* *GIVEN* the core `e2e` CI job, whose stack images are selected entirely by the `EXASOL_IMAGE` variable that the Makefile and `docker-compose.yml` both already read
* *WHEN* CI runs that job
* *THEN* it SHALL run the whole existing E2E suite TWICE — once against the current default `2025.x` image and once against an `8.29.x` image — as two legs of ONE matrixed job with identical steps, and MUST NOT duplicate the job body or swap the default image
* *AND* each leg SHALL pass its image through `EXASOL_IMAGE` so the image reaches both the `docker compose` steps and `make test-e2e`, requiring NO change to `docker-compose.yml` or the Makefile
* *AND* exactly ONE leg's status-check name SHALL be EXACTLY `E2E`, so `main`'s existing required-check requirement keeps being satisfied by a reporting check; the other leg SHALL carry a distinct name that names its Exasol version, and adding it to the ruleset SHALL be recorded as an operator action rather than assumed
* *AND* each leg's failure-log artifact SHALL carry a name unique to that leg, because `upload-artifact@v7` rejects a name already used by another upload in the same run — two legs both uploading `exa-logs` would fail the upload instead of surfacing the diagnostic
* *AND* the `release` job SHALL keep depending on `e2e` and therefore SHALL wait for BOTH legs, so neither version can be skipped on the way to a release
* *AND* `e2e-lakekeeper`, `e2e-unity`, and `e2e-azure` SHALL stay single-version, because they gate catalog integrations orthogonal to the Exasol engine version and a second leg of each would triple the stack cost for no new coverage

> The projection/filter/LIMIT, shard fan-out, file-pruning, nested-JSON-column, timestamp
> round-trip, and rendered-string oracle scenarios live in
> `e2e-harness/e2e-harness-scan-correctness`.
