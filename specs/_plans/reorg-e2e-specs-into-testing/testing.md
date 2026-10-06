# Testing

This file holds the rules for how this repository tests its behavior and how it keeps its test environments clean. Feature behavior lives in the feature specs under `specs/<domain>/<feature>/spec.md`. This file states no feature behavior. It states how tests prove that behavior, how the end-to-end (E2E) suites are built and run, and how operators run the test and benchmark infrastructure.

## Testing

### Coverage rule

- Every scenario in a feature spec has at least one integration test or E2E test that exercises it. A unit test alone is enough only for pure computation with no I/O.
- A test that implements a scenario carries one `/// Scenario: <title>` line per scenario, quoting the title verbatim (`AGENTS.md` § Code style).
- An E2E suite is a place where feature scenarios get proof against a live Exasol. It holds no requirement of its own. When an E2E test finds behavior that no feature spec states, that behavior goes into a feature spec, not into this file.
- An E2E test proves the behavior a user sees through Exasol SQL. A property that Exasol SQL cannot observe stays proved by its unit or integration test, and an E2E test does not assert it.
- A scenario whose state no well-formed Exasol request can reach is proved by a unit test over a synthesized request, not by an E2E test.

### Test tiers

- Unit tests run on the host with `cargo test`. They live in a sibling `_tests.rs` file (`AGENTS.md` § Code style).
- Integration tests run on the host with `cargo test` against local fixtures, for example a local-filesystem object store over the vendored Delta fixtures. They need no Exasol.
- E2E tests run against a live Exasol and a real or containerized catalog and object store. Each E2E suite is gated by its own cargo feature, so the plain `cargo test` run never needs a stack.

### Unit tests

- The type-relaxation unit suite asserts `arrow::compute::can_cast_types` for every supported-set Arrow pair, reads each pair through a real Parquet file written at the physical type under a logical schema at the target type and asserts the values, fails when an `arrow-cast` upgrade withdraws a pair, and asserts that `long` to `double` is absent from the supported set.
- The test list `supported_relaxation_pairs` stays a concrete hand-written pin asserted against the production widening owner, never generated from it. A supported-set row with no matching rule in that owner fails the suite.
- The Exasol-dialect sweep test in `crates/vs-expression/src/lib_tests.rs` keeps a banned-token list that names every DataFusion-only function name the translator can emit, including the checked-division function. The list never names `CAST`, which is valid Exasol SQL.
- A unit test that pins a case fold uses a constructed name whose Unicode and ASCII folds differ, such as `STRAßE`, builds its input lists as literals, and calls no builder, so it tests the lookup alone and fails under the other fold.
- No test restates standard-library behavior, such as asserting that two callers of one `str::to_uppercase` fold agree.

### E2E suites

| Suite | Cargo feature | Test binaries | Make target | CI job (check name) | Stack | In `release` needs |
|-------|---------------|---------------|-------------|---------------------|-------|--------------------|
| Local Docker | `exasol-e2e` | every binary on the `test-e2e` recipe line | `make test-e2e` | `e2e` (`E2E`, `E2E (8.29.x)`) | Exasol, SeaweedFS, Iceberg REST catalog (`docker-compose.yml`) | yes |
| Lakekeeper | `lakekeeper-e2e` | `e2e_lakekeeper_test` | `make test-e2e-lakekeeper` | `e2e-lakekeeper` (`E2E (Lakekeeper)`) | Local Docker stack plus Lakekeeper, its PostgreSQL database, Keycloak, OpenFGA | yes |
| Unity Catalog | `unity-e2e` | `e2e_unity_test` | `make test-e2e-unity` | `e2e-unity` (`E2E (Unity)`) | SeaweedFS, Exasol, OSS Unity Catalog (`docker-compose.unity.yml`) | yes |
| Azure | `azure-e2e` | `e2e_azure_test` | `make test-e2e-azure` | `e2e-azure` (`E2E (Azure)`) | Lakekeeper stack plus a real ADLS Gen2 account | no |
| Glue | `glue-e2e` | `e2e_glue_test` | `make test-e2e-glue` | `e2e-glue` (`E2E (Glue)`) | Local Exasol plus a real AWS Glue Data Catalog and S3 bucket | no |
| Cloud (opt-in) | `cloud-e2e` | `cloud_e2e_test` | none | none | A real Exasol cluster plus the AWS Glue Iceberg REST catalog | no |

- A new E2E binary is listed in its suite's Make target. Cargo discovers every `tests/*.rs` target, but each Make target names its binaries explicitly with `--test`, and CI runs the Make target. A binary missing from that list never runs.
- `crates/lakehouse-engine/tests/build_convention.rs` reads the `Makefile` and asserts that the `test-e2e` recipe line names `e2e_type_relaxation_test` and `e2e_direct_storage_test`. A new guard for a new binary extends that file. No second guard file is added.
- Each suite runs with `--test-threads=1`, because all tests of one binary share one provisioning.

### Failure contract

- Every suite except `cloud-e2e` fails, and never skips, when its stack, a credential variable, or its cloud account is unavailable. The suite never reports the affected tests as skipped or passed.
- A missing or empty credential variable fails the suite with a message that names the variable and never its value. The Glue suite fails on a missing variable before it creates any cloud resource. The Azure suite names the variable because its blob client scans no environment, so an absent variable would otherwise surface as an authorization failure.
- The Lakekeeper suite fails when the stack reports an authorization backend other than OpenFGA.
- A fork pull request receives no repository secrets. The Azure and Glue jobs therefore run and fail loudly naming the missing variable.
- The `cloud-e2e` suite is the one opt-in suite. It skips cleanly, naming the absent variable, when its AWS variables are not set, and then it makes no network call to AWS or Exasol. The same skip applies to the assume-role variables and to a `GLUE_CATALOG_URI` that is not a standard `https://glue.<AWS_REGION>.amazonaws.com/` endpoint.
- The cloud performance smoke test records the wall-clock duration of its aggregate query for manual inspection. It asserts sane row counts and aggregate values, and it asserts no latency threshold.

### Shared harness

- One module, `crates/lakehouse-engine/tests/common/e2e_harness.rs`, defines the scan-path provisioning: the SLC install, the `.so` upload, the script creation, and the virtual schema creation. Every E2E binary uses it, so the script DDL is byte-identical across binaries.
- The harness installs the scripts in the shape production uses: `LAKEHOUSE_SCAN` as a SCALAR script that EMITS its dynamic output columns and references the uploaded `.so`, `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET script that passes each shard's `files` value through and references no `.so`, and the adapter script.
- The harness issues `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` for both scripts to `CURRENT_USER` after the last `CREATE OR REPLACE` of the CONNECTION or a script, because a replacement drops the grant. It skips the grant for `SYS`, which Exasol refuses and which holds every CONNECTION implicitly.
- A binary passes its own values as explicit parameters: the virtual schema name, the namespace, the CONNECTION name and password, `PARALLELISM_FACTOR`, `JOIN_BROADCAST_MAX_BYTES`, `CATALOG_KIND`, and `MERGE_SCHEMA`. A parameter the binary does not supply is omitted from the generated `CREATE VIRTUAL SCHEMA`, never emitted empty. No binary re-declares the provisioning logic.
- Each binary runs one `OnceLock`-guarded setup.
- The shared virtual schema DDL sets `ALLOW_HTTP = 'true'`.
- Every DSN and connection string sets `validateservercertificate=0`, because the Docker image uses a self-signed certificate.
- The seed path for a cloud store selects a storage backend on the one shared seed-catalog configuration. No suite forks the table create, write, and commit logic.
- The E2E harness resolves fixture file lists in the test process through the same format-reader seam the adapter uses, with a `CatalogSession` it builds itself (`resolve_fixture_files` in `tests/common/e2e_harness.rs`).

### Exasol session client

- Every suite drives Exasol through the shared `crates/lakehouse-engine/tests/common/exasol_ws.rs` WebSocket client (`ExaConn`).
- The client sends `resultSetMaxRows` `0`, Exasol's own "no limit" default, unless the call site declares a cap with `capped_result_sets(n)`. A declared cap is not only a delivery choice: on a real execution it reaches the adapter as a pushdown `limit`, and `EXPLAIN VIRTUAL` cannot observe it, because `EXPLAIN VIRTUAL` is a separate exchange. `docs/debugging-pushdown.md` records the measured shapes. A test that inspects plan shape keeps its connection uncapped.
- The client reads a result set to completion. It issues successive `fetch` requests until it holds the row count the result-set metadata reports in `numRows`. It fails loudly when a response returns zero rows while rows remain.
- The client has an opt-in redacting mode (`connect_redacting`). In that mode an `execute()` failure message omits the SQL statement and the Exasol response. Suites that send credential-bearing DDL use it. The local Docker suite keeps the SQL in its failure messages for debugging.

### Correctness oracles

- A row assertion compares against one of three oracles: the seeded source data, the same query evaluated without pushdown or on a single DataFusion node, or the key-first ordering of the same grouped query with the column positions transposed.
- A test that must prove a query reached the scan UDF captures the generated pushdown SQL or reads `EXPLAIN VIRTUAL`, and asserts that the SQL drives the scan UDF. A silent fallback to an unaccelerated wrapper then fails the test instead of passing on correct rows.
- Evidence of partial aggregation is the `group_keys` entry in the scan spec plus the outer wrapper's merge of the `PARTIAL_*` columns, with no `IPROC()` and no raw row scan. A `GROUP BY shard_key` fan-out appears only when the file list spans more than one shard, so it is not evidence of pushdown.
- A regression test uses a discriminating fixture: its predicate matches a strict subset of the rows, so the test cannot pass against the defect it exists to catch.
- A test that depends on shard placement forces the placement through the shard count or `PARALLELISM_FACTOR`. It never relies on hash-partitioning luck.
- A test that asserts a declared Exasol timestamp type reads the expected precision from the live session's engine version, through one shared test helper. The helper is a test-owned table and does not call the production version rule. The assertion matches the declared type in full, not by prefix. An oracle that compares rendered timestamp strings casts to the declared type the same helper returns.
- The expected value of a widened `float` column is the f32's exact double expansion, never the decimal literal that was written.
- The Lakekeeper suite's vended CONNECTION carries empty `access_key`, `secret_key`, `endpoint`, and `region` values, and the test asserts that shape. A non-empty CONNECTION `endpoint` would win over the vended one, so the scan would then prove nothing about vended addressing.
- Prefix-tolerant type assertions stay in place where they hold on both Exasol versions. An exact assertion is added beside them, never in place of them.
- When a fixture's ability to reproduce a defect is uncertain, the suite establishes it by observation and fails with a message that says the fixture cannot reproduce the defect. The Lakekeeper suite does this for vended credential scope: it records the `prefix` of the `storage_credentials` entry selected for each table, reads one data file of the other table with the first table's vended credential, and fails when that read is allowed.
- A scenario about a pushdown shape the adapter advertises is proved against a live Exasol with `EXPLAIN VIRTUAL` and the executed query, never inferred from the capability registry or from code inspection.
- A type-coverage test compares the declared types read from `SYS.EXA_ALL_COLUMNS` with an exact expected list, compares one `SELECT ID, <columns> ... ORDER BY ID` over the readable columns with exact expected values, reads each refused column alone, and shares the column values its fixtures write with the other suites.
- A timestamp-precision test first asserts the expected `TIMESTAMP(p)` literal in the generated `EMITS` clause, then asserts the distinct value count the declared width admits; a failed value assertion under a query that raised no error marks an engine accept-and-clamp, not an SLC rejection.
- Each advertised translated date-difference function has an E2E parity test that compares the pushed-down result with native Exasol; a function whose parity test diverges loses its DataFusion-dialect rendering and its capability.
- A native-oracle comparison of a `FLOAT_DIV` result asserts bit-exact equality for an integer (scale-0) numerator and equality within a relative tolerance of about 1 ULP for a non-zero-scale decimal numerator, never string equality.
- When a declined filter would return the same rows as a pushed one, the test asserts the generated pushdown SQL, not the row set.
- A test that asserts a session-time-zone-dependent value sets the session time zone explicitly (`ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'`) and fails loudly when the resulting UTC offset is zero; it compares a moving clock value with the native value within 60 seconds and requires the deviation to be strictly smaller than the UTC offset, so it fails if the adapter emits the UTC instant.
- A test asserts a constant result, such as a projected `TIMESTAMP WITH LOCAL TIME ZONE` literal, by exact value; only a moving clock value is compared within a tolerance.
- When the adapter relies on a path being unreachable, a live E2E test asserts that premise in addition to any `debug_assert!`, because the release `.so` compiles `debug_assert!` out: an ungrouped aggregate with a non-zero `OFFSET` must still fail with `sqlCode 42000`, and `ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2` must equal single-node evaluation, so an Exasol change fails a test instead of returning wrong rows.
- A test that proves a predicate prunes nothing on Parquet statistics uses a multi-row-group file whose per-group statistics would falsely exclude a matching row, never a single-row-group file that pruned nothing.

### Regression guards and probes

- A change that a feature spec declares behavior-preserving keeps every existing unit, integration, and E2E test assertion, golden-SQL string, and `dispatch_golden` fixture unchanged. The only exceptions are the edits the change itself requires, such as import paths and call sites of a renamed or demoted item, and the one value the spec names as changing. No assertion is weakened, disabled, or deleted to make a test pass.
- A committed golden SQL fixture (`dispatch_golden`) changes only with an intended change of the generated SQL. A diff in any other change is a regression, not an expected update.
- A test whose assertion a change makes false is deleted together with its section banner and its test-only imports.
- When code moves to another crate, its test module moves with it and keeps every test name and expected value. A test of a property of the code left behind stays with that code. A helper that several test modules share is declared once in the crate's `#[cfg(test)]` support module. No test is deleted, disabled, or weakened to fit a visibility change.
- A test-only helper that unwraps a production type's private payload is declared in its crate's `#[cfg(test)]` support module and is never `pub`.
- A crate with an enumerated `pub` set has an external-vantage reachability probe in its `tests/` directory that names every `pub` item and asserts that each demoted item is not declared `pub`.
- A structural contract that no runtime test can observe is pinned by a compile-time test that stays intact and unweakened. `capabilities_are_assembled_without_the_catalog_kind` pins that the capability set takes no catalog-kind input, and `crates/lakehouse-engine/tests/catalog_session_signatures.rs` pins that the Iceberg scan source carries a shared `CatalogSession`.

### Fixtures

- Iceberg fixtures are written through the iceberg-rust writer stack in `crates/lakehouse-engine/tests/common/seed.rs`. A fixture with nested columns builds its Arrow batch from the catalog-assigned schema after `create_table`, because the REST catalog reassigns nested field ids.
- Raw Parquet fixtures are written by `crates/lakehouse-engine/tests/common/raw_parquet.rs`. It takes an object key and a `RecordBatch` and puts one Parquet file at that key. It creates no catalog table, commits no snapshot, writes no manifest, and attaches no Iceberg field-id metadata. It takes its store from `local_stack_storage()` and declares no SeaweedFS endpoint or key of its own.
- Apache Spark (Iceberg Spark runtime) writes the Iceberg fixtures that no iceberg-rust API can author: the positional-delete, INT96, and type-promotion fixtures below. The one-shot `spark-iceberg-fixtures` Compose job writes them against the same shared Iceberg REST catalog over SeaweedFS that every other E2E table uses, so the virtual schema resolves them through its normal catalog path.
- `run_fixtures.sh` runs each fixture script through its own explicit `spark-sql -f` line and does not glob the directory, so a new script needs its own line or it never runs. A new script reuses the shared `SPARK_CONF` array unchanged. Only the INT96 script adds settings, because it needs Spark's native Parquet writer and an `add_files` import.
- A Spark fixture step fails, and never skips, when the Spark service, the REST catalog, or SeaweedFS is unavailable.
- Each Spark fixture's ground truth (table name, columns, inserted rows, deleted rows, values) lives in the Rust test harness and stays in lockstep with the Spark SQL script that writes it.
- Positional-delete fixtures: two merge-on-read tables (`write.delete.mode=merge-on-read`), one at `write.delete.granularity=file` and one at `write.delete.granularity=partition`. The partition table spans at least two partitions with at least two data files each, and its `DELETE` or `MERGE` touches data files in more than one partition, so its delete files reference several data files across partitions. Both record the exact deleted rows. Spark writes them because iceberg-rust 0.10 has no position-delete writer (apache/iceberg-rust #340, the condition for dropping Spark) and pyiceberg is copy-on-write only. No equality-delete, Puffin deletion-vector, ORC, or Avro fixture is written: the plan-time refusal covers those.
- INT96 fixture (`int96_ts_far_future`): Spark's native Parquet writer, with `spark.sql.parquet.outputTimestampType=INT96` set for this script only, writes an Iceberg `timestamp` (without time zone) column holding `9999-12-31 23:59:59`, and the Iceberg `add_files` procedure registers the file unchanged. Iceberg's own Spark writer always writes INT64, so `add_files` is the only route to INT96. The value lies outside the Arrow nanosecond range, inside the Arrow microsecond range, and inside Exasol's `TIMESTAMP` maximum. A fixture-shape test asserts that the committed data file is physically INT96.
- Type-promotion fixture: one format-version-2 table with an `int`, a `float`, and a `decimal(10,2)` column. Spark inserts rows, promotes the columns to `long`, `double`, and `decimal(20,2)` with `ALTER TABLE … ALTER COLUMN … TYPE`, then inserts more rows, so at least one data file predates the promotion. A fixture-shape test asserts that file's columns are physically `INT32`, `FLOAT`, and `INT64` with the `DECIMAL(10,2)` annotation. A post-promotion `int_long` value lies outside the 32-bit range, and the decimal widens precision only. No `date` to `timestamp` fixture exists, because Apache Iceberg Java does not implement that promotion. Unit tests over a synthetic `TableMetadata` cover its refusal.
- Delta fixtures are vendored from delta-kernel-rs and are never mutated (`scripts/unity/fixtures/PROVENANCE.md`). `make unity-up` seeds them onto SeaweedFS and registers them in Unity Catalog under `unity.delta_e2e`. The seed is idempotent and exits non-zero on any failure. The Unity Catalog column registration in `scripts/unity/seed.sh` lists every column a test selects, because a column absent from Unity Catalog is not selectable from Exasol.
- A fixture-shape test reads the written file's own Parquet footer and asserts its physical encoding, so a read test cannot pass against a fixture whose types were normalized.
- Fixture authoring is idempotent across runs, because the SeaweedFS volume outlives one run.
- Each standalone fixture gets its own namespace and virtual schema, so it stays invisible to every other suite's enumeration.
- The Glue fixture set writes no data file for the cases the listing skips: the ORC partition, the partition-projection table, the view, the ORC table, and the Delta table.
- A fixture whose test requires a failed enumeration sits under a base path that no passing test enumerates, because enumeration walks every first-level directory of the base path.
- Suites that compare two credential arms seed both from one deterministic 20-row shape (`id` 1 to 20, `score` = 5.0 × `id`). Equal rows across arms then prove both arms read correctly. They do not prove both arms read the same bytes.
- Type-coverage tables sit beside a suite's other fixtures and are read through the virtual schema that suite already creates, with no extra virtual schema or CONNECTION.
- Each all-types fixture table holds the ids 1, 2, and 3: rows 1 and 2 carry values, and row 3 is NULL in every column but `id`.
- Timestamp values in type-coverage fixtures are exact to the millisecond, so they read the same at every engine-gated precision.

### Per-run cloud resources

- Azure: the harness creates one blob container per run, named `lhrs-e2e-<sanitized-user>-<millis>`. The name has 3 to 63 characters from lowercase letters, digits, and hyphens, has no consecutive hyphens, and does not begin or end with a hyphen, whatever `$USER` contains. The harness creates the container with the Entra ID service principal before it creates any warehouse, because Lakekeeper creates no filesystem and validates access at warehouse creation.
- Azure: two Lakekeeper warehouses share that container: `<container>-static` with `sas-enabled` false and `<container>-vended` with `sas-enabled` true. Each `key-prefix` equals its warehouse name, and both carry the same account key as an `az` `shared-access-key` credential. Both are created through the one warehouse-creation helper, and both are seeded through the one shared seed-catalog configuration with no per-arm override. Raw Parquet fixtures for the direct-storage case sit under the container's `direct/` prefix.
- Azure: the suite reads the vended warehouse's `sas-enabled` value and `filesystem` back through the Lakekeeper management API and does not assume them from the request it sent. The vended arm needs no environment variable, Make target, or Docker service beyond those of the static arm.
- Azure: both credential arms share one fixture and one test function. The harness provisions the vended arm's warehouse, seed, and virtual schema before the static arm's, and every vended-arm assertion except the cross-arm row comparison runs before the static arm's assertions, so a static-arm regression cannot mask the vended proof. A static-arm provisioning failure still aborts the shared fixture.
- Azure: a `Drop` guard deletes the container when its scope ends, on a normal return and while unwinding from a panic, including inside an active Tokio runtime. A delete failure during unwinding is reported without a second panic. A name collision at create time fails the run. A container already absent at delete time counts as deleted.
- Glue: each run creates the Glue database `lh_e2e_<run id>` and writes every fixture object under `s3://<GLUE_FIXTURE_BUCKET>/lh_e2e/<run id>/`. The run id is the sanitized user name plus the epoch milliseconds, from lowercase letters, digits, and `_`. The harness deletes the database, its tables, and the prefix when the owning scope ends, normally or on panic. A teardown failure names the leaked database or prefix and does not panic. CI and a local run build the fixtures from the same harness code.
- Lakekeeper: the harness obtains a Keycloak token through the OAuth2 client-credentials grant, calls `POST /management/v1/bootstrap` once, and creates one warehouse per credential mode through `POST /management/v1/warehouse`. Each warehouse has an `s3` storage profile with flavor `s3-compat`, the internal SeaweedFS endpoint, and path-style access. The static warehouse disables `sts-enabled` and `remote-signing-enabled`. The vended warehouse enables `sts-enabled` against the SeaweedFS STS role. Two Keycloak principals hold opposite table grants in one warehouse.
- Unity Catalog: the virtual schema's CONNECTION supplies the SeaweedFS endpoint and static storage credentials, because the OSS Unity Catalog server vends no endpoint. For plan-time resolution in the test process, the suite injects the SeaweedFS endpoint client-side.
- A killed process skips every teardown above. The sweeps in § Ops reclaim what a killed run leaves.

### Credentials in tests

- No credential value appears in test output, an assertion message, a panic message, or a log line. This covers account keys, client secrets, bearer tokens, static and vended storage keys, session tokens, SAS tokens, and external ids. Tests assert on the values supplied, not on field-name spellings.
- A failure in a credential-bearing helper names the endpoint and the HTTP status and never prints the response body.
- Cloud credentials reach a suite as plain environment variables. Locally they come from the gitignored `test.env`, which the Azure and Glue Make targets load when it exists, on the same recipe line as `cargo`. In CI they come from repository secrets and variables. `.gitignore` lists `test.env`. The committed `test.env.example` names every variable with a placeholder value, states which variables reach the CONNECTION under test and which drive only the container lifecycle, and contains no real credential.
- The Azure suite reads five variables: `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` for the data path, and `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` for the container lifecycle only. The scans and the seed writer never see the service principal. The account should belong to a dedicated test-only StorageV2 account with hierarchical namespace enabled, and the service principal holds Storage Blob Data Contributor on it and no more.
- The Glue suite reads eight variables. Five are repository secrets: `GLUE_ACCESS_KEY_ID`, `GLUE_SECRET_ACCESS_KEY`, `GLUE_ASSUME_ROLE_BASE_ACCESS_KEY_ID`, `GLUE_ASSUME_ROLE_BASE_SECRET_ACCESS_KEY`, and `GLUE_ASSUME_ROLE_EXTERNAL_ID`. Three are repository variables: `GLUE_REGION`, `GLUE_FIXTURE_BUCKET`, and `GLUE_ASSUME_ROLE_ARN`.
- The Azure suite does not request `loadTable` itself to inspect a vended payload, because that would place a live SAS in the test process for diagnostic value only.
- The cloud suite's CONNECTION carries static AWS keys, so a passing vended scan alone cannot prove that Glue vended a key pair. The suite asserts that the credential source selected from Glue's `loadTable` response carries a non-empty `s3.access-key-id` and `s3.secret-access-key`, and fails naming the absent key. It reports, without failing, whether that source carries `client.region`, `s3.endpoint`, and `s3.session-token`.

### Make targets and CI

- Every E2E Make target depends on `cross-udf-build`, so the suite never runs against a stale `.so`.
- `make test-e2e-unity` runs `make unity-up` first. Its `cargo` line stays flag-identical to the `Run Unity Catalog E2E suite` step of the `e2e-unity` job in `.github/workflows/ci.yml`, which is the authority.
- Every E2E CI job needs only `build-so`. A draft pull request skips them through that dependency.
- The `e2e` job runs the whole local Docker suite twice, as two legs of one matrix with identical steps: one leg on a 2025.x image with the check name `E2E`, and one on an 8.29.x image with the check name `E2E (8.29.x)`. Each leg passes its image through `EXASOL_IMAGE`, which both `docker-compose.yml` and the `Makefile` read. Each leg uploads its failure logs under its own artifact name (`exa-logs`, `exa-logs-8x`), because `upload-artifact` rejects a name another upload in the same run already used.
- `E2E` is a required status check on `main`, so one leg keeps that exact name. Adding `E2E (8.29.x)` to the ruleset is an operator action.
- The `release` job needs `e2e`, so it waits for both legs.
- `e2e-lakekeeper`, `e2e-unity`, `e2e-azure`, and `e2e-glue` run on one Exasol version, because they gate catalog integrations that do not vary with the engine version.
- `E2E (Azure)` and `E2E (Glue)` are not required checks and are not in `release`'s needs, so a cloud outage or a rotated key blocks no merge and no release.
- E2E tests run only on x86_64, because `exasol/docker-db` publishes amd64-only images.
- The `arm64` CI job (`Unit Tests (arm64)`) runs `cargo test --workspace` on `ubuntu-24.04-arm`, with no E2E test, coverage instrumentation, or Sonar analysis. Its cargo cache key is architecture-specific, so the x86_64 and arm64 jobs never share a cache.

## Ops

### Orphaned fixture sweeps

Two scheduled GitHub Actions workflows in this repository reclaim the per-run cloud fixtures that a killed E2E run leaves behind: `.github/workflows/azure-orphan-sweep.yml` for Azure containers and `.github/workflows/glue-orphan-sweep.yml` for Glue databases and S3 objects. A killed process (`SIGKILL`, CI cancellation, OOM) skips the in-process teardown, and Azure lifecycle-management rules act on blobs, never on containers. The in-process teardown stays the first-line cleanup. The sweeps are the backstop. Both follow one contract.

- Schedule: each workflow runs weekly on a `schedule` cron (Azure `0 2 * * 1`, Glue `0 3 * * 1`) and on `workflow_dispatch`.
- Selection: a sweep considers only resources the suite names: Azure containers whose name starts with `lhrs-e2e-`, Glue databases whose name starts with `lh_e2e_`, and S3 objects under `lh_e2e/`. It never deletes anything else, so a shared account is safe.
- Retention floor: a resource is stale when the cloud's own timestamp is more than 24 hours before the run start. Azure reads the container's `last_modified`. Glue reads the database `CreateTime` and the object `LastModified`. A sweep never judges age by the millisecond suffix in the name, so it never deletes a fixture of a running suite.
- Scheduled run: a scheduled run carries no `dry_run` input and always deletes every stale candidate.
- Manual run: `workflow_dispatch` exposes a boolean `dry_run` input that defaults to `true`. A dry run lists what a real run would delete and deletes nothing. A run with `dry_run` set to `false` deletes exactly as a scheduled run does. The guard compares the input with the string `'true'`, because a dispatch boolean arrives as the string `'true'` or `'false'` and the non-empty string `'false'` is truthy.
- Nothing to reclaim: a run with no stale candidate succeeds and deletes nothing. An empty candidate list does not fail the step under `set -euo pipefail`.
- Fail loudly: a missing or empty variable fails the run with a message naming the variable and never its value. Any cloud CLI call that exits non-zero fails the run red, and the run never swallows the error. The workflows send no notification. A red run is the signal.
- No credential in the log: no client secret, access token, or secret access key appears in the run log. A step never echoes a secret. A secret reaches the CLI only through the masked environment.
- Azure specifics: the sweep authenticates with the Entra ID service principal from the secrets `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`, and reads the account name from the variable `AZURE_STORAGE_ACCOUNT_NAME`. It never reads `AZURE_STORAGE_ACCOUNT_KEY`, because it touches only the container control plane. It lists each candidate as its name followed by its `last_modified`, in that fixed order. A container already absent at delete time counts as deleted, because `az storage container delete` without `--fail-not-exist` succeeds on a missing container.
- Glue specifics: the sweep reads `GLUE_ACCESS_KEY_ID`, `GLUE_SECRET_ACCESS_KEY`, `GLUE_REGION`, and `GLUE_FIXTURE_BUCKET`, and uses the AWS CLI. A real run prints each deleted name and a final count.

### Remote benchmark harness

`bench/run.sh` runs the benchmark query set against the local Docker stack (`BENCH_TARGET=docker`) or against an external Exasol cluster in AWS (`BENCH_TARGET=remote`). An operator also uses the remote target for a live demo: the demo queries the virtual schema a benchmark run leaves behind. Benchmark and demo differ only in who issues the commands. No setting selects between them.

- The remote target selects its catalog from `BENCH_CATALOG`. Unset or `glue` takes the Glue path with the same required variables, catalog URI, CONNECTION password, and virtual schema properties as before the selector existed. `lakekeeper` takes the Lakekeeper path. Any other value exits with an error that names the accepted values. The docker target ignores the variable, because the local stack has one catalog.
- The Lakekeeper path requires the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, and fails naming the first empty one.
- Each catalog has its own CONNECTION password builder, because the adapter rejects a CONNECTION that combines `use_sigv4` with `client_id` or `client_secret`. The Lakekeeper password carries the warehouse name, the OAuth2 client id, client secret, and token endpoint, plus the static S3 endpoint, region, access key, and secret key. It sets neither the SigV4 flag nor the vended-credentials flag. Both builders double every single quote in an embedded value before the JSON reaches a SQL string literal.
- The Lakekeeper path sets `ALLOW_HTTP` on the virtual schema, because the in-VPC Lakekeeper and Keycloak endpoints are plain HTTP. The Glue path's extra-properties block carries no `ALLOW_HTTP`.
- Both remote paths pass `PARALLELISM_FACTOR` as a virtual schema property from `BENCH_PARALLELISM_FACTOR`, with the same default the docker target uses, and never emit an empty extra-properties block. The harness passes no `NR_OF_CORES` property and reads no `BENCH_NR_OF_CORES` variable, because the adapter detects the core count on its executing node. The remote run sizes its per-instance thread and connection budgets from that detected count.
- On the remote target only, the report header names the catalog the run used. The field carries the catalog name and never an `s3://`-shaped value, because `bench/import_ceiling.sh` greps the report for `s3://.../lineitem`. The docker target's header carries no such field.
- The harness drops the virtual schema only as the first half of a drop-then-create pair at the start of a run. It never drops the virtual schema at the end of a run and never drops the CONNECTION, so both stay queryable for a demo. The offline self-check asserts this against the source text of `bench/run.sh` only.
- That guarantee covers the harness, not the operator wrapper. `deploy/scripts/bench-remote.sh` runs `deploy/scripts/cluster-down.sh <env>` on every exit unless `KEEP_ALIVE=1` is exported, which destroys the cluster and with it the CONNECTION and the virtual schema. A live demo therefore never uses a default `bench-remote.sh` run.
- `deploy/README.md` states the full ordered demo runbook in two forms, both with `BENCH_CATALOG=lakekeeper` set explicitly and both running `deploy/scripts/lakekeeper-up.sh <env>` before any `secrets.sh <env>`. Wrapper form: `lakekeeper-up.sh <env>`, then `BENCH_CATALOG=lakekeeper KEEP_ALIVE=1 ./bench-remote.sh <env>`. Unwrapped form: the `cluster-stack` `tofu apply`, then `lakekeeper-up.sh <env>`, `cluster-up.sh <env>`, `secrets.sh <env>`, and `BENCH_CATALOG=lakekeeper make bench`. The runbook ends with `cluster-down.sh <env>` and `lakekeeper-down.sh <env>`, because both forms leave a running, billing cluster.
- The offline self-check asserts the extra-properties block of each path and prints no client secret, access key, or secret key.

### AWS Lakekeeper benchmark catalog

An opt-in, ephemeral Lakekeeper Iceberg REST catalog in the AWS benchmark environment serves the TPC-H tables that Glue already catalogs, by reference. OpenTofu and bash deploy and provision it. It adds no engine, adapter, CONNECTION field, or virtual schema property, and no Rust code. With no new variable set, every existing `deploy/` and `bench/` path behaves as before.

Stack lifecycle:

- `deploy/scripts/lakekeeper-up.sh <env>` applies a separate OpenTofu stack layered on `data-stack`, modeled on `deploy/trino-stack/`. It reads `data-stack` values only and no `cluster-stack` output, so it can run before the cluster exists. No `data-stack`, `cluster-stack`, or `trino-stack` apply creates, changes, or destroys it.
- The stack creates one EC2 instance in the `data-stack` subnet, named with the `<project>-<env>-lakekeeper` prefix and carrying the shared `exa:*` default tags. Its user data runs PostgreSQL, Keycloak, a run-once Lakekeeper `migrate`, and Lakekeeper `serve`, mirroring `docker-compose.lakekeeper.yml`, and reads the `data-stack` S3 bucket.
- The security group admits SSH from the operator allowlist only, and the Lakekeeper and Keycloak ports from the operator allowlist and the VPC CIDR. The allowlist defaults to the apply machine's public IP as a `/32`, resolved at apply time.
- The script waits for the Lakekeeper health endpoint before provisioning, then prints the connection details and a cost-and-teardown reminder that names `lakekeeper-down.sh <env>`.
- The stack needs no change to `deploy/iam/deployer-policy.json`, because every resource it creates is wildcard-permitted or carries the `<project>-*` prefix.
- The stack publishes every non-secret connection value under its own SSM root as a `String` parameter (warehouse name, OAuth2 client id, catalog and token URIs for both vantages) and the OAuth2 client secret as a `SecureString`. It also exposes the same values as outputs. Outputs and parameters never diverge, and the stack generates no second copy of a value.
- `deploy/scripts/lakekeeper-down.sh <env>` destroys that environment's Lakekeeper workspace only: the instance, its security group, its IAM user and access key, and its SSM parameters. It never touches the `data-stack` bucket, the Glue catalog, the Exasol cluster, or the Trino stack. A later `lakekeeper-up.sh` yields a working catalog again from a clean box.

OIDC:

- The box has a public IP for the operator and a private IP for the cluster. Keycloak stamps the token issuer from the request host. Lakekeeper's primary OIDC provider names the private-IP issuer, which the UDF's token carries. Its additional issuers name the public-IP issuer, which the operator's provisioning token carries.
- The realm (`iceberg`), client id (`lakehouse`), client secret, and audience (`lakekeeper`) are the values in `scripts/keycloak-realm-iceberg.json`, the same ones the local Lakekeeper suite uses. The stack never generates a second client secret. That committed secret is a named, accepted seam whose only control is the security group, and `deploy/README.md` § Known seams names it.
- The Keycloak health gate tests the imported realm: it succeeds only once `/realms/iceberg/.well-known/openid-configuration` returns a body containing `jwks_uri`.
- The stack generates the PostgreSQL password, the Lakekeeper metadata-encryption key, and the Keycloak bootstrap admin password, and stores them as SSM `SecureString` parameters under a Lakekeeper-specific root. None of them is a literal from `docker-compose.lakekeeper.yml`. No generated secret appears in script output or in a non-sensitive OpenTofu output.

Storage credential:

- The engine's `engine-reader` IAM user is read-only, and Lakekeeper validates a new warehouse by writing, reading, and deleting a probe object. The stack therefore creates its own IAM user, `<project>-<env>-lakekeeper`, with object put, get, and delete plus bucket list on the `data-stack` bucket and nothing else. It builds the user from `aws_iam_user`, `aws_iam_policy`, `aws_iam_user_policy_attachment`, and `aws_iam_access_key`, never from an inline `aws_iam_user_policy`, because the deployer policy lacks `iam:PutUserPolicy`.
- The write and delete grants are bucket-wide, a named, accepted risk: the warehouse prefix is derived after the apply, so the policy cannot name it.
- The stack stores that user's key pair as SSM `SecureString` parameters, the only channel to the provisioning script, and destroys the user with the stack. The deployment never disables Lakekeeper's storage validation.
- The Exasol CONNECTION keeps the read-only `engine-reader` keys, and the warehouse sets `sts-enabled` false, so the query path gains no write permission.
- The warehouse's `delete-profile` is the soft profile, sent as `{"type": "soft", "expiration-seconds": 604800}`. It defers file removal and does not prevent it, so it is the secondary control. The primary control is that `deploy/scripts/lakekeeper-provision.sh` contains no destructive verb: no HTTP `DELETE` (`-X DELETE`, `--request DELETE`), no `purgeRequested`, and no `aws s3 rm`, `aws s3api delete-object`, or `aws s3api delete-objects`. A source-text check scans that one script. `lakekeeper-down.sh` and `deploy/scripts/tests/lakekeeper-local.test.sh` are out of its scope.
- The local verification script is the single permitted exception: it drops its own throwaway source tables without purging, never against AWS, and registers a non-colliding table pair as a positive control.
- The provisioning hop to Lakekeeper and Keycloak is plain HTTP, a named, accepted seam recorded as the fourth entry of `deploy/README.md` § Known seams. From the operator's laptop, the warehouse body with the write-and-delete key pair, the OAuth2 client secret, and every bearer token cross the internet in cleartext. A captured key stays usable until `lakekeeper-down.sh` destroys the user. Every deployment includes at least one such public-vantage run, because `lakekeeper-up.sh` always provisions from the operator's machine. The security group bounds who can reach the port. It is a reachability control, not a confidentiality control, and no artifact claims otherwise.

Provisioning script (`deploy/scripts/lakekeeper-provision.sh`):

- One script serves both run sites, and `lakekeeper-up.sh` invokes it. It obtains AWS credentials only through the AWS CLI's standard chain. It never passes `--profile`, never reads `~/.aws/credentials` or `~/.aws/config`, and never queries the instance metadata service. It passes an explicit `--region` on every AWS call, because an EC2 instance profile supplies no region. It never invokes `tofu`, `ssh`, or `scp`.
- `lakekeeper-up.sh` and `lakekeeper-down.sh` run on an operator machine only. An EC2 run site re-provisions by calling the provisioning script directly against a stack already applied. It reads every `LK_SOURCE_*` value from the `data-stack` SSM root and every `LK_TARGET_*` value from this stack's SSM root, and reads no OpenTofu output. It needs an instance profile with `ssm:GetParameter` on both roots, `kms:Decrypt`, `glue:GetTables`, and `s3:GetObject` on the data prefix. The operator supplies that box. No stack in this repository creates it.
- The target URIs belong to the run site: a caller inside the VPC receives the private-IP URIs, a caller outside receives the public-IP URIs. The script reads them verbatim from `LK_TARGET_*` and never rewrites them.
- No credential appears in the argv of any process the script spawns, including `curl`, `jq`, and `aws`, because `/proc/<pid>/cmdline` is world-readable. Credentials reach `curl` through a file descriptor. A credential-bearing request body reads the credential from `jq`'s environment (`env.LK_TARGET_SECRET_ACCESS_KEY`, `env.LK_TARGET_ACCESS_KEY_ID`), never from `--arg`. Non-credential values may use `--arg`.
- Every request body is built with `jq -n` and typed argument flags, never by string interpolation or a heredoc. `set -x` appears nowhere in the script.
- A response body kept for classification goes to a file under a `mktemp -d` directory removed by an `EXIT` trap, and no error path prints it. An error message names the endpoint, the table or warehouse, and the HTTP status only. No success path echoes a credential.
- The offline stubbed-PATH harness records the argv of every stubbed command and asserts that no secret appears in any of them. It also asserts each emitted request body's exact structure against the Lakekeeper v0.13.1 shapes. The local Docker verification sends every body to a real Lakekeeper 0.13.1.

Bootstrap and warehouse:

- The script obtains a Keycloak token through the client-credentials grant before its first management request. It decides whether to bootstrap from the server-info `bootstrapped` boolean, treating an ambiguous answer as not bootstrapped. The bootstrap request accepts the terms of use, and both success and `409 Conflict` count as bootstrapped.
- The script creates exactly one warehouse whose S3 profile names the `data-stack` bucket, its region, `sts-enabled` false, the soft delete profile, and the derived key prefix. The profile uses the AWS S3 flavor with no path-style setting when no S3 endpoint is configured, and the S3-compatibility flavor with path-style access when one is. It carries no STS role. The storage credential uses the canonical `access-key-id` and `secret-access-key` field names.
- Warehouse creation succeeds over a prefix that already holds the source tables' files.
- Any 2xx, a `409 Conflict`, and a `400 Bad Request` reporting a storage-profile overlap count as already present. After any already-present answer, the script reads the warehouse back and fails, naming expected and returned values, unless the bucket and key prefix equal the derived ones. Any other status fails naming the endpoint, the warehouse, and the status, never the body.
- A re-run against a provisioned server exits successfully and changes nothing.

Registration:

- The script enumerates the source namespace (the Glue database at SSM `/<project>/<env>/namespace/tpch` on AWS) instead of a fixed table list, and normalizes every table to `(name, metadata_location, table_location)`. `LK_SOURCE_KIND=glue`, the default, reads the source with `aws glue get-tables`. `LK_SOURCE_KIND=rest` reads it from an OAuth2-bearer Iceberg REST catalog, so the local verification drives the same downstream code. The Glue Iceberg REST endpoint is not used, because `curl --aws-sigv4` cannot send the session token an instance profile needs.
- The script fails naming a table that has no `metadata_location`. It takes each table's root from the `location` field inside the metadata document.
- The script derives the warehouse bucket and key prefix so that every table's metadata location and recorded root are strict sublocations of `s3://<bucket>/<key-prefix>`, shortening a prefix equal to a table root to its parent, because Lakekeeper rejects a location equal to the warehouse base. It fails naming the tables when two tables sit in different buckets, and fails naming the bucket when the derived prefix would be empty.
- The script creates the target namespace before registering, treating already-exists as success, and fails naming the namespace when it is one Lakekeeper reserves (`system`, `examples`, `information_schema`).
- For each table, the script sends one register-table request (`POST {catalog-base}/v1/{prefix}/namespaces/{namespace}/register`) with the metadata location verbatim, the source name byte-identical, and `overwrite` set explicitly to `false`. It writes, copies, or rewrites no metadata, manifest, or data file.
- An already-registered table counts as success. The script confirms every fresh `2xx` and every unidentified `409` with a `loadTable` read-back of the metadata location, and treats a mismatch as a distinct failure, because Lakekeeper 0.13.1 answers a location conflict and an already-registered re-run with the same `409` body. Response-text matching may be logged and is never the sole basis for the outcome.
- The script prints a per-table summary of registered, already-present, and failed tables, and exits non-zero when any table failed.
- `--source-only` runs enumeration and normalization, prints each triple, and exits. It sends no target request and writes nothing, and exits non-zero naming a table without `metadata_location`. Any other argument is rejected.
- The TPC-H `part`/`partsupp` location shape registers without error on Lakekeeper 0.13.1. The local verification registers that pair in both orders as a regression test.

Bench secrets:

- `deploy/scripts/secrets.sh <env>` keeps the existing Glue, AWS, and Exasol variables in `bench/.env` unchanged. When a Lakekeeper workspace exists for the same environment, it adds the Lakekeeper catalog URI, warehouse name, OAuth2 client id, client secret, and token endpoint, with every URI built from the box's private IP. It never sets `BENCH_CATALOG`. Without a Lakekeeper workspace, it omits the block, prints a note, and exits successfully. The file keeps owner-only permissions, and no secret is echoed.
