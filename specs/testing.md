# Testing

This file holds the rules for how this repository tests its behavior and how it keeps its test environments clean. Feature behavior lives in the feature specs under `specs/<domain>/<feature>/spec.md`. This file states no feature behavior. It states how tests prove that behavior, how the end-to-end (E2E) suites are built and run, and how operators clean up the test environments. The benchmark harness and the AWS Lakekeeper benchmark catalog are not in the spec library. Their rules live in `bench/requirements.md`.

## Testing

### Coverage rule

- Every scenario in a feature spec has at least one E2E test or host integration test over local fixtures that exercises it. A test against a fake server does not count as that proof (§ Testing strategy). A unit test alone is enough only for pure computation with no I/O.
- A test that implements a scenario carries one `/// Scenario: <title>` line per scenario, quoting the title verbatim (`AGENTS.md` § Code style).
- An E2E suite is a place where feature scenarios get proof against a live Exasol. It holds no requirement of its own. When an E2E test finds behavior that no feature spec states, that behavior goes into a feature spec, not into this file.
- An E2E test proves the behavior a user sees through Exasol SQL. A property that Exasol SQL cannot observe stays proved by its unit or integration test, and an E2E test does not assert it.
- A scenario whose state no well-formed Exasol request can reach is proved by a unit test over a synthesized request, not by an E2E test.

### Test tiers

- Unit tests run on the host with `cargo test`. They live in a sibling `_tests.rs` file (`AGENTS.md` § Code style).
- Integration tests run on the host with `cargo test` against local fixtures, for example a local-filesystem object store over the vendored Delta fixtures. They need no Exasol. A test against a fake server is a fault-injection test, not an integration test.
- Scan UDF tests run `run()` on the host through `exasol_udf_sdk::test_support::TestContext` (`EmitPolicy`, `NextPolicy`). A new scan test uses this pattern.
- E2E tests run against a live Exasol and a real or containerized catalog and object store. Each E2E suite is gated by its own cargo feature, so the plain `cargo test` run never needs a stack.

### Testing strategy

- Two layers only. E2E tests prove the behavior a user sees, against the real stack. Unit tests cover pure logic (parsing, mapping, validation, building requests, reading answers) and what coverage needs. Each behavior is tested at one layer, not repeated at several.
- A fake server is for fault injection only: failures an E2E test cannot produce, such as timeouts, cut-off answers, and unreachable hosts. There is at most one shared fake per external system. A fake never stands in for a catalog, object store, or STS to prove behavior, because it tests our idea of the external system, not the system.
- Unit tests stay simple: plain inputs and expected outputs. They assert nothing on a fake's request log or call order, and no test reads source files.
- No production function exists only so a test can inject a value.
- Access control is proved end to end. A permission feature needs E2E tests for an allowed user, a denied user, a query over several tables where only some are granted, a view, and a revoked grant.

### Unit tests

- A unit test that pins a rule uses an input that fails under the wrong rule. A feature-specific rule of this kind lives in that feature's spec.
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
- When a declined filter would return the same rows as a pushed one, the test asserts the generated pushdown SQL, not the row set.
- A test asserts a constant result, such as a projected `TIMESTAMP WITH LOCAL TIME ZONE` literal, by exact value; only a moving clock value is compared within a tolerance.
- A test that proves a predicate prunes nothing on Parquet statistics uses a multi-row-group file whose per-group statistics would falsely exclude a matching row, never a single-row-group file that pruned nothing.

### Regression guards

- A change that a feature spec declares behavior-preserving keeps every existing unit, integration, and E2E test assertion, golden-SQL string, and `dispatch_golden` fixture unchanged. The only exceptions are the edits the change itself requires, such as import paths and call sites of a renamed or demoted item, and the one value the spec names as changing. No assertion is weakened, disabled, or deleted to make a test pass. A dedicated test cleanup may delete a test, provided the coverage stays the same and each scenario is still proved at one layer.
- A committed golden SQL fixture (`dispatch_golden`) changes only with an intended change of the generated SQL. A diff in any other change is a regression, not an expected update.
- A test whose assertion a change makes false is deleted together with its section banner and its test-only imports.
- When code moves to another crate, its test module moves with it and keeps every test name and expected value. A test of a property of the code left behind stays with that code. A helper that several test modules share is declared once in the crate's `#[cfg(test)]` support module. No test is deleted, disabled, or weakened to fit a visibility change.
- A test-only helper that unwraps a production type's private payload is declared in its crate's `#[cfg(test)]` support module and is never `pub`.
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
