# Testing

This file states how tests prove the behavior that the feature specs (`specs/<domain>/<feature>/spec.md`) describe, how the end-to-end (E2E) suites are built and run, and how operators clean up after them. It states no feature behavior. Benchmark rules live in `bench/requirements.md`.

## Testing

### Coverage rule

- Every scenario in a feature spec has at least one E2E test, with four exceptions. A unit test alone proves a scenario of pure computation with no I/O. A fault-injection test alone proves a failure scenario that no E2E test can produce, such as a timeout, a cut-off answer, a transient failure and its retries, or an unreachable host. A scan-plan test alone proves a scan property that no SQL result shows. A request-property test against a fake server alone proves a request property that no SQL result shows (§ Testing strategy). Any other host test that reads files or lists a store proves no scenario.
- A scenario states behavior a user or an operator can see. It never names an internal function or type.
- A test that implements a scenario carries one `/// Scenario: <title>` line per scenario (`AGENTS.md` § Code style). A unit test carries the line only when it is the scenario's one proof.
- An E2E test proves what a user sees through Exasol SQL. A unit test proves a property that Exasol SQL cannot observe. A unit test over a synthesized request proves a state that no well-formed Exasol request can reach.
- An E2E suite holds no requirement of its own. Behavior that an E2E test finds and no feature spec states goes into a feature spec.

### Test tiers

- Unit tests run on the host and live in a sibling `_tests.rs` file (`AGENTS.md` § Code style).
- Host tests are unit tests, fault-injection tests against a fake server, scan-plan tests, compile-time contract tests, repo-convention tests, and the scan UDF tests below. There is no other host integration tier. A host test that reads a catalog or a remote object store, or chains adapter modules to prove behavior, is an E2E test in the wrong place.
- A scan-plan test writes Parquet files under its own `tempfile::tempdir()` and runs the engine's scan plan over them. It proves a property that no SQL result shows, such as the row groups or files a predicate prunes, the shape of the DataFusion plan, or the columns a scan reads. A property that the returned rows show is proved by an E2E test.
- A repo-convention test reads repository files to pin wiring that no build checks, such as a test module that no target declares or a dependency that a crate must not take. It asserts on structure, never on the wording of prose.
- Scan UDF tests run `run()` on the host through `exasol_udf_sdk::test_support::TestContext` (`EmitPolicy`, `NextPolicy`).
- A crate's host tests under `tests/` build into one binary, `tests/it/main.rs`, because each test binary statically links the whole engine. A new test file is a module declared there. A top-level `tests/*.rs` file is a separate target for a test that needs its own process.
- The `it` binary runs its tests as parallel threads of one process. A test writes its own files only under its own `tempfile::tempdir()`. A test that asserts on process-global state shares one lock with every test that mutates that state, or keeps its own binary. A file at a fixed path that production code owns, such as the telemetry log, is process-global state.
- E2E tests live in `crates/lakehouse-e2e`, which does not depend on the engine crates, so no E2E binary links or rebuilds the engine. Each suite is a `[[test]]` target whose `required-features` names its cargo feature, so a plain `cargo test` never builds a suite. A suite runs as `cargo test --workspace --features lakehouse-e2e/<feature> --test <binary>`.
- Every host cargo run selects the whole workspace and narrows with `--lib`, `--test`, or a name filter, because `-p` or `--exclude` changes the enabled dependency features and compiles a second copy of the dependency graph. `make cross-udf-build` is the one exception: it builds with `-p lakehouse-engine`, so the shipped `.so` carries no feature that only a test crate enables.
- `make lint` runs clippy with `--all-targets --all-features`, because `required-features` hides the E2E targets otherwise. Only `lakehouse-e2e` declares features, and they enable no dependency feature.

### Testing strategy

- There are two layers. E2E tests prove user-visible behavior against the real stack. Unit tests cover pure logic: parsing, mapping, validation, building requests, and reading answers. Each behavior is tested at one layer. A coverage gap is closed by a unit test of pure logic, never by a multi-module host test or a fake.
- A fake server injects only the faults an E2E test cannot produce (§ Coverage rule). There is at most one fake per external system. A fault-injection test proves how the engine reacts to a fault. It never proves how the external system behaves, because a fake encodes an assumption about that system, not the system.
- A fake server also serves a test of a request property that no SQL result shows, such as the engine resolving a join's tables concurrently. That test asserts only that property. No fake-server test asserts which internal module sent a request.
- A unit test has one unit, plain inputs, and expected outputs. It does no I/O, writes no file, starts no server, asserts nothing on a fake's request log or call order, and reads no source file.
- A unit test that pins a rule uses an input that fails under the wrong rule.
- No test restates standard-library behavior.
- No production function exists only so a test can inject a value.
- Access control is proved end to end: an allowed user, a denied user, a query over several tables where only some are granted, a view, and a revoked grant.

### Test code

Test code gets the same design care as production code. A test reads as the one behavior it proves: its body holds only the inputs and expectations that set it apart from its neighbors, and the setup it shares with them lives in a support module.

- An area keeps its shared test helpers in a `#[cfg(test)]` support module, in a file ending in `test_support_tests.rs` in the area's directory. A crate's `tests/` binaries share theirs in a module directory under `tests/`. A new test starts from the helpers there.
- A helper that a second test file needs moves into its area's support module. A helper that one file uses stays in that file.
- A support module opens with a one- or two-line doc comment that says what it builds, so a reader can pick the right module without reading it whole.
- Tests build request JSON, column nodes, Exasol types, file entries, scan specs, and join specs through the support builders, not as inline JSON or struct literals. A literal spells out every field, so a field added later edits every test instead of one builder. A test that pins the wire shape of one request node keeps that node as literal JSON.
- `#[cfg(test)]` code is visible neither to another crate nor to the crate's own `tests/` binaries. A builder needed on both sides of that boundary has one copy on each side, with the same name and signature.
- A builder defaults every value that most tests share. A test sets only the values its behavior depends on.
- The change that writes the second copy of a setup block, fixture, or assertion helper extracts it into a shared helper in the same change.
- Variants of one scenario are rows of one case table, not copies of one test body.
- A constructor on a production type serves tests only when production code calls it too. A constructor that only tests call is a test helper and lives in the support module.
- A test-only helper that unwraps a production type's private payload lives in its area's support module and is never `pub`.

### E2E suites

| Suite | Cargo feature | Test binaries | Make targets | CI job (check name) | Stack | In `release` needs |
|-------|---------------|---------------|-------------|---------------------|-------|--------------------|
| Local Docker | `exasol-e2e` | `e2e_local` | `make e2e-up test-e2e` | `e2e` (`E2E`, plus one check per older Exasol version) | Exasol, SeaweedFS, Iceberg REST catalog, forward proxy (`docker-compose.yml`) | yes |
| Lakekeeper | `lakekeeper-e2e` | `e2e_lakekeeper_test` | `make lakekeeper-up test-e2e-lakekeeper` | `e2e-lakekeeper` (`E2E (Lakekeeper)`) | Exasol, SeaweedFS, Iceberg REST catalog, forward proxy, Lakekeeper, its PostgreSQL database, Keycloak, OpenFGA | yes |
| Unity Catalog | `unity-e2e` | `e2e_unity_test` | `make unity-up test-e2e-unity` | `e2e-unity` (`E2E (Unity)`) | SeaweedFS, Exasol, forward proxy, OSS Unity Catalog (`docker-compose.unity.yml`) | yes |
| Azure | `azure-e2e` | `e2e_azure_test` | `make azure-up test-e2e-azure` | `e2e-azure` (`E2E (Azure)`) | Exasol, forward proxy, Lakekeeper, its PostgreSQL database, Keycloak, OpenFGA, plus a real ADLS Gen2 account | no |
| Glue | `glue-e2e` | `e2e_glue_test` | `make glue-up test-e2e-glue` | `e2e-glue` (`E2E (Glue)`) | Local Exasol and forward proxy plus a real AWS Glue Data Catalog and S3 bucket | no |
| Cloud (opt-in) | `cloud-e2e` | `cloud_e2e_test` | none | none | A real Exasol cluster plus the AWS Glue Iceberg REST catalog | no |

- A new test module of the Local Docker suite is declared in `crates/lakehouse-e2e/tests/e2e_local/main.rs`. A module file the suite does not declare never compiles and never runs.
- Each suite runs with `--test-threads=1`, because all tests of one binary share one provisioning.

### Failure contract

- Every suite except `cloud-e2e` fails, and never skips, when its stack, a credential variable, or its cloud account is unavailable.
- A missing or empty credential variable fails the suite with a message that names the variable and never its value. The Glue suite checks its variables before it creates any cloud resource.
- The Lakekeeper suite fails when the stack reports an authorization backend other than OpenFGA.
- A fork pull request receives no repository secrets, so the Azure and Glue jobs fail naming the missing variable.
- `cloud-e2e` is the one opt-in suite. It skips, naming the absent variable, when its AWS or assume-role variables are unset, and then it makes no network call.
- No test asserts a latency threshold. The cloud performance smoke test records its query duration for manual inspection.

### Shared harness

- `crates/lakehouse-e2e/tests/common/e2e_harness.rs` is the one scan-path provisioning: the SLC install, the `.so` upload, the scripts, and the virtual schema. Every E2E binary uses it, so the script DDL is identical across binaries, and no binary re-declares it. `cloud-e2e` targets an installation that already exists, so it creates its virtual schemas through one helper of its own, over that installation's adapter script and without `ALLOW_HTTP`.
- The harness installs the scripts in their production shape: `LAKEHOUSE_SCAN` as a SCALAR script with dynamic EMITS over the uploaded `.so`, `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET script with no `.so`, and the adapter script.
- The harness grants `ACCESS ON CONNECTION ... FOR SCRIPT` to `CURRENT_USER` after the last `CREATE OR REPLACE` of the CONNECTION or a script, because a replacement drops the grant. It skips the grant for `SYS`, which Exasol refuses and which holds every CONNECTION implicitly.
- A binary passes its virtual schema names and properties as explicit parameters. A parameter it does not supply is omitted from `CREATE VIRTUAL SCHEMA`, never emitted empty.
- Each binary runs one stack setup plus one lazy setup per fixture, so a name filter provisions only the fixtures it uses.
- The shared virtual schema DDL sets `ALLOW_HTTP = 'true'`.
- A test that needs a persisted state the current adapter cannot produce, such as a stray property or a legacy adapter note, swaps a stub adapter script in to persist it, then restores the engine adapter. It is the E2E form of a synthesized request (§ Coverage rule).
- A test reads a failing statement's error text only through `error_text`, which asserts the error status and names the statement or case.
- Async fixture work runs on the harness's one shared Tokio runtime. Only a teardown that runs from `Drop` builds its own runtime, on its own thread.
- Every seed writes through the one shared seed-catalog configuration, which selects the storage backend. No suite forks the table create, write, and commit logic. The Glue suite passes the shared writer a memory catalog, because Glue holds only the pointer to a table's metadata file.
- No E2E code uses `lakehouse-engine`, `lakehouse-catalog`, or `vs-expression`, because the suites treat the deployed `.so` as a black box. A name the engine emits into pushed SQL is a test-owned constant, so renaming it in the engine fails the test.
- The harness resolves fixture file lists in the test process through iceberg-rust's scan planner (`resolve_fixture_files`) and returns harness-owned types.

### Exasol session client

- Every suite drives Exasol through `ExaConn` in `crates/lakehouse-e2e/tests/common/exasol_ws.rs`.
- The client sends `resultSetMaxRows` `0`, meaning no limit, unless the call site declares `capped_result_sets(n)`. A cap reaches the adapter as a pushdown `limit` that `EXPLAIN VIRTUAL` cannot see (`docs/debugging-pushdown.md`), so a test that inspects plan shape stays uncapped.
- The client fetches until it holds the `numRows` that the result-set metadata reports, and fails when a fetch returns zero rows while rows remain.
- Every connection that sends a real cloud credential in DDL is made with `connect_redacting`, whose failure messages omit the SQL and the Exasol response. Connections that send only a local stack's dev credentials keep the SQL for debugging.

### Correctness oracles

- An E2E test asserts only on what Exasol shows: the pushdown SQL from `EXPLAIN VIRTUAL`, the returned rows, the column metadata in the `SYS` views, and the error text of a failing statement. A fixture guard is the one exception.
- No E2E assertion uses engine code to compute or judge its expected value. A credential check compares against the secrets the test supplied, not against the engine's redaction logic.
- Tests do not re-verify iceberg-rust, DataFusion, Arrow, or Parquet. The row oracles guard their integration with the engine.
- A fixture guard proves only that a fixture has the shape its row test needs, so the row test cannot pass vacuously. It reads the fixture through the library that wrote or plans it, or asks the catalog server over its own REST API.
- A row assertion compares against one of three oracles: the seeded source data, the same query without pushdown or on a single DataFusion node, or the key-first ordering of the same grouped query with the column positions transposed.
- Row deletes, such as positional deletes and deletion vectors, are proved by the absence of the deleted rows. How delete references reach the scan UDF is a wire contract that unit tests prove.
- A test that must prove a query reached the scan UDF asserts on the pushdown SQL, so a silent fallback to an unaccelerated wrapper fails instead of passing on correct rows. When a declined filter would return the same rows as a pushed one, the test also asserts the SQL, not the rows.
- A pushdown shape the adapter advertises is proved against a live Exasol, never inferred from the capability registry or from code.
- Partial aggregation shows as the `group_keys` entry in the scan spec and the wrapper's merge of the `PARTIAL_*` columns, with no `IPROC()` and no raw row scan. A `GROUP BY shard_key` fan-out is no evidence, because it appears only when the files span several shards.
- A regression test uses a discriminating fixture whose predicate matches a strict subset of the rows. A test that proves a predicate prunes nothing uses a multi-row-group file whose per-group statistics would falsely exclude a matching row.
- When it is uncertain whether a fixture reproduces a defect, the suite establishes it by observation and fails saying so.
- A test that depends on shard placement forces it through the shard count or `PARALLELISM_FACTOR`.
- A constant result is asserted by exact value. A tolerance is allowed only for a moving clock value and for a floating-point aggregate whose low bits depend on the order in which shards merge, such as `AVG`, `STDDEV`, or a quotient. A sum of whole-number doubles is exact in any order. The expected value of a widened `float` column is the f32's exact double expansion, not the decimal literal that was written.
- A declared timestamp type is asserted in full, with the precision read from the live engine version through one shared, test-owned helper. A rendered-timestamp oracle casts to the type that helper returns.
- A type assertion that must hold on both Exasol versions matches by prefix. An exact assertion sits beside it and does not replace it.
- A timestamp-precision test asserts the `TIMESTAMP(p)` literal in the generated `EMITS` clause, then the distinct value count the width admits. A wrong value under a query that raised no error marks an engine accept-and-clamp, not an SLC rejection.
- A type-coverage test compares the declared types in `SYS.EXA_ALL_COLUMNS` with an exact list, compares one `SELECT ID, <columns> ... ORDER BY ID` over the readable columns with exact values, reads each refused column alone, and shares its column values with the other suites.
- A test that proves vended credentials rules out the static path. Its CONNECTION carries no storage endpoint or keys, and the test asserts that, because CONNECTION values override vended ones. Where the CONNECTION must carry static keys, the test asserts that the catalog's answer vended a key pair.

### Regression guards

- A change that a feature spec declares behavior-preserving leaves every test assertion unchanged. The exceptions are the edits the change requires, such as import paths and call sites of a renamed item, and the one value the spec names as changing.
- No assertion is weakened, disabled, or deleted to make a test pass or to fit a visibility change. A dedicated test cleanup may delete a test when each scenario keeps its proof at one layer.
- A golden-SQL fixture (`dispatch_golden`) changes only with an intended change of the generated SQL.
- A test whose assertion a change makes false is deleted with its section banner and its test-only imports.
- Code that moves to another crate takes its tests along, with every test name and expected value. Tests of the code left behind stay with that code.
- A structural contract that no runtime test can observe is pinned by a compile-time test.

### Fixtures

- Iceberg fixtures write through `crates/lakehouse-e2e/tests/common/seed.rs`: one shared create-or-load path and one shared data-file writer that takes a file-name prefix, a partition value, and Parquet writer properties. A fixture that evolves its schema creates its own table. A fixture whose values iceberg-rust refuses writes its Parquet file directly. A fixture with nested columns builds its batch from the catalog-assigned schema, because the REST catalog reassigns nested field ids.
- Raw Parquet fixtures write through `crates/lakehouse-e2e/tests/common/raw_parquet.rs`, one file per object key, with no catalog table, snapshot, manifest, or field ids. The local stacks write to `local_stack_s3_store()`, and the Glue and Azure suites write to their run's bucket prefix or container, through the same `put_object`. It deletes every object under a key prefix, and it deletes one object by key, because a prefix listing matches whole path segments and misses an object whose key equals the prefix.
- Fixture authoring is idempotent, because the SeaweedFS volume outlives a run. A persisted table whose schema differs from the authored one is dropped and rewritten.
- A fixture whose test enumerates its namespace or mutates its tables has its own namespace and virtual schema, so no other test's enumeration sees it. Other fixtures, the Spark fixtures among them, share the suite's namespace. A fixture that a test mutates also has its own base path, which the test empties before it writes the initial state. It asserts no exact skipped-table list before its first mutation, because SeaweedFS keeps an emptied directory as a listed prefix.
- A fixture whose test needs a failed enumeration sits under a base path that no passing test enumerates.
- A fixture-shape test reads the written file's own Parquet footer and asserts its physical encoding, so a read test cannot pass against a normalized fixture. Every Spark fixture whose test depends on a physical encoding has one. The positional-delete and `mor_dv` fixtures hold their shape in metadata, so their guards read it through the scan planner (§ Correctness oracles).
- Suites that compare two credential arms seed both from one deterministic row shape. Equal rows prove both arms read correctly, not that they read the same bytes.
- Type-coverage tables sit beside a suite's other fixtures and use the virtual schema the suite already creates. Each holds the ids 1 to 3, with row 3 NULL in every column but `id`, and timestamps exact to the millisecond, so they read the same at every precision.
- Pruning fixture (`tests/common/pruning_fixture.rs`): the same rows in an Iceberg table, a Hive directory, a Delta table in Unity Catalog under `unity.e2e_pruning`, and a native oracle table with a `FILE_LABEL` column naming each row's data file. The writers produce several row groups per file and several pages per row group, with a page index, and fixture-shape tests assert that, including that the Delta log statistics match the footers. One case table runs on all three formats, and the oracle declares each column with the type the virtual schema maps it to.
- Apache Spark writes the Iceberg fixtures that iceberg-rust cannot author, through the one-shot `spark-iceberg-fixtures` Compose job against the shared REST catalog. `run_fixtures.sh` lists each script explicitly, so a new script needs its own entry. All scripts run in one `spark-sql` session to save startup time, with the INT96 script last because it switches the session's Parquet writer. The step fails, and never skips, when a service it needs is down. Each fixture's ground truth lives in the Rust harness, in lockstep with its SQL script.
- The Spark fixtures:
  - Positional deletes: merge-on-read tables at `file` and `partition` granularity. The partition table's deletes reference data files in several partitions. Spark writes them because iceberg-rust has no position-delete writer.
  - `mor_dv`: a format-version-2 table upgraded to version 3, with a kept positional delete, a delete merged into a deletion vector, a replaced vector, a vector that spans several roaring containers, and two vectors in one Puffin file.
  - `unsupported_format`: a Parquet and an ORC data file over disjoint `id` ranges. No equality-delete fixture exists, because Spark writes none.
  - `int96_ts_far_future`: an INT96 `timestamp` holding `9999-12-31 23:59:59`, registered with `add_files`, the only route to INT96. The value lies outside the Arrow nanosecond range and inside both the Arrow microsecond range and Exasol's `TIMESTAMP` maximum.
  - `ident_part_absent` and `ident_part_types`: Hive-style files without the partition column, registered with `add_files` so only the manifest carries the value. They mix in native files, null partition values, a file whose stored values differ from its partition value, a partition-spec change, and an identity field on each primitive type.
  - `nested_name_map`: an imported file without field ids, then renames at every nesting level, including a member renamed before the import and an old name reused so that it contradicts a native file's embedded id.
  - `projection_by_id`: a hand-set name mapping that omits a column and a struct member the imported file holds, plus a column and a member dropped and re-added after a native file was written.
  - Type promotion: `int` to `long`, `float` to `double`, and `decimal(10,2)` to `decimal(20,2)`, with a data file from before the promotion and a later value outside the 32-bit range. No `date` to `timestamp` fixture exists, because Apache Iceberg Java does not implement that promotion.
- Delta fixtures are vendored from delta-kernel-rs and never mutated (`scripts/unity/fixtures/PROVENANCE.md`). `make unity-up` seeds them under `unity.delta_e2e`. A test that needs a controlled Delta layout writes its own table in `e2e_unity_test.rs` under its own Unity Catalog schema. The Unity Catalog registration lists every column a test selects, because an unregistered column is not selectable from Exasol.
- The Glue fixture set writes no data file for a table or partition that the listing skips.
- A forward-proxy test addresses local services by `*-via-proxy` names that only the proxy resolves, so a bypass fails on DNS, and it first asserts that the name does not resolve inside Exasol. Its scripts sit in their own adapter schema, `LHVS_PROXY`, so their `%env` lines reach no other test. Against real cloud hosts, the proof is the `CONNECT` line in the proxy's log.
- The proxy's URL filter (`scripts/proxy/filter`) injects catalog faults by answering a matching request with HTTP 403. A test that needs one table's `loadTable` to fail routes only its own adapter schema through the proxy, instead of corrupting the metadata file, which the REST fixture keeps retrying before it answers.

### Per-run cloud resources

- Azure: the harness creates one blob container per run, `lhrs-e2e-<sanitized-user>-<millis>`, a valid container name whatever `$USER` contains. The service principal creates it before any warehouse, because Lakekeeper creates no filesystem and validates access at warehouse creation.
- Azure: two Lakekeeper warehouses share the container, `<container>-static` with `sas-enabled` false and `<container>-vended` with it true. Each `key-prefix` equals its warehouse name, both use the account key, one helper creates both, and both seed through the shared seed configuration with no per-arm override. Direct-storage Parquet sits under `direct/`. The suite reads `sas-enabled` and `filesystem` back through the management API and does not trust its own request.
- Azure: both credential arms share one fixture and one test function. The vended arm is provisioned and asserted first, so a static-arm regression cannot mask the vended proof.
- Azure: a `Drop` guard deletes the container on a normal return and on panic, also inside an active Tokio runtime, and reports a delete failure during unwinding without a second panic. A name collision at create time fails the run. A container already absent at delete time counts as deleted.
- Glue: each run creates the database `lh_e2e_<run id>` and writes under `s3://<GLUE_FIXTURE_BUCKET>/lh_e2e/<run id>/`, where the run id is the sanitized user name plus the epoch milliseconds. Teardown on return or panic deletes both and names anything it leaks, without panicking. CI and a local run share the harness code.
- Lakekeeper: the harness gets a Keycloak token through the client-credentials grant, bootstraps once, and creates one warehouse per credential mode on SeaweedFS (`s3-compat`, path-style). The static warehouse disables STS and remote signing, and the vended warehouse enables STS. Two Keycloak principals hold opposite table grants in one warehouse.
- Unity Catalog: the CONNECTION supplies the SeaweedFS endpoint and static credentials, because OSS Unity Catalog vends no endpoint. A second virtual schema with `use_vended_credentials` must return the same rows.
- A killed process skips every teardown above. The sweeps in § Ops reclaim what it leaves.

### Credentials in tests

- No credential value appears in test output, an assertion or panic message, or a log line. This covers account keys, client secrets, bearer tokens, storage keys, session tokens, SAS tokens, and external ids. Tests assert on the values they supplied.
- A failure in a credential-bearing helper names the endpoint and the HTTP status. It never prints a response body that can carry a credential, such as a token grant or a `loadTable` answer with vended credentials.
- Cloud credentials reach the Azure and Glue suites as environment variables: locally from the gitignored `test.env`, which their Make targets load, and in CI from repository secrets and variables. The committed `test.env.example` names every variable of those two suites with a placeholder and says which ones reach the CONNECTION under test. `cloud-e2e` reads its variables from the environment only.
- The Azure service principal drives the container lifecycle only. Scans and the seed writer see only the account name and key. The principal holds Storage Blob Data Contributor on a dedicated test-only account and nothing more.
- A test requests a vended payload from the catalog itself only when it asserts on it, because each request puts a live credential in the test process.

### Make targets and CI

- Every E2E Make target depends on `cross-udf-build`, so no suite runs against a stale `.so`. With `REBUILD_SO=0`, a missing or stale `.so` fails the target instead of rebuilding it.
- Each stack in § E2E suites has `<stack>-start`, `<stack>-wait`, `<stack>-up` for both, and `<stack>-down`, which removes its volumes. `scripts/e2e-stack.sh` holds their commands. No test target starts its stack.
- Every E2E CI job needs only `build-so`, so a draft pull request skips it. It sets `REBUILD_SO=0` and runs its suite and stack through the Make targets. It compiles the suite between `<stack>-start` and `<stack>-wait`, with the same cargo flags as the Make test target, so compilation overlaps Exasol's boot.
- The `e2e` job runs the local suite once per supported Exasol major version, each leg selecting its image through `EXASOL_IMAGE`. The leg on the newest version carries the required check name `E2E`, and `release` needs every leg. The catalog suites run on one Exasol version, because catalog integrations do not vary with the engine version.
- `E2E (Azure)` and `E2E (Glue)` are neither required nor in `release`'s needs, so a cloud outage or a rotated key blocks no merge and no release.
- E2E tests run only on x86_64, because `exasol/docker-db` publishes amd64-only images. The `arm64` job runs only `cargo test --workspace`.

## Ops

### Orphaned fixture sweeps

`.github/workflows/azure-orphan-sweep.yml` reclaims the Azure containers, and `.github/workflows/glue-orphan-sweep.yml` the Glue databases and S3 objects, that a killed E2E run leaves behind. In-process teardown is the first-line cleanup. The sweeps are the backstop, because a killed process skips teardown and Azure lifecycle rules never delete containers. Both follow one contract.

- Each workflow runs weekly on a cron and on `workflow_dispatch`.
- A sweep considers only resources the suites name: containers starting with `lhrs-e2e-`, databases starting with `lh_e2e_`, and objects under `lh_e2e/`. It never deletes anything else, so a shared account is safe.
- A resource is stale when the cloud's own timestamp (`last_modified`, `CreateTime`, `LastModified`) is more than 24 hours before the run start. A sweep never judges age by the name's millisecond suffix, so it never deletes a running suite's fixture.
- A scheduled run deletes every stale candidate. A manual run has a `dry_run` input that defaults to `true` and lists without deleting. The guard compares the input with the string `'true'`, because a dispatch boolean arrives as a string and the non-empty string `'false'` is truthy.
- A run with no stale candidate succeeds. An empty candidate list does not fail the step under `set -euo pipefail`.
- A missing variable or a failing cloud CLI call fails the run red, naming the variable and never its value. The workflows send no notification, so a red run is the signal.
- No secret appears in the run log. A secret reaches the CLI only from a masked secret variable, and no step echoes it.
- The Azure sweep authenticates with the service principal and never reads the account key, because it touches only the container control plane. A container already absent at delete time counts as deleted.
- The Glue sweep uses the AWS CLI and prints each deleted name and a final count.
