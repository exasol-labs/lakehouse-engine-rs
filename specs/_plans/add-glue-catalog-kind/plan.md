# Plan: add-glue-catalog-kind

## Summary

Add `CATALOG_KIND = 'GLUE'`, which reads the AWS Glue Data Catalog API and routes each table to the existing Iceberg planner or the existing catalog-declared Parquet reader, so one virtual schema serves a Glue database that mixes Iceberg and Hive Parquet tables (issue #410, work unit 4 of "Support for non-Iceberg tables"). Every rule this needs is one shared implementation: skipped tables reach `ADAPTER_NOTES` for every kind, one partition predicate compares under declared types, one listing takes a file pattern, and every binary column is refused on every source until #351, while a live type matrix, run first on unchanged `main`, proves the read-path type behavior on every Parquet-file source.

## Design

### Context

A Glue estate is largely Hive external Parquet. Glue's Iceberg REST endpoint cannot load those tables, so an `ICEBERG_REST` virtual schema lists them as zero tables with no visible message (#410). The Glue Data Catalog API (`GetTables`, `GetTable`, `GetPartitions`) describes both kinds. The engine already owns every reader a Glue table needs. The guiding rule from the interview: behavior depends on what the source can tell us, not on which catalog it is.

- **Goals**
  - List a Glue database's Iceberg and Hive Parquet tables, and state every skipped table with its reason in `ADAPTER_NOTES`, for every catalog kind.
  - Plan a Glue Iceberg table from `metadata_location` with the one Iceberg planner, and a Glue Parquet table with the one catalog-declared Parquet reader.
  - Take partition values, types, locations, and formats from `GetPartitions`, and prune them under declared types.
  - Refuse every binary column on every source at every depth until #351: Iceberg `binary`, `fixed(L)`, and `uuid`, Delta, Unity, and Glue `binary`, and each direct-storage column that a Parquet file declares binary. A top-level Parquet `ENUM` column is text, so it is read as text.
  - Prove the read-path type behavior against a live Exasol (`CLAUDE.md` § Verification discipline): a type matrix runs on unchanged `main` before any behavior change, and stays in the E2E suite for direct storage, Iceberg REST, and Glue.
  - Gate the kind with a real-AWS E2E job that is not in `release`'s needs.
- **Non-Goals**
  - Predicate pushdown into the `GetPartitions` `Expression` (decision [1]).
  - Every deferred item in § Open Questions.

### Decision

#### Architecture

```
createVirtualSchema                               pushdown
  construct_catalog_client(Glue)                    TableScanResolver::for_request(Glue)
    GlueCatalogSession (lakehouse-catalog)            RequestSession::Glue(GlueCatalogSession)
      aws-sdk-glue, CONNECTION creds only           resolve: load_table_for_planning (GetTable)
      GetTables pages -> route(table)               format_reader(ScanSource::Glue{session, table})
        table_type ICEBERG -> Iceberg                 match table.format
          (columns from metadata.json)                  Iceberg -> IcebergFormatReader
        no table_type + MapredParquet -> Parquet                    { MetadataFile(table) } --+
        else / view / projection -> SkipReason          Parquet -> CatalogParquetFormatReader   |
  shared listing pipeline                                          { GluePartitions } --------+ |
    build_listing_virtual_tables                        Delta   -> refused                   | |
    ADAPTER_NOTES: TABLE_MAP + SKIPPED_TABLES                                                | |
                                                                                             | |
IcebergFormatReader = obtain metadata (RestLoadTable | MetadataFile) -> ONE Iceberg planner <-+ |
                                                                                               |
CatalogParquetFormatReader (was UnityParquetFormatReader)                                     |
  type source: Unity type_json | Glue hive_type -> types::hive_type -> ONE Spark classifier    |
  file source: TableDirectory (Unity: **/*.parquet, key=value values)                         |
             | GluePartitions (GetPartitions values, typed keep, raw-key LIST with `*`) <------+
  keep: PartitionPredicate over declared types (also used by direct storage with utf8)
  listing: parquet_directory list_files(store, prefix, FilePattern)
```

#### Spec compliance

- **Iceberg table spec.** § Optimistic Concurrency: "Once a writer has created an update, it commits by swapping the table’s metadata file pointer from the base version to the new version." § Metastore Tables: "The atomic swap needed to commit new versions of table metadata can be implemented by storing a pointer in a metastore or database that is updated with a check-and-put operation". Glue's `metadata_location` is that pointer. The plan reads it and plans the named `metadata.json` with the one Iceberg planner, so every recorded Iceberg reader rule applies. `metadata.json` is the schema authority. Glue's Hive-string copies of Iceberg columns are ignored.
- **Iceberg `binary`, `fixed(L)`, and `uuid`.** § Primitive Types defines `binary` as "Arbitrary-length byte array", `fixed(L)` as "Fixed-length byte array of length L", and `uuid` as "Universally unique identifiers" that "Should use 16-byte fixed". Refusing all three is a deliberate, scoped exception until #351. The baseline was measured live on current `main` (Docker Exasol 2025.1.16, Iceberg REST fixture, iceberg-rust-written Parquet; Spark-written files unverified):
  - Each type declares `VARCHAR(2000000)`.
  - A `binary` column with only valid UTF-8 values returns text. An empty value and NULL both return NULL.
  - A `binary` value that is not valid UTF-8 fails the whole query (sqlCode 22002, `emit_batch: IPC read: Invalid UTF8 sequence`).
  - Every scan of a `fixed(16)` or `uuid` column fails (sqlCode 22002, `Cannot cast column ... from 'FixedSizeBinary(16)' to 'Utf8'`).
  - A nested `struct<x binary>` member renders as hexadecimal JSON (`{"x":"fffe"}`).
- **Direct-storage binary.** A Parquet file declares no table type, so the refusal reads each footer's annotations. Parquet LogicalTypes § STRING and § ENUM make a `STRING` or `ENUM` `BYTE_ARRAY` a UTF-8 string, and § UUID annotates a 16-byte `FIXED_LEN_BYTE_ARRAY` (quoted in `vs-adapter/binary-column-refusal`).
  - `parquet` 58.3.0, the locked version, folds an unannotated `BYTE_ARRAY` and the `ENUM`, `BSON`, `GEOMETRY`, and `GEOGRAPHY` annotations to Arrow `Binary` (`src/arrow/schema/primitive.rs:280-300`), and a `UUID`-annotated `FIXED_LEN_BYTE_ARRAY(16)` to `FixedSizeBinary(16)` (`:348`). The lines are identical in 58.4.0.
  - An `ENUM` column is therefore read as text by its annotation, not refused by its Arrow type. Every other binary leaf is refused (user-approved, § Impact).
  - A nested `ENUM` member is refused, a scoped exception (#TBD): the JSON renderer reads the member's physical `Binary` type and would render the text as hexadecimal.
  - No live run measured direct-storage binary behavior on `main`. Task 0.3 records it in `notes/type-matrix-baseline.md`.
- **Delta protocol.** Not implicated. A Glue Delta registration is skipped at listing. The Delta reader and `delta_predicate` stay as recorded.
- **Exasol trade-offs.** Nested Hive types surface as JSON `VARCHAR(2000000)`, because Exasol has no nested type.

#### Quick diagnostic (new modules and boundaries)

| Question | Answer |
|----------|--------|
| One-sentence responsibility | `GlueCatalogSession`: Glue metadata as neutral tables and partitions. `CatalogParquetFormatReader`: plan a catalog-declared Parquet table. `FilePattern`: which listed objects are data files. `IcebergMetadataSource`: where an Iceberg table's current metadata comes from. |
| Easier to call than to rebuild | Yes. The session hides pagination, routing, retries, and error codes behind `list_tables` and `partitions`. |
| Internal change leaks outside | No. The SDK types stay inside `glue/`, and the Hive parser stays in `types/`. |
| Doc comment states intent | Each new type states why it exists (per `CLAUDE.md` comment rules, 1-2 lines). |
| One owner per decision | Routing: `glue/routing.rs`. Type mapping: the Spark classifier. Pruning: `partition_predicate`. File eligibility: `FilePattern`. Binary cause text: `delta_schema::binary_cause`. A Parquet file's binary leaves: `parquet_directory`. |
| Boundary visible without reading internals | Yes: the catalog crate returns neutral types, and the engine chooses readers by format tag. |
| Tactical shortcut with follow-up | Same-bucket partitions only (#TBD), no `FILE_PATTERN` property (#TBD), and a refused nested `ENUM` member (#TBD). |
| Business logic depends inward | Yes. The reader depends on `CatalogPartition`, never on `aws-sdk-glue`. |

### Consequences

Decisions [1] to [9] in `decision-log.md` hold the full alternatives. This table keeps only the choices without an entry of their own, plus the one an interview follow-up settled.

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Typed local pruning | Glue `Expression` | A second translator's errors return wrong rows under full pushdown delegation. |
| Glue lists `*` per partition | Recursive `**/*.parquet` | Trino writes extensionless files. Direct children avoid reading a nested partition location twice. |
| Zero-length objects are never data files, under every pattern ([7]) | Apply the rule only under `*` | Hadoop `<dir>_$folder$` sibling markers and empty files hold no rows. The listing already drops a marker at the listed location. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| glue-catalog-client | NEW | `vs-adapter/glue-catalog-client/spec.md` |
| glue-hive-type-mapping | NEW | `vs-adapter/glue-hive-type-mapping/spec.md` |
| glue-table-planning | NEW | `vs-adapter/glue-table-planning/spec.md` |
| partition-predicate-declared-types | NEW | `vs-adapter/partition-predicate-declared-types/spec.md` |
| binary-column-refusal | NEW | `vs-adapter/binary-column-refusal/spec.md` |
| catalog-crate-public-surface-extensions-glue | NEW | `vs-adapter/catalog-crate-public-surface-extensions-glue/spec.md` |
| glue-e2e-harness | NEW | `glue-e2e/glue-e2e-harness/spec.md` |
| glue-orphan-sweep | NEW | `glue-e2e/glue-orphan-sweep/spec.md` |
| catalog-kind-selection | CHANGED | `vs-adapter/catalog-kind-selection/spec.md` |
| parquet-directory-seam | CHANGED | `vs-adapter/parquet-directory-seam/spec.md` |
| unity-parquet-table-planning | CHANGED | `vs-adapter/unity-parquet-table-planning/spec.md` |
| create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| delta-type-mapping | CHANGED | `vs-adapter/delta-type-mapping/spec.md` |
| nested-json-rendering | CHANGED | `datafusion-scan/nested-json-rendering/spec.md` |
| type-mapping | CHANGED | `datafusion-scan/type-mapping/spec.md` |
| type-mapping-live-matrix | NEW | `datafusion-scan/type-mapping-live-matrix/spec.md` |

## Impact

- New `CATALOG_KIND = 'GLUE'`. `ICEBERG_REST` against Glue keeps its behavior.
- **Breaking, Iceberg (user-approved):** a query that reads or emits a `binary`, `fixed(L)`, or `uuid` column, or a column containing one, fails at plan time naming the declared type and #351. Two cases worked before and now break: a `binary` column holding only valid UTF-8, and a nested `binary` member, which rendered as hexadecimal JSON. A `fixed(L)` or `uuid` column, and a `binary` value that is not valid UTF-8, already failed at scan time, so the change replaces that scan error with a plan-time message (§ Spec compliance). The listing still declares each column.
- **Breaking, direct storage (user-approved):** a query that reads or emits a column that a Parquet file declares binary, at any depth, fails at plan time naming the file's type (`binary`, `fixed(L)`, `uuid`, `bson`, `geometry`, or `geography`) and #351. This covers an unannotated `BYTE_ARRAY`, the form in which Impala and Hive write legacy strings, and every `UUID` column. A top-level `ENUM` column reads as text. A nested `ENUM` member is refused (#TBD). `datafusion-scan/type-mapping` records a `CAST(col AS VARCHAR)` path for these columns, and no live run measured it. Task 0.3 records the `main` behavior before the change. The listing still declares each column.
- Unity Parquet: a predicate on a non-string partition column (`INT`, `DATE`, and others) now prunes files. Results are unchanged.
- Every kind: after `CREATE` or `REFRESH`, `ADAPTER_NOTES` holds `SKIPPED_TABLES`. Warning texts are unchanged.
- **Behavior change, direct storage and Unity Parquet (user-approved):** a zero-length object is no longer read. Before, a zero-length `*.parquet` object failed the footer read.
- `reqwest` moves to rustls with the system CA store, so the `.so` links no OpenSSL (checked in task 1.4). The `.so` grows by the Glue SDK (measured in task 8.2).
- New CI job `e2e-glue` and weekly workflow `glue-orphan-sweep`. A fork PR fails `e2e-glue` for lack of secrets, as it fails `e2e-azure`.

## Dependencies

- `aws-sdk-glue 1.170` with default features off and `behavior-version-latest`, `default-https-client`, `rt-tokio`. The SDK features `rustls` and `legacy-https-client` stay off, because they pull hyper 0.14, rustls 0.21, and http 0.2. The AWS crates declare MSRV 1.94.1.
- `glob 0.3`, already in the tree through `datafusion-datasource`.
- An IAM user, a fixture bucket, and four repository settings for the E2E gate (open question 6).

## Migration

| Current | New |
|---------|-----|
| An Iceberg `binary` column with valid UTF-8 values returns text, and a nested `binary` member returns hexadecimal JSON | The query fails at plan time citing #351. Select other columns, or wait for #351. |
| A direct-storage column that a Parquet file declares binary (unannotated `BYTE_ARRAY`, `UUID`) is scanned | The query fails at plan time citing #351. Select other columns. For a legacy string column, rewrite the files with the `STRING` annotation. |
| Skip reasons visible only through `SCRIPT_OUTPUT_ADDRESS` | Run `ALTER VIRTUAL SCHEMA … REFRESH` and read `SKIPPED_TABLES` from `EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` |
| `ICEBERG_REST` CONNECTION to `https://glue.<region>.amazonaws.com/iceberg` | Optional: a `GLUE` CONNECTION to `https://glue.<region>.amazonaws.com` lists the Hive tables too |

## Implementation Tasks

The implementing commit carries `Closes #410`.

### 0. Run-first type baseline on unchanged `main`

Group 0 runs before any production change. Its run checks every type-related delta against observed behavior.

- [ ] 0.1 Add the data table and fixtures of `datafusion-scan/type-mapping-live-matrix`, with no production change:
  - `crates/lakehouse-engine/tests/common/type_matrix.rs` (cfg `exasol-e2e`, and `glue-e2e` from task 7.1), declared in `tests/common/mod.rs`. One `const` table holds a row per case: the column name (`c_<case>`, for example `c_binary`, `c_enum`, `c_uuid`), the type each source writes or declares or the reason it omits the row, the expected Exasol declaration, and the expected outcome (`Values`, `Refused(reason)`, or `Fails(error)`). Each expectation states the behavior after this plan: the type-mapping rules, the refusal reasons of `vs-adapter/binary-column-refusal`, and `ENUM` as text.
  - Cases: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Boolean, Utf8, LargeUtf8, Decimal128(10,2), Decimal128(38,10), Decimal256(50,2), Date32, Timestamp(µs), `INT96`, Time32(ms), Time64(µs), Duration(µs), Interval(DayTime), Binary, LargeBinary, FixedSizeBinary(16), `ENUM`, `UUID` FLBA(16), an unannotated `BYTE_ARRAY` with valid UTF-8, List<Int32>, Struct<a: Int32, b: Utf8>, Map<Utf8, Int32>, Struct<x: Binary>, and Struct<k: `ENUM`>. The `binary_values` table holds an unannotated `BYTE_ARRAY` that is not valid UTF-8. Glue adds a `string` column over an unannotated `BYTE_ARRAY`.
  - Direct storage: write `all_types` through `raw_parquet.rs`, and `annotated` and `binary_values` through `parquet::file::writer::SerializedFileWriter`, under `s3://warehouse/type_matrix/`.
  - Iceberg: write `all_types` and `binary_values` through `seed.rs` `create_and_append` in the namespace `e2e_type_matrix`, so no other suite enumerates them.
- [ ] 0.2 Add `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` (`#![cfg(feature = "exasol-e2e")]`). It creates the virtual schemas `TYPE_MATRIX_DS` and `TYPE_MATRIX_ICEBERG` and holds `direct_storage_type_matrix_matches_every_row` and `iceberg_type_matrix_matches_every_row`. Each test collects every mismatch and fails once, listing all of them. Add `--test e2e_type_matrix_test` to the `test-e2e` recipe (`Makefile:66`).
- [ ] 0.3 On unchanged `main`, run `docker compose up -d --wait`, `make cross-udf-build`, and `LH_EXASOL_CPUSET=0-1 cargo test --features exasol-e2e --test e2e_type_matrix_test -- --test-threads=1`. Record each row's observed declaration, values, and error text in `specs/_plans/add-glue-catalog-kind/notes/type-matrix-baseline.md`. The binary rows fail on `main`, because they expect the refusal this plan adds.
- [ ] 0.4 Check the note against `datafusion-scan/type-mapping`, `datafusion-scan/nested-json-rendering`, `vs-adapter/delta-type-mapping`, and `vs-adapter/binary-column-refusal`. If one of these holds, stop the implementation and return the plan for review with the note:
  - A declaration or a non-binary value contradicts `datafusion-scan/type-mapping` or `datafusion-scan/nested-json-rendering` as recorded. A scan error on a type of the `CAST(col AS VARCHAR)` set counts.
  - The top-level `ENUM` column does not return its UTF-8 text.
  - The direct-storage `binary_values` column returns its bytes faithfully (for example, as hexadecimal), so refusing an unannotated `BYTE_ARRAY` would remove a working rendering.
  - The Iceberg `binary_values` column, a `utf8`-tagged column over bytes that are not valid UTF-8, returns an altered value instead of failing the query (`vs-adapter/binary-column-refusal`, scenario 3).

  Otherwise:
  - Copy each observed display text into the rows whose value the recorded spec leaves unspecified: Decimal128(38,10), Decimal256, Time32, Time64, Duration, and Interval.
  - Do not fix another bug the run finds. List it in § Open Questions with `(#TBD)`, state it as a scoped exception in the Background of the owning spec delta, and set its row to the observed outcome with that reference.
  - Add the direct-storage `main` baseline to § Spec compliance.

### A. Dependencies and TLS stack

- [ ] 1.1 Root `Cargo.toml` `[workspace.dependencies]`: add `aws-sdk-glue = { version = "1.170", default-features = false, features = ["behavior-version-latest", "default-https-client", "rt-tokio"] }` and `glob = "0.3"`. Set `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls-native-roots", "http2"] }`. Bump the pins to the lock: `aws-sigv4 = "1.6"`, `aws-credential-types = "1.3"`, `aws-smithy-runtime-api = { version = "1.18", features = ["client"] }`. Never add `aws-config`.
- [ ] 1.2 `crates/lakehouse-catalog/Cargo.toml`: add `aws-sdk-glue = { workspace = true }`. Replace the stale `# MSRV 1.91.1: ...` comment with `# MSRV 1.94.1 (aws-sdk-glue, aws-smithy-runtime-api): must stay buildable by the rust:1.94-trixie UDF builder.` In `crates/lakehouse-engine/Cargo.toml`, add `glob = { workspace = true }`, the dev-dependency `aws-sdk-glue = { workspace = true }`, and the feature `glue-e2e = []` with the comment `# Requires real AWS Glue + S3; same FAIL contract as exasol-e2e.`
- [ ] 1.3 Confirm every compiler is at least 1.94.1. `rust:1.94-trixie` reports 1.94.1 locally. The CI jobs pin `dtolnay/rust-toolchain@8fae6aad…` ("toolchain 1.94.0 baked into action.yml") beside `rust-toolchain.toml` `channel = "1.94"`. If a CI job fails with "requires rustc 1.94.1", set `rust-toolchain.toml` to `1.94.1` and give each dtolnay step `toolchain: 1.94.1`.
- [ ] 1.4 Run `cargo check --workspace --all-targets`, once per E2E feature. Run `cargo tree -p lakehouse-engine -e normal -i native-tls` and `-i openssl-sys`: each MUST report no package in the normal graph (the dev-only `tungstenite` path is allowed). Run `cargo tree --workspace -i aws-config`: it MUST match no package. Run `cargo tree -p lakehouse-engine -e normal -d` and confirm one version each of `rustls`, `hyper`, and `aws-lc-rs`. Run `cargo deny check licenses advisories`.

### B. Glue metadata model: catalog client, neutral types, Hive types

- [ ] 2.1 In `crates/lakehouse-catalog/src/client.rs`, add `CatalogTable.metadata_location: Option<String>`, `ColumnSourceType::Glue { hive_type: String }`, `SkipReason::NotPlannableGlueTable { detail: String }`, and `pub struct CatalogPartition { values: BTreeMap<String, Option<String>>, location: String, format: Option<TableFormat>, input_format: String }`. Census:
  - `metadata_location: None` at every `CatalogTable` literal: `lakehouse-catalog/src/client.rs:186`, `lakehouse-catalog/src/unity/client.rs:235`, `lakehouse-engine/src/adapter/direct_storage.rs:101`, `lakehouse-catalog/src/client_tests.rs:10,28`, `lakehouse-catalog/tests/catalog_public_surface.rs:133,205`, `lakehouse-engine/src/adapter/adapter_tests.rs:1410`, `lakehouse-engine/src/adapter/catalog_client_tests.rs:31,104`, `format/delta_format_reader_tests.rs:42`, `format/format_tests.rs:35`, `format/unity_parquet_format_reader_tests.rs:56`. The `..id_table()` sites at `unity_parquet_format_reader_tests.rs:337,341` need no edit.
  - Glue arm at every exhaustive `ColumnSourceType` match: `lakehouse-engine/src/types/mapping.rs:391` (task 2.7) and `lakehouse-catalog/src/client_tests.rs:205`. The `let … else` at `unity_parquet_format_reader.rs:166` and the wildcard at `catalog_public_surface.rs:195` need no edit.
  - Glue arm at the one exhaustive `SkipReason` match, `lakehouse-engine/src/adapter/mod.rs:247` (`skip_warning`): `createVirtualSchema: skipping Glue table '{id}' ({detail})`.
- [ ] 2.2 Add `crates/lakehouse-catalog/src/glue/` (`mod.rs`, `client.rs`, `routing.rs`, `partitions.rs`). `GlueCatalogSession::new(address, storage, creds) -> Result<Self, UdfError>` builds `aws_sdk_glue::Config` with `Credentials::new(access_key, secret_key, session_token, None, "exasol-connection")`, `Region::new(creds.sigv4_signing_region(address))`, `endpoint_url(address)`, `RetryConfig::standard().with_max_attempts(5)`, `TimeoutConfig::builder().operation_timeout(30 s)`, and `BehaviorVersion::latest()`. It reads no environment, profile, or IMDS source. Map errors by `ProvideErrorMetadata::code()`: name the operation, code, and message, and state "does not exist" for `EntityNotFoundException`. Map a signing-time rejection that survives the SDK's skew correction to a clock-skew message. Redact every text with `redact_secret_values`. [expert]
- [ ] 2.3 `glue/routing.rs` (pure): route `(TableType, Parameters, StorageDescriptor.InputFormat)` to Iceberg (with `metadata_location`), Parquet, or a skip detail, per `vs-adapter/glue-catalog-client`. A view and a present non-`ICEBERG` `table_type` never reach the storage-descriptor check. Parquet requires `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`. `projection.enabled=true` skips.
- [ ] 2.4 `list_tables`: page `GetTables` until the token is absent or empty or the page is empty. Require a one-segment NAMESPACE. Send `CatalogId` only for a non-empty `warehouse`. Parquet tables: storage-descriptor columns then `PartitionKeys`, each `ColumnSourceType::Glue`, with partition columns and the location without its trailing `/`. `load_table_for_planning(ident)` runs `GetTable` and the routing, reads no metadata file, and fails naming a missing or no-longer-plannable table.
- [ ] 2.5 Glue Iceberg tables at listing: read `metadata.json` with `iceberg::spec::TableMetadata::read_from(&storage.file_io(), location)`, at most 16 at a time. Take the columns through a function extracted from `IcebergRestCatalogClient::load_on_session` (`client.rs:159`), so REST and Glue share it. The REST listing output MUST stay byte-identical (`client_tests.rs`, `catalog_public_surface.rs`). `CatalogClient::load_table` performs the listing load for one table. [expert]
- [ ] 2.6 `partitions(ident, partition_columns)`: page `GetPartitions` with `ExcludeColumnSchema` under the same stop rule. Zip `Values` with the key names, and fail on a count mismatch naming the location. Map `__HIVE_DEFAULT_PARTITION__` to `None`. Take the format from the partition's `InputFormat`, falling back to the table's. Remove a trailing `/` from the location.
- [ ] 2.7 Add `crates/lakehouse-engine/src/types/hive_type.rs`: `parse_hive_type(&str) -> Result<delta_kernel::schema::DataType, String>`, a recursive-descent parser per `vs-adapter/glue-hive-type-mapping` (case-insensitive, whitespace-tolerant, members nullable, `decimal` defaults to `(10,0)`). In `types/mapping.rs` `column_source_type_to_exasol`, add the Glue arm: parse, map a primitive to the Unity `type_name` that `unity_type_name_to_exasol` matches (`BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `STRING`, `DATE`, `TIMESTAMP_NTZ`, `DECIMAL` with its precision and scale), and call it. A nested or unparseable type declares `VARCHAR(2000000)`.
- [ ] 2.8 In `lakehouse-catalog/src/lib.rs`, export `GlueCatalogSession` and `CatalogPartition`. In `tests/catalog_public_surface.rs`, construct and observe every addition.
- [ ] 2.9 Add `glue/mock_glue_tests.rs`, a loopback JSON 1.1 responder keyed on `X-Amz-Target` that records request headers and bodies. Add `glue/client_tests.rs`, `glue/routing_tests.rs`, `types/hive_type_tests.rs`, and the `types/mapping_tests.rs` addition, named in Scenario Coverage. Add `crates/lakehouse-catalog/tests/glue_ambient_credentials.rs`, an integration binary with the one test `requests_are_signed_with_the_connection_key_not_the_environment`:
  - Before it starts a runtime, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_CONFIG_FILE`, and `AWS_SHARED_CREDENTIALS_FILE` to another identity. The `unsafe` `set_var` is sound because the binary holds one test.
  - Answer one `GetTables` call on a loopback listener in the same file.
  - Assert `Credential=<CONNECTION access_key>/<date>/<CONNECTION region>/glue/aws4_request` in the recorded `Authorization` header. Task 1.4's `aws-config` check covers instance metadata.

### C. File pattern, raw-key locations, and the typed partition predicate

- [ ] 3.1 In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, add `FilePattern` (`Copy`, a `&'static str` glob, constants for `**/*.parquet` and `*`, `*.parquet` accepted), matched with `glob::Pattern` under `require_literal_separator`. A pattern without `**` lists through `list_with_delimiter`. Split `list_data_files` into a plain listing step (pattern, fixed `_`/`.` segment rule, zero-length exclusion, sorted by path) and the `key=value` parsing on top. Remove the hard-coded `.parquet` check from `data_file_segments` (`:224`). `resolve_parquet_directory` and `list_parquet_files` keep their signatures and pass `**/*.parquet`. Internal call sites: `:78`, `:143`, `:196`.
- [ ] 3.2 Add `raw_location_prefix(location) -> Result<(String, StorePath), UdfError>`: store root `<scheme>://<bucket>`, with `s3a` normalized to `s3`, and the key through `StorePath::parse` without percent-decoding. Add `list_location_files(store, prefix, FilePattern) -> Result<Vec<ListedFile>, UdfError>`. Keep `store_prefix` for URL-shaped URIs.
- [ ] 3.3 In `parquet_directory_tests.rs`, add `file_pattern_selects_listing_depth_and_file_name_rule`, `a_raw_key_location_is_listed_without_percent_decoding`, and `a_raw_key_file_path_resolves_back_to_its_object_key`. The last one runs `file_entry` → `reconstruct_abs_uri` → `ListingTableUrl::parse(..).prefix()` and compares against `StorePath::parse(key)` for `p_str=a b%2Fc/f1`, inside and outside the table root. If it fails, fix `encode_file_path` for that input and keep the test. `listing_is_recursive_filtered_and_deterministic` MUST pass without edits.
- [ ] 3.4 In `format/partition_predicate.rs`, make `PartitionPredicate::from_filter(filter_json, declared: &[(String, DataType)])` bind each partition column's declared Arrow type, with `utf8` for an undeclared column. Convert value and literal with `ScalarValue::try_from_string` and compare with `partial_cmp`. Accept only these pairs: string/non-empty `literal_string`, integer/integral `literal_exactnumeric` in range, decimal/`literal_exactnumeric` with scale ≤ the column's, date/`literal_date`, timestamp/`literal_timestamp`, boolean/`literal_bool`. Every other pair, a failed conversion, or `partial_cmp == None` reaches every truth value. Census: `format/parquet_format_reader.rs:47` (pass `&[]`), `format/unity_parquet_format_reader.rs:209` (replace `string_partition_keep` by a keep over every partition column typed from the logical schema, and drop `CatalogSchema.string_partition_columns`), `format/partition_predicate_tests.rs:15,269` (pass `&[]`). [expert]
- [ ] 3.5 Add `partition_values_compare_under_their_declared_type` and `an_undecidable_comparison_keeps_the_file` to `partition_predicate_tests.rs`. Add `a_numeric_literal_prunes_no_direct_storage_file` to `parquet_format_reader_tests.rs`. Rename `only_string_partition_columns_prune_files` in `unity_parquet_format_reader_tests.rs` to `a_partition_predicate_prunes_under_the_declared_type`, and assert that `year = 2024` prunes.

### D1. Binary refusal on Iceberg and direct storage

- [ ] 4.1 In `format/delta_schema.rs`, make the one cause owner `pub(super) fn binary_cause(declared: &str) -> String`, with the text `has type '<declared>', which this engine refuses: rendering binary data is tracked as issue #351`. Its one caller (`:334`) passes `"binary"`, so every Delta, Unity, and Glue text reads `has type 'binary'`. In `format/iceberg.rs`, after `build_logical_schema`, refuse each top-level field whose Iceberg type is `binary`, `fixed(L)`, or `uuid`, or contains one at any depth:
  - Name the column, and a nested member by its path, with `binary_cause("binary")`, `binary_cause("fixed(<L>)")`, or `binary_cause("uuid")`.
  - Drop the field from the logical schema, and call `ensure_table_has_a_mappable_column(.., "Iceberg")`.
  - Fill the `refused_columns: Vec::new()` literal of the Iceberg `ResolvedScan` (`iceberg.rs:129`).
- [ ] 4.2 Add `iceberg_binary_fixed_and_uuid_columns_are_refused` (`iceberg_tests.rs`), which asserts that the `f` cause names `fixed(16)` and the `u` cause names `uuid`. Add `a_binary_column_refuses_only_the_requests_that_read_it_on_iceberg` (`pushdown_tests.rs`, modeled on `a_refused_delta_column_refuses_only_the_requests_that_reference_it` at `:1994` and `assert_refuses_binary_col` at `:2051`). Add `binary_cause_names_the_declared_type_and_issue_351` (`delta_schema_tests.rs`). These tests MUST pass without edits:
  - `mapping_tests.rs`: `iceberg_types_map_to_exasol_type`, `incompatible_types_map_to_varchar_json`, `iceberg_primitive_mappings_are_exhaustive_so_a_new_variant_breaks_the_build`
  - `parquet_directory_tests.rs`: `nested_and_unrepresentable_types_fold_to_the_string_declaration`
  - `raw_scan_tests.rs`: `build_scan_sql_keeps_a_non_nested_incompatible_column_cast_unchanged`
  - `field_id_projection_tests.rs`: `each_physical_type_is_admitted_or_refused_under_its_declared_type`
- [ ] 4.3 In `adapter/parquet_directory.rs`, classify every leaf of each read footer's Parquet schema (`metadata().file_metadata().schema_descr()`), through list, map, and struct levels, per `vs-adapter/binary-column-refusal`: [expert]
  - A `STRING`, `JSON`, or `ENUM` `BYTE_ARRAY` leaf is text, except that an `ENUM` leaf below the top level declares `enum`.
  - An unannotated or unknown-annotated `BYTE_ARRAY` declares `binary`. `BSON`, `GEOMETRY`, and `GEOGRAPHY` declare `bson`, `geometry`, and `geography`.
  - A `UUID` `FIXED_LEN_BYTE_ARRAY` declares `uuid`, and an unannotated one of length L declares `fixed(L)`. A `DECIMAL`, `FLOAT16`, or `INTERVAL` one is not binary.
  - Add `ParquetDirectory.binary_columns`: per column, the first binary leaf any read footer declares, with its member path and declared type. Census: the literal at `parquet_directory.rs:128` and the destructure at `format/parquet_format_reader.rs:48`. The folded schema and the listing's declarations do not change.
- [ ] 4.4 In `format/parquet_format_reader.rs`, turn each `binary_columns` entry into a `RefusedColumn` whose reason names the column, the member path, and `binary_cause(declared)`. An `enum` entry's reason states that a Parquet `ENUM` is read as text only at the top level. Drop each refused column from the logical schema, fill `refused_columns` (`:72`), and call `ensure_table_has_a_mappable_column(.., "Direct storage")`.
- [ ] 4.5 In `parquet_format_reader_tests.rs`, add `unannotated_bson_and_geospatial_byte_arrays_are_refused_naming_their_type`, `an_enum_column_reads_as_text_and_a_nested_enum_member_is_refused`, and `uuid_and_unannotated_fixed_len_columns_are_refused_naming_their_type`. Each writes its files with `SerializedFileWriter`. In `parquet_directory_tests.rs`, add `an_embedded_arrow_schema_binary_leaf_declares_binary` for `LargeBinary` and `BinaryView`. Then run task 0.3's command: every direct-storage and Iceberg row MUST pass.

### D2. Glue table planning

- [ ] 5.1 Rename `format/unity_parquet_format_reader.rs` and its `_tests.rs` to `catalog_parquet_format_reader*.rs`. Update the `#[path]` at `:27` and the `mod`/`use` at `format/mod.rs:29,37`. `CatalogParquetFormatReader { table, files: ParquetFileSource }`, where `ParquetFileSource` is `TableDirectory(UnityTableStorage)` or `GluePartitions { session, storage }`. `spark_field` parses `ColumnSourceType::Unity { type_json }` as today and `ColumnSourceType::Glue { hive_type }` through `types::hive_type::parse_hive_type`. Add `classify_spark_schema(schema, mode, partition_columns, label)` to `delta_schema.rs`. `build_delta_table_schema` delegates to it with `"Delta"`, so its 29 call sites and texts need no edit. The catalog reader passes `"Unity Parquet"` or `"Glue"`. [expert]
- [ ] 5.2 Glue file source: require a non-empty storage location, with the Unity empty-location text naming Glue. Use the static `connection.storage`. When the table has partition columns, fetch `session.partitions`. Otherwise use one pseudo-partition at the table location with no values. Split the rest into `plan_glue_partitions(store, table_root, partitions, keep)`:
  - Apply the typed keep to each partition's values.
  - Fail a kept partition that is not Parquet, or whose bucket differs from the table's (`s3`/`s3a` equal), naming its values, its location, and the cause.
  - List each kept location through `raw_location_prefix` and `list_location_files(.., "*")`, concurrently on the store from `build_table_root_store(.., DEFAULT_S3_MAX_CONNECTIONS, ..)`.
  - Give each file its partition's values, map it through `file_entry`, sort by path, and redact every error. [expert]
- [ ] 5.3 In `format/iceberg.rs`, split `IcebergFormatReader` into `{ metadata: IcebergMetadataSource, connection }`:
  - `RestLoadTable { session, catalog_props }` keeps today's `load_table_any_auth` and the vending decision.
  - `MetadataFile { table }` reads `TableMetadata::read_from(&connection.storage.file_io(), metadata_location)`, uses static storage, and builds the `TableIdent` from the neutral ident.
  - Everything after obtaining the metadata becomes one shared function: date-promotion refusal, location check, table build, name mapping, delete-mechanism check, file planning, and task 4.1's refusal.
  - Literal census: `format/mod.rs:136`, `iceberg_tests.rs:79,388,805,905`. REST output MUST stay byte-identical. [expert]
- [ ] 5.4 In `format/mod.rs`, add `ScanSource::Glue { session: &GlueCatalogSession, table: &CatalogTable }`. Its `format_reader` arm matches `table.format` exhaustively: Iceberg selects the metadata-file Iceberg reader, Parquet selects the catalog reader with `GluePartitions`, and Delta returns a `UdfError` naming the table and format. The Unity arm builds the catalog reader with `TableDirectory`. The `ScanSource` constructions in tests and E2E files stay valid, because no existing variant changes.
- [ ] 5.5 Add to `format/test_support_tests.rs` a delimiter listing on the S3 loopback. Add the Glue tests to `catalog_parquet_format_reader_tests.rs`. Add `a_glue_iceberg_table_is_planned_from_its_metadata_file` (`iceberg_tests.rs`). Add `format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog` and `format_reader_refuses_a_delta_table_under_the_glue_source` (`format_tests.rs`).

### E. Adapter: kind, CONNECTION, listing, pushdown session, skipped tables

- [ ] 6.1 In `adapter/catalog_kind.rs`, add `CatalogKind::Glue`, `CATALOG_KIND_GLUE = "GLUE"`, the resolver arm, and an error text that lists every accepted choice. Add `glue_catalog_kind_resolves_case_insensitively` to `catalog_kind_tests.rs`, and make `unrecognized_catalog_kind_is_rejected` (`:50`) assert `GLUE`.
- [ ] 6.2 In `adapter/connection.rs` `read_connection`, reject a JSON `use_sigv4: false` under Glue before parsing, and set `creds.use_sigv4 = true` after parsing. Add a Glue arm to `validate_kind_preconditions` (`:93`) that rejects `use_vended_credentials` and lists every supplied token/OAuth2 field in one error. `validate_sigv4_creds` then applies without edits. The comparison at `:43` stays. None of the 65 `read_connection` calls in `connection_tests.rs` changes. Add the four Glue tests named in Scenario Coverage.
- [ ] 6.3 In `adapter/mod.rs`, add the `construct_catalog_client` Glue arm (`:452`), calling `GlueCatalogSession::new(catalog_uri, storage, creds)`. Make the NAMESPACE match at `:174` exhaustive. In `catalog_client_tests.rs`, rename `construction_site_is_exhaustive_and_fallible_for_three_kinds` to `construction_site_is_exhaustive_and_fallible_for_every_kind` and add the Glue arm.
- [ ] 6.4 In `adapter/pushdown/scan_resolution.rs`, add `RequestSession::Glue(Box<GlueCatalogSession>)`. The `for_request` Glue arm (`:55`) validates every identifier with `glue_table_ident`, which splits at the first `.` and requires both parts non-empty, and then builds the session. The `resolve` arm (`:111`) calls `load_table_for_planning` and then `format_reader(ScanSource::Glue { .. })`. Add `glue_table_identity_round_trips_through_the_recorded_identifier`, and add Glue to `request_session_has_one_variant_per_kind` (`:431-449`).
- [ ] 6.5 Split `skip_warning` into `skip_reason(entry: &lakehouse_catalog::SkippedTable)` and the warning line built from it. The Iceberg and Unity warning texts MUST stay byte-identical (`adapter_tests.rs:1480,1489`). Give `build_adapter_notes` the parameter `skipped: &[lakehouse_catalog::SkippedTable]`, the type `adapter/mod.rs:35` already imports. Inside it, build each `{"table","reason"}` object from `catalog_identifier_string(&entry.ident)` and `skip_reason(entry)`, and always write `NOTE_SKIPPED_TABLES = "SKIPPED_TABLES"` as their array. Declare no second `SkippedTable`. Census (pass `&[]`): `adapter/mod.rs:224`, and `adapter_tests.rs:329,359,399,447,487,610,653,710,867,896,1025,1113,1150,1185,1230,1363`. Add the three tests named in Scenario Coverage.
- [ ] 6.6 After task 6.5 and `make cross-udf-build`, measure the adapterNotes size limit on the Docker Exasol stack, per `CLAUDE.md` § Verification discipline:
  - Read the declared type of `ADAPTER_NOTES` from `SYS.EXA_SYS_COLUMNS` for `EXA_ALL_VIRTUAL_SCHEMAS`.
  - Create a `DIRECT_STORAGE` virtual schema over a MinIO prefix holding enough directories, each with only a `_SUCCESS` object, that `SKIPPED_TABLES` exceeds that size.
  - Record whether CREATE fails, whether the stored value truncates, and the measured limit in the verification report.
  - If a limit exists, cap `SKIPPED_TABLES` at the largest entry count that keeps adapterNotes within the measured limit. Add a scenario for the cap to `vs-adapter/create-virtual-schema-adapter-notes` in the same change.
- [ ] 6.7 Run `cargo test` and `cargo clippy --all-targets`, once without features and once per E2E feature (`exasol-e2e`, `unity-e2e`, `azure-e2e`, `lakekeeper-e2e`, `cloud-e2e`, `glue-e2e`), so every gated test crate compiles.

### F. Glue E2E gate

- [ ] 7.1 Add `crates/lakehouse-engine/tests/common/glue.rs`, and add `glue-e2e` to the cfg lists in `tests/common/mod.rs` (`:3-13`, `e2e_harness`, `raw_parquet`, `seed`, `type_matrix`). Give it fail-loud readers of the four variables, modeled on `common/azure.rs` `require_var`. The run id is `<sanitized user>_<epoch millis>`. `GlueRun` creates `lh_e2e_<run id>` through `aws-sdk-glue` with explicit credentials, and an `object_store` S3 store for `lh_e2e/<run id>/`. Its `Drop` deletes the tables, the database, and the prefix on its own thread and runtime, and prints `LEAKED …` on failure, like `AzureContainer`. Add the units `run_id_is_a_legal_glue_database_name` and `missing_glue_variable_fails_loud`.
- [ ] 7.2 Fixtures in `common/glue.rs`, per `glue-e2e/glue-e2e-harness`:
  - Table names: `iceberg_orders`, `all_types`, `binary_values`, `partitioned`, `projected`, `a_view`, `orc_table`, `delta_table`.
  - `iceberg_orders` is written through `iceberg`'s memory catalog over S3 `FileIO` and registered with `CreateTable`.
  - The Hive Parquet files are written through `raw_parquet.rs` with extensionless names. `all_types` and `binary_values` are the Glue tables of the task 0.1 type matrix. `all_types` holds a column per matrix row with a Hive type string, plus the Hive types of `vs-adapter/glue-hive-type-mapping`. `binary_values` declares `payload string` over bytes that are not valid UTF-8.
  - `partitioned` partitions go through `BatchCreatePartition`: `p_str` `__HIVE_DEFAULT_PARTITION__`, the key `p_str=a b%2Fc/`, an out-of-root location, `s3a://`, and an ORC input format at `p_int=9` with no file. Every other partition has `p_int < 9`, so a query carrying `P_INT < 9` prunes the ORC partition.
  - The projection table, the view, the ORC table, and the Delta table are metadata only.
  - The timestamp column is INT64 microseconds. INT96 stays covered by `e2e_int96_timestamp_test.rs`.
- [ ] 7.3 Add `crates/lakehouse-engine/tests/e2e_glue_test.rs` (`#![cfg(feature = "glue-e2e")]`). Run the shared setup (`wait_for_exasol`, `install_slc`, `upload_so`, `create_schema_and_scripts`), create a CONNECTION and a virtual schema with `CATALOG_KIND='GLUE'`, and add the tests named in Scenario Coverage. `glue_type_matrix_matches_every_row` reads the Glue rows of `common/type_matrix.rs` and edits none of them.
- [ ] 7.4 In the `Makefile`, add `test-e2e-glue: cross-udf-build` with one recipe line: `if [ -f ./test.env ]; then set -a; . ./test.env; set +a; fi; cargo test --features glue-e2e --test e2e_glue_test -- --test-threads=1`. Add it to `.PHONY` (`:218`). In `test.env.example`, add a `# --- Glue E2E ---` section with four `<NAME>=placeholder` lines, and reword the header so it names both suites. Above the four lines, add a comment naming the IAM policy the key needs: scoped to the Glue databases `lh_e2e_*` and the prefix `lh_e2e/`, granting `glue:CreateDatabase`, `glue:DeleteDatabase`, `glue:GetDatabase`, `glue:GetDatabases`, `glue:CreateTable`, `glue:DeleteTable`, `glue:GetTable`, `glue:GetTables`, `glue:BatchCreatePartition`, `glue:GetPartitions`, `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject`.
- [ ] 7.5 In `.github/workflows/ci.yml`, add a job `e2e-glue` after `e2e-azure`:
  - `needs: [build-so]`, `timeout-minutes: 45`, `LH_EXASOL_CPUSET: "0-1"`.
  - Steps: checkout, `./.github/actions/e2e-setup`, `docker compose pull --quiet exasol`, and `docker compose up -d --wait exasol`.
  - Run `make test-e2e-glue` with the two secrets and the two `vars`, dump the Exasol logs on failure, and stop the stack always.
  - Keep it out of `release.needs`, and extend the comment at `:639` to name `e2e-glue`. Add `e2e-glue` to the description of `.github/actions/e2e-setup/action.yml`.

### F2. Glue orphan sweep

- [ ] 7.6 Add `.github/workflows/glue-orphan-sweep.yml`, modeled on `azure-orphan-sweep.yml`:
  - Triggers: cron `0 3 * * 1`, and `workflow_dispatch` with `dry_run` defaulting to true. Set `permissions: {}`.
  - Guard the four variables. Delete databases matching `lh_e2e_*` whose `CreateTime` is older than 24 h (`aws glue get-databases` / `delete-database`).
  - Delete objects under `lh_e2e/` whose `LastModified` is older than 24 h (`aws s3api list-objects-v2` / `delete-objects`).
  - Capture each CLI output in a variable so `set -e` trips, and never enable `set -x`.

### G. Documentation, binary size, final verification

- [ ] 8.1 Update `docs/catalogs.md`:
  - The supported-kinds table and the Connection fields table.
  - A new section "AWS Glue Data Catalog (`CATALOG_KIND = 'GLUE'`)": CONNECTION example, routing table, Hive types, `GetPartitions` values, the `*` file rule, typed pruning, ORC and foreign-bucket failures, and scoped exceptions.
  - A "required IAM actions" list for the Glue kind: `glue:GetTables`, `glue:GetTable`, `glue:GetPartitions`, `s3:ListBucket`, and `s3:GetObject`.
  - The Unity section: typed pruning replaces "only string partition columns prune", and the `.parquet` rule stays.
  - The direct-storage section: the `**/*.parquet` rule and the zero-length rule.
  - The type notes: every binary type is refused on every source until #351. Name Iceberg `binary`, `fixed(L)`, and `uuid`, Delta, Unity, and Glue `binary`, and the direct-storage Parquet forms (an unannotated `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`, `UUID`, `BSON`, `GEOMETRY`, and `GEOGRAPHY`). A top-level Parquet `ENUM` column reads as text, and a nested `ENUM` member is refused. State the breaking cases: an Iceberg `binary` column that holds UTF-8 text, and a direct-storage legacy string column, which the user can rewrite with the `STRING` annotation.
  - How to read `SKIPPED_TABLES`: `SELECT ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '…'`.
  - In `docs/security.md`, state that the Glue kind reads with the CONNECTION's static IAM credentials, evaluates no Lake Formation grant, and uses no Lake Formation credential vending. A cross-account `CatalogId` and assume-role credentials are untested (#TBD).
- [ ] 8.2 Measure the `.so`: run `make cross-udf-build` on the merge base (in a worktree) and on the branch. Record both `strip --strip-unneeded` sizes in the verification report.
- [ ] 8.3 Run `cargo fmt --check`, task 6.7's clippy and test matrix, `docker compose up -d` with `LH_EXASOL_CPUSET=0-1 make test-e2e`, `make unity-up && make test-e2e-unity`, and `make test-e2e-glue` with real `GLUE_*` values in `./test.env`. The `e2e-glue` job confirms that the SLC CA bundle serves the SDK's TLS client, because the adapter calls Glue over HTTPS from inside the UDF. If it fails with a certificate error, stop and report the SLC CA bundle as a blocker.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| 0: Run-first type baseline | 0.1-0.4 | None | spec delta `datafusion-scan/type-mapping-live-matrix`; `crates/lakehouse-engine/tests/{e2e_type_matrix_test.rs,common/type_matrix.rs,common/raw_parquet.rs,common/seed.rs,common/mod.rs}`, the `Makefile` `test-e2e` recipe, `notes/type-matrix-baseline.md` |
| A: Dependencies | 1.1-1.4 | 0 | root `Cargo.toml`, `crates/lakehouse-catalog/Cargo.toml`, `crates/lakehouse-engine/Cargo.toml`, `rust-toolchain.toml`, `.github/workflows/ci.yml` toolchain steps |
| B: Glue metadata model | 2.1-2.9 | A | spec deltas `vs-adapter/glue-catalog-client`, `vs-adapter/catalog-crate-public-surface-extensions-glue`, `vs-adapter/glue-hive-type-mapping`; `crates/lakehouse-catalog/src/{client.rs,lib.rs,glue/}`, `crates/lakehouse-catalog/tests/{catalog_public_surface.rs,glue_ambient_credentials.rs}`, `crates/lakehouse-engine/src/types/{mod.rs,hive_type.rs,mapping.rs}` and their `_tests.rs` |
| D1: Binary refusal | 4.1-4.5 | A | spec deltas `vs-adapter/binary-column-refusal`, `vs-adapter/delta-type-mapping`, `datafusion-scan/nested-json-rendering`, `datafusion-scan/type-mapping`; `format/{delta_schema.rs,iceberg.rs,parquet_format_reader.rs}`, `adapter/parquet_directory.rs`, `format/{delta_schema_tests.rs,iceberg_tests.rs,parquet_format_reader_tests.rs}`, `adapter/parquet_directory_tests.rs`, `pushdown_tests.rs` |
| C: Listing pattern and typed predicate | 3.1-3.5 | B, D1 | spec deltas `vs-adapter/parquet-directory-seam`, `vs-adapter/partition-predicate-declared-types`, `vs-adapter/unity-parquet-table-planning`; `adapter/parquet_directory.rs`, `format/{partition_predicate.rs,parquet_format_reader.rs,unity_parquet_format_reader.rs}`, and their `_tests.rs` |
| D2: Glue table planning | 5.1-5.5 | C, D1 | spec delta `vs-adapter/glue-table-planning`; `format/{mod.rs,delta_schema.rs,iceberg.rs,catalog_parquet_format_reader.rs,test_support_tests.rs}` and their `_tests.rs` |
| E: Adapter kind, CONNECTION, skipped tables | 6.1-6.7 | D2 | spec deltas `vs-adapter/catalog-kind-selection`, `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema`; `adapter/{catalog_kind.rs,connection.rs,mod.rs}`, `adapter/pushdown/scan_resolution.rs`, and their `_tests.rs` |
| F: Glue E2E gate | 7.1-7.5 | E | spec delta `glue-e2e/glue-e2e-harness`; `crates/lakehouse-engine/tests/{e2e_glue_test.rs,common/}`, `Makefile`, `test.env.example`, `.github/workflows/ci.yml`, `.github/actions/e2e-setup/action.yml` |
| F2: Glue orphan sweep | 7.6 | None | spec delta `glue-e2e/glue-orphan-sweep`; `.github/workflows/glue-orphan-sweep.yml` |
| G: Docs and verification | 8.1-8.3 | F, F2 | `docs/catalogs.md`, `docs/security.md`, every spec delta above |

Group 0 runs first on unchanged `main` and gates every group except F2, which touches only its own workflow file. If task 0.4 stops the run, no other group starts. D1 and F2 run in parallel with B. The other groups run in sequence, for these shared files:

- Task 2.1's census adds one-line literals to files that C, D2, and E own, so B precedes them.
- D1 and C both edit `adapter/parquet_directory.rs`, `format/parquet_format_reader.rs`, and its `_tests.rs`, so C follows D1.
- D1 (task 4.5) and F (task 7.3) run the matrix that group 0 writes, and neither edits a row of it.
- D2 follows C and D1: it renames the reader C edits, and it reshapes the `iceberg.rs` and `delta_schema.rs` code D1 edits.
- E follows D2, because the `CatalogKind::Glue` arm in `scan_resolution.rs` needs `ScanSource::Glue`. Task 6.5 stays in E, because it shares `adapter/mod.rs` with task 6.3.
- Tasks 7.4 and 7.5 stay in F: they share the harness spec delta with tasks 7.1-7.3, and the CI job runs the suite task 7.3 writes.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `string_partition_keep` in `format/unity_parquet_format_reader.rs` | Replaced by the typed keep (task 3.4) |
| Field | `CatalogSchema.string_partition_columns` | Only `string_partition_keep` read it |
| File | `format/unity_parquet_format_reader.rs`, `_tests.rs` | Renamed to `catalog_parquet_format_reader*.rs` (task 5.1) |
| Check | the `.parquet` suffix test in `data_file_segments` | Moved into `FilePattern` (task 3.1) |

## Open Questions

1. User-facing `FILE_PATTERN` virtual-schema property (#TBD).
2. `binary` rendering (#351).
3. A nested Parquet `ENUM` member on direct storage is refused, because the JSON renderer reads the member's physical `Binary` type (#TBD). Reading it as text needs a decode hint for nested leaves.
4. `ICEBERG_REST` classifies a non-loadable table by HTTP status alone (`is_not_loadable_iceberg_table`), not by the service's error type (#TBD).
5. Lake Formation-governed tables, cross-account `CatalogId`, assume-role and STS credentials (#TBD).
6. Partitions stored in another bucket than the table (#TBD). This plan fails the query naming them.
7. E2E ownership, for a human to decide: who provisions the IAM user, the fixture bucket, and the four repository settings, and who owns the AWS bill and any cross-region egress. The harness spec lets a fork PR fail `e2e-glue` loudly, like `e2e-azure`. A human confirms that behavior. The Glue type matrix adds two tables of three rows and about 60 small queries per run.
8. `CLAUDE.md` § Data types lists Binary, LargeBinary, and FixedSizeBinary among the JSON `VARCHAR(2000000)` types. This plan refuses every binary type on every source and reads a top-level Parquet `ENUM` as text. A human decides whether to update `CLAUDE.md`.

## Verification

### Scenario Coverage

`CT` = `crates/lakehouse-catalog/src/glue/`, `FMT` = `crates/lakehouse-engine/src/adapter/pushdown/format/`, `E2E` = `crates/lakehouse-engine/tests/e2e_glue_test.rs`.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The client routes a table by its declared table type before its storage descriptor | Integration + Unit | `CT/client_tests.rs`, `CT/routing_tests.rs` | `list_tables_routes_each_registration_to_its_reader_or_a_skip`, `route_*` units |
| An Iceberg table takes its columns from its current metadata file | Integration | `CT/client_tests.rs` | `iceberg_columns_come_from_the_metadata_file_and_the_planning_load_reads_none`, `an_iceberg_table_without_metadata_location_is_skipped` |
| A Parquet table declares its Glue columns and partition keys | Integration | `CT/client_tests.rs` | `parquet_table_declares_storage_columns_then_partition_keys`, `a_projection_enabled_table_is_skipped` |
| Partitions carry their Glue values, location, and format | Integration | `CT/client_tests.rs` | `partitions_carry_glue_values_the_default_partition_as_null_and_their_own_format` |
| Every listing follows its continuation tokens and stops on an empty page | Integration | `CT/client_tests.rs` | `listings_follow_tokens_and_stop_on_an_empty_page` |
| The CatalogId is sent only when the CONNECTION names one, and the NAMESPACE names one database | Integration | `CT/client_tests.rs` | `catalog_id_is_sent_only_for_a_non_empty_warehouse`, `a_multi_segment_namespace_is_refused` |
| Service errors are classified by their error code, not their HTTP status | Integration | `CT/client_tests.rs` | `errors_are_classified_by_code_and_name_the_operation` |
| Throttling and server errors are retried within a bounded time | Integration | `CT/client_tests.rs` | `throttling_and_503_are_retried_within_the_deadline`, `persistent_throttling_fails_naming_the_operation` |
| A clock-skew signing failure names the clock | Integration | `CT/client_tests.rs` | `a_persistent_signing_time_rejection_names_the_clock` |
| The client signs only with the CONNECTION's credentials | Integration | `crates/lakehouse-catalog/tests/glue_ambient_credentials.rs` | `requests_are_signed_with_the_connection_key_not_the_environment` |
| Every Hive primitive type maps to its Spark type | Unit | `crates/lakehouse-engine/src/types/hive_type_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs` | `every_hive_primitive_parses_to_its_spark_type`, `glue_columns_are_classified_by_the_spark_type_classifier` |
| Nested Hive types parse recursively and render as JSON text | Unit | same | `nested_hive_types_parse_recursively_with_nullable_members`, `glue_nested_columns_carry_the_string_tag_and_a_nested_descriptor` |
| A binary, unrecognized, or malformed Hive type refuses only its column | Integration | `FMT/catalog_parquet_format_reader_tests.rs` | `binary_unrecognized_and_malformed_glue_types_refuse_only_their_column` |
| The listing declares each Glue column through the Spark listing mapping | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `glue_columns_declare_the_spark_listing_type` |
| A Glue Iceberg table is planned from its metadata location by the one Iceberg planner | Integration | `FMT/iceberg_tests.rs` | `a_glue_iceberg_table_is_planned_from_its_metadata_file`, `iceberg_reader_owns_resolution_and_keeps_its_encoding` (existing) |
| A Glue Parquet table is planned by the shared catalog-declared Parquet reader | Integration | `FMT/catalog_parquet_format_reader_tests.rs`, `FMT/format_tests.rs` | `glue_parquet_table_is_planned_by_the_shared_reader_with_nullable_catalog_columns`, `format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog`, `format_reader_refuses_a_delta_table_under_the_glue_source` |
| Each kept partition's location is listed and its files carry the partition's Glue values | Integration | `FMT/catalog_parquet_format_reader_tests.rs`, E2E | `each_kept_partition_is_listed_and_carries_its_glue_values`, `glue_partition_cases_return_their_glue_values` |
| A kept partition the reader cannot read faithfully fails the query naming it | Integration | `FMT/catalog_parquet_format_reader_tests.rs` | `a_kept_orc_or_foreign_bucket_partition_fails_naming_it_and_a_pruned_one_does_not` |
| A Glue location's data files are its direct children of any name | Integration | `FMT/catalog_parquet_format_reader_tests.rs` | `a_glue_location_reads_direct_children_of_any_name` |
| A partition predicate prunes partitions before their locations are listed | Integration | `FMT/catalog_parquet_format_reader_tests.rs`, E2E | `glue_partitions_are_pruned_on_glue_values_before_listing`, `glue_partition_predicate_reduces_the_scan_file_list` |
| A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable | Integration | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs`, `CT/client_tests.rs` | `glue_table_identity_round_trips_through_the_recorded_identifier`, `the_planning_load_fails_for_a_dropped_or_no_longer_plannable_table` |
| A partition value compares under its column's declared type | Unit | `FMT/partition_predicate_tests.rs` | `partition_values_compare_under_their_declared_type` |
| A comparison the declared type cannot decide exactly keeps the file | Unit | `FMT/partition_predicate_tests.rs` | `an_undecidable_comparison_keeps_the_file` |
| Direct storage passes every partition column to the one predicate as a string | Integration | `FMT/parquet_format_reader_tests.rs` | `a_numeric_literal_prunes_no_direct_storage_file` |
| A predicate on a partition column prunes files under the column's declared type | Integration | `FMT/catalog_parquet_format_reader_tests.rs` | `a_partition_predicate_prunes_under_the_declared_type` |
| Every skipped table is recorded with its reason under every catalog kind | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, E2E | `skipped_tables_are_recorded_with_reasons_for_every_kind`, `skip_warning_renders_the_legacy_iceberg_line_and_the_unity_detail_line` (existing), `glue_listing_includes_routed_tables_and_records_every_skip` |
| A listing with no skip records an empty list that replaces the previous one | Integration | `adapter_tests.rs` | `a_listing_without_skips_records_an_empty_list_replacing_the_previous` |
| A namespace whose every table is skipped still creates an empty virtual schema | Integration | `adapter_tests.rs` | `an_all_skipped_namespace_creates_an_empty_schema_recording_every_skip` |
| A skipped-table list too long for adapterNotes is capped to the longest prefix that fits | Unit + measured live | `adapter_tests.rs`, `notes/adapter-notes-limit.md` | `an_oversized_skipped_list_is_capped_to_the_longest_prefix_that_fits_the_exasol_limit`, `a_skipped_list_that_fits_keeps_every_entry_and_drops_a_stale_omitted_count` |
| Create virtual schema enumerates every table in the configured namespace | Integration | `adapter_tests.rs` | `skipped_tables_are_recorded_with_reasons_for_every_kind`, `refresh_rebuilds_table_map_preserves_notes` (existing) |
| A binary column is refused on every catalog-declared format at every depth | Integration + E2E | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `FMT/iceberg_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `a_binary_column_refuses_only_the_requests_that_read_it_on_iceberg`, `iceberg_binary_fixed_and_uuid_columns_are_refused`, `a_refused_delta_column_refuses_only_the_requests_that_reference_it` (existing), `logical_schema_is_the_catalog_column_list_and_undescribed_columns_are_refused` (existing, Unity), `binary_unrecognized_and_malformed_glue_types_refuse_only_their_column`, `iceberg_type_matrix_matches_every_row` |
| The listing declares a binary column | Unit + E2E | `mapping_tests.rs`, `parquet_directory_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `iceberg_types_map_to_exasol_type`, `incompatible_types_map_to_varchar_json`, `nested_and_unrepresentable_types_fold_to_the_string_declaration` (existing), `glue_columns_declare_the_spark_listing_type`, `direct_storage_type_matrix_matches_every_row`, `iceberg_type_matrix_matches_every_row` |
| A catalog string column over binary file data reads as text | Integration + E2E | `FMT/catalog_parquet_format_reader_tests.rs`, E2E | `a_catalog_string_column_over_unannotated_byte_array_reads_as_text`, `glue_type_matrix_matches_every_row` |
| A direct-storage unannotated BYTE_ARRAY column is refused | Integration + E2E | `FMT/parquet_format_reader_tests.rs`, `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `unannotated_bson_and_geospatial_byte_arrays_are_refused_naming_their_type`, `an_embedded_arrow_schema_binary_leaf_declares_binary`, `direct_storage_type_matrix_matches_every_row` |
| A direct-storage ENUM column reads as text | Integration + E2E | `FMT/parquet_format_reader_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `an_enum_column_reads_as_text_and_a_nested_enum_member_is_refused`, `direct_storage_type_matrix_matches_every_row` |
| A direct-storage UUID or unannotated fixed-length column is refused | Integration + E2E | `FMT/parquet_format_reader_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `uuid_and_unannotated_fixed_len_columns_are_refused_naming_their_type`, `direct_storage_type_matrix_matches_every_row` |
| Every mapped type declares and returns as its matrix row states on each Parquet-file source | E2E | `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs`, E2E | `direct_storage_type_matrix_matches_every_row`, `iceberg_type_matrix_matches_every_row`, `glue_type_matrix_matches_every_row` |
| Incompatible Arrow types are serialized to JSON VARCHAR | Unit + E2E | `mapping_tests.rs`, `crates/lakehouse-engine/tests/e2e_type_matrix_test.rs` | `incompatible_types_map_to_varchar_json` (existing), `direct_storage_type_matrix_matches_every_row`, `iceberg_type_matrix_matches_every_row` |
| A Delta type whose Arrow form cannot be rendered faithfully is refused by name | Unit | `FMT/delta_schema_tests.rs` | `refused_set_is_binary_variant_and_containers_of_them` (existing), `binary_cause_names_the_declared_type_and_issue_351` |
| The Glue client and its neutral additions extend the crate's public surface through an explicit reviewed edit | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `glue_additions_are_reachable_from_outside_the_crate` |
| No AWS SDK type crosses the crate boundary | Integration | same | `glue_session_is_reachable_through_neutral_types_only` |
| Absent CATALOG_KIND resolves the Iceberg REST catalog kind | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs`, `crates/lakehouse-catalog/src/client_tests.rs` | `absent_catalog_kind_resolves_iceberg_rest`, `empty_namespace_builds_no_session_and_no_grant`, `enumeration_builds_exactly_one_session` (existing) |
| The catalog kind is matched at one construction site and nowhere else | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs`, `scan_resolution_tests.rs` | `construction_site_is_exhaustive_and_fallible_for_every_kind`, `request_session_has_one_variant_per_kind` |
| An unrecognized CATALOG_KIND value is rejected with a clear error | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `unrecognized_catalog_kind_is_rejected` |
| CATALOG_KIND naming Glue resolves the native Glue kind | Unit | `catalog_kind_tests.rs` | `glue_catalog_kind_resolves_case_insensitively` |
| Glue validation implies SigV4 and rejects catalog-auth and vending fields | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `glue_connection_implies_sigv4_and_makes_warehouse_optional`, `glue_connection_rejects_explicit_sigv4_false`, `glue_connection_rejects_vending_and_catalog_auth_fields`, `glue_connection_requires_the_sigv4_fields` |
| Data files are listed recursively in a deterministic order | Integration | `parquet_directory_tests.rs` | `listing_is_recursive_filtered_and_deterministic` (existing) |
| The file pattern selects the listing depth and the file-name rule | Integration | `parquet_directory_tests.rs` | `file_pattern_selects_listing_depth_and_file_name_rule` |
| A catalog-registered location is listed by its raw object key | Integration | `parquet_directory_tests.rs` | `a_raw_key_location_is_listed_without_percent_decoding`, `a_raw_key_file_path_resolves_back_to_its_object_key` |
| Each run provisions its own Glue database and S3 prefix and removes both, including on panic | E2E | E2E, `crates/lakehouse-engine/tests/common/glue.rs` | `glue_run_resources_are_removed_when_the_scope_ends_including_on_panic`, `run_id_is_a_legal_glue_database_name` |
| The fixture set covers every routing, partition, and type case | E2E | E2E | `glue_fixture_set_registers_every_case` |
| The listing includes the routed tables and records every skip | E2E | E2E | `glue_listing_includes_routed_tables_and_records_every_skip` |
| Queries through pushdown return the expected rows | E2E | E2E | `glue_queries_return_expected_rows_through_pushdown`, `glue_partition_cases_return_their_glue_values`, `glue_type_matrix_matches_every_row`, `glue_orc_partition_fails_loud`, `glue_partition_predicate_reduces_the_scan_file_list` |
| The suite fails, never skips, when a variable or the stack is missing | E2E + Unit | E2E, `common/glue.rs` | `glue_suite_fails_when_stack_unavailable`, `missing_glue_variable_fails_loud` |
| The Make target and the CI job run the suite as a non-release gate | CI run | `Makefile`, `.github/workflows/ci.yml`, `test.env.example` | the `e2e-glue` job of the plan's PR run; `make -n test-e2e-glue` (Manual Testing) |
| No credential value appears in output | E2E | E2E | `glue_credentials_never_appear_in_output` |
| A scheduled run deletes stale orphaned fixtures | Workflow run | `.github/workflows/glue-orphan-sweep.yml` | `gh workflow run glue-orphan-sweep.yml -f dry_run=false` against a planted stale database |
| A fixture younger than 24 hours is never swept | Workflow run | same | the same run keeps a fresh `lh_e2e_*` database |
| A manual dispatch previews by default | Workflow run | same | `gh workflow run glue-orphan-sweep.yml` (default `dry_run`) |
| The sweep fails loudly when a variable is absent or an AWS call fails | Workflow run | same | a dispatch with `GLUE_REGION` unset in a fork |
| No credential value appears in the run log | Workflow run | same | inspect the log of each run above |

No test reads the `Makefile` or `test.env.example` as source text. The CI run is the evidence for those two files. The `nested-json-rendering` delta changes only its Background, and `delta-type-mapping` changes its Background and one scenario. Their other copied scenarios are recorded and carry no marker.

### Manual Testing

Run these commands after `make cross-udf-build` and `docker compose up -d --wait exasol`. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`. `MY_GLUE` is a `GLUE` virtual schema, created with the DDL of `docs/catalogs.md`, over a Glue database that holds the task 7.2 fixture set.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| catalog-kind-selection | `exapump sql "CREATE VIRTUAL SCHEMA BAD USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION='GLUE_CATALOG_CREDS' CATALOG_KIND='GLUX' NAMESPACE='x'" -d "$DSN"` | An error listing absent, `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE` |
| glue-catalog-client | `exapump sql "SELECT TABLE_NAME FROM EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA='MY_GLUE' ORDER BY 1" -d "$DSN"` | The Iceberg and Parquet fixture tables, no view, ORC, Delta, or projection table |
| create-virtual-schema-adapter-notes | `exapump sql "SELECT ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME='MY_GLUE'" -d "$DSN"` | JSON whose `SKIPPED_TABLES` lists the view, ORC, Delta, and projection tables with reasons |
| create-virtual-schema | `exapump sql "ALTER VIRTUAL SCHEMA MY_GLUE REFRESH" -d "$DSN"`, then the `ADAPTER_NOTES` query above | The notes hold `TABLE_MAP`, `SKIPPED_TABLES`, and the budget entries, and no other catalog metadata |
| glue-hive-type-mapping | `exapump sql "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='MY_GLUE' AND COLUMN_TABLE='ALL_TYPES' ORDER BY COLUMN_ORDINAL_POSITION" -d "$DSN"` | `DECIMAL(3,0)`, `DECIMAL(5,0)`, `DECIMAL(10,0)`, `DECIMAL(20,0)`, `DOUBLE` for `float` and `double`, `BOOLEAN`, `DECIMAL(10,2)`, `VARCHAR(2000000)` for string, binary, and nested columns, `DATE`, `TIMESTAMP(6)` |
| glue-table-planning | `exapump sql "SELECT P_STR, COUNT(*) FROM MY_GLUE.PARTITIONED WHERE P_INT < 9 GROUP BY P_STR ORDER BY 1" -d "$DSN"` | One row per `P_STR` value, including `a b/c` and NULL |
| glue-table-planning | `exapump sql "SELECT COUNT(*) FROM MY_GLUE.PARTITIONED" -d "$DSN"` | An error naming the `p_int=9` ORC partition, its location, and its input format |
| partition-predicate-declared-types | `exapump sql "EXPLAIN VIRTUAL SELECT * FROM MY_GLUE.PARTITIONED WHERE P_INT = 1" -d "$DSN"` | A pushdown SQL whose scan spec lists only the `p_int=1` partition's files |
| parquet-directory-seam | `cargo test -p lakehouse-engine --lib parquet_directory` | 0 failures |
| unity-parquet-table-planning | `make unity-up && make test-e2e-unity` | 0 failures |
| binary-column-refusal | `exapump sql "SELECT C_BINARY FROM MY_GLUE.ALL_TYPES" -d "$DSN"`, then `SELECT COUNT(*) FROM MY_GLUE.ALL_TYPES` | The first fails naming column `c_binary`, type `binary`, and issue #351. The second returns the fixture's row count |
| binary-column-refusal | After the type-matrix run, `exapump sql "SELECT ID, C_ENUM FROM TYPE_MATRIX_DS.ANNOTATED ORDER BY ID" -d "$DSN"`, then `SELECT C_UUID FROM TYPE_MATRIX_DS.ANNOTATED` | The first returns the enum values as text. The second fails naming column `c_uuid`, type `uuid`, and issue #351 |
| type-mapping, type-mapping-live-matrix | `docker compose up -d --wait && make cross-udf-build && LH_EXASOL_CPUSET=0-1 cargo test --features exasol-e2e --test e2e_type_matrix_test -- --test-threads=1` | 0 failures |
| delta-type-mapping, nested-json-rendering | `cargo test -p lakehouse-engine --lib binary` | 0 failures, including `iceberg_binary_fixed_and_uuid_columns_are_refused` and `binary_cause_names_the_declared_type_and_issue_351` |
| catalog-crate-public-surface-extensions-glue | `cargo test -p lakehouse-catalog --test catalog_public_surface` | 0 failures |
| glue-e2e-harness | `make -n test-e2e-glue` then `make test-e2e-glue` | The dry run shows `cross-udf-build` and one recipe line with `--features glue-e2e`. The real run reports 0 failures, and afterwards `aws glue get-databases` lists no `lh_e2e_<run id>` database |
| glue-orphan-sweep | `gh workflow run glue-orphan-sweep.yml` | The run log prints "would delete:" lines only, and deletes nothing |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `docker compose up -d && LH_EXASOL_CPUSET=0-1 make test-e2e` | 0 failures |
| E2E (Unity) | `make unity-up && make test-e2e-unity` | 0 failures |
| E2E (Glue) | `make test-e2e-glue` with real `GLUE_*` values | 0 failures |
| Lint | `cargo clippy --all-targets`, then once per E2E feature (`--features exasol-e2e`, `unity-e2e`, `azure-e2e`, `lakekeeper-e2e`, `cloud-e2e`, `glue-e2e`) | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Dependencies | `cargo tree -p lakehouse-engine -e normal -i openssl-sys` | No package in the normal graph |
