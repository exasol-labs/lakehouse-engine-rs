# Planning notes: add-export-iceberg-append

## Files checked
- specs/_plans/add-export-iceberg-append/decision-log.md (Interview, kept as is)
- stack top `origin/docs/export-iceberg-lakekeeper-e2e-and-specs`, extracted to scratchpad/stack/:
  - specs/export/export-iceberg-table/spec.md (scenario "An export into an existing table is refused", lines 58-64; Background pre-read bullet line 11)
  - specs/export/export-options/spec.md, export-into-script/spec.md
  - docs/export.md
- specs/_decision/052-add-export-iceberg-rest-write.md (3 ADRs)
- specs/_recorded/013-add-export-iceberg-rest-write/{plan.md headings, tasks.md head}

## Lookups
- `git ls-tree -r -l <stack-top> -- crates/lakehouse-engine/src/export crates/lakehouse-catalog-write specs/export docs/export.md`
- `git diff --stat HEAD <stack-top> -- specs/`
- stack-top specs/architecture.md (grep export lines 43, 46, 62-69, 82-83, 109-111, 125); BASE hash on stack top d66175d2e63a6bd647976470a7a65e31dac52b9f, on this checkout 413cc9afd084083f7e140f2363a9f0fb470f0ed0
- stack-top specs/mission.md (grep export: item 14 line 60, line 71)
- specs/scan-types/type-mapping/spec.md lines 1-27 (read mapping: Int64 -> DECIMAL(20,0))
- `speq feature get scan-types/type-mapping-iceberg-logical`; grep outline of specs/file-planning/iceberg-type-promotion/spec.md
- docker-compose.yml lines 50-80 (spark-iceberg-fixtures job), scripts/spark-fixtures/run_fixtures.sh lines 1-40, scripts/e2e-stack.sh grep lakekeeper/spark
- docker-compose.lakekeeper.yml grep (keycloak realm iceberg, no warehouse bootstrap job)
- stack-top crates/lakehouse-catalog-write/src/iceberg_write.rs lines 225-260 (refuse_existing_table :233 uses session.table_exists; load_table :253), grep retries; retry.rs commit_retries (property else DEFAULT_RETRIES=4)
- stack-top crates/lakehouse-e2e/tests/common/lakekeeper.rs grep pub fns (bootstrap :133, create_warehouse :273, table_metadata :492); e2e_lakekeeper_test.rs lines 50-112 (setup bootstraps warehouses)
- stack-top crates/lakehouse-e2e/tests/e2e_local/export.rs grep fns (helpers table_exists :210, objects_under :226, export via DEFAULT_CATALOG_CONN_NAME with ALLOW_HTTP)

## Explorer results (stack top, file:line)
- engine export: generate_export_sql mod.rs:30, export_hook :40, target.prepare() :50, run_writer :36, write_group :222 (stage_table/open_table :241-245), WriterSpec :73 (deny_unknown_fields), column_names :134
- iceberg_rest.rs: IcebergRestTarget :24, prepare :52-56, iceberg_schema :99
- target.rs: ExportTarget :26 (prepare, open_table, stage_table), ExportSink :50 (schema, write, finish, abort)
- iceberg_sink.rs: IcebergSink :50, schema from table current schema :69/:76, ParquetWriterBuilder :85-97, build(None) :94 (spec id 0), reload before commit :129-138, abort :173-183
- batch.rs: BatchBuilder::bind :52 (by exact name, extra ignored, missing refused), RecordBatch::try_new :106, Values::of :112 (Decimal128, Float64, Boolean, Utf8, Date32, Timestamp us), refusal :127, rescaled :184, arrow_type :243, MAX_TIMESTAMP_PRECISION :230, unwritable :267
- write crate: table_exists via refuse_existing_table :233, ensure_namespace :242, load_table :253, create_table :261, stage_table :297, append_files :339, commit_staged :384, created_by_export :478, existing_table_error :699; fast_append with UuidMatch + RefSnapshotIdMatch only; spec id != 0 fails in iceberg-rust snapshot.rs:156
- E2E lakekeeper export_write.rs: export_refuses_an_existing_table :382; helpers Target::export :113, read_back :136, table_metadata lakekeeper.rs:492, summary_values :211
- docs/export.md stack top: lines 9, 42, 59-61, 83-87, 101, 119, 148 mention new-table-only
- Iceberg spec quotes: fetched via web-fetch (Schema Evolution, Nested Types, Default values, Parquet, Sorting, Row Lineage)
- stack-top crates/lakehouse-engine/src/export/target.rs lines 20-75 (ExportTarget, ExportSink); mod.rs lines 40-60 (export_hook), 222-262 (write_group, arrow_schema before open)
- `git grep 'fn spawn_server|struct RecordingCatalog'` on stack top: crates/lakehouse-catalog/src/exported_test_support_tests.rs
- suite roots: crates/lakehouse-e2e/tests/e2e_local/main.rs:14 (mod export), e2e_lakekeeper_test.rs:13 (mod export_write); e2e_local/export.rs private helpers assert_refused_before_reading, objects_under
- `speq plan validate add-export-iceberg-append`: pass (1 delta spec)

## Revision round 1 (review/round-1.md)
- stack-top specs/architecture.md sections Components 27-49, Data Flow 51-69, Interfaces 71-86, Constraints 88-121 copied into architecture.md (BASE d66175d2e63a6bd647976470a7a65e31dac52b9f via `git show <stack-top>:specs/architecture.md | git hash-object --stdin`)
- stack-top specs/export/export-into-script/spec.md grep "no rows" (Background line 9, scenario lines 29-31)
- stack-top specs/export/export-iceberg-table/spec.md scenario "Two exports that create the same table leave one winner" lines 66-72
- stack-top docs/export.md grep: lines 42, 59, 84, 87, 101, 103, 107-108, 117, 119
- specs/testing.md grep "support module" lines 37-48
- `git ls-tree --name-only <stack-top> crates/lakehouse-e2e/tests/...` (no export support module exists)
- `speq plan validate add-export-iceberg-append`: pass

## Revision round 2 (review/round-2.md)
- `git rev-parse cc728feb` = cc728feb7c24501defa1d839f1a98fbb7bc31081; `git show cc728feb:specs/architecture.md | git hash-object --stdin` = d66175d2 (matches BASE)
- `git rev-parse origin/docs/export-iceberg-lakekeeper-e2e-and-specs` = bc43ebbc
- stack-top docs/security.md lines 129-137 (§ Who can bypass the export script); docs/index.md line 33
- stack-top specs/export/export-iceberg-table/spec.md scenario "A failed SINGLE_COMMIT export leaves no table" lines 148-154
- stack-top scripts/spark-fixtures/run_fixtures.sh lines 44-51 (fixture order, INT96 last)
- `speq plan validate add-export-iceberg-append`: pass
