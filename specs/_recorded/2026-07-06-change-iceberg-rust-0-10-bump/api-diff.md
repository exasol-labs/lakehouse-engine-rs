# API diff: iceberg-rust 0.9.1 → 0.10.0-rc.2

Task 2.1 output. Every signature below was verified against the **tag source** of
`apache/iceberg-rust` at `v0.10.0-rc.2` (commit `be6cc96eaeb1cac4574cabb11ea6e1e92e0aad45`),
using each crate's checked-in `public-api.txt` (rustdoc public-API dump) plus the raw `src/`,
and diffed against the 0.9.1 crates.io source vendored in
`~/.cargo/registry/src/.../iceberg{,-catalog-rest,-storage-opendal}-0.9.1`.

## TL;DR — the only breaking API change

Across **every** call site this repo uses, exactly **one** iceberg-rust API changed:
`OpenDalStorageFactory::S3` dropped its `configured_scheme: String` field. Three construction
sites must drop that one field. **Everything else is byte-for-byte signature-identical** between
0.9.1 and 0.10.0-rc.2 — the whole writer stack in `seed.rs` included. The real work of this bump
is the arrow 57→58 unification (test fixtures) and the dependency pin, **not** iceberg API churn.

---

## Confirmed facts (plan Task 1 a/b/c + arrow)

- **(a) `iceberg-storage-opendal` S3 factory API CHANGED.** In 0.9.1
  `OpenDalStorageFactory::S3 { configured_scheme: String, customized_credential_load: Option<..> }`.
  In 0.10.0-rc.2 the variant is `S3 { customized_credential_load: Option<CustomAwsCredentialLoader> }`
  — the `configured_scheme` field is **removed**. Additionally every variant is now `#[cfg(feature = ...)]`
  gated (`opendal-memory`/`opendal-fs`/`opendal-s3`/`opendal-gcs`/`opendal-oss`/`opendal-azdls`/`opendal-hf`),
  with `default = ["opendal-memory","opendal-fs","opendal-s3"]`. The repo already pins
  `default-features = false, features = ["opendal-s3"]`; `opendal-s3` still exists → **the feature spec
  needs no change**. **StorageFactory registration is STILL mandatory for S3**: iceberg core ships only
  `LocalFsStorageFactory` and `MemoryStorageFactory` in `iceberg::io`; there is no built-in S3 storage,
  so `RestCatalogBuilder::with_storage_factory(...)` / `FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3{..}))`
  remain required. Keep (and re-verify) the "StorageFactory is mandatory" comment.

- **(b) Crate names / module split UNCHANGED.** Package names are still `iceberg`,
  `iceberg-catalog-rest`, `iceberg-storage-opendal`; Rust import paths are still `iceberg::…`,
  `iceberg_catalog_rest::…`, `iceberg_storage_opendal::…`. (The opendal crate moved directory inside
  the iceberg-rust repo from `crates/catalog/opendal` → `crates/storage/opendal`, but that is internal
  to the upstream repo and invisible to consumers.) All three are workspace members under the single
  `git`+`tag` triple.

- **(c) `RestCatalog::load_table` STILL returns only `Table` — UNCHANGED.** Signature at the tag:
  `load_table(&self, &TableIdent) -> Result<iceberg::table::Table>`. It does **not** return
  `config` / `storage_credentials`. The cloud-credentials path is therefore safe. (Note: production
  does not actually call `catalog.load_table`; it self-issues the `loadTable` GET and deserializes
  `iceberg_catalog_rest::LoadTableResult` directly — whose fields `metadata`, `metadata_location`,
  `config`, `storage_credentials` are all **unchanged**, see LoadTableResult section. This is the
  fact the self-issued GET relies on and it holds.)

- **arrow/parquet = 58 CONFIRMED.** The tag workspace `Cargo.toml` sets `arrow = "58"` (+ all
  `arrow-*` subcrates) and `parquet = "58"`; the `iceberg` crate consumes them via `workspace = true`.
  (Incidental: it also carries `serde_arrow 0.14` with the `arrow-58` feature.) So 0.10.0-rc.2 links
  arrow/parquet 58, matching datafusion 54 + `exasol-udf-sdk` 0.20.2 → single arrow-58 tree is
  achievable.

- **Out of scope (issue #11):** `ArrowReaderBuilder` was deliberately NOT researched. Production
  never calls it (only `plan_files()`/`FileScanTask` path+size metadata crosses into DataFusion).

---

## Production call sites

### 1. `OpenDalStorageFactory::S3` construction — **BREAKING (drop one field)**
Sites: `adapter/pushdown.rs::build_rest_catalog` (~line 73) and
`adapter/pushdown.rs::build_s3_file_io` (~line 173). (Third site in `seed.rs`, see below.)
- **Old (0.9.1):**
  ```rust
  OpenDalStorageFactory::S3 {
      configured_scheme: "s3".to_string(),
      customized_credential_load: None,
  }
  ```
- **New (0.10.0-rc.2):**
  ```rust
  OpenDalStorageFactory::S3 {
      customized_credential_load: None,
  }
  ```
- **Edit needed:** Delete the `configured_scheme: "s3".to_string(),` line at both sites. Nothing else.

### 2. `RestCatalogBuilder` construction — UNCHANGED
- **Old = New:** `RestCatalogBuilder::default()` → `.with_storage_factory(Arc<dyn StorageFactory>)`
  → `.load(impl Into<String>, HashMap<String,String>) -> impl Future<Output = Result<RestCatalog>>`.
  `REST_CATALOG_PROP_URI` / `REST_CATALOG_PROP_WAREHOUSE` still exported as `&str` consts.
- **Edit needed:** None (the only change here is the nested `S3 {}` literal above).

### 3. `iceberg::io::{FileIOBuilder, S3_* consts}` — UNCHANGED
- **Old = New:** `FileIOBuilder::new(Arc<dyn StorageFactory>)`, `.with_prop(impl ToString, impl ToString)`,
  `.build() -> FileIO`. Consts `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`,
  `S3_SESSION_TOKEN`, `S3_PATH_STYLE_ACCESS` all present in `iceberg::io`.
- **Edit needed:** None.

### 4. `catalog.list_tables` / `list_namespaces` — UNCHANGED
- **Old = New:** `RestCatalog::list_tables(&self, &NamespaceIdent) -> Result<Vec<TableIdent>>`;
  `list_namespaces(&self, Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>>`.
- **Edit needed:** None.

### 5. `iceberg_catalog_rest::LoadTableResult` / `TableMetadata` — UNCHANGED
- **Old = New:** `LoadTableResult` fields: `metadata: TableMetadata`,
  `metadata_location: Option<String>`, `config: HashMap<String,String>`,
  `storage_credentials: Option<Vec<StorageCredential>>`. `StorageCredential { prefix, config }`
  still present (used by `extract_vended_keys`/`extract_vended_region`).
  `TableMetadata::location(&self) -> &str` and `current_schema(&self) -> &SchemaRef` unchanged.
- **Edit needed:** None.

### 6. `iceberg::table::Table` builder + accessors — UNCHANGED
- **Old = New:** `Table::builder() -> TableBuilder`; `TableBuilder::identifier(TableIdent)`,
  `.file_io(FileIO)`, `.metadata<T: Into<TableMetadataRef>>(T)`,
  `.metadata_location<T: Into<String>>(T)`, `.build() -> Result<Table>`.
  Passing an owned `TableMetadata` to `.metadata(...)` still works (`Into<Arc<TableMetadata>>`).
  `Table::metadata() -> &TableMetadata`, `Table::scan() -> TableScanBuilder`,
  `Table::file_io()` unchanged.
- **Edit needed:** None.

### 7. Scan path: `scan().with_filter(..).select_all().build()` → `plan_files()` → `FileScanTask` — UNCHANGED
- **Old = New:** `TableScanBuilder::with_filter(Predicate)`, `.select_all()`,
  `.build() -> Result<TableScan>`; `TableScan::plan_files(&self) -> Result<FileScanTaskStream>`.
  `FileScanTask::data_file_path(&self) -> &str` and public field `file_size_in_bytes: u64` unchanged.
- **Edit needed:** None. (`FileScanTask` gained/kept `deletes`, `partition_spec`, etc. — irrelevant here;
  the path+size accessors used are stable.)

### 8. `iceberg::expr::{Predicate, Reference, Datum}` + `iceberg::spec::Schema` — UNCHANGED
- **Old = New:** `Reference::new(impl Into<String>)`; `.equal_to/.less_than/.less_than_or_equal_to/`
  `.greater_than/.greater_than_or_equal_to(Datum) -> Predicate`; `.is_null()/.is_not_null()`;
  `.is_in(impl IntoIterator<Item=Datum>) -> Predicate`. `Predicate::and/or(self, Predicate)`,
  `Predicate::negate(self)`. `Datum::{bool,int,long,float,double,string,date_from_str,`
  `timestamp_from_str,timestamptz_from_str,timestamp_nanos,timestamptz_nanos}` all present with
  identical signatures. `Literal::from(Datum)` and `PrimitiveLiteral::Long(i64)` unchanged.
  `Schema::field_by_name_case_insensitive(&str) -> Option<&NestedFieldRef>`, `Schema::as_struct()`,
  `Schema::builder()/SchemaBuilder::{with_schema_id, with_fields, build}` unchanged.
  `NestedField::{required,optional}(i32, impl ToString, Type)`, `NestedField.field_type: Box<Type>`,
  `Type::as_primitive_type(&self) -> Option<&PrimitiveType>` unchanged.
- **Edit needed:** None (`adapter/iceberg_predicate.rs` unchanged).

### 9. `iceberg::{TableIdent, NamespaceIdent}` — UNCHANGED
- **Old = New:** `TableIdent::new(NamespaceIdent, String)`, fields `.namespace` / `.name`,
  `NamespaceIdent::from_vec(Vec<String>) -> Result<..>`, `NamespaceIdent: AsRef<[String]>`,
  `.join(".")` on the ident. `iceberg::{Catalog, CatalogBuilder}` traits + import paths unchanged.
- **Edit needed:** None (`adapter/tables.rs`, `adapter/mod.rs` unchanged).

### 10. `iceberg::spec::{Type, PrimitiveType}` — UNCHANGED
- **Old = New:** `PrimitiveType` variants: `Boolean, Int, Long, Float, Double,
  Decimal { precision: u32, scale: u32 }, Date, Time, Timestamp, TimestampNs, Timestamptz,
  TimestamptzNs, String, Uuid, Fixed(u64), Binary`. `Type` variants: `Primitive, Struct, List, Map`.
- **Edit needed:** None (`types/mapping.rs` unchanged).

---

## Test fixtures — writer stack (`tests/common/seed.rs`)

**The entire iceberg writer stack API is IDENTICAL between 0.9.1 and 0.10.0-rc.2** (these APIs —
`RollingFileWriterBuilder`, `PartitionKey`, `ApplyTransactionAction`, per-`build(Option<PartitionKey>)`
writers — already shipped in 0.9.1). `seed.rs` needs **no writer-stack API edits**. The only edits are
(1) the arrow 57→58 import swap and (2) the same `OpenDalStorageFactory::S3` field drop.

### 11. `OpenDalStorageFactory::S3` in `seed.rs` (~line 147) — **BREAKING (drop one field)**
Same edit as production site #1: delete `configured_scheme: "s3".to_string(),`.

### 12. Writer builders — UNCHANGED (verified in both 0.9.1 and tag)
- `DefaultFileNameGenerator::new(prefix: String, suffix: Option<String>, format: DataFileFormat)` — same.
- `ParquetWriterBuilder::new(props: parquet::…WriterProperties, schema: SchemaRef)` — same.
  (Tag also adds `new_with_match_mode(..)`; not needed.)
- `RollingFileWriterBuilder::new_with_default_file_size(inner_builder: B, file_io: FileIO,`
  `location_generator: L, file_name_generator: F)` — same 4-arg form.
- `DataFileWriterBuilder::new(inner: RollingFileWriterBuilder<B,L,F>)` — same.
- `IcebergWriterBuilder::build(&self, Option<PartitionKey>) -> Result<R>` — same;
  `IcebergWriter::write(I)`, `IcebergWriter::close() -> Result<O>` — same.
- `LocationGenerator::generate_location(&self, Option<&PartitionKey>, &str) -> String` — same trait shape
  (`FlatLocationGenerator` impl unaffected).
- `PartitionKey::new(spec: PartitionSpec, schema: SchemaRef, data: Struct) -> Self` — same.
  `TableMetadata::default_partition_spec() -> &PartitionSpecRef` — same (`.as_ref().clone()` still works).
- **Edit needed:** None (beyond the arrow import swap so batches/schema are arrow-58 typed).

### 13. Transaction / append — UNCHANGED
- `Transaction::new(&Table)`; `Transaction::fast_append() -> FastAppendAction`;
  `FastAppendAction::add_data_files(impl IntoIterator<Item=DataFile>) -> Self`;
  `ApplyTransactionAction::apply(self, Transaction) -> Result<Transaction>`;
  `Transaction::commit(self, &dyn Catalog) -> Result<Table>`. All identical.
- **Edit needed:** None.

### 14. `TableCreation` / `TableUpdate` / `TableRequirement` — UNCHANGED
- `TableCreation` struct + `derive_builder` setters (`.name`, `.schema`, `.partition_spec`,
  `.sort_order`, `.properties`, `.location`, `.format_version` default V2) — byte-identical.
- `TableUpdate::AddSchema { schema: Schema }` — **only `schema`** in both 0.9.1 and tag.
  (The `last_column_id: Option<i32>` field belongs to `ViewUpdate::AddSchema`, which `seed.rs`
  does not use — so there is **no** diff here despite first appearances.)
  `TableUpdate::SetCurrentSchema { schema_id: i32 }` — same.
- `TableRequirement::CurrentSchemaIdMatch { current_schema_id: SchemaId }` — same.
- **Edit needed:** None.

### 15. `UnboundPartitionSpec` / `UnboundPartitionField` — UNCHANGED
- `UnboundPartitionSpec::builder()` → `.with_spec_id(i32)`, `.add_partition_fields(impl IntoIterator)`
  (returns `Result<Self>`), `.build() -> UnboundPartitionSpec`. `UnboundPartitionField::builder()`
  → `.source_id(i32)`, `.field_id(i32)`, `.name(impl ToString)`, `.transform(Transform)`, `.build()`.
  Struct fields identical (`source_id: i32`, `field_id: Option<i32>`, `name: String`, `transform: Transform`).
- **Edit needed:** None.

### 16. Arrow / parquet imports in `seed.rs` + `tests/tpch_loader.rs` — swap 57 → 58 (NOT an iceberg API change)
- `use ice_arrow_array::…` / `ice_arrow_schema::{DataType, Field, Schema, TimeUnit}` /
  `ice_parquet::{arrow::PARQUET_FIELD_ID_META_KEY, file::properties::WriterProperties}` →
  workspace `arrow`/`arrow-array`/`arrow-schema`/`parquet` (58). After the bump, iceberg links
  arrow-58, so batches fed to `IcebergWriter::write` and schemas passed to `ParquetWriterBuilder::new`
  must be **workspace arrow-58** types (they already are the right shape; only the crate version moves).
  `PARQUET_FIELD_ID_META_KEY` is an arrow-rs `parquet` const (stable across 57→58): re-import from
  workspace `parquet::arrow`.
- `tests/tpch_loader.rs::arrow_to_iceberg_type` matches on `DataType` (incl. `Utf8View`) — these are the
  **arrow** `DataType` enum, retargeted to arrow-58. The iceberg side it builds
  (`PrimitiveType`/`Type`/`NestedField`/`Schema::builder`) is unchanged (section 10 / 8).
  Task 6 separately removes `tpchgen-arrow` and hand-builds arrow-58 batches — a fixture concern, not
  an iceberg API diff. Pick the arrow-58 string type the writer accepts (`Utf8` is safe; keep the
  existing normalization).

---

## Net edit inventory for the fix tasks

| Task | File | iceberg-API edits required |
|------|------|----------------------------|
| 3 | `adapter/pushdown.rs` | Drop `configured_scheme` at 2 `OpenDalStorageFactory::S3` sites. Nothing else. |
| 4 | `adapter/iceberg_predicate.rs`, `adapter/tables.rs`, `adapter/mod.rs`, `types/mapping.rs` | **None** — all APIs unchanged. |
| 5 | `tests/common/seed.rs` | Drop `configured_scheme` at 1 `S3` site; swap arrow/parquet 57→58 imports. No writer-stack API edits. |
| 6/7 | `tests/tpch_loader.rs`, dev-deps | Retarget `DataType` matches to arrow-58; remove `tpchgen-arrow`. No iceberg API edits. |

**Biggest surprise:** the writer stack (the largest surface the plan flagged) is entirely
API-unchanged — 0.9.1 already had the 0.10-shaped rolling-writer / partition-key / apply-action APIs.
And `load_table` still returns bare `Table` (fact (c) holds). The single genuine breaking change in
the whole bump is the one-field `OpenDalStorageFactory::S3` drop.

---

## Runtime-surfaced corrections (found during e2e)

The api-diff above was compile-only and therefore missed one change that is enforced at **runtime**,
not compile time:

- **`TableBuilder` gained a mandatory `.runtime(Runtime)` in 0.10.** `Table::builder()` now carries an
  `Option<Runtime>` field. `TableBuilder::build()` (`crates/iceberg/src/table.rs:157-161`) returns
  `Error(DataInvalid, "Runtime must be provided with TableBuilder.runtime()")` when it was never set.
  Because the field is optional in the type, the builder chain **still compiles** without it — the
  compile-only api-diff could not see this. It first surfaced at query time in e2e as
  `F-UDF-CL-RUST-9001: ... failed to build Iceberg table: DataInvalid => Runtime must be provided with
  TableBuilder.runtime()`. iceberg's own `RestCatalog::load_table` sets it via
  `.runtime(Runtime::try_current()?)` (`table.rs:357,376`); `TableCreation`-based catalog paths (e.g.
  `seed.rs`) set the runtime internally, which is why seeding was unaffected.
- **This corrects the earlier "Table::builder chain unchanged" finding** in the task-3 inventory: the
  chain DID change — it acquired a required `.runtime()` step.
- **Fix:** `adapter/pushdown.rs` (the only production `Table::builder()` site) now calls
  `iceberg::Runtime::try_current()` (public re-export of the private `runtime` module; confirmed in the
  tag's `public-api.txt`: `pub fn iceberg::Runtime::try_current() -> iceberg::Result<Self>`) and adds
  `.runtime(runtime)` to the builder chain. The `try_current()` error is mapped with the same
  redacted `UdfError::User("failed to build Iceberg table: ...")` shape as the existing `.build()`
  error, so a missing-runtime failure surfaces identically. `try_current()` succeeds because the
  function is `async` and always runs inside the UDF's tokio runtime. e2e is green after the fix
  (scan 43 / capability 7 / count_distinct 6, 0 failures).
