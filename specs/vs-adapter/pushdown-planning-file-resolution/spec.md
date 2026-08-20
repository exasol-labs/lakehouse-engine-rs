# Feature: Pushdown File Resolution

Resolves a table's identity and current file state exactly once per pushdown, before any
SQL is built. Recovers the target table from the Exasol involved-table name via the
persisted `TABLE_MAP` and hands it to the format reader that owns its table format, which
returns the active data-file list, each file's byte size, the logical schema, the table
root, and — where the format has them — each file's associated delete references, all at one
resolve-once seam so the scan UDF never discovers files, delete files, or sizes itself. That
orchestration has no Delta counterpart feature because it never needed one: a Delta table
reaches it by the same route an Iceberg one does
(`vs-adapter/pushdown-format-neutral-resolution`). This feature owns the ICEBERG reader's
half of the seam — the multi-level `TableIdent` build, the Iceberg snapshot and
`current_schema()` read that produce the field-id-carrying logical schema, and the
merge-on-read positional-delete resolution; the Delta reader's half is owned by
`vs-adapter/delta-table-planning`. A `loadTable` response that carries no table `location`
is rejected here, before the vended/static storage split, so every path depending on a table
root — including each join side — fails identically rather than resolving an empty root. See
`vs-adapter/pushdown-planning` for how the resolved table identity, file list, byte sizes,
delete-file references, and logical schema feed the scan-driving SQL.

## Background

* The resolve-once ORCHESTRATION — the `TABLE_MAP` lookup, one resolve per pushdown, and one
  `ScanSpec` build — is format-neutral: every table format reaches it by the same route
  (`vs-adapter/pushdown-format-neutral-resolution`), and the scan UDF never discovers files,
  delete files, or sizes itself. Only the READER behind that seam is format-specific. This
  feature owns the ICEBERG reader's half — the multi-level `TableIdent` build, the snapshot and
  `current_schema()` read, each file's byte size from the Iceberg manifest, and merge-on-read
  positional-delete resolution; the Delta reader's half is owned by
  `vs-adapter/delta-table-planning`.
* The logical schema this feature's Iceberg reader produces identifies each column by its Iceberg field-id, current name, Arrow type, and nullability.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* The data-file list, each file's byte size, and each file's associated positional-delete files are resolved exactly once, at the same seam; the scan UDF never discovers files or delete files.
* Delete support keeps the wire surface minimal — per-file delete references only, with no serialized Iceberg schema and no bound predicate added to the spec.
* **This feature is the single owner of the absent-table-location rule.** The rule was
  previously stated only inside `vs-adapter/pushdown-planning-cloud-credentials`' vended
  scheme-selection scenario, which made it read as a vended-path guarantee. It is
  path-independent: it holds with vending disabled, with vending enabled, and on every join
  side. That feature now REFERENCES this one instead of restating the clause, so the
  normative text has exactly one home.
* **"Absent" covers TWO wire shapes, and each is rejected by a DIFFERENT mechanism.** A
  `loadTable` body may carry `"location": ""` (key present, value empty) or omit the
  `location` key entirely. Only the EMPTY shape reaches this feature's guard. An OMITTED key
  fails deserialization strictly earlier: `iceberg-0.10.0` declares `location: String` —
  non-`Option`, with no `#[serde(default)]` — on all three metadata variants
  (`src/spec/table_metadata.rs:810` `TableMetadataV2V3Shared`, `:855` `TableMetadataV1`),
  and `TableMetadata` deserializes via `#[serde(try_from = "TableMetadataEnum")]` over
  `#[serde(untagged)] enum TableMetadataEnum { V3, V2, V1 }` (`:783-788`), so an omitted key
  matches no variant. The catalog read therefore fails in `authed_get_json`
  (`crates/lakehouse-catalog/src/iceberg_io.rs:89-94`) with
  `UdfError::User("failed to parse catalog response: …")` before any location is read. Both
  shapes are consequently rejected as a `UdfError::User` and neither can substitute the
  `warehouse`; they differ ONLY in message specificity, which is a diagnostic-quality
  difference and not an Iceberg-spec deviation, because the spec constrains the field's
  presence rather than a reader's error wording. The guard is deliberately NOT widened to
  name the field on the omitted-key path: doing so requires inspecting the raw body in
  `load_table_any_auth`, which also serves `createVirtualSchema` — a path this feature leaves
  untouched by design.
* **Iceberg table-spec grounding, quoted from `apache/iceberg` `format/spec.md` (main),
  verified against the fetched file rather than from memory.** The Table Metadata field
  table marks `location` `_required_` in the v1, v2, AND v3 columns, described as "The
  table's base location. This is used by writers to determine where to store data files,
  manifest files, and table metadata files." Only in v4 does it become `_optional_`, and
  even there: "Must be an absolute path when present", with `## Table Location
  Specification` adding "When the `location` field is present in table metadata, it is used
  directly as the table's base location. When the `location` field is not present (v4 and
  later), the table location must be provided." A `loadTable` response for a v1/v2/v3 table
  that carries no `location` is therefore a MALFORMED response, and rejecting it is
  spec-conformance rather than arbitrary strictness.
* **The REST `warehouse` is a routing identifier and denotes no object store.** It builds the
  `loadTable` URL prefix only — the derived `catalogs/{account-id}` segment on the Glue path,
  or the `/v1/config` `overrides.prefix` segment elsewhere. Its value lives in a different
  namespace from a storage location: a bare AWS account id (`123456789012`) under Glue, a
  warehouse NAME (`lakehouse_static`) or a per-warehouse UUID under Lakekeeper. It is
  therefore never a substitute for an absent table location, with or without vended
  credentials, and the non-SigV4 no-override prefix fallback is already the EMPTY string
  rather than the warehouse.
* **The rejection is sited at the resolve-once seam, ABOVE the vended/static split, and
  deliberately not at the catalog-load seam.** Placing it in `load_table_any_auth`
  (`crates/lakehouse-catalog/src/session.rs`) would also reject the response on the
  `createVirtualSchema` schema-resolution path, which reads no location at all — failing a
  whole virtual-schema creation over a field that path never uses. The resolve-once seam is
  the narrowest site at which every path that DOES depend on a table location passes exactly
  once.
* **The `createVirtualSchema` path needs no second check to be consistent.**
  `resolve_table_schema` reads only `result.metadata.current_schema()`; it resolves no
  storage anchor and no table root, so there is no value for the `warehouse` to be
  substituted into. Consistency across that path is a property of what it reads, not of a
  guard it carries.
* **The join path inherits the rejection rather than repeating it.** `resolve_one_join_side`
  builds a per-side `CatalogProps` by overriding only `table` and then delegates entirely to
  `resolve_file_list`, so each side of a join is checked by the same single guard.
* **The empty-table-root branch of the file-list encoding is retained deliberately.** After
  this rejection, an empty table root is unreachable from a `loadTable` response, but
  `relativize_path_to_root`'s empty-root handling (empty root ⇒ every path stays absolute)
  is a total-function property of the wire encoding, not a storage-anchor fallback. It stays
  as written. The scan-side half of that property is owned by
  `datafusion-scan/scan-execution-file-metadata`, which keeps three empty-table-root clauses —
  two normative `SHALL` clauses and one descriptive Background bullet.
  `vs-adapter/pushdown-planning-file-encoding` states the table-root-once and relative/absolute
  encoding rules but no empty-root rule.

## Scenarios

### Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table

* *GIVEN* a virtual schema created over a namespace containing multiple Iceberg tables, whose `adapterNotes` carry the `TABLE_MAP` recorded at create time
* *AND* a `pushdown` request whose `involvedTables[0].name` is the Exasol (uppercased, `__`-flattened) name of one of those tables
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL read `TABLE_MAP` back from `schemaMetadataInfo.adapterNotes` and look up the involved virtual table name to recover its original-cased fully-qualified Iceberg identifier
* *AND* the adapter SHALL resolve the data-file list and build the scan-driving SQL for exactly that one Iceberg table, carrying its identifier in the per-shard `CatalogProps.table`
* *AND* a `pushdown` request whose involved virtual table name is absent from `TABLE_MAP` SHALL fail with an error naming the unknown virtual table, never silently scanning a different or stale table

### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and at that same seam extract the table's current Iceberg schema (from `current_schema()`) into a logical schema carrying, per column, its `field_id`, current name, Arrow type, and nullability
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the Iceberg table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size

### Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent

* *GIVEN* a `TABLE_MAP` entry whose value is a multi-level Iceberg identifier such as `prod.finance.orders`
* *WHEN* the adapter resolves that identifier to load the table from the catalog
* *THEN* the adapter SHALL split the identifier into all namespace segments and the trailing table name, building the iceberg `TableIdent` from a multi-segment `NamespaceIdent` rather than treating only the first segment as the namespace
* *AND* both the SigV4-signed and the unsigned catalog paths SHALL build the identifier the same way so multi-level namespaces load correctly under either path

### Scenario: Positional-delete file references are carried in the per-shard files argument

* *GIVEN* a virtual schema over an Iceberg merge-on-read table backed by MinIO, where `plan_files` associates each data file with its applicable Parquet positional-delete files (at `file` or `partition` granularity)
* *WHEN* Exasol sends the corresponding pushdown request
* *THEN* the adapter SHALL resolve the data-file list, each file's byte size, and each file's associated positional-delete files exactly once, at the same resolve-once seam, and MUST NOT require the scan UDF to discover delete files itself
* *AND* the adapter SHALL carry each data file's associated positional-delete file references (path, byte size, delete content type) in the per-shard files argument alongside the data-file entry, keeping the wire surface minimal — no serialized Iceberg schema and no bound predicate are added for delete support
* *AND* the shard-invariant common spec (logical schema, projection, filter, LIMIT, credentials, table root) SHALL be unchanged by delete support, so a delete-free table produces a byte-identical common spec to before this feature

### Scenario: File resolution rejects a loadTable response that carries no table location

* *GIVEN* a `pushdown` request for a table whose `loadTable` response carries an EMPTY table metadata `location`
* *AND* a CONNECTION that supplies a non-empty `warehouse`
* *WHEN* the adapter resolves the file list for that table, under EITHER value of `use_vended_credentials`
* *THEN* the adapter SHALL return a `UdfError::User` naming the EMPTY table `location`, from ONE check that runs BEFORE the vended/static storage split, so both values of `use_vended_credentials` report the IDENTICAL error text
* *AND* that error text MUST be path-independent: it MUST NOT frame the failure as a vended-storage-backend resolution failure, because on the non-vended path the static storage backend resolves normally and only the table location is missing
* *AND* the adapter MUST NOT substitute the CONNECTION's `warehouse`, `endpoint`, or any other CONNECTION-derived value for the empty location, because the REST `warehouse` is a routing identifier — a bare AWS account id on the Glue path, a warehouse name on the Lakekeeper path — that denotes no object store
* *AND* the adapter MUST NOT go on to resolve the effective storage, build the Iceberg table, or plan any file, so a malformed response costs zero object-storage access
* *AND* that error MUST be returned as a `Result`, never raised as a panic, because a panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part
* *AND* that error MUST NOT contain any credential value
* *AND* every join side SHALL inherit this rejection through the same file-resolution call, so a join whose dimension side carries no location fails at plan time with the same error
* *AND* the `createVirtualSchema` schema-resolution path SHALL be unchanged, because it reads only the table's `current_schema()` and reads no table location, so nothing there can be substituted for one
* *AND* the empty-table-root branch SHALL be retained on both sides of the wire — `relativize_path_to_root` in the adapter and the three empty-table-root clauses of `datafusion-scan/scan-execution-file-metadata` (two normative `SHALL` clauses and one descriptive Background bullet) — because this rejection makes an empty root unreachable from a `loadTable` response, which makes that branch unreachable rather than dead, and it MUST NOT be deleted or converted into a second error
* *AND* a `loadTable` body that OMITS the `location` key SHALL also be rejected as a `UdfError::User`, at deserialization rather than by this guard, and MUST NOT be reported by any message that substitutes the `warehouse`; that message names the unparseable response rather than the `location` field, which this feature records as a known diagnostic-specificity difference rather than leaving it unstated
