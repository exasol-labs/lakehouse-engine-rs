<!-- DELTA:CHANGED -->
# Feature: Pushdown File Resolution

Resolves a table's identity and current file state exactly once per pushdown, before any
SQL is built. Recovers the target table from the Exasol involved-table name via the
persisted `TABLE_MAP` and hands it to the format reader that owns its table format, which
returns the active data-file list, each file's byte size, the table root, and — where the
format has them — each file's associated delete references, all at one resolve-once seam so
the scan UDF never discovers files, delete files, or sizes itself. The table's logical schema
comes from its column notes (`vs-adapter/column-source-notes`), never from the reader. That
orchestration has no Delta counterpart feature because it never needed one: a Delta table
reaches it by the same route an Iceberg one does
(`file-planning/pushdown-format-neutral-resolution`). This feature owns the ICEBERG reader's
half of the seam — the multi-level `TableIdent` build, the Iceberg snapshot read, the
field-id-carrying logical schema `createVirtualSchema` records from the table's
`current_schema()`, and the merge-on-read positional-delete resolution; the Delta reader's half
is owned by `delta/delta-table-planning`. A `loadTable` response that carries no table
`location` is rejected here, before the vended/static storage split, so every path depending on
a table root — including each join side — fails identically rather than resolving an empty
root. See `pushdown/pushdown-planning` for how the resolved table identity, file list, byte
sizes, delete-file references, and logical schema feed the scan-driving SQL.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The resolve-once ORCHESTRATION — the `TABLE_MAP` lookup, one resolve per pushdown, and one
  `ScanSpec` build — is format-neutral: every table format reaches it by the same route
  (`file-planning/pushdown-format-neutral-resolution`), and the scan UDF never discovers files,
  delete files, or sizes itself. Only the READER behind that seam is format-specific. This
  feature owns the ICEBERG reader's half — the multi-level `TableIdent` build, the snapshot read,
  each file's byte size from the Iceberg manifest, and merge-on-read
  positional-delete resolution; the Delta reader's half is owned by
  `delta/delta-table-planning`.
* An Iceberg table's logical schema comes from its column notes. `createVirtualSchema` builds each note from the table's `current_schema()` at that time: the column's Iceberg field-id, its name then, its Arrow type, its nullability, and its `initial-default`. The pushdown reader reads the current schema only to translate the filter for pruning (`file-planning/pushdown-file-pruning`) and to refuse a recorded `date` promotion.
* Each per-shard file entry carries both the file path and its byte size, so the scan UDF never re-discovers a size the adapter already resolved.
* The data-file list, each file's byte size, and each file's associated positional-delete files are resolved exactly once, at the same seam; the scan UDF never discovers files or delete files.
* Delete support keeps the wire surface minimal — per-file delete references only, with no serialized Iceberg schema and no bound predicate added to the spec.
* **This feature is the single owner of the absent-table-location rule.** The rule was
  previously stated only inside `storage-access/pushdown-planning-cloud-credentials-vended-storage`' vended
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
  `file-planning/pushdown-planning-file-encoding` states the table-root-once and relative/absolute
  encoding rules but no empty-root rule.

* **Iceberg table-spec grounding for a declaration older than the table's current schema, quoted
  from `apache/iceberg` `format/spec.md` (main).** § Column Projection: "Columns in Iceberg data
  files are selected by field id. The table schema's column names and order may change after a
  data file is written, and projection must be done using field ids." § Schema Evolution:
  "Renaming an existing field must change the name, but not the field ID", "Adding a new field
  assigns a new ID for that field and for any nested fields", and "The `initial-default` must be
  set when a field is added and cannot change". The Table Metadata field `last-column-id` is "the
  highest assigned column ID for the table. This is used to ensure columns are always assigned an
  unused ID when evolving schemas." A declared field id therefore names the same column for the
  table's whole life, and its declared `initial-default` never goes stale. A source-side rename
  between refreshes leaves the declaration correct under its old name, and no later column can
  take over a declared id.
* **Scoped exception: a column dropped at the source between refreshes.** § Schema Evolution:
  "Deleting a field removes it from the current schema." The pushdown plans against the declared
  schema, which still holds the dropped field id. Data files written before the drop still store
  that id, so until `REFRESH` the declared column reads the values those files store, and NULL or
  its declared `initial-default` from files written after the drop. Reading a schema other than the
  current one is a column projection the spec defines, so no row is misread, but the column shows
  data the current schema no longer exposes. A dropped column declared required with no
  `initial-default` instead fails a query that reads a file written after the drop, with the
  required-absent error `scan-read-path/scan-execution-field-id-projection-absent-fields` records
  ("Added required column absent from a file with no initial-default errors cleanly"). `REFRESH`
  removes the column.
* **A declared nullability older than the current schema.** Each note fixes its column's
  `nullable` flag from the field's `required` flag at `createVirtualSchema`. A catalog can relax a
  required field to optional later (`UpdateSchema.makeColumnOptional` in the Iceberg Java API).
  DataFusion rewrites `c IS NULL` to `false` and `c IS NOT NULL` to `true` for a non-nullable field,
  so a stale `nullable: false` would drop the rows a later writer stored as NULL without an error.
  The Iceberg reader therefore fails a query whose declared non-nullable column the current schema,
  looked up by the declared field id, marks optional (scenario below). A declared optional column
  that the current schema marks required plans normally, because no row is lost.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over a namespace whose tables are backed by SeaweedFS
* *AND* a query that projects a subset of columns from one of those tables
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL determine the target Iceberg table from the schema-metadata mapping, resolve that table's Iceberg snapshot, data-file list, and each file's byte size exactly once, and take the table's logical schema from its column notes, carrying, per column, its declared `field_id`, name, Arrow type, nullability, and `initial-default`
* *AND* the adapter MUST NOT build the logical schema from the table's `current_schema()` at pushdown
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the `LAKEHOUSE_SCAN` SCALAR EMIT UDF, carrying the logical schema AND the table root in the shard-invariant common spec spliced ONCE as the scalar scan's first-argument literal, and the resolved data-file list flowed through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor as the per-shard argument, where each per-shard entry carries the file path together with its resolved byte size
* *AND* the outer scalar scan select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the adapter MUST NOT require the scan UDF to discover files itself, and MUST NOT require the scan UDF to re-fetch any file's size
* *AND* the resolve-once orchestration this scenario describes — `TABLE_MAP` lookup, one resolve, one `ScanSpec` build — SHALL be reached identically by every table format, with the format reader supplying the file list, byte sizes, and table root, and the column notes supplying the logical schema (`file-planning/pushdown-format-neutral-resolution`); only the reader behind the seam is format-specific
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A column renamed at the source after the last REFRESH reads under its declared name

* *GIVEN* an Iceberg table with columns `id` (field id 1) and `score` (field id 2) holding rows in one data file, a virtual schema created over it, and the source column `score` afterwards renamed to `rating` with no `REFRESH`
* *WHEN* a query selects `ID, SCORE`
* *THEN* the query SHALL return each row's `score` value under `SCORE`, because the scan binds the declared field id 2, which the rename kept
* *AND* the virtual table SHALL NOT declare `RATING` until `ALTER VIRTUAL SCHEMA ... REFRESH` runs, after which the same values read under `RATING`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A column dropped at the source after the last REFRESH reads what older data files store

* *GIVEN* an Iceberg table with columns `id` and an optional column `note`, one data file written while `note` existed, a virtual schema created over it, and afterwards `note` dropped from the source schema and a second data file written without it, with no `REFRESH`
* *WHEN* a query selects `ID, NOTE`
* *THEN* the rows of the first data file SHALL carry their stored `note` values and the rows of the second SHALL carry NULL, per the scoped exception this feature's Background records
* *AND* the query MUST NOT fail, and MUST NOT bind `NOTE` to any other column
* *AND* after `ALTER VIRTUAL SCHEMA ... REFRESH` the virtual table SHALL NOT declare `NOTE`
* *AND* had `note` been required with no `initial-default`, the query SHALL instead fail on the second data file with the required-absent error `scan-read-path/scan-execution-field-id-projection-absent-fields` records, until `REFRESH`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A declared required column that the current schema marks optional fails the query

* *GIVEN* an Iceberg table with a required column `qty` (field id 2), a virtual schema created over it, and afterwards `qty` made optional at the source and a data file written that stores NULL in `qty`, with no `REFRESH`
* *WHEN* `SELECT ID FROM <table> WHERE QTY IS NULL` runs, and separately a query that reads only `ID`
* *THEN* each query SHALL fail before any file is planned with an error naming the virtual table and the column `QTY`, stating that the declaration marks it non-nullable while the current schema allows NULL, and naming `ALTER VIRTUAL SCHEMA ... REFRESH` as the fix
* *AND* the reader SHALL find the column in the current schema by its declared field id, not by its name
* *AND* after `REFRESH` the first query SHALL return the row whose `qty` is NULL
<!-- /DELTA:NEW -->
