# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves
the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and
any supported aggregate, extracts the table's current Iceberg schema for field-id-based
projection, and emits the SQL that drives the DataFusion scan. Cluster fan-out is
separated from the scan: a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor
subquery (`GROUP BY shard_key`) spreads each shard's per-file list across nodes, and an
outer ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list
node-locally and streams the rows. The scan-driving SQL splices the shard-invariant parts
(projection, filter, LIMIT, logical schema, credentials, and the Iceberg table root) once
as the scalar scan UDF's first-argument common literal and flows each shard's per-file
subset through the distributor as the second argument. A single-shard plan short-circuits
the distributor and calls the scalar scan directly on the file-list literal. See
`vs-adapter/pushdown-planning-file-encoding` for the table-root-once and relative/absolute
path encoding rules. See `vs-adapter/pushdown-planning-nested-aggregate-fallback` for the
guard against composed requests (e.g. an outer aggregate over an inner grouped-aggregate
sub-select) that don't map onto the source table's own columns. This feature also extends
the resolve-once seam to associate each data file's positional-delete files and carry them
minimally in the per-shard argument. Single-group aggregate pushdown (capability
advertisement, partial-aggregate scan-spec translation, wrapper merge SQL, and AVG
sum/count decomposition) is covered separately in
`vs-adapter/pushdown-planning-single-group-agg`.

## Background

* **This feature becomes the single owner of the absent-table-location rule.** The rule was
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

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
