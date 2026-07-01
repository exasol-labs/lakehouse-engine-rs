# Decision Log: fix-scan-field-id-projection

Date: 2026-07-01

## Interview

**Q:** Approach — field-id `PhysicalExprAdapter` + logical-schema registration, or use the iceberg-rust reader?
**A:** Field-id adapter. The two-Arrow-versions constraint (DataFusion/SDK on arrow/parquet 58; iceberg 0.9.1 on aliased arrow 57) rules out iceberg-rust's ArrowReader / iceberg-datafusion inside the DataFusion session. DataFusion 54 dictates the `PhysicalExprAdapter` mechanism because `with_schema_adapter_factory` is a deprecated no-op. AGREED.

**Q:** Should the spec carry the Iceberg `initial-default` so an added required column missing from an older file can be filled?
**A:** OUT OF SCOPE. Deferred to issue #27. The core reuses the DataFusion default adapter's null / cast / required-missing-error behavior and carries no defaults. An added nullable column absent from a file is correctly NULL-filled for free; an added required column missing from a file errors cleanly rather than fabricating data.

**Q:** How deep should the name fallback go — parse the `schema.name-mapping.default` table property?
**A:** OUT OF SCOPE. Deferred to issue #28. The fallback is a simple physical-name match (physical name == current logical name). Modern writers embed field-ids, so the fallback rarely fires.

## Design Decisions

### [1] Field-id resolution via a PhysicalExprAdapter, not the iceberg reader

- **Decision:** Bind columns by Iceberg field-id inside a custom `FieldIdExprAdapter` installed on the `ListingTable` via `ListingTableConfig::with_expr_adapter_factory`, keeping the whole fix in arrow-58 / DataFusion.
- **Alternatives:** iceberg-rust ArrowReader / iceberg-datafusion (crosses an incompatible Arrow version boundary); the deprecated `with_schema_adapter_factory` (no-op in DataFusion 54).
- **Rationale:** The two-Arrow-versions constraint and DataFusion 54's supported API leave the expr-adapter path as the only viable mechanism; it also handles divergent physical layouts within one shard because the Parquet opener applies the adapter per file.
- **Promotes to ADR:** yes

### [2] Override resolution only; reuse DefaultPhysicalExprAdapter for everything else

- **Decision:** `FieldIdExprAdapter` overrides only the column-resolution step (field-id-first, physical-name fallback) and delegates null-fill, type-diff cast, and required-missing-error to `DefaultPhysicalExprAdapter`. The per-column spec data is just `{field_id, name, arrow_type, nullable}` — no defaults.
- **Alternatives:** Reimplementing a full schema adapter with custom missing-column and cast handling.
- **Rationale:** Keeps the change minimal and correct by reusing battle-tested DataFusion behavior; the added-nullable NULL-fill and added-required clean-error both fall out for free.
- **Promotes to ADR:** yes

### [3] Carry the logical schema in ScanSpec as a backward-compatible optional field

- **Decision:** Add a `#[serde(default)]` logical-schema field to `ScanSpec`; when absent the UDF falls back to the existing first-file `infer_schema` / name-based path unchanged.
- **Alternatives:** Making the logical schema mandatory (breaks in-flight / older specs).
- **Rationale:** Established pattern in `scan/spec.rs`; preserves compatibility across the JSON-VARCHAR UDF boundary and lets the two paths coexist.
- **Promotes to ADR:** no

### [4] Extract the logical schema at the existing resolve-once seam

- **Decision:** Derive the logical schema from `table.metadata().current_schema()` in `resolve_file_list`, the same place the file list is resolved once per query.
- **Alternatives:** Resolving metadata in the UDF per node.
- **Rationale:** Honors the project's "resolve metadata once per query, in the VS layer, never once per node" architecture rule.
- **Promotes to ADR:** no

### [6] Logical-schema field carries the FULL current schema, kept separate from `projection`

- **Decision:** The new logical-schema field carries every column of `current_schema()` (full table schema), and stays a distinct field alongside the existing `projection: Vec<String>` — the two are NOT merged.
- **Alternatives:** (a) Merge into `projection` by enriching it from `Vec<String>` to `Vec<{field_id, name, arrow_type, nullable}>`; (b) carry only the projected columns in the logical schema.
- **Rationale:** The `FieldIdExprAdapter` must resolve every column referenced anywhere in the scan — projection ∪ filter ∪ group_keys ∪ aggregate columns — not just the output columns. `build_scan_sql` already aliases ALL columns, not only projected ones, precisely because a pushed-down filter or GROUP BY may reference a non-projected column (`scan/mod.rs` inner-SELECT: "All columns are aliased … because the filter may reference a column that is not projected."). Carrying only projected columns would leave a renamed filter/group-by column unbindable. The honest superset of "all referenced columns" is the whole schema, so the field carries the full schema. Because that is no longer a projection, merging it into `projection` would muddy `projection`'s "output selection / empty = all" meaning without saving JSON size; the two stay separate: `projection` = which columns to output and in what order; logical schema = binding/type/nullability metadata for every column the scan might touch.
- **Promotes to ADR:** no

### [5] Defer initial-default fill (#27) and name-mapping-property (#28)

- **Decision:** Scope the fix to field-id binding + simple name fallback; track initial-default fill as #27 and `schema.name-mapping.default` support as #28.
- **Alternatives:** Building both now.
- **Rationale:** Keeps this change small and shippable; the deferred behaviors are independent and rarely needed (modern writers embed field-ids; added-required-with-default is uncommon).
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
