# Decision Log: add-iceberg-predicate-pruning

Date: 2026-06-24

## Interview

**Q:** What pruning scope should the Iceberg predicate translation cover?
**A:** The FULL comparison set: `=`, `<`, `<=`, `BETWEEN`, `IN` (const list), `IS NULL`, `IS NOT NULL`, and `AND`/`OR`/`NOT`. (Exasol pre-normalises `>`→`<` and `>=`→`<=`, so only LESS/LESSEQUAL arrive.) It prunes on partitions AND per-file min/max bounds. Untranslatable conjuncts (LIKE, REGEXP_LIKE, scalar-function predicates) are dropped from the prune filter — never mistranslated.

**Q:** Where should the Iceberg-predicate translator live?
**A:** A NEW module in lakehouse-engine (e.g. `adapter/iceberg_predicate.rs`), consuming the same Exasol filter JSON the DataFusion path reads. This keeps `iceberg-rust` types out of the shared `vs-expression` crate, preserving its cross-project-shared design.

**Q:** Is this plan independent of the multi-table work?
**A:** No — it is implemented SERIALLY AFTER `change-multi-table-virtual-schema`. Assume multi-table has landed: per-pushdown table identity is derived from `involvedTables[0].name` and the scanned Iceberg `TableIdent` is already resolved into the scan inputs by file-resolution time. Do not re-plan multi-table.

**Q:** Does pruning change anything the user/Exasol sees through the capability handshake?
**A:** No. Pruning is an internal scan optimisation, invisible to capabilities. No capability change, no `ScanSpec` change, no scan-UDF/sharding/wrapper-SQL change.

**Q:** What is the non-negotiable correctness invariant?
**A:** The Iceberg filter is pruning-only — it may only skip files that provably contain zero matching rows. DataFusion always applies the full `ScanSpec.filter` and is the sole source of row-level correctness. The translation must be sound, not complete: drop anything it cannot translate soundly.

## Design Decisions

### [1] Sound-partial Iceberg translation with strict OR/NOT handling

- **Decision:** `to_iceberg_predicate` returns `Option<Predicate>` where `None` = "no constraint". Under `AND`, a `None` child is dropped (keeping the rest). Under `OR`, ANY `None` child collapses the whole `OR` to `None`. `NOT` of a `None` child is `None`. Leaves translate only when the column resolves and a type-matching `Datum` builds.
- **Alternatives:** (a) Decline pushdown / error when any node is untranslatable — rejected: DataFusion is the backstop, so less pruning is always safe and erroring needlessly forfeits the optimisation. (b) Prune on the translatable branch of an `OR` alone — rejected as unsound: a row matching the untranslatable branch may live in any file, so pruning would drop result rows.
- **Rationale:** This is the subtle correctness core. AND widens-on-drop (safe); OR/NOT must not narrow on partial knowledge. Mirrors the existing `render_df_filter_safe` conservative contract.
- **Promotes to ADR:** yes

### [2] New `adapter/iceberg_predicate.rs` module; `iceberg-rust` types stay out of `vs-expression`

- **Decision:** Author a dedicated lakehouse-engine module that consumes the raw Exasol filter JSON and emits `iceberg::expr::Predicate`. Do not extend the shared `vs-expression` crate.
- **Alternatives:** Extend `vs-expression` to also emit iceberg predicates — rejected to preserve that crate's cross-project-shared, iceberg-free design.
- **Rationale:** Keeps the cross-project crate dependency-clean; the iceberg coupling lives only where iceberg is already a dependency.
- **Promotes to ADR:** yes

### [3] Emit unbound predicate; apply at the `plan_files_from_table` seam

- **Decision:** Build the predicate inside `plan_files_from_table` from `table.metadata().current_schema()` and pass it to `scan().with_filter(...)` on both signed and unsigned paths. Emit an UNBOUND `Predicate` — iceberg 0.9.1 `plan_files` binds it internally with `case_sensitive: true`.
- **Alternatives:** (a) Build in `handle_pushdown` and pass a `Predicate` down — rejected: the Iceberg `Schema` is only assembled where the `Table` is built, on two paths; building at the seam avoids resolving schema twice. (b) Manually `predicate.bind(schema, true)` in the translator — rejected as redundant double-binding.
- **Rationale:** Single choke point that already has the schema; matches what `with_filter` expects.
- **Promotes to ADR:** no

### [4] Casing & Datum-typing reconciliation against the Iceberg schema

- **Decision:** Resolve the Exasol uppercased column name to the Iceberg field via case-insensitive lookup, then emit a `Reference` with the field's EXACT name and a `Datum` whose primitive variant matches the field type. Any unresolved column or type mismatch → drop the leaf (`None`).
- **Alternatives:** Pass the uppercased name straight through — rejected: iceberg binds case-sensitively (`case_sensitive: true`), so wrong casing errors or mis-binds; and a type-mismatched `Datum` could bind incorrectly.
- **Rationale:** Correctness and robustness; reuses the same casing-reconciliation principle the projection path already applies.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
