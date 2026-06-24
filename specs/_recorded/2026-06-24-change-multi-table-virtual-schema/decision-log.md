# Decision Log: change-multi-table-virtual-schema

Date: 2026-06-24

## Interview

**Q:** Should the multi-table VS advertise JOIN capabilities and push joins to DataFusion?
**A:** No. Multi-table VS only. Do NOT advertise JOIN (JOIN, JOIN_TYPE_*, JOIN_CONDITION_*). Exasol issues a separate per-table pushdown and joins the result sets itself — this works immediately with no engine change. DataFusion JOIN pushdown is a separate future plan, explicitly out of scope. The user reviewed the Exasol capability list and confirmed Exasol-side joins are the target.

**Q:** Which tables in the namespace should the VS expose — all, or a configurable subset?
**A:** Expose ALL tables in the namespace. No filter/allowlist property. The user controls scope by choosing a narrow namespace.

**Q:** Are multi-level Iceberg namespaces supported, and how are they named in Exasol?
**A:** Yes. Multi-level namespaces are flattened with `__` (e.g. iceberg `prod.finance.orders` → Exasol `PROD__FINANCE__ORDERS`). `__` collision risk is accepted as a known limitation.

**Q:** How should existing `TABLE_NAME`-based virtual schemas migrate?
**A:** Hard break. Drop `TABLE_NAME` support entirely, replace with `ICEBERG_NAMESPACE`. Benchmarking/PoC context; existing VS instances must be recreated. No backward-compat shim.

**Q:** The user mentioned "tables (and views)". Are Iceberg views in scope?
**A:** Tables are the requirement. Treat views as nice-to-have; if iceberg-rust's catalog trait doesn't cheaply list views, defer to a follow-up and say so. (Resolved: iceberg-rust 0.9.1 `Catalog` trait exposes no `list_views`; views deferred.)

## Design Decisions

### [1] Confine the change to the VS-adapter layer; scan crate unchanged

- **Decision:** Keep `ScanSpec`, `CatalogProps`, the scan UDF, and the sharding/fan-out SQL unchanged. Table identity moves from a create-time-fixed property to a per-pushdown value derived from `involvedTables[0].name`.
- **Alternatives:** Carry multiple tables in a single `ScanSpec`/pushdown. Rejected — Exasol issues one single-table pushdown per table even for JOINs, so each pushdown is already single-table.
- **Rationale:** The verified Exasol VS protocol behaviour means the entire multi-table capability is a VS-adapter table-identity concern; widening the scan seam would be dead complexity.
- **Promotes to ADR:** yes

### [2] Persist an Exasol-name to Iceberg-identifier map in adapterNotes (strategy B)

- **Decision:** `createVirtualSchema` enumerates the namespace and records a `TABLE_MAP` (`EXASOL_NAME → original-cased dot-joined Iceberg identifier`) in `adapterNotes`; pushdown reads it back to recover the exact identifier from `involvedTables[0].name`.
- **Alternatives:** (A) Re-list the namespace at pushdown and match `involvedTables[0].name` case-insensitively against the same flatten function. Rejected — adds a catalog call per query and would require implementing signed `list_namespaces`/`list_tables` for the SigV4/Glue path (only `load_table` is signed today); it also makes casing recovery heuristic rather than exact.
- **Rationale:** Create-time enumeration is required regardless (to build `schemaMetadata.tables`), so recording the map is nearly free; `adapterNotes` is the proven persisted round-trip channel; recovery of original casing and multi-level path is exact and collision-detectable.
- **Promotes to ADR:** yes

### [3] Hard break on TABLE_NAME → ICEBERG_NAMESPACE

- **Decision:** Remove `PROP_TABLE`/`TABLE_NAME` entirely; add `ICEBERG_NAMESPACE`. No compatibility shim.
- **Alternatives:** Accept either property during a transition. Rejected by the user (PoC/benchmark context).
- **Rationale:** Avoids dead branching for a context where instances are freely recreated.
- **Promotes to ADR:** no

### [4] `__` collision is a hard error at create time

- **Decision:** When two distinct Iceberg identifiers flatten to the same Exasol name, fail `createVirtualSchema` with an error naming the colliding Exasol table name.
- **Alternatives:** Silently overwrite, or auto-disambiguate with a suffix. Rejected — overwriting corrupts the map; auto-suffixing makes Exasol names unpredictable.
- **Rationale:** The `__` collision is an accepted known limitation, but it must fail loudly rather than produce a silently wrong virtual schema.
- **Promotes to ADR:** no

### [5] Defer Iceberg view support

- **Decision:** Expose tables only; do not list/map Iceberg views.
- **Alternatives:** Enumerate views alongside tables. Rejected for now — iceberg-rust 0.9.1 `Catalog` trait has no `list_views`.
- **Rationale:** Tables are the requirement; views would need extra catalog plumbing without a cheap trait method.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
