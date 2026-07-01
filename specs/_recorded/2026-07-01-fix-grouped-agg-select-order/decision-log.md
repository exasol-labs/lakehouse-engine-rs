# Decision Log: fix-grouped-agg-select-order

Date: 2026-07-01

## Interview

**Q:** How broad should the fix be — a narrow patch for the reported "aggregate before one key" case, or a general fix?
**A:** Full positional fix. Thread each select-list item's original index through detection so the outer SELECT/GROUP BY/cast items assemble in `selectList` order for ANY interleaving. Do not ship a fix that only handles "aggregate precedes one key" and leaves interleaved multi-key or expression-key-after-aggregate broken.

**Q:** Should the select-list ↔ groupBy linkage remain string-match based?
**A:** No — also harden it to index-based matching in the same refactor. `group_key_exasol_types` finds a key's type by comparing rendered SQL strings, and the detection expression-key fallback uses string containment; both are fragile (whitespace/casing drift → silent `VARCHAR(2000000)` with no CAST, wrong type, no error). Reuse the per-item index the main fix already carries; do not add a second mechanism.

**Q:** What test coverage is required?
**A:** All four E2E scenarios (aggregate-before-key #33 repro, interleaved multi-key, expression-key-after-aggregate, aggregate-first+HAVING), against Exasol Docker + MinIO + Iceberg, failing (not skipping) if the stack is unavailable. Every existing GROUP BY E2E case puts keys first — that is exactly why the bug shipped, so new cases must not follow that pattern. Also add unit-level coverage in `pushdown.rs` for the same four orderings since it runs without Docker and catches regressions fastest.

**Q:** Do `ScanSpec` / `AggregatePlan` (the wire spec) or the scan UDF side need to change?
**A:** Belief is no (only the adapter's outer-SELECT assembly needs reordering), but verify independently before finalizing — do not take it on faith.

## Design Decisions

### [1] Full positional reorder threading select-list index through detection

- **Decision:** Extend `detect_group_by_aggregates` to carry each `selectList` item's original index and classification (group-key projection vs aggregate); `build_grouped_aggregate_scan_sql` places already-typed group-key cast and merged-aggregate fragments at their original ordinals in the outer SELECT / GROUP BY.
- **Alternatives:** Narrow patch handling only "aggregate before a single key" — rejected because #33's root cause is general column transposition; the narrow patch leaves interleaved multi-key and expression-key-after-aggregate broken.
- **Rationale:** One change fixes all three sub-cases and matches Exasol's positional `selectListDataTypes` check for any arrangement.
- **Promotes to ADR:** yes

### [2] Keep the wire spec (`ScanSpec` / `AggregatePlan`) and scan UDF side unchanged

- **Decision:** Do not touch `ScanSpec.group_keys` / `ScanSpec.aggregates`, the inner fan-out EMITS clause, `build_grouped_partial_agg_sql`, or the scan emit loop.
- **Alternatives:** Add ordering to the wire spec so keys/aggregates interleave end-to-end.
- **Rationale:** Independently verified in `crates/lakehouse-engine/src/scan/mod.rs` that the scan SELECT (`build_grouped_partial_agg_sql` L390-423), the emit loop (L344-368), and the fan-out EMITS (`gk_emits.chain(partial_items)`) are keys-first on ALL sides and matched only against each other — never against the user `selectList`. The bug lives solely in the adapter's outer-merge assembly. Changing the wire shape would be churn with no correctness benefit.
- **Promotes to ADR:** yes

### [3] Fold index-based type/classification lookup into the same refactor

- **Decision:** Replace `group_key_exasol_types`' rendered-string `position` lookup and the detection expression-key `group_keys.contains(&sql)` branch with index-based matching reusing the classification from decision [1].
- **Alternatives:** Ship it as a separate follow-up.
- **Rationale:** The refactor already carries per-item index + classification; reusing it removes the string-match fragility for free instead of adding a second mechanism. Prevents silent `VARCHAR(2000000)` fallback on whitespace/casing drift between `groupBy` and `selectList` renderings.
- **Promotes to ADR:** no

### [4] New E2E cases must not be keys-first

- **Decision:** All four new E2E scenarios place an aggregate before/between the keys; assert results against the already-correct key-first ordering of the same query.
- **Alternatives:** Reuse the existing keys-first E2E scaffolding as-is.
- **Rationale:** Every existing GROUP BY E2E case is keys-first, which is precisely how this bug shipped undetected. Non-keys-first cases are the only ones that exercise the defect.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
