# Plan: fix-multi-column-group-by-pushdown

## Summary

Advertise Exasol's `AGGREGATE_GROUP_BY_TUPLE` capability so a GROUP BY over two or more
keys is pushed down to the scan UDF as node-local partial aggregation instead of falling
back to a raw row scan that Exasol aggregates itself, and prove the N-key detection /
SQL-building / result-type path is correct end-to-end (closes exasol-labs/lakehouse-engine-rs#53).

## Design

Small-to-moderate fix. The underlying detection (`detect_group_by_aggregates`), type
resolution (`group_key_exasol_types`), and scan-driving SQL builder
(`build_grouped_aggregate_scan_sql`) already iterate the full group-key list with no
cap of one, so the primary change is a capability advertisement plus verification and
test coverage. The one genuine engineering risk is that the N≥2 path has never been
exercised through the real pushdown path (Exasol never sent a multi-key pushdown request
because the capability was absent), so the plan budgets for finding and fixing real
bugs, not just flipping a flag.

### Context

- **Goals** — Multi-column GROUP BY (extremely common; e.g. `GROUP BY L_SHIPYEAR, L_RETURNFLAG`)
  pushes down and reduces network transfer via partial aggregation. Expression-valued tuple
  keys, mixed-type keys, interleaved ordering, HAVING+LIMIT, and high-cardinality multi-key
  spill are all proven correct. Existing multi-key E2E tests gain a pushdown-occurred
  assertion so they cannot silently exercise the wrong code path.
- **Non-Goals** — No new pushdown shapes beyond multi-key GROUP BY. No join pushdown, no
  `COUNT(DISTINCT)`, no changes to the single-group aggregate path. No new memory/spill
  mechanism (the existing bounded pool + `/tmp` spill backstop applies unchanged).

### Decision

Advertise `AGGREGATE_GROUP_BY_TUPLE`, reverse the prior explicit scoping decision that
excluded it (recorded 2026-06-22), and back the advertisement with unit + E2E coverage
of the N-key path. The three capability tests that asserted its absence are re-purposed
to assert its presence AND to protect the invariant that the capability is only
advertised because the multi-key detection/SQL path exists (cross-referenced to the
detection unit tests), rather than being blindly inverted.

#### Architecture

```
getCapabilities → CAPABILITIES incl. AGGREGATE_GROUP_BY_TUPLE
        │
        ▼
Exasol sends GROUP BY k1, k2, … pushdown  (previously: never sent → raw-scan fallback)
        │
        ▼
detect_group_by_aggregates  → group_keys[0..n], classified select items (already N-key)
        │
        ▼
group_key_exasol_types      → per-key declared type by select_index (already N-key)
        │
        ▼
build_grouped_aggregate_scan_sql → GK_0..GK_{n-1}, GROUP BY shard_key fan-out, outer merge
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Capability-backed-by-implementation | capability tests | Advertise a flag only when the code path that serves it exists and is tested |
| Verify-then-flag (spike first) | N-key path verification | The N≥2 path was never exercised via real pushdown; find real bugs before trusting the flag |
| Pushdown-occurred assertion via `EXPLAIN VIRTUAL` | multi-key E2E tests | Prevent a correctness-only test from silently passing on the raw-scan fallback |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Advertise `AGGREGATE_GROUP_BY_TUPLE` | Keep excluded (status quo) | Multi-column GROUP BY is extremely common; fallback defeats the network-transfer reduction grouped pushdown exists for (issue #53) |
| Spike/verify N-key path before trusting flag | Treat as one-line flag flip | Issue #53 explicitly warns the path is unverified end-to-end; a flag flip alone risks shipping latent N-key bugs |
| Re-purpose (not blindly invert) the 3 capability tests | Flip `!contains` → `contains` only | The tests should now protect a meaningful invariant (capability present AND backed by a working multi-key path), per issue #53 |
| Add EXPLAIN-based pushdown assertions to existing multi-key E2E | Add new correctness-only tests only | Existing multi-key tests passed via the raw-scan fallback and never proved pushdown; correctness alone gives no evidence of the code path |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| packaging/e2e-harness-grouped-order | CHANGED | `packaging/e2e-harness-grouped-order/spec.md` |
| datafusion-scan/scan-execution-grouped-agg | CHANGED | `datafusion-scan/scan-execution-grouped-agg/spec.md` |

## Dependencies

None beyond the existing crate. The Exasol Docker + MinIO + Iceberg REST stack is
required for the E2E tasks (per project rules, E2E tests MUST fail — not skip — if the
stack is unavailable).

## Implementation Tasks

1. Capability advertisement (`crates/lakehouse-engine/src/adapter/capabilities.rs`)
   - [ ] 1.1 Add `"AGGREGATE_GROUP_BY_TUPLE"` to the `CAPABILITIES` const array (near the other GROUP BY entries, ~line 132-134); update the adjacent comment
   - [ ] 1.2 Re-purpose the three capability tests that assert TUPLE absence (`reports_group_by_capabilities` ~L151, `reports_audited_capability_set` ~L184, `reports_supported_aggregate_capabilities` ~L391, plus the doc comment at ~L386-390) to assert TUPLE presence and protect the reconsidered invariant (present AND backed by the multi-key path) [expert]

2. Verify and harden the N-key pushdown path (`crates/lakehouse-engine/src/adapter/pushdown.rs`)
   - [ ] 2.1 Spike/verify the N≥2 group-key path end-to-end across `detect_group_by_aggregates` (~L762), `group_key_exasol_types` (~L836), and `build_grouped_aggregate_scan_sql` (~L1149); fix any real bugs in GK_n emission, per-key type resolution, outer-wrapper ordering, or HAVING/LIMIT interaction found [expert]
   - [ ] 2.2 Add unit test: all-expression multi-key tuple (each element an expression, e.g. `MOD(id,4), UPPER(name)`) is detected and rendered per element, and one untranslatable element forces full fallback
   - [ ] 2.3 Add unit test: mixed-type multi-key result-type mapping resolves each `GK_{i}` type by its own select-list index (DECIMAL key + VARCHAR key), not a shared/defaulted VARCHAR
   - [ ] 2.4 Add unit test: multi-key grouped SQL build with HAVING and LIMIT places HAVING and LIMIT only in the outer wrapper (never in the per-shard partial scan)

3. E2E coverage (`crates/lakehouse-engine/tests/e2e_scan_test.rs`)
   - [ ] 3.1 Add an `EXPLAIN VIRTUAL` pushdown-occurred assertion (contains `GROUP BY shard_key`, no `IPROC()`, not raw-scan fallback) to `test_group_by_interleaved_multi_key` (~L1428)
   - [ ] 3.2 Add the same pushdown-occurred assertion to `test_group_by_multi_key_with_filter` (~L1069)
   - [ ] 3.3 Add E2E test: expression-valued multi-key tuple GROUP BY (each element an expression) returns correct results with correct per-key declared types and is EXPLAIN-verified as pushed down
   - [ ] 3.4 Add E2E test: multi-key GROUP BY with HAVING and LIMIT returns correct results (HAVING + LIMIT applied in outer wrapper)
   - [ ] 3.5 Add E2E test: high-cardinality multi-key GROUP BY completes under the bounded memory pool (multi-key key space, e.g. `GROUP BY id, MOD(id,2)`)

4. Documentation and decision record
   - [ ] 4.1 Update `docs/capabilities.md`: move `AGGREGATE_GROUP_BY_TUPLE` from the "Exasol-side / unsupported" row (~L71) into the supported "Group by" row (~L56)

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 4.1 |
| Group B | 2.2, 2.3, 2.4 |
| Group C | 3.3, 3.4, 3.5 |

Sequential dependencies:
- 2.1 (verify/fix N-key path) → Group B and Group C (tests assert the corrected behavior)
- 1.1 → 1.2 (tests assert the flag added in 1.1)
- 1.1 → 3.1, 3.2 (pushdown only occurs once the capability is advertised)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| N/A | — | No code becomes obsolete; the three capability tests are re-purposed (changed), not deleted, and the N-key detection/SQL paths are retained and hardened |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Adapter advertises aggregate pushdown for supported functions (incl. TUPLE) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_group_by_capabilities`, `reports_audited_capability_set`, `reports_supported_aggregate_capabilities` |
| Multi-column GROUP BY is pushed down as partial aggregation rather than a raw row scan | Unit + Integration | `crates/lakehouse-engine/src/adapter/pushdown.rs`; `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `detect_group_by_aggregates_interleaved_multi_key_preserves_order` (existing, detection); `test_group_by_multi_key_with_filter` (EXPLAIN-verified) |
| Every element of a multi-key tuple may be an expression | Unit + Integration | `crates/lakehouse-engine/src/adapter/pushdown.rs`; `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `detect_group_by_all_expression_multi_key` (new, task 2.2); `test_group_by_expr_multi_key_tuple` (new, task 3.3) |
| Each group key in a multi-key tuple resolves its own declared result type | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `group_key_types_multi_key_mixed_types` (new, task 2.3) |
| End-to-end interleaved multi-key GROUP BY with an aggregate between the keys returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_interleaved_multi_key` (EXPLAIN assertion added, task 3.1) |
| End-to-end multi-column GROUP BY over plain columns is pushed down (EXPLAIN-verified) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_multi_key_with_filter` (task 3.2) |
| End-to-end expression-valued multi-key tuple GROUP BY returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_expr_multi_key_tuple` (new, task 3.3) |
| End-to-end multi-key GROUP BY with HAVING and LIMIT returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_multi_key_having_limit` (new, task 3.4) |
| High-cardinality multi-key grouped scan completes under the bounded memory pool | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_high_cardinality_multi_key_group_by_spill` (new, task 3.5) |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning | `EXPLAIN VIRTUAL SELECT MOD(id,4), MOD(id,2), COUNT(*) FROM <vs>.<table> GROUP BY MOD(id,4), MOD(id,2);` | Pushed scan spec carries `group_keys`, no `IPROC()`, not a raw `SELECT *` fallback (`GROUP BY shard_key` appears only when >1 shard) |
| vs-adapter/pushdown-planning-grouped-agg | `SELECT MOD(id,4), UPPER(name), COUNT(*) FROM <vs>.<table> GROUP BY MOD(id,4), UPPER(name);` | Succeeds with no "Data type mismatch in column number N"; MOD key typed DECIMAL and UPPER key typed VARCHAR (each by its own select index), correct per-group counts. NOTE: `CAST`/arithmetic group keys are NOT pushed (unadvertised, future scope) — use advertised scalar fns |
| packaging/e2e-harness-grouped-order | `SELECT MOD(id,4), MOD(id,2), SUM(score) FROM <vs>.<table> GROUP BY MOD(id,4), MOD(id,2) HAVING SUM(score) > 100 LIMIT 2;` | Succeeds; at most 2 groups, all with SUM(score) > 100 |
| datafusion-scan/scan-execution-grouped-agg | `SELECT id, MOD(id,2), COUNT(*) FROM <vs>.<table> GROUP BY id, MOD(id,2);` | Completes (spills rather than OOM) with one row per distinct (id, MOD(id,2)) group |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, if Docker/MinIO down) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
