# Decision Log: fix-multi-column-group-by-pushdown

Date: 2026-07-03

## Interview

**Q:** How wide should this fix's scope be, given the issue explicitly warns against treating it as a one-line capability flip?
**A:** Full scope — flip `AGGREGATE_GROUP_BY_TUPLE` on AND add test coverage for every open question the issue raises: expression-valued group keys (`MOD(id,4)`, casts, function calls — not just column refs), interleaved ordering with 2+ keys, HAVING + LIMIT combined with multi-key grouping, high-cardinality/spill behavior of the node-local partial aggregate, plus re-purposing the 3 existing capability tests that currently assert `AGGREGATE_GROUP_BY_TUPLE` is absent. Close the issue fully, not partially.

**Q:** Should the plan include end-to-end verification against a live cluster (never done for the multi-key pushdown path)?
**A:** Yes — add E2E test(s). Per project rules, E2E tests run against a real Exasol Docker container and MUST FAIL (not skip) if the DB/MinIO stack is unavailable.

**Q:** Is expression-valued tuple group keys (e.g. `GROUP BY MOD(id,4), UPPER(name)`) in scope?
**A:** In scope. This needs explicit verification, not an assumption that it works because single-key expression support (`AGGREGATE_GROUP_BY_EXPRESSION`) already exists.

## Design Decisions

### [1] Advertise `AGGREGATE_GROUP_BY_TUPLE`, reversing the prior exclusion

- **Decision:** Add `AGGREGATE_GROUP_BY_TUPLE` to `CAPABILITIES` so Exasol sends multi-key GROUP BY as a pushdown request, reversing decision [4] in the 2026-06-22 `add-group-by-and-sql-comprehension` decision log ("Do NOT add `AGGREGATE_GROUP_BY_TUPLE` … beyond scope").
- **Alternatives:** Keep it excluded — rejected: multi-column GROUP BY is extremely common; the raw-scan fallback ships every raw row to Exasol and defeats the network-transfer reduction that grouped pushdown exists for (issue #53).
- **Rationale:** The detection, type-resolution, and SQL-building code already handle an arbitrary number of group keys; the capability was the only thing gating the multi-key path.
- **Promotes to ADR:** yes

### [2] Verify the N-key path before trusting the flag (spike-first)

- **Decision:** Treat the capability flip as needing a verification spike across `detect_group_by_aggregates`, `group_key_exasol_types`, and `build_grouped_aggregate_scan_sql`, budgeting for real bug fixes, rather than assuming a one-line flag change suffices.
- **Alternatives:** Ship the flag alone — rejected: issue #53 explicitly states the N≥2 path "has not been verified end-to-end" because Exasol never sent a multi-key pushdown request while the capability was absent.
- **Rationale:** A capability advertised without a proven path risks shipping latent multi-key defects (GK_n ordering, per-key type resolution, HAVING/LIMIT interaction).
- **Promotes to ADR:** yes

### [3] Re-purpose the three capability tests rather than blindly inverting them

- **Decision:** Change the three tests that assert TUPLE absence so they assert presence AND protect the reconsidered invariant — that the capability is advertised only because the multi-key detection/SQL path exists and is tested — instead of a mechanical `!contains` → `contains` flip.
- **Alternatives:** Blind inversion — rejected per issue #53 ("their intent would need to be reconsidered, not just flipped").
- **Rationale:** A bare presence assertion is a weak guard; tying the advertisement to the working path keeps the flag honest.
- **Promotes to ADR:** no

### [4] Add EXPLAIN-based pushdown-occurred assertions to existing multi-key E2E tests

- **Decision:** Add an `EXPLAIN VIRTUAL` assertion (contains `GROUP BY shard_key`, no `IPROC()`, not a raw-scan shape) to `test_group_by_interleaved_multi_key` and `test_group_by_multi_key_with_filter`, not just new correctness-only tests.
- **Alternatives:** Add new correctness-only tests only — rejected: the existing multi-key tests passed via the raw-scan fallback and never proved the pushdown path; once the flag flips they would silently switch code paths with zero evidence either way.
- **Rationale:** Correctness plus a pushdown-shape assertion is the only combination that proves the multi-key partial-aggregation path is actually exercised.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
