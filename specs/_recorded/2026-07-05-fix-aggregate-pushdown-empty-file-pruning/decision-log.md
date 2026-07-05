# Decision Log: fix-aggregate-pushdown-empty-file-pruning

Date: 2026-07-04

## Interview

Planned in headless mode (`speq-plan-pr` path) — no live interview. The
orchestrator supplied context that stands in for interview answers; the
decisions below record the conventional calls made from that context.

**Q:** Where in the code does the bug live, and does the shape-detection run before or after the zero-files short-circuit?
**A:** `crates/lakehouse-engine/src/adapter/pushdown.rs::handle_pushdown`. `resolve_file_list` + the `files.is_empty()` early return (line ~2085) run BEFORE `detect_group_by_aggregates` (~2127) and single-group `detect_aggregates` (~2245). So at the short-circuit the code does not yet know the plan shape; that is the root cause.

**Q:** Is there an existing empty-aggregate semantics precedent to reuse?
**A:** Yes — ADR-008 / the zero-count NULL guard (`specs/decision-log.md`, and the merge SQL's `NULLIF(SUM(cnt),0)` / stat CASE guards). The convention is COUNT family → 0, SUM/AVG/MIN/MAX/STDDEV/VARIANCE → NULL over zero rows. The empty builder extends this rather than inventing new semantics.

**Q:** Where should the spec delta live?
**A:** `speq search` surfaced nothing on-point for "empty pushdown"/"empty aggregate", and the behavior cuts across row-scan, single-group, grouped, and COUNT(DISTINCT). A new feature `vs-adapter/pushdown-planning-empty-result` was chosen as the single coherent home.

## Design Decisions

### [1] Shape-aware zero-files short-circuit via a hoisted plan decision

- **Decision:** Move the request-shape decision ahead of the `files.is_empty()` short-circuit and dispatch to three empty-result builders (grouped zero-row / single-group one-row / row-scan projection), reusing the existing `detect_*` and type helpers so the empty shape is derived from the same sources as the non-empty shape.
- **Alternatives:** (a) Pass an "is-aggregate" flag into `empty_pushdown_sql` only — rejected, still would not carry grouped shape or per-`AggKind` semantics. (b) Let the empty case fall through the normal fan-out with zero shards — rejected: a single-group `COUNT` merges as `SUM(PARTIAL_count)` over zero fan-out rows = `NULL`, not `0` (wrong), and a grouped fan-out over zero shards is malformed.
- **Rationale:** Detection is pure over `pushdown_req` and independent of the resolved files, so the decision can be hoisted with no new I/O; deriving the shape from shared helpers guarantees empty/non-empty parity and keeps the VS thin (no execution change).
- **Promotes to ADR:** yes

### [2] Per-`AggKind` empty-literal semantics for the single-group case

- **Decision:** Single-group empty result is exactly one row; each output column is `CAST(<literal> AS <declared-type>)` where the literal is `0` for `Count`/`CountCol`/`CountDistinct` and `NULL` for `Sum`/`Min`/`Max`/`Avg`/`VarPop`/`VarSamp`/`StddevPop`/`StddevSamp`. Declared types come from `aggregate_exasol_types` (`selectListDataTypes`).
- **Alternatives:** Emit `NULL` for all kinds — rejected: `COUNT` over zero rows is `0` in single-node SQL; returning `NULL` would be a correctness regression.
- **Rationale:** Matches single-node SQL over zero input rows and the existing zero-count guard (ADR-008).
- **Promotes to ADR:** no

### [3] Grouped empty = zero rows in grouped shape; skip HAVING/ORDER-BY/validate decline paths

- **Decision:** Whenever `detect_group_by_aggregates` succeeds, the grouped empty result is zero rows (`FROM DUAL WHERE 1=0`) with the full grouped output shape (group-key + merged-aggregate + constant columns in select-list order). The grouped empty builder does NOT re-run the non-empty path's numeric-type validation, HAVING rendering, or ORDER BY resolution, and never returns `Err` for native retry.
- **Alternatives:** Mirror the non-empty path's decline branches exactly (Err → native retry) for non-numeric aggregates / unrenderable HAVING / unresolvable ORDER BY.
- **Rationale:** A zero-row result already satisfies any HAVING/ORDER BY/LIMIT and always matches `selectListDataTypes` positionally, so the decline branches would add code and risk for no user-visible difference (native retry over zero files also yields empty). The empty path is a strict simplification here.
- **Promotes to ADR:** no

### [4] Single-group path retains the `validate_agg_col_types` gate

- **Decision:** The single-group empty shape is chosen only when `detect_aggregates(...).filter(validate_agg_col_types)` is `Some`; a non-numeric single-group aggregate falls through to the row-scan empty shape.
- **Alternatives:** Always emit the single-group aggregate shape when `detect_aggregates` succeeds.
- **Rationale:** The non-empty path demotes a non-numeric single-group aggregate to a row scan, so `selectListDataTypes` reflects the row-scan shape; emitting an aggregate shape there would reintroduce the exact column mismatch this plan fixes. (Asymmetric with decision [3] because a grouped query's `selectListDataTypes` is always the grouped shape, whereas a demoted single-group query's is the row-scan shape.)
- **Promotes to ADR:** no

### [5] New feature home rather than scattered scenarios

- **Decision:** Author the behavior as a new feature `vs-adapter/pushdown-planning-empty-result` covering all plan shapes, instead of adding near-duplicate scenarios to `pushdown-planning`, `-grouped-agg`, and `-count-distinct`.
- **Alternatives:** Add a CHANGED scenario to each of the three existing features.
- **Rationale:** The empty-result behavior is one cross-cutting rule; a single home avoids drift between duplicated scenarios.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
