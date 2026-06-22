# Decision Log: add-capability-alignment

Date: 2026-06-22

## Interview

**Q:** What is the scope — fill the predicate gap, or a full systematic alignment?
**A:** Full systematic alignment: audit every Exasol capability category against what DataFusion 54 can execute. Add every capability DataFusion supports that we don't advertise (and add the matching `vs-expression` translation), and remove every capability we advertise incorrectly.

**Q:** What is the test requirement for newly added capabilities?
**A:** One E2E test per newly added capability *group* — not per individual `FN_` variant. E.g. one test exercising string-function pushdown, one for math, one for date, etc.

**Q:** What is explicitly out of scope?
**A:** ORDER_BY (distributed shard-merge ordering semantics unclear), JOIN/multi-table, geospatial (`FN_ST_*`), Exasol-specific session functions (`FN_CURRENT_USER`, `FN_SYS_GUID`, etc.), and `LITERAL_INTERVAL` (DataFusion interval support is partial).

**Q:** Do `FN_PRED_GREATER` / `FN_PRED_GREATEREQUAL` exist in Exasol's capability list?
**A (verified during planning):** No. Fetched `virtual-schema-common-java/doc/development/api/capabilities_list.md`; the predicate list contains `FN_PRED_LESS` and `FN_PRED_LESSEQUAL` but no greater-than variants. Exasol normalises `a > b` to `b < a` before the adapter sees it. They are therefore removed from `CAPABILITIES` (the translator's `predicate_greater(equal)` arms stay as defensive no-ops).

## Design Decisions

### [1] A capability is advertised only if the engine can back it correctly

- **Decision:** Establish the invariant that every name in `CAPABILITIES` must round-trip — either the `vs-expression` translator emits a correct DataFusion fragment, or the aggregate planner emits a correct shard-associative partial/merge plan. Audit drives both removals and additions from this rule.
- **Alternatives:** Maximise the advertised set for performance and rely on Exasol ignoring names we mishandle. Rejected — over-advertising a function we translate wrongly is a silent correctness bug.
- **Rationale:** Correctness is a first-class mission requirement; under-advertising only costs performance, over-advertising costs correctness.
- **Promotes to ADR:** yes

### [2] Remove FN_PRED_GREATER / FN_PRED_GREATEREQUAL from CAPABILITIES

- **Decision:** Delete both names from `CAPABILITIES`; keep the `predicate_greater`/`predicate_greaterequal` arms in `vs-expression` as defensive no-ops.
- **Alternatives:** Leave them (Exasol ignores unknown names). Rejected as misleading dead capability that a future reviewer would re-litigate.
- **Rationale:** The names are not in Exasol's vocabulary; Exasol normalises `>`/`>=` to `<`/`<=` before pushdown.
- **Promotes to ADR:** yes

### [3] STDDEV / VARIANCE pushdown via (count, sum, sum_sq) sufficient statistics

- **Decision:** Advertise and decompose the STDDEV/VARIANCE family by emitting `(COUNT(col), SUM(col), SUM(col*col))` per shard/group and reconstructing variance/stddev in the outer wrapper (population divisor `n`, sample divisor `n-1`, NULL/zero-count guard).
- **Alternatives:** Compute per-shard stddev and average them (mathematically wrong); or skip statistical aggregates entirely (leaves easy DataFusion-supported wins on the table).
- **Rationale:** Per-shard stddev is not shard-associative, but the three sufficient statistics are exactly associative across shards and reconstruct the exact result within float tolerance.
- **Promotes to ADR:** yes

### [4] Exclude non-decomposable aggregates

- **Decision:** Do NOT advertise `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT`, any `*_DISTINCT` form, `LISTAGG`, or `GROUP_CONCAT`; route them to the existing row-scan fallback.
- **Alternatives:** Advertise them too. Rejected — none decompose into shard-associative partials, so a partial/merge plan would yield wrong results.
- **Rationale:** Same correctness invariant as [1]; these have no exact merge.
- **Promotes to ADR:** no

### [5] HAVING applied in the outer wrapper only

- **Decision:** Render the HAVING predicate via the shared translator and apply it only in the outer merge wrapper SQL, never in the per-shard partial scan.
- **Alternatives:** Apply HAVING per shard. Rejected — a per-shard HAVING discards groups that only clear the threshold after cross-shard merge, producing wrong results.
- **Rationale:** HAVING is logically evaluated after grouping is complete; with sharded partials, "complete" means post-merge.
- **Promotes to ADR:** yes

### [6] Date/time functions as a separate feature

- **Decision:** Author date/time translation as a new `sql-comprehension/vs-expression-translator-date-fns` feature rather than folding it into scalar-ops.
- **Alternatives:** One large scalar-ops spec. Rejected — date semantics (EXTRACT field handling, session-free now-family, unsupported fall-through) are a distinct concern and keeping specs bounded aids review.
- **Rationale:** Spec manageability and clear feature boundaries.
- **Promotes to ADR:** no

### [7] Name-mapping aliases for non-1:1 DataFusion functions

- **Decision:** Most Exasol `FN_*` names lower-case directly to the DataFusion function; a documented set needs explicit aliasing: `SIGN`→`signum`, `LENGTH`→`character_length`, `MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos` (operand reorder), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`.
- **Alternatives:** Assume all names map 1:1. Rejected — verified against DataFusion 54 docs that several do not, and `mod()` does not exist (only `%`).
- **Rationale:** Verified against DataFusion 54 scalar-function documentation during Phase 0 research.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
