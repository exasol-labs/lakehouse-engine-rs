# Decision Log: add-join-pushdown-broadcast

Date: 2026-07-06

## Interview

Headless mode (`speq-plan-pr`): no live interview. Motivation and scope came from
`specs/backlog.md` BL-001 Phase 1 and the 2026-07-06 telemetry writeup in
`docs/performance.md`. The exchanges below are the assumptions the planner made in
lieu of a human, recorded so they can be re-litigated if wrong.

**Q:** How is the resolved small (dimension) side delivered to each scan invocation —
materialized rows embedded in the spec, or a file-list reference?
**A (assumed):** By file-list reference. The dimension side's full file list is carried
once in the shard-invariant common spec; every shard re-scans it from object storage in
its own DataFusion session. Keeps the VS thin and all execution in the UDF (repo CLAUDE.md
architecture boundaries), avoids a large VARCHAR blob repeated to every shard, and reuses
the existing `register_files` path.

**Q:** What happens to an inner equi-join that is pushed to the adapter but is not
broadcast-eligible (small side over threshold, or shape outside the contract)?
**A (assumed):** The adapter emits deterministic unaccelerated SQL — each table scanned
through its own sharded scan-UDF fan-out subquery, joined by Exasol's core engine. An
error (native retry) is only the last resort, when even that SQL cannot be built. This
avoids regressing currently-working join queries if Exasol does not cleanly re-plan on an
adapter error.

**Q:** Does the join condition need a table-qualified column renderer in `vs-expression`?
**A (assumed):** No. Reuse the translator unchanged (bare column names) and add a
disjoint-column-name eligibility guard; if the two tables share any column name the join is
declined to the unaccelerated path. Matches the user intent to reuse the translator as
filters do, and the TPC-H target tables have disjoint column prefixes.

**Q:** How is the broadcast threshold expressed and defaulted?
**A (assumed):** As a `JOIN_BROADCAST_MAX_BYTES` adapter note (bytes), default 134217728
(128 MiB), compared against each side's Iceberg-manifest `file_size_in_bytes` sum with no
Parquet read. Follows the existing `adapterNotes` configuration pattern.

## Design Decisions

### [1] Reference the dimension side by file list rather than materializing its rows

- **Decision:** Carry the dimension side's full file list (plus table root and logical
  schema) in the shard-invariant common spec; each UDF re-scans it and joins node-locally.
- **Alternatives:** Materialize the dimension rows in the VS to Arrow IPC, base64-embed
  them in the common spec, and register a MemTable in the UDF.
- **Rationale:** File-list reference keeps the VS thin (only file-list resolution, no data
  execution in the planner), avoids a large VARCHAR blob repeated to every shard invocation,
  reuses `register_files`, and the bounded small side makes per-shard re-scans cheap.
- **Promotes to ADR:** yes

### [2] Ineligible joins take a deterministic unaccelerated two-scan SQL, not an error

- **Decision:** For any inner equi-join the adapter cannot broadcast, emit SQL that scans
  each table through its own sharded scan-UDF fan-out subquery and lets Exasol's core engine
  join the two results. Error (native retry) is the last resort only.
- **Alternatives:** Always decline ineligible joins with an error and rely on Exasol
  re-planning without the join capability.
- **Rationale:** Capabilities are advertised once and statically, so once `JOIN` is
  advertised Exasol pushes every inner equi-join. A hard error would risk failing
  currently-working join queries if Exasol does not cleanly retry natively. The two-scan SQL
  reproduces today's behavior deterministically, keeping correctness inside adapter control.
- **Promotes to ADR:** yes

### [3] Reuse the vs-expression translator unchanged + disjoint-column-name guard

- **Decision:** Render the join condition, cross-table projection, and WHERE filter with the
  existing translator (bare column names); guard broadcast eligibility on the two tables
  having disjoint column-name sets.
- **Alternatives:** Extend the translator with a table-qualified column renderer that maps
  each Exasol table name to its DataFusion table alias.
- **Rationale:** Zero translator churn; DataFusion resolves bare names unambiguously when
  column sets are disjoint (true for the TPC-H star-schema target). A qualified renderer is
  a Phase-2/general-join concern, not needed for the broadcast star-schema case.
- **Promotes to ADR:** no

### [4] Broadcast eligibility from Iceberg metadata bytes against a configurable threshold

- **Decision:** Compute each side's byte size from manifest `file_size_in_bytes` (no data
  read), pick the smaller side, and broadcast only when it is at or below
  `JOIN_BROADCAST_MAX_BYTES` (adapter note, default 128 MiB).
- **Alternatives:** Row-count threshold; reading actual small-side rows to measure size.
- **Rationale:** File byte size is directly available in metadata and resolved once with the
  file list; bytes bound the DataFusion hash-join build-side memory the guard protects.
- **Promotes to ADR:** no

### [5] Shard only the fact side; large-side sharding model is unchanged

- **Decision:** The larger side keeps the existing G work-unit `GROUP BY shard_key`
  file-sharding; only the small side's delivery and the in-UDF join are new.
- **Alternatives:** Re-partition either side by join key (Phase 2 shuffle).
- **Rationale:** Broadcast is correct with no cross-shard exchange because the full small
  side is available to every shard. Phase 2 is explicitly out of scope (BL-001).
- **Promotes to ADR:** yes

## Post-implementation fix (2026-07-07): qualified two-scan fallback

E2E testing against the live Exasol Docker stack surfaced two correctness
regressions in the first-cut two-scan fallback. Both are fixed in
`crates/lakehouse-engine/src/adapter/pushdown.rs` (plus a small `vs-expression`
addition). The original design decisions [1]–[5] stand; this refines the *rendering*
and *routing* of the fallback that decision [2] promised.

### [6] The two-scan fallback renders TABLE-QUALIFIED columns, independent of the disjoint-column guard

- **Symptom.** `SELECT a.id, b.label FROM EVENTS a JOIN LABELS b ON a.id = b.id`
  (both tables have an `id` column) hard-failed. Once `JOIN` was advertised Exasol
  pushed the whole join; the disjoint-column guard (decision [3]) correctly rejected
  bare-name rendering; `render_broadcast_join` returned `None`; and `plan_eligible_join`
  then returned a `User` error instead of falling back. Exasol does NOT re-plan on
  that error — it surfaced as a hard SQL failure (state 22002), regressing a
  previously-working query.
- **Root cause.** The disjoint-column guard exists ONLY because the BROADCAST path
  renders bare (unqualified) column names against a COMBINED in-UDF DataFusion
  schema, which is ambiguous on a name collision. The UNACCELERATED two-scan path
  has no such constraint — it is Exasol's own engine joining two already-materialized
  sub-results (`… FROM (fan-out) AS "LHS_FACT" INNER JOIN (fan-out) AS "LHS_DIM" ON …`),
  which resolves table-qualified references natively even when both sides share a
  column name. The first cut wrongly reused the guard-gated bare-name rendering for
  the two-scan path.
- **Decision.** The two-scan wrapper renders its OWN join condition, WHERE filter,
  select list, GROUP BY, HAVING, and ORDER BY with table-QUALIFIED references
  (`"LHS_FACT"."COL"` / `"LHS_DIM"."COL"`), resolved from each `column` node's
  `tableName` against the side that owns it — never against a combined bare-name
  schema, and NOT gated on the disjoint guard. Implemented by annotating each
  `column` node with a `tableAlias` (`annotate_columns_with_alias`) and teaching the
  shared `vs-expression` translator to emit `"ALIAS"."NAME"` when `tableAlias` is
  present (bare name otherwise — the single-table path is byte-for-byte unchanged).
  A disjoint-guard failure is therefore a plain reason the broadcast path is
  unavailable, NOT an error. A hard `Err` (native retry) is now reserved for the
  genuine last resort: a condition (or pushed select/GROUP BY/HAVING/ORDER BY
  element) that cannot be rendered against EITHER a bare or a qualified schema.
- **Promotes to ADR:** yes (supersedes the rendering half of [3] for the two-scan
  path; the broadcast path keeps bare-name rendering + the guard unchanged).

### [7] Aggregate-over-join routes through the two-scan path (Exasol aggregates the join), not a decline

- **Symptom.** `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM CUSTOMER JOIN ORDERS ON …`
  failed with "Expected number of columns is 2 but pushdown query has 5" — the
  fallback ignored the aggregate select list and emitted the full cross-table row
  projection.
- **Decision.** Any request that carries an aggregate, GROUP BY, ORDER BY, LIMIT, or
  HAVING (`join_requires_exasol_postprocessing`) is routed to the two-scan path
  regardless of broadcast eligibility, because none of those can ride the broadcast
  in-UDF join (it renders only projection + filter + join condition). The two-scan
  wrapper renders the aggregate select list as ordinary Exasol SQL over the
  materialized join (`SELECT <aggregates> FROM (fact fan-out) JOIN (dim fan-out) ON …
  [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`), so Exasol evaluates the
  aggregate over the joined-and-materialized rows — exactly what happened before any
  `JOIN` capability was advertised (two independent scans, Exasol joins AND
  aggregates). The aggregate function name is Exasol's own (it pushed it), so it is
  spliced verbatim; only its column argument is table-qualified. This is preferred
  over declining aggregate-over-join because a decline is a hard error Exasol does
  not cleanly re-plan (same reasoning as decision [2]).
- **Promotes to ADR:** yes.

## Review Findings

<!-- Populated by speq-implement after code review. -->
