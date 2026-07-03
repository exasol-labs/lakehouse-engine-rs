# Decision Log: fix-nested-aggregate-pushdown

Date: 2026-07-03

## Interview

**Q:** Root-cause approach — repro-first spike then fix, or skip root-causing and add a defensive fallback for any unrecognized shape?
**A:** (No response within the wait window; recommended default taken.) Repro-first spike, then fix. The first task must reproduce the failing query against the local Exasol Docker + Iceberg/MinIO E2E harness and capture the actual `pushdownRequest` JSON Exasol sends for this exact nested-aggregate SQL shape, so the real request structure is known before any fix is written. Only after that capture should the plan decompose the actual code fix + regression test. The blanket-defensive-fallback alternative was rejected as the default because it risks masking the real defect without knowing whether the current fallback-detection logic is even the right layer to patch.

**Q:** Repro environment — is the local Exasol Docker + Iceberg/MinIO E2E harness sufficient, or is live-cluster (AWS Glue) re-verification required?
**A:** (No response within the wait window; recommended default taken.) Local Exasol Docker + Iceberg/MinIO E2E is sufficient. This is treated as generic Exasol optimizer/pushdown-composition behavior, not Glue/AWS-specific, so the project's standard local-Docker E2E convention (must FAIL, not skip, if unavailable) is the required and sufficient reproduction environment. No live-cluster re-verification is required by default — but this assumption is flagged in the plan so the user can override it during review if they still want live-cluster verification before closing the issue.

## Design Decisions

### [1] Issue #52's stated root-cause hypothesis does not match the code

- **Decision:** Discard issue #52's specific hypothesis ("the adapter substitutes a literal NULL where a field reference is expected when composing an outer COUNT(*) over an already-pushed-down inner GROUP BY") as the working theory, and treat the real defect mechanism as UNKNOWN pending the diagnostic spike.
- **Alternatives:** Accept the hypothesis and go straight to a fix in the aggregate-composition path (there is no such path).
- **Rationale:** An Explore pass found the hypothesis matches no code: the `COUNT(*)` arms (`pushdown.rs:920-941`, `scan/mod.rs:557-560`) render `COUNT(*)` with no column arg and no NULL placeholder; `emit_null_partial_row` (`scan/mod.rs:478-497`) emits `Value::Int64(0)` not `Value::Null`; the only genuine `"NULL"` SQL-literal producers (`vs-expression/src/lib.rs:44,122,137,148`; `empty_pushdown_sql` at `pushdown.rs:2543`) do not feed an aggregate argument; and the adapter has NO recursive nested-`from`/sub-select parsing at all (`adapter/mod.rs:454-465` reads flat `involvedTables[0].name`; `selectList`/`groupBy`/`filter` read only at top level). The error is a DataFusion SQL error raised inside the UDF's own session from `scan/mod.rs::build_scan_sql`, so the adapter builds a ScanSpec whose rendered SQL references a phantom `"NULL"` — but by what mechanism is unknown.
- **Promotes to ADR:** no

### [2] Spike-first task ordering; fix acceptance is behavioral, not a prescribed edit

- **Decision:** Front-load a bounded diagnostic spike (Task 1) that captures the real `pushdownRequest` JSON and generated SQL against the local Docker stack; its output determines the fix layer and fix family. The fix task's (Task 3) acceptance criteria are behavioral (correct result OR clean fallback to row-scan, never a planning-time crash), not a mandated implementation approach.
- **Alternatives:** Pre-commit to a specific edit such as "add sub-select walking to `adapter/mod.rs`"; or add a blanket catch-all fallback.
- **Rationale:** The correct code layer is unknown until the request shape is captured; prescribing an edit now risks patching the wrong layer. Behavioral acceptance lets the implementer apply whichever of the two fix families (correct-pushdown vs safe-fallback) the captured data warrants.
- **Promotes to ADR:** no

### [3] Fix scoped to this SQL shape, not general nested/subquery aggregate pushdown

- **Decision:** Bound the fix to making the Q7 nested-aggregate shape correct-or-safe (correct composed pushdown OR fall back to non-pushed row-scan). Do NOT build general multi-level nested-aggregate / subquery pushdown composition.
- **Alternatives:** Add subquery-pushdown composition as a new adapter capability.
- **Rationale:** `specs/mission.md` explicitly lists "Join pushdown, complex query rewrites" under Out of Scope. The goal is correct, safe behavior for this shape (Athena/Trino/Spark all handle it; the engine must at minimum not crash), not a new capability. Fail-closed to the existing row-scan fallback is lower-risk than growing pushdown surface area.
- **Promotes to ADR:** yes

### [4] Root-cause write-up (POPULATED BY THE SPIKE, 2026-07-03)

Captured empirically against the local Exasol Docker + MinIO + Iceberg REST E2E stack
(Exasol 2025.2.1) with temporary dump instrumentation in
`adapter::mod::handle_pushdown_request` (routed the raw VS request + generated SQL through
the query error channel; instrumentation has been removed, tree is clean). Query run over
the seeded `events` table: `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM
MY_LAKEHOUSE.EVENTS GROUP BY id) t`.

#### Captured `pushdownRequest` (the key finding)

```json
{
  "aggregationType": "group_by",
  "from": { "name": "EVENTS", "type": "table" },
  "groupBy": [ { "columnNr": 0, "name": "ID", "tableName": "EVENTS", "type": "column" } ],
  "selectList": [ { "type": "literal_null" } ],
  "selectListDataTypes": [ { "type": "BOOLEAN" } ],
  "type": "select"
}
```

`involvedTables[0].name` = `"EVENTS"`; `filter` = absent; `having` = absent.

**Exasol does NOT send a nested `from`/sub-select and does NOT compose the outer `COUNT(*)`
into the request.** It sends ONE flat, single-level `pushdownRequest`. The optimizer rewrote
`COUNT(*) FROM (SELECT id, COUNT(*) ... GROUP BY id)` into "count the number of distinct `id`
groups": it pushes the inner `GROUP BY id` down (`aggregationType: "group_by"`,
`groupBy:[ID]`), but because the outer needs only the group *count* — neither `id` nor the
inner `cnt` value — it replaces the entire inner `selectList` with a single **`literal_null`**
placeholder (declared type `BOOLEAN`). Exasol then computes the outer `COUNT(*)` itself over
the rows the VS returns. This kills issue #52's hypothesis (no nested-subquery parsing exists
or is needed) and prior entry [1]'s open question: the phantom `NULL` originates from a
`literal_null` **projection placeholder Exasol itself emits**, not from any adapter-side
subquery composition.

#### Generated scan-driving SQL (the defect made concrete)

```sql
SELECT * FROM (
  SELECT "LHVS".LAKEHOUSE_SCAN(
    '{"table_root":"s3://warehouse/e2e_lakehouse/events",
      "projection":["NULL"],                 <-- phantom column name
      "emit_exa_types":["BOOLEAN"], ... }',   <-- no "aggregates", no "group_keys": a ROW SCAN
    files
  ) EMITS ("NULL" BOOLEAN)                     <-- phantom EMITS identifier
  FROM (VALUES (0,'[[...parquet,2409]]'),(1,'[[...parquet,2409]]')) AS shards(shard_key, files)
  GROUP BY shard_key
)
```

Note the embedded `ScanSpec` is a plain **row-scan** spec (`aggregates` and `group_keys`
both absent) with `projection:["NULL"]`. At scan time `scan::build_scan_sql` renders it as
`SELECT "NULL" FROM (SELECT "id" AS "ID", ... FROM <table>)`; DataFusion treats `"NULL"` as a
case-sensitive quoted column identifier, finds no such field, and raises
`Schema error: No field named "NULL"` (surfaced as `F-UDF-CL-RUST-9001`).

#### Precise mechanism — the exact line(s) responsible

1. `detect_group_by_aggregates` (`adapter/pushdown.rs:762`) correctly **rejects** the request:
   `group_keys = [render_expression(column ID)] = ["ID"]` (quoted, `vs-expression/src/lib.rs:153-158`),
   then the single `literal_null` select item hits the non-aggregate arm
   (`pushdown.rs:803-816`): `render_expression(literal_null)` → bare `NULL`
   (`vs-expression/src/lib.rs:122`), `group_keys.iter().position(|gk| *gk == "NULL")` finds no
   match (`"ID"` ≠ `NULL`), so `?` returns `None`. So far so good.
2. `detect_aggregates` (`pushdown.rs:667`) also returns `None` — `groupBy` is present and
   non-empty (`pushdown.rs:669-675`).
3. Execution therefore falls to the **row-scan** branch, and `extract_projection`
   (`pushdown.rs:2376`) is the actual culprit: the `literal_null` select item matches the
   literal arm at **`pushdown.rs:2474`**, `render_expression_safe(e)` returns `Some("NULL")`,
   and the code does `names.push(sql_frag)` at **`pushdown.rs:2481`** — pushing the rendered
   *expression string* `NULL` into `proj_cols` **as if it were a column name**. A
   `literal_null` is translatable-as-an-expression but is NOT a projectable source column, so
   it never trips the `needs_full_fallback` guard.
4. `scan::build_scan_sql` (`scan/mod.rs:998-1013`) then wraps each projection entry with
   `quote_ident(&upper)`, turning `NULL` into the quoted column reference `"NULL"` — the phantom
   identifier DataFusion rejects.

- **Decision (recommended fix family): (a) correct-parsing, NOT (b) tighten-guard-to-fallback.**
  The correct patch point is the group-by detection/projection layer. When a `group_by`
  pushdown carries a `selectList` composed solely of constant/literal placeholders (no
  aggregate, no group-key projection — the "count the groups" rewrite), the adapter must still
  emit a **grouped** scan: `GROUP BY id` returning one row per distinct group (projecting the
  group key or a constant), so the returned row count equals the number of groups. Candidate
  edit: extend `detect_group_by_aggregates` (`pushdown.rs:762`, non-aggregate arm at
  `:803-816`) to accept a pure-literal select item as a "constant projection over the group
  keys" and drive the existing grouped-aggregate scan builder with an empty `plans` list; or,
  equivalently, special-case a literal-only `group_by` selectList to render
  `SELECT <group_key> FROM t GROUP BY <group_key>`. Secondarily harden `extract_projection`
  (`pushdown.rs:2474-2481`) so a rendered literal is never emitted as a bare projection column
  name on the row-scan path (route it through `needs_full_fallback` or drop it), as a
  defence-in-depth backstop.

- **Alternatives:**
  - **(b) Tighten the guard so the row-scan fallback engages** (make detection return `None`
    and let the plain row scan run). **REJECTED as unsafe.** The row-scan path returns one row
    **per source row**, not per group. On the seeded `events` table `id` is unique (20 rows =
    20 distinct groups) so this *coincidentally* yields the correct `COUNT(*)=20` — but on any
    duplicate-key table (e.g. TPC-H `LINEITEM.L_ORDERKEY`, the query in the issue) it returns
    the raw row count instead of the distinct-group count: silently **wrong results**. Exasol
    advertised the group-by capability and will not re-group the VS output, so grouping MUST be
    preserved by the VS.
  - **Return an error to force native retry** — a VS has no native data path, so an error just
    fails the query. Not acceptable for a query Athena/Trino/Spark all answer.

- **Rationale:** The empirically captured request is a well-formed, standard optimizer rewrite
  (constant-projection over a pushed-down `GROUP BY`), not a malformed or exotic shape. The
  defect is purely adapter-side: a translatable *expression* (`literal_null → "NULL"`) is
  mis-used as a projection *column identifier*. Correct handling preserves grouping semantics
  and is correct for all tables; the fallback family is a latent correctness bug that the
  `events`-based E2E test cannot detect.

- **Caveat for Task 5 (regression test):** because `events.id` is unique, an outer `COUNT(*)`
  over `GROUP BY id` returns 20 under BOTH a correct grouped fix and the broken raw-row
  fallback — so that exact assertion does NOT discriminate the correct fix from the unsafe one.
  The regression test should additionally cover a **duplicate-key** group column, e.g.
  `SELECT COUNT(*) FROM (SELECT MOD(id,4) k, COUNT(*) FROM EVENTS GROUP BY MOD(id,4)) t` which
  must return **4** (a raw-row fallback would return 20), to actually lock in grouping
  correctness.

- **Promotes to ADR:** yes

### [5] Local-Docker E2E is the required repro/verification environment

- **Decision:** Reproduce and verify entirely on the local Exasol Docker + Iceberg/MinIO E2E stack; no live-cluster re-verification step by default.
- **Alternatives:** Require AWS Glue live-cluster re-verification before closing #52.
- **Rationale:** Treated as generic Exasol pushdown-composition behavior, reproducible on local Docker; matches the repo's standard E2E convention (fail, not skip). Flagged in the plan for user override at review (interview Q2 default).
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
