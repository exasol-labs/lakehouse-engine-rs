# Decision Log: fix-join-fallback-self-join-attribution

## Interview

**Q:** Research found the table-name-keyed attribution flaw at three call sites sharing one root cause — qualified column rendering (the reported cross product), side-local WHERE push-down into each leg (a silent over-filtering bug, not what #361 reports), and the N-way FROM-chain's condition attachment (inert at N = 2, wrong at N ≥ 3 with a repeated table). Fix only the reported symptom, or all three?

**A:** Fix all three call sites. Do not file the second and third as separate follow-up issues — fix them in this plan, under issue #361, since they are the same root-cause defect.

**Q:** The unified fallback is documented as N ≥ 2-general. Include explicit three-way self-join test coverage (same table three times), or defer it to a smaller follow-up?

**A:** Include it in this plan. If the fix design is correct it should work for free — a test proves that rather than assuming it.

## Design Decisions

### [1] Resolve legs from the FROM-tree leaf `alias`, not from column-node aliases

- **Decision:** Retain each FROM-tree `table` leaf's `alias` in `collect_join_tree`, store it on `JoinLeaf`, and resolve a `column` node to a leg by matching the pair (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs.
- **Alternatives:** Reconstruct the mapping by collecting the distinct `tableAlias` values a request references per `tableName` and binding them to that name's leg positions in a deterministic order (ascending alias string to ascending leg index). This was the design going into planning, on the research finding that the leaf nodes were `{"name":…, "type":"table"}` with no alias.
- **Rationale:** The live capture disproved that finding — a leaf carries `alias` (`{"type":"table","name":"FACT_ORDERS","alias":"A"}`). Reading it makes leg resolution exact. Reconstruction needs an arbitrary bijection plus a rule for alias-count mismatches, and would have justified itself on the interchangeability of same-table legs — a correct but far weaker argument than reading the signal Exasol already sends. Task 1.1 re-verifies the premise before any production edit and halts if it fails.
- **Promotes to ADR:** yes

### [2] `(tableName, alias)` is the leg key, with an absent alias part of the key

- **Decision:** Treat an absent alias as a distinct key value rather than as a missing signal. A `tableName` naming exactly one leg resolves by name alone and never consults an alias.
- **Alternatives:** Key on the alias alone; require an alias match on every column.
- **Rationale:** Two live observations force both halves. A genuine self-join may leave one occurrence unaliased (`FROM T JOIN T b`, which returned 100 rows instead of 10), so alias-alone is not total. And Exasol stamps no `tableAlias` at all when the user writes no alias, so requiring a match would break every unaliased join — the common, currently-correct case. The pair is injective by SQL's own rules: two occurrences of one table cannot share an alias, and at most one can be alias-less.
- **Promotes to ADR:** yes

### [3] One attribution owner rather than four corrected re-derivations

- **Decision:** Introduce `JoinLegs` in a new `joins/attribution.rs` as the sole resolver of column-to-leg attribution, reachable only through `DetectedJoin`, and delete the four `tableName`-keyed derivations it replaces.
- **Alternatives:** Correct each of the four call sites in place, each reading the leaf alias itself.
- **Rationale:** Four independent derivations of one decision is what produced four defects from one mistake. In-place correction would leave the same structure and the same drift risk. `attribution` depends on neither `planning` nor `rendering`, so the dependency direction stays acyclic and the boundary is visible without reading internals.
- **Promotes to ADR:** yes

### [4] An unattributable column reference is a hard client-facing error

- **Decision:** When a `column` node's pair matches no leaf key and its `tableName` names more than one leg, return the wrapper's existing hard error naming the column and its table.
- **Alternatives:** Render the column bare and unqualified (today's behavior for an unknown `tableName`); resolve to leg 0.
- **Rationale:** Bare rendering is ambiguous across the wrapper's subqueries, and an arbitrary leg reintroduces exactly the silent wrong-rows failure this plan removes. The state is unreachable for a well-formed request — Exasol stamps the occurrence alias on every column of an aliased FROM clause, including columns written unqualified, verified live — so it is pinned by a unit test over a synthesized request rather than guarded on a live path. This is a narrow, deliberate exception to the feature's standing preference against adding a failure branch for an unreachable state: there the alternative was a previously-successful query failing, here the alternative is wrong rows.
- **Promotes to ADR:** no

### [5] No same-table broadcast guard is added

- **Decision:** Leave broadcast eligibility for a self-join owned by `disjoint_schema_guard`, and pin the consequence with a test.
- **Alternatives:** Add an explicit "reject a repeated table" gate ahead of the broadcast sizing.
- **Rationale:** A self-join declares an identical column set on both sides, so the disjoint guard already declines it to the fallback. A second guard would give one decision two owners and would drift the moment one is edited. A test states the property without duplicating the decision.
- **Promotes to ADR:** no

### [6] The refused-column check stays name-keyed

- **Decision:** Leave `ensure_no_side_refuses_a_referenced_column` attributing by `tableName`, unchanged, even though it is a fourth name-keyed site.
- **Alternatives:** Re-key it to legs alongside the other four.
- **Rationale:** Over-charging is the fail-safe direction for a refusal — it refuses a query that might read a refused column and never admits one that does — and the function already charges an untagged column to every side by design. Narrowing it to a leg would make it admit more, which is the wrong direction for a safety check. Recorded here so the omission reads as deliberate rather than as a missed site.
- **Promotes to ADR:** no

### [7] The stale self-join scenario title is removed rather than rewritten in place

- **Decision:** Mark "Join conditions attach greedily by table-name set and side-local filters push into each leg" `DELTA:REMOVED` and add the corrected scenario under a new title naming the LEG set.
- **Alternatives:** Keep the recorded title and change only the body under `DELTA:CHANGED`, as this feature's earlier deltas did.
- **Rationale:** The title states the defect. Keeping it would leave the merged library asserting attribution by table-name set in a heading while its clauses say the opposite, and headings are what a reader scans first.
- **Promotes to ADR:** no

### [8] The Iceberg specification check is recorded as non-applicable

- **Decision:** State explicitly in the spec Background that the Iceberg table spec does not bear on this change, rather than quoting a section or silently skipping the project rule.
- **Alternatives:** Quote a nominally related normative section to satisfy the rule's letter.
- **Rationale:** The change is Exasol-SQL column-to-leg attribution over the pushdown JSON and generated wrapper SQL. It reads no manifest, snapshot, field id, or type mapping, and behaves identically on Iceberg and Delta — the established format-agnostic property of every SQL-shape pushdown decision. There is no section to quote and no deviation to track; a token quotation would be worse than an honest statement of non-applicability.
- **Promotes to ADR:** no

### [9] Tests that recorded the collapse as intended are corrected as a first-class task

- **Decision:** Task 4.2 replaces `seam_trailing`'s hardcoded one-entry alias map and its doc comment ("A self-join's alias map collapses to ONE entry … so this is exactly what the seam sees"), and gives self-join capture rows 9-11 the per-occurrence `tableAlias` production sees.
- **Alternatives:** Leave them; they pass either way since they exercise a seam below the attribution layer.
- **Rationale:** They are the reason the defect survived. A test fixture that hardcodes the collapsed map, and a comment that explains it as expected, together make the bug look like a decision. Leaving them means the next reader re-derives the same wrong conclusion.
- **Promotes to ADR:** no

### [10] The live evidence gaps are closed before any production edit

- **Decision:** Task 1.1 is a gate: reproduce both #361 shapes and answer five unverified questions (quoted mixed-case alias, four-leg join, right-deep nesting, self-join with a WHERE filter, self-join on a Delta/Unity virtual schema) as OBSERVED, halting if any contradicts the leaf-alias premise.
- **Alternatives:** Proceed on the six shapes already captured during planning.
- **Rationale:** The design rests entirely on the leaf `alias` being present and on the alias comparison being verbatim. Both are verified for unquoted aliases on an Iceberg virtual schema and unverified elsewhere. A quoted alias that arrives case-folded on one side and not the other, or a Delta virtual schema that omits the leaf alias, would break the fix silently on a shape no unit test can see. The project's verification discipline also requires the bug be reproduced live before it is fixed, which this task satisfies.
- **Promotes to ADR:** no

## Task 1.1 Evidence Capture (2026-08-19)

Captured against the running Docker stack (`exasol`, `minio`, `iceberg-rest`, `unitycatalog`, all healthy) via `exapump sql -p docker -f csv "EXPLAIN VIRTUAL <query>"`, reading element `[2]`'s (a `type: "pushdown"` element carrying `pushdownRequest`) `pushdownRequest` field of the `PUSHDOWN_JSON` array. Table used: `MY_LAKEHOUSE.FACT_ORDERS` (10 rows), per the plan's own convention; `UNITY_DELTA_E2E_VS.BASIC_PARTITIONED` (6 rows) for the Delta/Unity gap.

### #361 repros — both confirmed failing

| Shape | Query | Row count observed | Expected (no bug) |
|-------|-------|---------------------|--------------------|
| Two-leg self-join | `SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN MY_LAKEHOUSE.FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY` | **100** | 10 |
| Three-leg self-join | `... FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY JOIN FACT_ORDERS c ON b.O_ORDERKEY = c.O_ORDERKEY` | **1000** | 10 |

The two-leg capture's generated SQL matches the plan's Context section byte-for-byte: `ON (("LHS_T1"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY"))`, `LHS_T0` unconstrained, select list wrongly all-`LHS_T1`.

### Gap 1 — quoted mixed-case alias (`FROM T "myAlias"`)

Query: `SELECT "myAlias".O_ORDERKEY FROM MY_LAKEHOUSE.FACT_ORDERS "myAlias"`.

```json
"from": { "alias": "myAlias", "name": "FACT_ORDERS", "type": "table" },
"selectList": [ { "columnNr": 0, "name": "O_ORDERKEY", "tableAlias": "myAlias", "tableName": "FACT_ORDERS", "type": "column" } ]
```

OBSERVED: the quoted mixed-case alias arrives verbatim — `myAlias`, not `MYALIAS` or `myalias` — identically on the leaf's `alias` and the column's `tableAlias`. No case-folding on either side; a verbatim string comparison between the two is sufficient.

### Gap 2 — four-leg join

Query: `SELECT a.O_ORDERKEY FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY JOIN FACT_ORDERS c ON b.O_ORDERKEY = c.O_ORDERKEY JOIN FACT_ORDERS d ON c.O_ORDERKEY = d.O_ORDERKEY`.

The `from` tree is left-deep: `((A join B) join C) join D`, each of the four leaves carrying its own alias:

```json
"left": { "alias": "A", "name": "FACT_ORDERS", "type": "table" }
"right": { "alias": "B", "name": "FACT_ORDERS", "type": "table" }
...
"right": { "alias": "C", "name": "FACT_ORDERS", "type": "table" }
...
"right": { "alias": "D", "name": "FACT_ORDERS", "type": "table" }
```

OBSERVED: all four leaves at four different tree depths each carry a distinct `alias` (A, B, C, D); no leaf is left bare.

### Gap 3 — right-deep / parenthesized nesting (`A JOIN (B JOIN C)`)

Query: `SELECT a.O_ORDERKEY FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN (MY_LAKEHOUSE.FACT_ORDERS b JOIN MY_LAKEHOUSE.FACT_ORDERS c ON b.O_ORDERKEY = c.O_ORDERKEY) ON a.O_ORDERKEY = b.O_ORDERKEY`.

```json
"from": {
  "join_type": "inner",
  "left": { "alias": "A", "name": "FACT_ORDERS", "type": "table" },
  "right": {
    "type": "join",
    "join_type": "inner",
    "left": { "alias": "B", "name": "FACT_ORDERS", "type": "table" },
    "right": { "alias": "C", "name": "FACT_ORDERS", "type": "table" }
  }
}
```

OBSERVED: the parenthesized sub-join appears as a nested `join`-type node under `right` (not flattened to left-deep), and its own two leaves each carry their alias (B, C) independent of nesting side. The premise — a leaf carries its occurrence's alias regardless of tree shape — holds under right-deep nesting too.

### Gap 4 — self-join carrying a WHERE filter

Query: `SELECT a.O_ORDERKEY FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN MY_LAKEHOUSE.FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY WHERE a.O_CUSTKEY > 100`.

```json
"filter": {
  "type": "predicate_less",
  "left": { "type": "literal_exactnumeric", "value": "100" },
  "right": { "columnNr": 1, "name": "O_CUSTKEY", "tableAlias": "A", "tableName": "FACT_ORDERS", "type": "column" }
}
```

OBSERVED: the WHERE-clause column node carries `tableAlias: "A"`, matching the `from` tree's leaf `alias: "A"` for the same occurrence — the filter side is attributable by leaf alias exactly like `ON` and `selectList` columns.

### Gap 5 — self-join over a Delta/Unity virtual schema

`UNITY_DELTA_E2E_VS` (backed by the `unitycatalog` container, confirmed healthy) is already provisioned in this Docker environment with self-joinable tables (e.g. `BASIC_PARTITIONED`, 6 rows). Query: `SELECT a.LETTER FROM UNITY_DELTA_E2E_VS.BASIC_PARTITIONED a JOIN UNITY_DELTA_E2E_VS.BASIC_PARTITIONED b ON a.LETTER = b.LETTER`.

```json
"from": {
  "type": "join",
  "join_type": "inner",
  "left": { "alias": "A", "name": "BASIC_PARTITIONED", "type": "table" },
  "right": { "alias": "B", "name": "BASIC_PARTITIONED", "type": "table" }
},
"condition": {
  "left": { "tableAlias": "A", "tableName": "BASIC_PARTITIONED", "name": "LETTER", "type": "column" },
  "right": { "tableAlias": "B", "tableName": "BASIC_PARTITIONED", "name": "LETTER", "type": "column" }
}
```

OBSERVED: the same `pushdownRequest` shape — leaf `alias`, column `tableAlias` — is emitted identically for the Delta/Unity virtual schema; the FROM-tree/leaf-alias signal is format-agnostic, consistent with the mission's established format-agnostic property of SQL-shape pushdown decisions. Also confirmed the bug is live here too: the self-join returned 30 rows against a 6-row table (`SELECT COUNT(*) ...` executed directly), not merely the pushdown shape.

### Verdict

No observation contradicts the premise that a FROM-tree leaf carries its occurrence's alias. All five gaps are answered OBSERVED with raw JSON evidence above; the alias signal is present, verbatim (no case-folding), and format-agnostic across every nesting shape, filter placement, and virtual-schema backend tested. **The design premise holds — proceed to task 2.1.**

## Review Findings
