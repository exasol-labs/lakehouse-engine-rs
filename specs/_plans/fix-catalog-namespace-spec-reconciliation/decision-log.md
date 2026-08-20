# Decision Log: fix-catalog-namespace-spec-reconciliation

## Interview

**Q:** The TPC-H loader and bench scripts read the `ICEBERG_NAMESPACE` env var (separate from the VS
SQL property). Should the rename be fully exhaustive (env var too), or keep the env var name and only
rename the VS property?
**A:** Rename everywhere, env var included — full consistency: the `ICEBERG_NAMESPACE` env var also
becomes `NAMESPACE` across `bench/*.sh`, `bench/.env.example`, `deploy/scripts/secrets.sh`, and
`tpch_loader.rs`.

**Q:** The sweep found 5 additional `spec.md` files with stale or incorrect Iceberg-only language
beyond what issue #324 explicitly lists (3 are behavioral mis-statements, not just wording). Should
this plan fix all of them now, or only the items #324 explicitly enumerates?
**A:** Fix everything the sweep found — rewrite every affected `spec.md`'s Background and Purpose
prose.

**Q:** Should archived `specs/_recorded/**` files (frozen historical plans that also reference
`ICEBERG_NAMESPACE`) be edited for the rename, or left untouched as a historical record?
**A:** Leave `_recorded/**` untouched — they are a frozen snapshot of decisions made at the time;
only the permanent `specs/<domain>` library and current code get renamed.

**Q:** Issue item 3 asks to document the two Databricks-UniForm routes and their differing
correctness dependencies. Where should that documentation live?
**A:** "Don't need to document that. the issue is wrong. Ignore that part." — skip item 3 entirely,
do not document it anywhere.

## Design Decisions

### [1] Prose corrections go through direct edits, deltas carry only scenario clauses

- **Decision:** A change to a `## Scenarios` clause is authored as a spec delta under
  `specs/_plans/<plan>/`. A change to a feature-description line or a `## Background` bullet is an
  implementation task that edits `specs/<domain>/<feature>/spec.md` directly. Where a delta file and
  a direct edit both carry a feature-description line, the task writes the delta's line verbatim, so
  the record-time merge order cannot change the result.
- **Alternatives:** Wrap Background bullets and description lines in `DELTA:CHANGED` markers and let
  `/speq:record` merge them. Rejected: `/speq:spec-merge`'s marker table defines `NEW`/`CHANGED`/
  `REMOVED` for SCENARIOS only, and the observed behavior across `specs/_recorded/**` is that
  Background bullets ACCUMULATE across plans — `scan-execution-spec-reconstitution`'s permanent
  Background now carries five plans' worth of appended bullets. A delta-carried correction would land
  BESIDE the stale sentence instead of replacing it.
- **Rationale:** The mechanism has to be able to carry the change. Using the conventional mechanism
  where it cannot would leave this plan's own output as a new instance of the drift it removes.
  `azure-e2e-ci-scope-simplification` set the precedent (its task 1.4 amends a Background bullet
  directly, and its § Features records why); this plan generalizes the rule and states the seam
  invariant that makes mixing the two safe.
- **Promotes to ADR:** yes

### [2] `lakehouse-catalog`'s error names the namespace value, not the VS property

- **Decision:** `crates/lakehouse-catalog/src/namespace.rs:59` stops naming the adapter's VS
  property. `"invalid ICEBERG_NAMESPACE '{}': {}"` becomes `"invalid namespace '{}': {}"`, matching
  the sibling error already in the same file at `:31` (`invalid namespace in '{qualified}': {e}`).
- **Alternatives:** (a) Rename the literal to `NAMESPACE`, the minimal edit. (b) Move
  `PROP_NAMESPACE` down into `lakehouse-catalog` and have the adapter import it, giving the name one
  owner.
- **Rationale:** That line is why an ADAPTER property rename reaches a second crate at all — a
  hardcoded copy of a decision `PROP_ICEBERG_NAMESPACE` owns, with nothing enforcing agreement
  between them. Option (a) reinstates the same leak under a new name and leaves the next rename with
  the same two-crate edit. Option (b) inverts the dependency: `lakehouse-catalog` would own a
  VS-adapter protocol name it has no other reason to know. Removing the mention removes the second
  owner, and it costs nothing in diagnostics — the message already carries the offending namespace
  value, which is the actionable half.
- **Promotes to ADR:** yes

### [3] `specs/_decision/**` is left untouched, exactly like `specs/_recorded/**`

- **Decision:** Neither directory is edited. `specs/_decision/**` holds two "Iceberg namespace" prose
  mentions (`017-refactor-e2e-harness.md`, `001-migrate-legacy-decision-log.md`) and zero occurrences
  of the literal property token; both stay.
- **Alternatives:** Rename the two prose mentions for consistency with the rest of the sweep.
- **Rationale:** The user was asked about `_recorded/**` and chose to leave it as a frozen record.
  `_decision/**` is the same kind of artifact — an append-only log of decisions as they were taken,
  which `/speq:spec-merge` itself treats as write-once ("MUST NOT edit any other file in
  `specs/_decision/`"). Editing it would rewrite history to match a later decision. This extension of
  the user's instruction is a planner judgment call, recorded here rather than left implicit, and
  neither mention holds the property token in any case.
- **Promotes to ADR:** yes

### [4] `NAMESPACE`'s legality as a VS property name was verified live before planning

- **Decision:** The rename's load-bearing premise was checked against the Docker Exasol container
  during planning rather than assumed, and task 1.1 re-confirms it and extends it to the
  `ALTER … SET` path before any edit lands.
- **Alternatives:** Assume `NAMESPACE` is safe because the adapter's own comment says only `TABLE`
  was a problem.
- **Rationale:** `adapter/mod.rs`'s comment records that the property was named `ICEBERG_NAMESPACE`
  precisely because `TABLE` is an Exasol reserved keyword — so a reserved-word collision is a known,
  previously-realized failure mode for this exact property, and `CLAUDE.md` § Verification discipline
  forbids assuming a SQL capability from documentation or memory. Captured live:
  - `SELECT KEYWORD, RESERVED FROM SYS.EXA_SQL_KEYWORDS WHERE KEYWORD IN ('NAMESPACE','TABLE',
    'ICEBERG_NAMESPACE','SCHEMA')` returned 2 rows — `SCHEMA,true` and `TABLE,true`. `NAMESPACE` is
    absent from the keyword catalog entirely.
  - `CREATE VIRTUAL SCHEMA NS_PROBE_VS USING NONEXISTENT_SCHEMA.NONEXISTENT_ADAPTER WITH NAMESPACE =
    'probe' CATALOG_CONNECTION = 'X'` → `Could not find adapter script
    NONEXISTENT_SCHEMA.NONEXISTENT_ADAPTER` (SQL state 04000). The statement PARSED; it failed only
    at adapter lookup.
  - The negative control `… WITH TABLE = 'probe'` → `syntax error, unexpected TABLE_, expecting
    END_OF_INPUT_ or ';' [line 1, column 86]` (SQL state 42000). The probe therefore discriminates a
    property-name rejection from an adapter-lookup failure.
  - NOT yet captured: the `ALTER VIRTUAL SCHEMA … SET NAMESPACE='…'` path. The container stack went
    down before that capture; task 1.1 owns it and HALTS the plan if it fails.
- **Promotes to ADR:** no

### [5] `ICEBERG_NAMESPACE` is removed outright, with no alias and no deprecation window

- **Decision:** The old property name is accepted nowhere after this plan. A `CREATE VIRTUAL SCHEMA`
  or `ALTER VIRTUAL SCHEMA … SET` still supplying it fails with the existing required-property error
  naming `NAMESPACE`.
- **Alternatives:** Accept both names for one release; accept the old name indefinitely.
- **Rationale:** Issue #324 states no deployment needs maintaining. An alias would let stale DDL keep
  working while every doc and spec says otherwise — which is the same divergence between what the
  engine does and what the library says that this plan exists to close. A loud required-property
  error is a one-line fix for the operator and cannot be missed.
- **Promotes to ADR:** no

### [6] The scenario whose TITLE mis-states behavior is retired, not edited around

- **Decision:** `vs-adapter/pushdown-planning-join`'s "Small-side selection uses Iceberg metadata and
  the broadcast threshold" is carried as `DELTA:REMOVED` plus a `DELTA:NEW` "Small-side selection
  uses table-format metadata and the broadcast threshold" restating every invariant it held.
- **Alternatives:** Keep the title and change only its steps.
- **Rationale:** A scenario title is its merge key and its index entry. Leaving an Iceberg-only title
  over neutral steps would keep the wrong answer visible to anyone scanning the feature list —
  exactly how this defect class is found. Verified that no spec, test, or code references the title,
  so retiring it breaks no cross-reference.
- **Promotes to ADR:** no

### [7] No new normative Delta claim is stated anywhere in this plan

- **Decision:** Where a rewritten clause would need a statement about Delta path layout, delete
  semantics, or file-list guarantees to be complete, the plan states none and keeps the existing
  Iceberg reasoning explicitly scoped to Iceberg. `vs-adapter/pushdown-planning-file-encoding` is the
  clearest case: its out-of-root justification (`write.data.path`, `write.object-storage.enabled`
  hash injection, migrated/Databricks layouts) stays labelled as the Iceberg reason.
- **Alternatives:** Generalize the out-of-root rule to both formats on the strength of a recalled
  reading of `PROTOCOL.md`.
- **Rationale:** This plan ADDS the rule that a Delta-touching feature must quote `PROTOCOL.md`
  rather than rely on memory. Introducing an unquoted Delta claim in the same change would violate
  the rule at the moment of writing it. The clauses need no such claim: the strip-when-prefix rule is
  conditional for every format, which is what makes it safe without knowing either format's
  guarantees.
- **Promotes to ADR:** no

### [8] `pushdown-planning-empty-result` keeps its Iceberg FIXTURE and loses its Iceberg pruning claim

- **Decision:** "prunes 100% of the table's data files at the Iceberg level" becomes "…during
  plan-time file pruning" in all six scenarios; the first scenario's GIVEN "a virtual schema over an
  Iceberg table backed by MinIO" stays.
- **Alternatives:** Neutralize both; neutralize neither.
- **Rationale:** The two phrases are different kinds of statement. The pruning-level phrase asserts
  WHERE pruning happens for every request the short-circuit serves, and is false for a Delta table
  whose `add` statistics pruned the list (`vs-adapter/delta-file-pruning`). The fixture phrase
  describes the actual table the E2E scenario runs against, and is true. Neutralizing a true fixture
  description would make the scenario vaguer, not more correct.
- **Promotes to ADR:** no

### [9] The Iceberg spec-compliance obligation was evaluated and does not apply

- **Decision:** No normative Iceberg spec section is quoted, because none is implicated.
- **Alternatives:** Quote a section anyway to satisfy the letter of `CLAUDE.md` § Iceberg
  specification compliance.
- **Rationale:** That rule binds a feature "that touches scanning, pushdown, or schema/type
  handling". This plan changes no runtime behavior at all: it renames one VS property string and
  corrects prose. Nothing reads a manifest, snapshot, field id, partition value, or type differently,
  and every generated SQL string and wire encoding stays byte-identical. There is no section to quote
  and no deviation to track. Recorded explicitly rather than skipped silently, per the same rule's
  intent.
- **Promotes to ADR:** no

### [10] MINOR version bump despite the `fix` plan prefix

- **Decision:** 0.40.1 → 0.41.0.
- **Alternatives:** PATCH, following the `fix` prefix and the fact that no code behavior changes.
- **Rationale:** The property rename is breaking for operator DDL and for the bench and deploy
  environment. Under 0.x, MINOR is the breaking-change slot. The plan's `fix` prefix describes what
  is wrong (a library that mis-states the engine), not the compatibility class of the remedy.
- **Promotes to ADR:** no

### [11] Issue #324 item 3 is dropped, and the plan states the boundary rather than staying silent

- **Decision:** Nothing anywhere documents the two routes to a Databricks UniForm table or their
  differing correctness dependencies. Task 6.1 carries an explicit prohibition, naming what MUST NOT
  be added (issues #11/#12, Delta deletion vectors, a route-comparison table).
- **Alternatives:** Simply omit the item and let the implementer infer the scope.
- **Rationale:** Item 2 requires Core Capability 7 to say a Databricks table is now reachable by two
  different code paths chosen by `CATALOG_KIND`, and item 3 forbids documenting what differs between
  them. Those two sit one sentence apart, and an implementer working from the issue alone would very
  plausibly write the forbidden sentence while satisfying the required one. Stating the boundary in
  the task is what keeps the dropped item dropped.
- **Promotes to ADR:** no

### [12] A mixed-format join is unreachable, so no clause claims one

- **Decision:** No delta states that a join's two sides may be of different table formats. The
  neutrality clauses instead state the reachable outcome — a join whose BOTH sides are Delta tables
  reached through Unity Catalog takes the same broadcast path with no Iceberg-specific step.
- **Alternatives:** State the stronger and more natural-sounding "the two sides MAY be of different
  table formats", which the neutral code appears to permit.
- **Rationale:** It is unreachable, and specifying an unreachable capability is how a future planner
  ends up building for it. `scan_resolution.rs` is documented as "the pushdown path's ONE catalog-kind
  match": the kind is resolved once per REQUEST from the virtual schema's properties, and every
  involved table of one pushdown belongs to one virtual schema. Both join sides therefore always
  share a catalog kind, and with it a table format. The neutrality that matters is real —
  `TableScanResolver` reads no format identity downstream of that single match — but it buys
  format-agnostic CODE, not heterogeneous joins.
- **Promotes to ADR:** yes

### [13] Every added neutrality clause states an observable outcome, not an absence of a branch

- **Decision:** Clauses drafted as "MUST NOT branch on which table format produced the spec" were
  rewritten to name what is observable instead — that a spec produced by either format reader is
  served by the same path and returns the same shape.
- **Alternatives:** Keep the structural phrasing, which reads more directly as the anti-drift rule
  the plan wants.
- **Rationale:** A structural prohibition on a branch is not verifiable from outside the code: a
  branch that changes nothing violates it while failing no test, and a branch that changes something
  is already caught by the outcome clause. Per `/speq:writing-guardrails`, a normative statement has
  to be verifiable; the outcome form is pinned by the existing Delta E2E coverage, the structural
  form by nothing.
- **Promotes to ADR:** no

### [14] Task 1.1 (2026-08-20): `NAMESPACE` re-confirmed legal on both the CREATE and ALTER…SET paths

- **Decision:** Task 1.1's live re-verification against the Docker Exasol container (`docker compose
  up -d exasol`, `exapump wait -p docker`) is complete and PASSES on all three captures. The plan's
  rename premise is confirmed; no HALT.
  - **CREATE probe** — `exapump sql -p docker "CREATE VIRTUAL SCHEMA NS_PROBE_VS2 USING
    NONEXISTENT_SCHEMA.NONEXISTENT_ADAPTER WITH NAMESPACE = 'probe' CATALOG_CONNECTION = 'X'"` →
    `Query execution failed: Protocol error: Could not find adapter script
    NONEXISTENT_SCHEMA.NONEXISTENT_ADAPTER (SQL state: 04000)`. The statement PARSED; it failed only
    at adapter lookup, exactly as decision [4] recorded during planning.
  - **`TABLE` negative control** — `exapump sql -p docker "CREATE VIRTUAL SCHEMA NS_PROBE_VS3 USING
    NONEXISTENT_SCHEMA.NONEXISTENT_ADAPTER WITH TABLE = 'probe' CATALOG_CONNECTION = 'X'"` → `Query
    execution failed: Protocol error: syntax error, unexpected TABLE_, expecting END_OF_INPUT_ or ';'
    [line 1, column 86] (SQL state: 42000)`. Failed to parse, discriminating the property-name
    rejection from the adapter-lookup failure above.
  - **`ALTER … SET` capture (not captured during planning — this is the new evidence)** — a genuine
    Virtual Schema was created over a real adapter script (a minimal stub `LUA ADAPTER SCRIPT
    NS_PROBE_SCHEMA.NS_PROBE_ADAPTER` implementing `createVirtualSchema`/`setProperties`/`refresh`/
    `dropVirtualSchema`/`getCapabilities`, since building the real `.so` was unnecessary complexity
    for a pure DDL-parse question and the acceptance criterion is syntactic, not behavioral):
    `exapump sql -p docker "CREATE VIRTUAL SCHEMA NS_PROBE_REAL_VS USING NS_PROBE_SCHEMA.NS_PROBE_ADAPTER
    WITH NAMESPACE = 'ns_probe_a' CATALOG_CONNECTION = 'X'"` → `OK`. Then
    `exapump sql -p docker "ALTER VIRTUAL SCHEMA NS_PROBE_REAL_VS SET NAMESPACE='ns_probe_b'"` → `OK`
    — the statement parsed AND succeeded end-to-end (the stub's `setProperties` handler accepts any
    property name), so there is no ambiguity about whether the acceptance was merely "failed for a
    reason other than syntax": it fully succeeded.
  - Probe objects (`NS_PROBE_REAL_VS`, `NS_PROBE_SCHEMA` cascade) were dropped after capture; the two
    Iceberg REST namespaces created for the probe (`ns_probe_a`, `ns_probe_b`) were left in the
    ephemeral local MinIO/Iceberg-REST stack.
- **Alternatives:** Build the real `.so` via `make cross-musl-udf-build`, install the SLC, and stand
  up the full `LAKEHOUSE_ADAPTER` script against a live Iceberg REST catalog before running the ALTER
  capture (the literal reading of "the existing adapter script").
- **Rationale:** The acceptance criterion for this task is purely syntactic — whether `NAMESPACE` is
  legal in the `ALTER … SET` grammar — not whether the real adapter's `setProperties` handler
  recognizes it as a known property (that is task 2's concern, after the rename lands). A minimal
  stub adapter is sufficient to make the Virtual Schema genuinely exist (required for `ALTER` to have
  a target at all, unlike the CREATE probe's nonexistent-adapter shortcut) without the ~10+ minute
  DataFusion cross-build, which this task does not otherwise need. The captured `OK` result is
  strictly stronger evidence than a mere non-syntax-error would have been.
- **Promotes to ADR:** no

## Review Findings

No adversarial `plan-reviewer` round was run for this plan — the review phase was skipped by explicit
user instruction. This section is therefore empty by circumstance, not because a review found
nothing.
