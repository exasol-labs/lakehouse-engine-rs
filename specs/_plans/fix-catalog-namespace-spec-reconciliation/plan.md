# Plan: fix-catalog-namespace-spec-reconciliation

## Summary

Make the mission, `CLAUDE.md`, and the permanent spec library describe the engine that actually
ships — two catalog kinds (Iceberg REST, native Unity Catalog) and two table formats (Iceberg,
Delta) — and discharge the `ICEBERG_NAMESPACE` → `NAMESPACE` rename that
`vs-adapter/unity-catalog-create-virtual-schema` explicitly deferred to issue #324. No runtime
behavior changes: one VS property string is renamed across 27 files and every other edit corrects
prose that misdescribes behavior the engine already has.

## Design

### Context

Two milestones landed a second catalog kind and a second table format without a matching pass over
the descriptive layer. `CatalogKind::{IcebergRest, UnityCatalogNative}` is matched exhaustively in
`TableScanResolver`, a `delta-kernel-rs` 0.26 reader sits behind the same `FormatReader` trait the
Iceberg reader does, and `e2e-harness/unity-catalog-e2e-harness-delta-queries` covers a broadcast
inner equi-join plus a grouped aggregate over Delta tables reached through Unity Catalog. The
per-feature record cycles merged each new feature's own spec, but nothing owned the cross-cutting
sentences in already-recorded specs that said "Iceberg" where the code says nothing at all.

The result is three distinct defect classes, and they need three different treatments:

1. **A misnamed VS property.** `ICEBERG_NAMESPACE` names a namespace for BOTH catalog kinds.
   `vs-adapter/unity-catalog-create-virtual-schema` recorded the mismatch and deferred the fix here.
2. **Behavioural mis-statements.** Three specs state normatively that broadcast-join sizing, join
   scan registration, and file resolution read *Iceberg* metadata. Delta reaches all three by the
   same code, so those clauses are false, not merely narrow. A reader consulting them to decide
   whether a Delta join is supported gets the wrong answer.
3. **Terminology staleness.** `ScanSpec.table_root` and `LogicalField` are format-neutral fields
   (`refactor-neutralize-scan-spec`, issue #342) that fifteen recorded clauses still call "the
   Iceberg table root" and "the logical Iceberg schema".

Class 2 is what makes this a `fix` rather than a docs chore: a normative clause that contradicts the
code is a defect in the library, and the library is what the next planner reads.

- **Goals** — one catalog-neutral property name with no alias; every normative clause that names a
  table format either true or renamed; mission and `CLAUDE.md` agreeing with the spec library; the
  Delta protocol given the same planning-time spec-check discipline the Iceberg spec already has.
- **Non-Goals** — any runtime behavior change; any new capability; backwards compatibility for the
  old property name; documenting the two routes to a Databricks UniForm table or their differing
  correctness dependencies (issue #324 item 3, dropped by the user); editing `specs/_recorded/**` or
  `specs/_decision/**`; neutralizing prose that is legitimately Iceberg-specific.

### Decision

Route each artifact by the mechanism that can actually carry the change, and never by which one is
conventional.

#### Architecture

```
        CHANGE                                      MECHANISM
──────────────────────────────────────────  ───────────────────────────────────
a `## Scenarios` clause                     spec DELTA under specs/_plans/…
  (Gherkin GIVEN/WHEN/THEN/AND)             merged by scenario name at record time
                                            → 13 delta files, this plan authors them

a feature-description line                  DIRECT EDIT of specs/<domain>/<feature>/spec.md
a `## Background` bullet                    → implementation task, no delta
                                            (precedent: azure-e2e-ci-scope-simplification 1.4)

specs/mission.md, CLAUDE.md                 DIRECT EDIT (neither is a delta artifact)

production code, tests, bench, deploy, docs DIRECT EDIT
```

`/speq:spec-merge` merges by scenario name and has no marker semantics for a description line or a
Background bullet; observed behavior across `specs/_recorded/**` is that Background bullets
ACCUMULATE. Routing prose corrections through deltas would therefore append a corrected sentence
beside the wrong one it was meant to replace — the exact drift this plan exists to remove.

The two mechanisms meet at one seam, and it is made safe by construction: where a delta file and a
direct edit both carry a feature-description line, the task writes the delta's line VERBATIM. If the
recorder replaces the line, it writes what is already there; if it does not, the direct edit already
did. Neither order produces a different file.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Route by mechanism capability | delta vs. direct edit | The delta merge is scenario-keyed; a Background bullet has no key to merge on |
| Byte-identical duplication at the seam | delta description line == direct-edit description line | Makes the record-time merge order irrelevant instead of relying on recorder behavior nobody has pinned |
| Remove a leak rather than rename it | `lakehouse-catalog`'s error message | A second unenforced copy of a VS property name is why this rename crosses a crate boundary at all |
| Neutralize the claim, keep the mechanism | every rewritten clause | "Iceberg manifest `file_size_in_bytes`" stays — as the ICEBERG READER's answer, not as the rule |
| Rename by removal + replacement | `Small-side selection uses Iceberg metadata…` | A scenario title is its merge key; a title that mis-states behavior has to be retired, not edited around |

#### Quick Diagnostic

Only one change touches a module boundary: `crates/lakehouse-catalog/src/namespace.rs:59` names the
VS property `ICEBERG_NAMESPACE` in a user-facing error, a second hardcoded copy of a decision the
adapter owns in `PROP_ICEBERG_NAMESPACE`.

| Question | Answer |
|----------|--------|
| Would changing how a module works internally force an edit outside it? | Yes, today — renaming an ADAPTER property forces an edit in the CATALOG crate. That is the leak |
| Is there exactly one module that owns each significant design decision? | Not for the property's spelling. Making the catalog error name the NAMESPACE VALUE instead of the property removes the second owner rather than renaming it |
| Does business logic depend only inward? | After the fix, yes: `lakehouse-catalog` names no VS-adapter property. Moving the constant DOWN into the catalog crate was rejected for the same reason — it would make the lower crate own an upper-layer protocol name |
| Deep module? | Unchanged. This edits one error string; it adds no interface |

The remaining changes introduce no module, interface, or boundary, so the rest of the diagnostic
does not apply to them.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `ICEBERG_NAMESPACE` is REMOVED, not aliased | Accept both names for one release; accept the old name silently forever | No deployment exists to migrate, and an alias would let a stale `CREATE VIRTUAL SCHEMA` keep working while the docs say otherwise. A required-property error naming `NAMESPACE` is the loud failure |
| Rename the bench/deploy ENV var too | Keep `ICEBERG_NAMESPACE` in shell tooling, rename only the VS property | User decision, taken in the interview: one name for one concept. Two spellings for the same value is how the drift started |
| `lakehouse-catalog`'s error names the namespace VALUE, not the property | Rename the literal to `NAMESPACE`; share one constant across both crates | Renaming reinstates the leak under a new name. Sharing forces the catalog crate to own a VS-protocol name. The file's own sibling error at `:31` already reads `invalid namespace in '{qualified}'`, so this is convergence, not invention |
| Prose corrections are direct edits, not deltas | Wrap Background bullets in `DELTA:CHANGED` markers | The merge procedure defines markers for scenarios only; Background bullets are observed to accumulate. A marker the merger does not read is worse than no marker |
| Neutralize six `empty-result` scenarios but keep their Iceberg FIXTURE | Neutralize everything; neutralize nothing | "prunes … at the Iceberg level" asserts WHERE pruning happens for every request and is false for Delta; "a virtual schema over an Iceberg table backed by MinIO" names the actual fixture and is true |
| No new normative Delta claim is stated anywhere | Generalize the Iceberg out-of-root path rule to Delta | `CLAUDE.md` requires a Delta claim be quoted from `PROTOCOL.md`, not recalled. This plan does no external research, so it states none and keeps the Iceberg reasoning explicitly scoped to Iceberg |
| `specs/_decision/**` is left untouched, like `specs/_recorded/**` | Rename its two "Iceberg namespace" prose mentions | Both are frozen historical records of decisions taken at the time. The user directed this for `_recorded/`; `_decision/` is the same kind of artifact and gets the same treatment. Neither holds the literal property token |
| Iceberg-spec check recorded as NOT triggered | Quote a normative section anyway | No scanning, pushdown, or type-handling behavior changes. Nothing reads a manifest, snapshot, field id, or type differently. There is no section to quote and no deviation to track — stated rather than skipped |
| MINOR version bump (0.40.1 → 0.41.0) | PATCH, matching the `fix` prefix | The property rename is BREAKING for any operator DDL. Under 0.x, MINOR is the breaking-change slot |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/create-virtual-schema` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/create-virtual-schema/spec.md` |
| `vs-adapter/refresh-and-set-properties` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/refresh-and-set-properties/spec.md` |
| `vs-adapter/unity-catalog-create-virtual-schema` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| `e2e-harness/unity-catalog-e2e-harness` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/e2e-harness/unity-catalog-e2e-harness/spec.md` |
| `vs-adapter/pushdown-planning-join` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/pushdown-planning-join/spec.md` |
| `vs-adapter/pushdown-planning-file-resolution` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/pushdown-planning-file-resolution/spec.md` |
| `vs-adapter/pushdown-planning-file-encoding` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/pushdown-planning-file-encoding/spec.md` |
| `vs-adapter/pushdown-planning-empty-result` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/vs-adapter/pushdown-planning-empty-result/spec.md` |
| `datafusion-scan/scan-execution` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/datafusion-scan/scan-execution/spec.md` |
| `datafusion-scan/scan-execution-join` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/datafusion-scan/scan-execution-join/spec.md` |
| `datafusion-scan/scan-execution-file-metadata` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/datafusion-scan/scan-execution-file-metadata/spec.md` |
| `datafusion-scan/scan-execution-spec-reconstitution` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| `parallelism/work-unit-sharding` | CHANGED | `specs/_plans/fix-catalog-namespace-spec-reconciliation/parallelism/work-unit-sharding/spec.md` |

`vs-adapter/pushdown-planning` changes too, but carries no `## Scenarios` clause change: its two
stale mentions are a description line and a Background bullet, and the Delta-predicate
cross-reference it lacks is a Background bullet. It therefore has no delta file, exactly as
`azure-e2e-ci-scope-simplification` recorded for `e2e-harness/azure-e2e-harness`. Task 5.6 amends it
directly. Tasks 5.1-5.7 likewise amend the description lines and Background bullets of the thirteen
features above; the delta files carry only their scenario clauses.

The split has one consequence the implementer must not work around: a rename or correction that lives
in a scenario clause is NOT applied to the permanent spec during implementation. It lands when
`/speq:record` merges the delta. Six `ICEBERG_NAMESPACE` occurrences therefore survive in four
permanent spec files until then, and editing them by hand would double-apply the change and leave the
delta pointing at text that no longer exists. Task 8.3 enumerates exactly which six.

## Impact

**Breaking for operators.** The `ICEBERG_NAMESPACE` virtual-schema property is renamed to
`NAMESPACE` with no alias: every `CREATE VIRTUAL SCHEMA` and `ALTER VIRTUAL SCHEMA … SET` must use
the new name, and a statement still using the old one fails with the required-property error naming
`NAMESPACE`. The same rename applies to the `ICEBERG_NAMESPACE` environment variable read by
`bench/*.sh`, `bench/.env.example`, `deploy/scripts/secrets.sh`, and the TPC-H loader, so a stale
`bench/.env` silently falls back to the `tpch` default instead of the namespace it names. No data,
wire-format, catalog, or query-result change; a redeploy of the `.so` plus updated DDL is the whole
migration. Everything else in this plan is descriptive: the mission, `CLAUDE.md`, and the spec
library stop claiming the engine is Iceberg-only, and `CLAUDE.md` gains a Delta-protocol
spec-check rule that binds future plans. Version bump: MINOR (0.40.1 → 0.41.0) — breaking under 0.x.

## Requirements

| Requirement | Details |
|-------------|---------|
| `NAMESPACE` must be a legal VS property name | VERIFIED live before planning, against the Docker container: `CREATE VIRTUAL SCHEMA … WITH NAMESPACE = 'probe' CATALOG_CONNECTION = 'X'` parsed and failed only on adapter lookup (`Could not find adapter script`, SQL state 04000), while the same statement with `TABLE = 'probe'` failed to parse (`syntax error, unexpected TABLE_`, SQL state 42000). `SYS.EXA_SQL_KEYWORDS` lists `TABLE` and `SCHEMA` and does NOT list `NAMESPACE`. Task 1.1 re-confirms this and extends it to the `ALTER … SET` path |
| No behavior change | Every response, generated SQL string, wire encoding, and query result MUST be identical for an equivalent request; only the property string differs |
| No alias | `ICEBERG_NAMESPACE` MUST NOT be accepted anywhere after this plan |
| Frozen records untouched | `specs/_recorded/**` and `specs/_decision/**` MUST keep their 12 and 2 historical mentions unedited |
| Legitimate Iceberg prose preserved | A clause naming Iceberg manifests, snapshots, field ids, INT96, the Iceberg table spec, or `iceberg::expr::Predicate` where the mechanism really is Iceberg's MUST NOT be neutralized |
| Issue linkage | The implementing commit MUST read `Closes #324` |

## Dependencies

Running Docker stack (`exasol`) for task 1.1 and for the E2E checklist steps. No new crates, no SDK
or SLC version change, no external documentation lookup.

## Migration

| Current | New |
|---------|-----|
| `CREATE VIRTUAL SCHEMA … WITH ICEBERG_NAMESPACE = '<ns>'` | `CREATE VIRTUAL SCHEMA … WITH NAMESPACE = '<ns>'` |
| `ALTER VIRTUAL SCHEMA … SET ICEBERG_NAMESPACE='<ns>'` | `ALTER VIRTUAL SCHEMA … SET NAMESPACE='<ns>'` |
| `ICEBERG_NAMESPACE=<ns>` in `bench/.env`, `deploy/scripts/secrets.sh`, TPC-H loader env | `NAMESPACE=<ns>` |

## Implementation Tasks

### 1. Confirm the premise

- [ ] 1.1 Confirm live, against the Docker Exasol container, that `NAMESPACE` is a legal VS property
  name on BOTH DDL paths, before any rename edit. Bring the stack up
  (`docker compose up -d exasol`), then run the CREATE probe recorded in § Requirements and its
  `TABLE` negative control, and additionally create a real virtual schema over the existing adapter
  script and run `ALTER VIRTUAL SCHEMA <vs> SET NAMESPACE='<other-ns>'`. **Acceptance:** the CREATE
  probe fails only on adapter lookup (04000) while the `TABLE` control fails to parse (42000), and
  the `ALTER … SET` statement parses. Record all three captures in `decision-log.md`. **If
  `NAMESPACE` is rejected on either path, HALT and escalate — the entire rename rests on it.**

### 2. Rename the property in production code

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/mod.rs`, rename `PROP_ICEBERG_NAMESPACE` to
  `PROP_NAMESPACE` and its literal to `"NAMESPACE"` (Serena `rename_symbol`, not a raw edit), rename
  the local `iceberg_namespace` binding to `namespace`, and replace the two comment lines above the
  constant. The replacement states what the constant is — the namespace to expose, for either
  catalog kind — and MUST NOT carry forward the `TABLE_NAME` migration note or the "TABLE is an
  Exasol reserved keyword" aside, both of which document a rename that finished long ago.
  **Acceptance:** `cargo build` green; no identifier or literal in the crate contains
  `ICEBERG_NAMESPACE`.
- [ ] 2.2 In `crates/lakehouse-catalog/src/namespace.rs:59`, change the error message to
  `invalid namespace '{}': {}`, so the catalog crate names the offending namespace rather than a
  VS-adapter property it does not own. Match the phrasing of the sibling error at `:31`
  (`invalid namespace in '{qualified}': {e}`). **Acceptance:** no file under
  `crates/lakehouse-catalog/` contains the string `ICEBERG_NAMESPACE` or `NAMESPACE` as a VS
  property name; `cargo test -p lakehouse-catalog` green.

### 3. Rename the property in tests

- [ ] 3.1 Update the nine unit-test sites: `crates/lakehouse-engine/src/adapter/adapter_tests.rs`
  (JSON keys, the `assert_eq!` literal, the doc comment, and the two `PROP_*` references) and
  `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` (one JSON key). Note that
  `unity_schema_tests.rs` already declares `const NAMESPACE: &str = "sales_catalog.public";`, so the
  renamed line reads `"NAMESPACE": NAMESPACE,` — a JSON key string beside a same-named const. That is
  correct and MUST NOT be "fixed" by renaming either side. Additionally, ADD one unit test to
  `adapter_tests.rs` pinning the no-alias contract: a `createVirtualSchema` request supplying only
  `ICEBERG_NAMESPACE` and no `NAMESPACE` fails with the required-property error naming `NAMESPACE`.
  **Acceptance:** `cargo test` green, and the new test fails if an alias is ever reintroduced.
- [ ] 3.2 Update the E2E DDL and doc-comment sites: `crates/lakehouse-engine/tests/e2e_unity_test.rs`,
  `cloud_e2e_test.rs`, `e2e_scan_test.rs`, `e2e_refresh_test.rs` (including the
  `ALTER VIRTUAL SCHEMA REFRESH_SETPROPS_VS SET …` statement), and the shared
  `crates/lakehouse-engine/tests/common/e2e_harness.rs`. **Acceptance:** `cargo build --tests` green;
  no file under `crates/lakehouse-engine/tests/` contains `ICEBERG_NAMESPACE`.
- [ ] 3.3 Rename the environment variable the TPC-H loader reads in
  `crates/lakehouse-engine/tests/tpch_loader.rs` (the `std::env::var` call and the module doc
  comment), keeping the `tpch` default unchanged.

### 4. Rename the environment variable in bench, deploy, and docs

- [ ] 4.1 Rename the env var across `bench/run.sh`, `bench/batch_size_aggcheck.sh`,
  `bench/batch_size_sweep.sh`, `bench/emit_s3conn_sweep.sh`, `bench/.env.example`, and
  `bench/README.md`. `bench/run.sh` needs care and MUST NOT be sed-swept blindly: it already has a
  local shell variable named `NAMESPACE`, so `NAMESPACE="${ICEBERG_NAMESPACE:-tpch}"` becomes the
  self-defaulting `NAMESPACE="${NAMESPACE:-tpch}"` and the later `NAMESPACE="$ICEBERG_NAMESPACE"`
  becomes a self-assignment that SHALL be deleted rather than left in. The separate
  `BENCH_DELETE_NAMESPACE` variable is a different variable and MUST NOT be renamed.
  **Acceptance:** `bash -n` parses every touched script; `shellcheck` reports no new finding; a
  docker-mode `bench/run.sh` dry read resolves the namespace from `NAMESPACE`.
- [ ] 4.2 Rename in `deploy/scripts/install.sh` (the emitted `CREATE VIRTUAL SCHEMA` instruction
  text) and `deploy/scripts/secrets.sh` (the exported assignment).
- [ ] 4.3 Rename in `README.md`, `docs/install.md`, `docs/catalogs.md`, `docs/tuning.md`, and
  `docs/benchmark.md` — SQL examples, the `docs/tuning.md` property table row (whose description
  "Iceberg namespace. Every table in the namespace becomes a virtual table." also stops naming one
  format), and the surrounding prose.

### 5. Correct the permanent spec library's non-scenario prose

Every task below edits `specs/<domain>/<feature>/spec.md` DIRECTLY — a description line or a
Background bullet, neither of which the delta merge can carry (see § Features).

Exactly SIX features have a rewritten description line, and for each of them the corrected text is
already written in this plan's delta file: `datafusion-scan/scan-execution`,
`datafusion-scan/scan-execution-join`, `parallelism/work-unit-sharding`,
`vs-adapter/pushdown-planning-file-encoding`, `vs-adapter/pushdown-planning-file-resolution`, and
`vs-adapter/pushdown-planning-empty-result`. COPY each one verbatim rather than re-deriving it.
**Acceptance for every one:** after the task, the block between the `# Feature:` heading and
`## Background` is byte-identical in the permanent spec and in this plan's delta file for that
feature. The other seven features keep their recorded description line unchanged.

- [ ] 5.1 `vs-adapter/create-virtual-schema`: in the Background paragraph, rename the ONE
  `ICEBERG_NAMESPACE` property mention. The Background's other "Iceberg namespace" mention — the
  bullet requiring the non-ASCII fixture to live in its own namespace — names the Iceberg concept,
  not the property, and stays. `vs-adapter/refresh-and-set-properties` needs no direct edit at all:
  its only stale mentions sit inside one scenario, carried by the delta.
- [ ] 5.2 `vs-adapter/unity-catalog-create-virtual-schema`: in the Background paragraph, rename the
  property AND delete the now-discharged deferral clause "the property keeps its Iceberg-era name
  under this plan and a catalog-neutral rename is deferred to #324". **Acceptance:** no sentence in
  the permanent library defers a rename that has landed.
- [ ] 5.3 `datafusion-scan/scan-execution`, `datafusion-scan/scan-execution-spec-reconstitution`,
  `parallelism/work-unit-sharding`, and `vs-adapter/pushdown-planning-file-encoding`: replace every
  description-line and Background occurrence of "the Iceberg table root" with "the table root", plus
  the sibling neutral-field mentions — `scan-execution`'s "the Iceberg/Parquet data files" and "the
  logical Iceberg schema", `spec-reconstitution`'s "resolved from the Iceberg manifest by the
  adapter", `work-unit-sharding`'s "the once-resolved Iceberg data-file list" and "from the Iceberg
  `FileScanTask`", and `pushdown-planning-file-encoding`'s "The Iceberg table root
  (`table.metadata().location()` …)", which becomes the neutral table root the format reader
  resolves, with `table.metadata().location()` named as the ICEBERG reader's source. Leave
  `pushdown-planning-file-encoding`'s Iceberg out-of-root justification bullet scoped to Iceberg, and
  leave every genuinely Iceberg clause of `scan-execution` — field-id binding, the INT96 tolerance
  and its spec grounding, the microsecond-truncation trade-off — unedited. **Acceptance:**
  `grep -n "Iceberg table root"` over these four files returns nothing. [expert]
- [ ] 5.4 `vs-adapter/pushdown-planning-file-resolution`: replace the whole feature-description
  paragraph with the corrected one from this plan's delta file, and neutralize the Background bullet
  "The data-file list, each file's byte size (from the Iceberg manifest), and the current Iceberg
  schema are resolved exactly once per pushdown". The rewrite names the resolve-once ORCHESTRATION as
  format-neutral, names this feature as the owner of the ICEBERG READER's half, and points at
  `vs-adapter/pushdown-format-neutral-resolution` and `vs-adapter/delta-table-planning`. [expert]
- [ ] 5.5 `vs-adapter/pushdown-planning-join` and `datafusion-scan/scan-execution-join`: neutralize
  the two `pushdown-planning-join` Background bullets that state both sides' *Iceberg* snapshot are
  resolved once and that the threshold is compared against an *Iceberg-metadata* byte size, and the
  `scan-execution-join` description line ("registers both sides as Iceberg tables") and Background
  bullet ("each declared against its own logical Iceberg schema"). Both files keep every
  Iceberg-spec-grounded clause — `scan-execution-join`'s Appendix E path-routing support and its
  tracked `(#304)` multi-bucket refusal are quoted normative Iceberg statements and MUST survive
  unedited. [expert]
- [ ] 5.6 `vs-adapter/pushdown-planning`: rename the two "Iceberg table root" mentions (description
  line and Background bullet), and ADD one Background bullet recording that the plan-time
  file-pruning predicate this feature dispatches has a per-format owner — `iceberg::expr::Predicate`
  for the Iceberg reader (`vs-adapter/pushdown-file-pruning`), the Delta stats predicate for the
  Delta reader (`vs-adapter/delta-file-pruning`) — so the scenario clauses naming
  `iceberg::expr::Predicate` read as the Iceberg arm rather than as the whole rule. Those scenario
  clauses themselves stay unedited: they are true of the Iceberg arm.
- [ ] 5.7 `vs-adapter/pushdown-planning-empty-result`: replace "When Iceberg-level file pruning" in
  the description line with "When plan-time file pruning", and neutralize the Background bullet
  "the adapter resolves the Iceberg data-file list exactly once".

### 6. Reconcile `specs/mission.md`

- [ ] 6.1 Reword Core Capability 7. It currently claims "there is no separate Databricks-specific
  code path". A Databricks table is now reachable by two genuinely different paths chosen by the
  configured `CATALOG_KIND` — Iceberg REST through `iceberg-rust`, or native Unity Catalog through
  `delta-kernel-rs` — and the capability must say so. **It MUST NOT go further:** do not document the
  two routes' differing correctness dependencies, do not cite issues #11/#12 or Delta deletion
  vectors, and do not add a comparison table. That was issue #324 item 3 and the user dropped it.
- [ ] 6.2 Correct the remaining single-format claims in `specs/mission.md`: Core Capability 2
  ("registers Iceberg tables" → Iceberg and Delta), Core Capability 3 ("resolve the Iceberg file
  list" → format-neutral), Core Capability 6 ("applies Iceberg positional/row-level deletes" → also
  Delta deletion vectors, whose behavior is recorded in
  `datafusion-scan/scan-execution-delta-deletion-vectors`), the Tech Stack Lakehouse row
  (`iceberg-rust` alone → `iceberg-rust` plus `delta-kernel-rs` 0.26, and Unity Catalog beside the
  Iceberg REST catalogs), the Project Structure crate comments, the sibling-projects note's
  "`crates/lakehouse-catalog` (Iceberg REST catalog access)", the Architecture data-flow line
  "resolve Iceberg snapshot + file list ONCE per query", and the External Dependencies table, which
  lists only "Iceberg catalog" and "Databricks (Iceberg)" and must also carry Unity Catalog as a
  catalog kind and the Delta route's dependency. **Acceptance:** every Core Capability, the Tech
  Stack, and the External Dependencies table name both catalog kinds and both table formats.

### 7. Reconcile `CLAUDE.md`

- [ ] 7.1 Update both crate descriptions in the Build section so `crates/lakehouse-engine` covers
  Iceberg AND Delta file planning and `crates/lakehouse-catalog` covers Iceberg REST AND Unity
  Catalog access. These two lines mirror `specs/mission.md`'s Project Structure and MUST stay in
  sync with what task 6.2 writes there.
- [ ] 7.2 Extend the "Iceberg specification compliance" section to cover the Delta Lake protocol.
  Keep the existing Iceberg rule intact and add the Delta half: a feature touching Delta scanning,
  pushdown, or schema/type handling MUST be checked against the Delta Lake protocol
  (`https://github.com/delta-io/delta/blob/master/PROTOCOL.md`) during planning, quoting the relevant
  normative section rather than relying on memory, with a known deviation either fixed in the same
  plan or recorded as a tracked exception cited inline in the spec. Follow the citation convention
  the library already uses — `delta-io/delta`, `PROTOCOL.md`, `master`, § <Section> — as in
  `datafusion-scan/type-relaxation` and `vs-adapter/delta-reader-feature-gating`. Retitle the section
  so it names both specs. **Acceptance:** the section obliges a Delta-touching plan to the same
  quote-the-section standard the Iceberg rule already imposes.

### 8. Close the one coverage gap and verify the sweep

- [ ] 8.1 Add the missing scan-side test for `datafusion-scan/scan-execution-file-metadata`'s
  "Delete-file relative and absolute paths resolve like data-file paths". The scenario has no
  dedicated test today: `positional_deletes_tests.rs`'s
  `both_delete_mechanisms_converge_on_one_position_map` exercises a relative delete path but asserts
  the position map rather than the resolution rule, `object_store_tests.rs`'s
  `a_delete_file_under_a_different_root_is_rejected` covers only the rejection side, and
  `shard_paths_tests.rs`'s `delete_file_paths_use_relative_absolute_encoding` tests the PLAN side of
  the same rule, not the scan side. Add one unit test in
  `crates/lakehouse-engine/src/scan/spec_tests.rs` asserting all three clauses: a relative
  delete-file entry joined onto the table root, an absolute delete-file entry passed through
  unchanged, and an empty table root leaving every delete-file entry absolute. **Acceptance:** the
  new test fails if any one of the three clauses is broken, and is the only test this plan adds.
- [ ] 8.2 Remove the stale doc comment on `JoinPlan`/`select_broadcast_sides` at
  `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs:269` — "deciding broadcast
  eligibility from Iceberg-manifest byte sizes" — replacing "Iceberg-manifest" with the neutral
  per-file metadata size. It is the one production doc comment that would contradict the corrected
  `vs-adapter/pushdown-planning-join` spec.
- [ ] 8.3 Run the completeness guards and record their output in the verification report.
  `grep -rIn "ICEBERG_NAMESPACE\|iceberg_namespace" . --exclude-dir=target --exclude-dir=.git` MUST
  return hits in exactly three places and nowhere else:
  1. `specs/_recorded/**` — 12 occurrences, frozen, untouched by this plan.
  2. `specs/_plans/fix-catalog-namespace-spec-reconciliation/**` — this plan's own artifacts, which
     narrate the rename and necessarily name the old spelling.
  3. SIX occurrences inside `## Scenarios` clauses of four permanent specs —
     `specs/vs-adapter/create-virtual-schema/spec.md` (1),
     `specs/vs-adapter/refresh-and-set-properties/spec.md` (3),
     `specs/vs-adapter/unity-catalog-create-virtual-schema/spec.md` (1), and
     `specs/e2e-harness/unity-catalog-e2e-harness/spec.md` (1). These are carried by this plan's
     delta files and land at RECORD time, not implementation time. **The implementer MUST NOT edit
     them directly** — doing so would double-apply the change and desynchronize the delta from its
     target. Zero occurrences anywhere under `crates/`, `bench/`, `deploy/`, `docs/`, `README.md`,
     or any `## Background` bullet or feature-description line of the permanent library.

  Two further guards: `grep -rn "Iceberg table root" specs --include=spec.md` returns nothing outside
  `specs/_recorded/`, and `git diff --stat specs/_recorded specs/_decision` is empty.
  **Acceptance:** all three guards pass with exactly the counts above. After `/speq:record` merges the
  deltas, the six scenario occurrences are gone and only groups 1 and the merged Background
  narration remain.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 2.1, 2.2 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 4.1, 4.2, 4.3 |
| Group E | 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7 |
| Group F | 6.1, 6.2, 7.1, 7.2 |
| Group G | 8.1, 8.2 |
| Group H | 8.3 |

Sequential dependencies:
- Group A → Group B (the property name is proven legal before any edit lands)
- Group B → Group C (the renamed constant compiles before tests reference it)
- Group B, C, D, E, F, G → Group H (the guards run last, over the finished tree)

Groups C, D, E, and F touch disjoint file sets and run concurrently with each other. Within Group B,
2.1 and 2.2 are in different crates and run concurrently. Within Group E, 5.1-5.7 each own their own
spec files and run concurrently — each of the seven owns a disjoint file set, with
`specs/vs-adapter/pushdown-planning/spec.md` owned solely by 5.6. Within Group F, 6.2 and 7.1 must
agree on the crate-description wording, so run them in one agent. Agents share one
working tree: no agent may `git stash`, `git reset`, or `git checkout` a path another group owns.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `PROP_ICEBERG_NAMESPACE` (`crates/lakehouse-engine/src/adapter/mod.rs:37`) | Renamed to `PROP_NAMESPACE`; the old spelling has no remaining reader |
| Comment | "Replaces the old TABLE_NAME property. (TABLE is an Exasol reserved keyword; ICEBERG_NAMESPACE is not.)" (`adapter/mod.rs:35-36`) | Documents a rename that completed several releases ago and names a property that no longer exists |
| Spec clause | "the property keeps its Iceberg-era name under this plan and a catalog-neutral rename is deferred to #324" (`specs/vs-adapter/unity-catalog-create-virtual-schema/spec.md:7`) | The deferral is discharged by this plan; a standing deferral to a closed issue is a false claim |
| Scenario | `Small-side selection uses Iceberg metadata and the broadcast threshold` (`specs/vs-adapter/pushdown-planning-join/spec.md:127`) | Title and sizing clause both name Iceberg manifests as the only source of the broadcast metric; replaced by the neutral scenario |
| Doc comment | "deciding broadcast eligibility from Iceberg-manifest byte sizes" (`joins/planning.rs:269`) | Contradicts the corrected spec and the code beneath it, which sums a neutral `FileEntry::size` |
| Shell statement | `NAMESPACE="$ICEBERG_NAMESPACE"` (`bench/run.sh:217`) | Becomes a self-assignment once the env var is renamed |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Create virtual schema enumerates every table in the configured namespace | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_create_vs_enumerates_namespace_tables` |
| Create virtual schema enumerates every table in the configured namespace | Unit | `crates/lakehouse-catalog/src/client_tests.rs` | `enumeration_builds_exactly_one_session` |
| Create virtual schema enumerates every table in the configured namespace | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `both_kinds_share_one_listing_pipeline` |
| Create virtual schema enumerates every table in the configured namespace (no-alias clause) | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | ADDED BY TASK 3.1 — pins that `ICEBERG_NAMESPACE` is not accepted |
| Set properties overrides persisted properties and re-enumerates | Integration | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `set_properties_retargets_namespace` |
| Set properties overrides persisted properties and re-enumerates | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `merge_set_properties_new_wins_and_null_unsets` |
| Set properties overrides persisted properties and re-enumerates | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `set_properties_null_unset_required_property_errors_not_panic` |
| Create virtual schema enumerates every table in the configured Unity Catalog namespace | Unit | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `enumerates_unity_namespace_tables` |
| Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| Scan registers only its assigned files and returns matching rows | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `scan_registers_only_assigned_files_two_arg` |
| Scan registers only its assigned files and returns matching rows | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `scan_registers_assigned_files_via_parquet_provider` |
| Scan reconstitutes the ScanSpec from the common and per-shard arguments | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `from_parts_reconstitutes_files_tuples_and_table_root` |
| Scan reconstitutes the ScanSpec from the common and per-shard arguments | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `spec_reconstitutes_with_delete_entries` |
| Relative paths resolve against the table root and absolute paths pass through | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `reconstruct_relative_entry_normalizes_single_separator` |
| Relative paths resolve against the table root and absolute paths pass through | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `reconstruct_absolute_entry_passes_through` |
| Relative paths resolve against the table root and absolute paths pass through | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | `relative_and_absolute_entries_resolve_to_same_files` |
| Delete-file relative and absolute paths resolve like data-file paths | Unit | `crates/lakehouse-engine/src/scan/store_router_tests.rs` | ADDED BY TASK 8.1 (relocated to the seam owner by task 4.2) — `delete_file_paths_resolve_against_the_table_root_like_data_file_paths` |
| File list is partitioned into G byte-balanced disjoint shards covering every file | Unit | `crates/lakehouse-engine/src/adapter/sharding_tests.rs` | `partition_by_bytes_disjoint_full_coverage` |
| File list is partitioned into G byte-balanced disjoint shards covering every file | Unit | `crates/lakehouse-engine/src/adapter/sharding_tests.rs` | `partition_by_bytes_propagates_size_into_shards` |
| Scan-driving query fans out via a nested distributor over a scalar scan UDF | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `row_scan_fans_out_via_nested_distributor_over_scalar_scan` |
| Scan-driving query fans out via a nested distributor over a scalar scan UDF | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `fan_out_primitive_wraps_distributor_in_ungrouped_scalar_scan` |
| Table root is carried once and paths under it are emitted relative | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `table_root_stripped_from_under_root_paths_and_carried_once` |
| Table root is carried once and paths under it are emitted relative | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `pushdown_carries_table_root_and_sizes_in_common_and_shards` |
| A data-file path not under the table root is carried as an absolute path | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `path_not_under_root_stays_absolute` |
| A data-file path not under the table root is carried as an absolute path | Unit | `crates/lakehouse-engine/src/adapter/pushdown/shard_paths_tests.rs` | `sibling_prefix_paths_are_not_relativized` |
| Pushdown resolves the file list once and builds a scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `pushdown_resolves_files_once_builds_scan_sql` |
| Pushdown resolves the file list once and builds a scan-driving query | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_owns_resolution_and_keeps_its_encoding` |
| Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | `broadcast_fact_side_uses_distributor_scalar_scan` |
| Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_result_correct` |
| Broadcast-eligible inner equi-join is planned as a broadcast fan-out | Integration | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` |
| Small-side selection uses table-format metadata and the broadcast threshold | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` | `resolved_side_sums_file_bytes_saturating` |
| Small-side selection uses table-format metadata and the broadcast threshold | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` | `dimension_over_threshold_is_not_broadcast_eligible` |
| Small-side selection uses table-format metadata and the broadcast threshold | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` | `threshold_boundary_is_inclusive` |
| Small-side selection uses table-format metadata and the broadcast threshold | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_above_threshold_result_matches_broadcast` |
| Scan registers both tables and executes the inner equi-join | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_registers_each_side_against_its_own_backend` |
| Row-scan query with all files pruned returns a typed empty projection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_file_list_returns_empty_select` |
| Single-group aggregate with all files pruned returns one shape-correct empty row | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types` |
| Single-group COUNT(DISTINCT) with all files pruned returns zero | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_agg_sql_count_distinct_emits_zero_no_merge_udf` |
| Multi-distinct or mixed single-group request with all files pruned matches the non-empty aggregate shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_case_2_3_matches_non_empty_aggregate_shape` |
| Grouped aggregate with all files pruned returns zero rows in grouped shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_grouped_sql_emits_zero_rows_in_grouped_shape` |
| Empty-result shape matches the plan the non-empty path would commit to | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_result_sql_dispatches_by_plan_shape` |

Every scenario above already has a passing test EXCEPT one, and finding that exception is a result
of this plan rather than an oversight in it. No scenario changes what the software does — only what
the library says about it — so the listed tests are the EXISTING coverage and MUST stay green.

Two groups of them are not left untouched, for two different reasons. The rename scenarios' tests
(`create-virtual-schema`, `refresh-and-set-properties`, and both Unity scenarios) are edited by tasks
3.1-3.2: they carry the property name in DDL strings and JSON keys, and they are what prove the
rename reached the DDL rather than only the constant.
`set_properties_null_unset_required_property_errors_not_panic` asserts the required-property error
names the property, so it fails if the rename is partial. Separately,
`datafusion-scan/scan-execution-file-metadata`'s delete-file path scenario has NO dedicated test at
all — the plan-side mirror of the rule is tested in `shard_paths_tests.rs` and the rejection side in
`object_store_tests.rs`, but the scan-side resolution is unasserted. Task 8.1 adds the one test this
plan contributes, and task 3.1 adds the one guarding the no-alias contract.

The unit tests above cover pure functions of their inputs — SQL string generation, JSON
(de)serialization, byte-balanced partitioning — with no I/O. Each is paired with an integration test
wherever a live round trip can observe the same property. The empty-result scenarios are unit-only
because their whole subject is a response the adapter returns WITHOUT invoking any scan.

The test-name column is the mapping this plan asserts against the tree at planning time. The
implementer MUST reconcile a name that has since changed against the real test tree and correct this
table, never add a duplicate test.


### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-adapter/create-virtual-schema` | `exapump sql -p docker "CREATE VIRTUAL SCHEMA MY_LAKEHOUSE USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'ICEBERG_CONN' NAMESPACE = 'e2e_lakehouse' ALLOW_HTTP = 'true'"` | Succeeds; `SELECT * FROM SYS.EXA_ALL_VIRTUAL_TABLES` lists the namespace's tables |
| `vs-adapter/create-virtual-schema` | the same statement with `ICEBERG_NAMESPACE` in place of `NAMESPACE` | Fails with the required-property error naming `NAMESPACE` — proving no alias survives |
| `vs-adapter/refresh-and-set-properties` | `exapump sql -p docker "ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET NAMESPACE='other_ns'"` | Succeeds and re-enumerates; the virtual schema now lists `other_ns`'s tables. This is also task 1.1's `ALTER` capture |
| `vs-adapter/unity-catalog-create-virtual-schema` | `exapump sql -p docker "CREATE VIRTUAL SCHEMA UNITY_VS USING LHVS.LAKEHOUSE_ADAPTER WITH CATALOG_CONNECTION = 'UNITY_CONN' CATALOG_KIND = 'UNITY_CATALOG' NAMESPACE = 'unity.delta_e2e' ALLOW_HTTP = 'true'"` | Succeeds; the fixture's Delta base tables appear with their mapped Exasol types |
| `parallelism/work-unit-sharding` | `NAMESPACE=tpch bench/run.sh` in docker mode, after moving any stray `bench/.env` aside per `CLAUDE.md` § Bench harness gotchas | The run's report header reads `namespace=tpch` because the variable was READ, not because the `tpch` default absorbed a name the script no longer knows |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e > /tmp/e2e.log 2>&1; echo "rc=$?"` then read the file | `rc=0`, 0 failures. Do not judge the run from a piped `tail` — capture the exit code and read the log |
| E2E (Unity) | `make test-e2e-unity > /tmp/e2e-unity.log 2>&1; echo "rc=$?"` then read the file | `rc=0`, 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Spec validation | `speq feature validate` | pass |
| Mission sync | `speq search query "namespace property catalog kind"` | The top hits name `NAMESPACE`, never `ICEBERG_NAMESPACE` |
