# Decisions: fix-catalog-namespace-spec-reconciliation

## ADR: Prose corrections go through direct edits, deltas carry only scenario clauses

**ID:** prose-corrections-direct-edit-not-delta
**Plan:** fix-catalog-namespace-spec-reconciliation
**Status:** Accepted

### Context

`/speq:spec-merge`'s marker table defines `NEW`/`CHANGED`/`REMOVED` semantics for `## Scenarios`
clauses only. Observed behavior across `specs/_recorded/**` shows `## Background` bullets
ACCUMULATE across plans instead of being replaced — `scan-execution-spec-reconstitution`'s
permanent Background already carries five plans' worth of appended bullets. This plan needed to
correct feature-description lines and Background bullets across thirteen features without adding
to that drift.

### Decision

A change to a `## Scenarios` clause is authored as a spec delta under `specs/_plans/<plan>/`. A
change to a feature-description line or a `## Background` bullet is an implementation task that
edits `specs/<domain>/<feature>/spec.md` directly. Where a delta file and a direct edit both carry
a feature-description line, the task writes the delta's line verbatim, so the record-time merge
order cannot change the result.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route prose corrections through direct edits; deltas carry only scenario clauses | ✓ Chosen — matches what each mechanism can actually carry |
| Wrap Background bullets and description lines in `DELTA:CHANGED` markers | ✗ Rejected — the merge procedure has no marker semantics for them, and they are observed to accumulate rather than replace |

### Consequences

A recorder merging this plan's deltas never touches a Background bullet or description line —
those land only via the direct edits already committed during implementation. This keeps the
delta-merge mechanism scoped to what it can safely carry, and sets a precedent (after
`azure-e2e-ci-scope-simplification`) for routing non-scenario prose corrections the same way in
future plans.

## ADR: `lakehouse-catalog`'s error names the namespace value, not the VS property

**ID:** catalog-crate-error-names-value-not-property
**Plan:** fix-catalog-namespace-spec-reconciliation
**Status:** Accepted

### Context

`crates/lakehouse-catalog/src/namespace.rs:59` named the adapter's VS property
(`"invalid ICEBERG_NAMESPACE '{}': {}"`) — a hardcoded copy of a decision `PROP_ICEBERG_NAMESPACE`
owns, with nothing enforcing agreement between the two crates. That is why renaming an
adapter-level property forced an edit inside the catalog crate at all.

### Decision

The error message becomes `"invalid namespace '{}': {}"`, matching the sibling error already in
the same file at `:31` (`invalid namespace in '{qualified}': {e}`). `lakehouse-catalog` names no
VS-adapter property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name the namespace value instead of the property | ✓ Chosen — removes the second owner and costs nothing in diagnostics; the message already carries the actionable namespace value |
| Rename the literal to `NAMESPACE` (minimal edit) | ✗ Rejected — reinstates the same leak under a new name, leaving the next rename with the same two-crate edit |
| Move `PROP_NAMESPACE` down into `lakehouse-catalog` | ✗ Rejected — inverts the dependency, making the lower crate own a VS-adapter protocol name it has no other reason to know |

### Consequences

A future rename of the VS-adapter property touches only the adapter crate. `lakehouse-catalog`
depends inward on no VS-adapter naming decision.

## ADR: `specs/_decision/**` is left untouched, exactly like `specs/_recorded/**`

**ID:** decision-log-frozen-like-recorded-specs
**Plan:** fix-catalog-namespace-spec-reconciliation
**Status:** Accepted

### Context

`specs/_decision/**` holds two "Iceberg namespace" prose mentions
(`017-refactor-e2e-harness.md`, `001-migrate-legacy-decision-log.md`) and zero occurrences of the
literal `ICEBERG_NAMESPACE` property token. The user was asked about `specs/_recorded/**` (also
holding stale mentions) and chose to leave it as a frozen historical record.

### Decision

Neither `specs/_recorded/**` nor `specs/_decision/**` is edited by this plan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave both directories untouched | ✓ Chosen — both are append-only, write-once logs of decisions as they were taken |
| Rename the two prose mentions in `specs/_decision/**` for consistency | ✗ Rejected — would rewrite history to match a later decision |

### Consequences

`specs/_decision/**` keeps recording decisions exactly as they stood at the time they were made,
consistent with `/speq:spec-merge`'s own write-once treatment of that directory. Neither frozen
mention holds the renamed property token, so no reader is misled about the current property name.

## ADR: A mixed-format join is unreachable, so no clause claims one

**ID:** join-sides-share-one-catalog-kind-per-request
**Plan:** fix-catalog-namespace-spec-reconciliation
**Status:** Accepted

### Context

Neutralizing the join-planning specs raised the question of whether to state the stronger, more
natural-sounding claim that a join's two sides "MAY be of different table formats" — which the
neutral code appears to permit, since `TableScanResolver` reads no format identity downstream of
its one `CatalogKind` match.

### Decision

No delta states that a join's two sides may be of different table formats. The neutrality clauses
instead state the reachable outcome — a join whose BOTH sides are Delta tables reached through
Unity Catalog takes the same broadcast path with no Iceberg-specific step.

### Options Considered

| Option | Verdict |
|--------|---------|
| State only the reachable same-format-both-sides outcome | ✓ Chosen — matches what a single pushdown request can actually produce |
| State the stronger "sides MAY differ in format" claim | ✗ Rejected — unreachable in practice, and specifying an unreachable capability invites building for it |

### Consequences

`scan_resolution.rs`'s catalog-kind match is resolved once per request from the virtual schema's
properties, and every involved table of one pushdown belongs to one virtual schema — so both join
sides always share a catalog kind, and with it a table format. The recorded neutrality clauses
describe format-agnostic CODE, not a heterogeneous-join capability, keeping the spec library from
promising a shape the adapter never produces.
