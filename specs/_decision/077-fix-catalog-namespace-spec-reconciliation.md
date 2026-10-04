# Decisions: fix-catalog-namespace-spec-reconciliation

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

