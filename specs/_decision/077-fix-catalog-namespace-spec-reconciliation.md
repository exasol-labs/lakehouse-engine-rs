# Decisions: fix-catalog-namespace-spec-reconciliation

## ADR: `lakehouse-catalog`'s error names the namespace value, not the VS property

**ID:** catalog-crate-error-names-value-not-property
**Plan:** fix-catalog-namespace-spec-reconciliation
**Status:** Accepted

### Context

The catalog crate's namespace error hardcoded the adapter's VS property name, a copy of a decision `PROP_ICEBERG_NAMESPACE` owns. Renaming the property therefore forced an edit inside the catalog crate.

### Decision

The error reads `"invalid namespace '{}': {}"`, matching the sibling error in the same file. `lakehouse-catalog` names no VS-adapter property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rename the literal to `NAMESPACE` | Rejected: keeps the same leak under a new name |
| Move the property name into `lakehouse-catalog` | Rejected: inverts the dependency |

### Consequences

A future property rename touches only the adapter crate.
