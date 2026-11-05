# Decisions: change-exasol-type-model

## ADR: The Exasol SQL type lives in vs-expression, with no separate types crate

**ID:** exasol-sql-type-lives-in-vs-expression
**Plan:** change-exasol-type-model
**Status:** Accepted

### Context

`vs-expression`, type-mapping, and the pushdown planner share one Exasol SQL type. `vs-expression` cannot depend on `lakehouse-engine`, and `lakehouse-engine` already depends on `vs-expression`.

### Decision

`vs-expression` defines the Exasol SQL type, reads it from VS `dataType` JSON, and renders it as Exasol SQL and as `dataType` JSON. `lakehouse-engine` re-exports the type and keeps only the mapping from Arrow, Iceberg, Delta, and Hive types to it.

### Options Considered

- A new leaf crate for the type: rejected because it groups code by technical role and adds a crate whose only consumers already depend on each other in one direction.
