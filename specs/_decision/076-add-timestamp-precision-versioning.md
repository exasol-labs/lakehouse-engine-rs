# Decisions: add-timestamp-precision-versioning

## ADR: `TimestampPrecision` owns the version rule and both declaration strings

**ID:** timestamp-precision-enum-owns-version-rule-and-declaration-strings
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

The Iceberg and Unity type mappers each hardcoded the `"TIMESTAMP"` literal, the shape that let the catalog-decimal guard drift into four copies before issue #329. A version-gated declaration needs one place that owns the threshold and both strings.

### Decision

A two-variant `Copy` enum, `TimestampPrecision` (millisecond, microsecond), owns the version rule and both declaration strings. Both mappers read it and neither keeps its own literal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Boolean `supports_parameterized_timestamp` parameter | Rejected: inverts silently at any of five call sites |
| Free function returning a string | Rejected: leaves the rule and the strings without one owner |
| Broader `EngineFeatures` struct | Rejected: no second version-gated decision exists |

### Consequences

An Iceberg `timestamp` and a Delta `timestamp` are declared at the same precision by construction. A future version-gated decision can extend the enum.

## ADR: Empty and unparseable versions both take the microsecond default

**ID:** timestamp-precision-empty-and-unparseable-default-to-microsecond
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`UdfContext::database_version()` returns an empty string when handshake metadata is not populated, and no caller exercises that case. The declaration needs a defined answer for an empty or unparseable version.

### Decision

An empty version and any version whose leading component is not an integer both yield `TIMESTAMP(6)`, through one default arm. The user chose this deliberately over the conservative bare `TIMESTAMP`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Default to bare `TIMESTAMP` | Rejected: silently truncates every timestamp on an engine the parse misjudges |
| Separate arms for empty and unparseable | Rejected: neither carries information the other lacks, and two arms invite drift |

### Consequences

An engine that rejects `TIMESTAMP(6)` was expected to fail loudly at `createVirtualSchema`. Live capture on Exasol 8.29.13 showed it clamps instead, which corrects the failure mode but not the decision.
