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

## ADR: The version STRING crosses into `types/mapping.rs`; the `UdfContext` does not

**ID:** timestamp-precision-version-string-crosses-not-udfcontext
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`types/mapping.rs` reads no ambient state and performs no I/O. Passing `UdfContext` into it would make the type-mapping module depend on the adapter's runtime context, reversing the dependency direction.

### Decision

`handle_create_virtual_schema` reads the database version once, inline, and passes the resolved `TimestampPrecision` as a plain parameter down to both mappers. `TimestampPrecision` parses from a string.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pass `UdfContext` into `types/mapping.rs` | Rejected: makes the module perform I/O and read ambient state |
| Wrapper function that reads the version from the context | Rejected: forwards one call and adds a name without a decision |
| Read the version again in the scan UDF | Rejected: the scan's `EMITS` types already arrive in the request JSON, so the decision would have two owners |

### Consequences

The type-mapping module stays a pure function of its inputs, and the decision has one owner across both entry points.

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
