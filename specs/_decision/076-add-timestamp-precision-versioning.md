# Decisions: add-timestamp-precision-versioning

## ADR: `TimestampPrecision` owns the version rule and both declaration strings

**ID:** timestamp-precision-enum-owns-version-rule-and-declaration-strings
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` each independently hardcoded the
literal `"TIMESTAMP"` — the exact shape that let the catalog-decimal guard drift into four copies
before issue #329 consolidated it into one. A version-gated declaration needs one place that decides
the threshold and both resulting strings, so the two producers cannot diverge.

### Decision

Add a two-variant `Copy` enum, `TimestampPrecision::{Millisecond, Microsecond}`, to
`crates/lakehouse-engine/src/types/mapping.rs`, owning `from_database_version(&str)` and both
declaration strings. Both producers read it; neither keeps its own `"TIMESTAMP"` literal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Named `Copy` enum owning the rule and both strings | ✓ Chosen — variant names carry meaning at every call site, and the module already owns Exasol's own type domain (`exasol_representable_catalog_decimal`) |
| A `bool supports_parameterized_timestamp` parameter | ✗ Rejected — inverts silently at any of five call sites |
| A free function returning `&'static str` | ✗ Rejected — leaves the version rule and the two strings without one named owner |
| A broader `EngineFeatures` struct | ✗ Rejected as over-engineering — generalizes for a second version-gated decision that does not exist |

### Consequences

An Iceberg `timestamp` and a Delta `timestamp` are declared at the same precision by construction,
not by coincidence. A future version-gated decision has a precedent to extend rather than a template
to widen prematurely.

## ADR: The version STRING crosses into `types/mapping.rs`; the `UdfContext` does not

**ID:** timestamp-precision-version-string-crosses-not-udfcontext
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`types/mapping.rs` reads no ambient state and performs no I/O today. Threading `&dyn UdfContext` into
it to reach `database_version()` would make the type-mapping module depend on the adapter's runtime
context, reversing the direction the dependency has always pointed. `cluster_nodes_from_context` is
the recorded precedent for reading a context value once at the adapter edge and passing a plain value
onward, but it earns its own name by normalizing `0` to `1` — a wrapper here would only forward one
call.

### Decision

`handle_create_virtual_schema` reads `ctx.database_version()` exactly once, inline, and threads the
resolved `TimestampPrecision` as a plain parameter through `build_listing_virtual_tables` →
`column_source_type_to_exasol` → both producers. `TimestampPrecision::from_database_version` takes
`&str`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read inline at the adapter edge, thread a plain `Copy` value | ✓ Chosen — keeps `types/mapping.rs` free of `UdfContext`, matching `cluster_nodes_from_context`'s shape |
| Thread `&dyn UdfContext` into `types/mapping.rs` | ✗ Rejected — makes the type-mapping module perform I/O and read ambient state it has never needed |
| Add a `timestamp_precision_from_context(ctx)` wrapper | ✗ Rejected — a wrapper whose whole body forwards one call adds a name without adding a decision |
| Read the version again in the scan UDF entry point | ✗ Rejected — the scan's `EMITS` types already arrive in the pushdown request's own `dataType` JSON; a second read would give one decision two owners |

### Consequences

The type-mapping module stays a pure function of its inputs. The scan UDF entry point needs no
version read at all, so the decision has exactly one owner across both entry points.

## ADR: Empty and unparseable versions both take the microsecond default

**ID:** timestamp-precision-empty-and-unparseable-default-to-microsecond
**Plan:** add-timestamp-precision-versioning
**Status:** Accepted

### Context

`UdfContext::database_version()` returns `String::new()` on a context that does not populate
handshake metadata, and no call site for it exists anywhere in the repo today, so its unpopulated
behavior is untested in practice. A version-gated declaration needs a defined answer for a version
string that is empty or that fails to parse.

### Decision

An empty string and any string whose leading dot-separated component does not parse as an integer
both yield `TIMESTAMP(6)` — one default arm, not two — rather than falling back to the conservative
bare `TIMESTAMP`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Default both to `TIMESTAMP(6)` (the modern declaration) | ✓ Chosen — the user's explicit, deliberate choice, opposite of the recommended conservative default |
| Default both to bare `TIMESTAMP` (the conservative option) | ✗ Rejected — silently truncates every timestamp value on any engine the parse misjudges |
| Distinguish empty from unparseable with two separate arms | ✗ Rejected — neither input carries information the other lacks, and two arms invite drift |

### Consequences

A hypothetical engine that rejects `TIMESTAMP(6)` fails loudly at `createVirtualSchema` rather than
silently truncating — the trade-off the user chose. Live capture against Exasol 8.29.13 later showed
this risk does not materialize as a loud failure on that engine (it clamps instead), which corrects
the rationale's claimed failure mode without changing the decision itself.

