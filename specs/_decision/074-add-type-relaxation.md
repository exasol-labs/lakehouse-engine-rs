# Decisions: add-type-relaxation

## ADR: The recorded `.without_row_transforms()` correctness hole does not exist

**ID:** delta-without-row-transforms-hole-does-not-exist
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

A recorded claim says that building the kernel scan with `.without_row_transforms()` leaves widened Delta columns read at the old physical Parquet type, giving wrong values. `delta_kernel` has no type-widening cast, so that call discards no cast transform.

### Decision

The recorded claim is superseded. The engine's own format-neutral adapter chain performs the widening cast: the table schema is registered from the logical schema, column binding renames without comparing types, and DataFusion's `DefaultPhysicalExprAdapter` inserts a cast on any field inequality.

### Options Considered

| Option | Verdict |
|--------|---------|
| Treat this plan as adding the missing cast | Rejected: duplicates an existing mechanism |
| Take the kernel's transforms instead | Rejected: the kernel has no widening cast, and it would reintroduce partition and column-mapping handling this engine owns |

### Consequences

Removing the false claim stops the next reader from building an unnecessary cast layer. The plan verifies the existing chain with tests instead of adding infrastructure.

## ADR: Iceberg `date` → `timestamp` / `timestamp_ns` is refused at plan time from the schema history

**ID:** refuse-iceberg-date-promotion-from-schema-history
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The Iceberg library reads `timestamp` and `timestamp_ns` manifest bounds as 8 bytes, so a pre-promotion 4-byte `date` bound fails during manifest decode. This happens even for an unfiltered `SELECT *`, and a second bounds decode in the same crate can panic, which makes the engine SIGKILL every sibling VM of the statement part.

### Decision

The planner refuses the two Iceberg `date` promotions before any manifest is loaded, using the table's schema history, with an error naming the table, column, both types, and a tracked issue. Delta's `date` to `timestampNtz` stays supported, because the difference lies in the metadata format: typed JSON stats for Delta, untyped Avro byte buffers for Iceberg.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vendor the missing bounds-width inference | Rejected: a dependency fork for two promotion pairs |
| Catch and re-word the manifest decode error | Rejected: sits downstream of the panic path and depends on an error string |
| Leave the opaque failure | Rejected: it names neither column nor promotion |

### Consequences

The gate refuses on the recorded promotion alone, without checking whether a pre-promotion file survives, because that check needs the manifest read that fails.

## ADR: Iceberg `unknown` → any type is recorded as unreachable, with a build tripwire, and no gate

**ID:** iceberg-unknown-type-unreachable-build-tripwire
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The Iceberg library's `PrimitiveType` has no `Unknown` variant and cannot deserialize the type name, so a schema declaring it fails before engine code runs. Both engine type-mapping functions are already exhaustive with no catch-all arm.

### Decision

The engine has no refusal or mapping arm for `unknown`. The spec records it as a tracked exception citing its issue and upstream `apache/iceberg-rust#2581`. A test pins the exhaustiveness of both mapping functions, so a dependency upgrade that adds the variant breaks the build instead of falling through to the `utf8`/`VARCHAR` fallback.

### Options Considered

| Option | Verdict |
|--------|---------|
| Write a named refusal | Rejected: unreachable dead code |
| Upgrade the Iceberg library in this plan | Rejected: out of scope |

### Consequences

The exhaustiveness pin is stronger than a runtime refusal. An upgrade that adds `Unknown`, `variant`, `geometry`, or `geography` fails the build.
