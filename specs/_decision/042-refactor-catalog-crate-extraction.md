# Decisions: refactor-catalog-crate-extraction

## ADR: `lakehouse-catalog` owns the three credential/config types; no shared-types crate

**ID:** catalog-crate-owns-credential-types-no-shared-types-crate
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

Only a crate boundary can expose the catalog session while hiding catalog authentication. The catalog code depends on three credential and config types, and the dependency runs only from the engine crate to the catalog crate, so a type both name must live in the catalog crate.

### Decision

The catalog crate declares the three types once, and the engine crate re-exports them, so no consumer changes. The Exasol-facing parsers stay in the engine crate. There are two crates, not three.

### Options Considered

| Option | Verdict |
|--------|---------|
| A third `lakehouse-types` crate | Rejected: groups by technical role and separates the credentials from the functions that interpret them |
| Leave the types in `lakehouse-engine` | Rejected: creates a dependency cycle |
| Parallel config types plus boundary conversions | Rejected: duplicates the types on a struct that must stay byte-stable on the UDF wire, and forces moved tests to stop constructing the credentials directly |

### Consequences

- Each type lives with its producer.
- Cost: an S3 storage config type sits in a crate named "catalog".

## ADR: The crate boundary is drawn at catalog access, not at Iceberg file planning

**ID:** catalog-crate-boundary-at-access-not-file-planning
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

Iceberg file planning consumes the catalog's output and produces scan-spec wire types and Arrow type tags. The extraction targets the catalog HTTP and session code.

### Decision

File planning stays in the engine crate. The catalog crate takes catalog authentication, the session, table load, namespace enumeration, SigV4 signing, vended-storage resolution, the credential types, and credential redaction.

### Options Considered

| Option | Verdict |
|--------|---------|
| Move file resolution into the crate too | Rejected: moves the scan-spec wire types and Arrow type mapping across the boundary, giving the crate a second responsibility |

### Consequences

- The catalog crate declares no Arrow, Parquet, DataFusion, object-store, or roaring dependency, checked by a boundary test.
- The scan-spec wire format and Arrow type mapping stay in one crate.
- The storage backend exposes three methods, because the dependency ban rules out a fourth.
