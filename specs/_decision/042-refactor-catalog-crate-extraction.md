# Decisions: refactor-catalog-crate-extraction

## ADR: `lakehouse-catalog` owns the three credential/config types; no shared-types crate

**ID:** catalog-crate-owns-credential-types-no-shared-types-crate
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

A crate boundary is the only way to make `CatalogSession` public while keeping `CatalogAuth`, the OAuth grant, and the prefix lookup unreachable. The catalog code depends on `ConnectionCreds`, `CatalogProps`, and `StorageProps`, and the dependency can only run from `lakehouse-engine` to `lakehouse-catalog`, so any type both crates name must live in the catalog crate.

### Decision

`lakehouse-catalog` declares `StorageProps`, `CatalogProps`, and `ConnectionCreds` once, and `lakehouse-engine` re-exports them at their pre-move paths, so no consumer changes. The Exasol-facing parsers stay in the engine. There are two crates, not three.

### Options Considered

| Option | Verdict |
|--------|---------|
| A third `lakehouse-types` crate | Rejected: groups by technical role, the wrong axis, and separates `ConnectionCreds` from the functions that interpret it |
| Leave the types in `lakehouse-engine` | Rejected: creates a dependency cycle |
| Parallel config types plus boundary conversions | Rejected: duplicates most of `ConnectionCreds` and all of `StorageProps` on a struct that must stay byte-stable on the UDF wire, and forces about 1,805 moved test lines to stop constructing `ConnectionCreds` directly |

### Consequences

Each type lives with its producer: `CatalogProps` is mis-homed in the scan module today, `ConnectionCreds` appears only in planning-layer files, and the catalog produces `StorageProps` for the scan UDF to consume as JSON. The cost is a naming smell, an S3 storage config type in a crate named "catalog", which the crate's doc comment states.

## ADR: The crate boundary is drawn at catalog access, not at Iceberg file planning

**ID:** catalog-crate-boundary-at-access-not-file-planning
**Plan:** refactor-catalog-crate-extraction
**Status:** Accepted

### Context

Iceberg file planning consumes the catalog's output and produces scan-spec wire types and Arrow type tags. Issue #204 asks to extract the catalog HTTP and session code.

### Decision

File planning stays in `lakehouse-engine`. The catalog crate takes catalog authentication, the session, the table load, namespace enumeration, SigV4 signing, vended-storage resolution, the credential types, and credential redaction.

### Options Considered

| Option | Verdict |
|--------|---------|
| Move file resolution into the crate too | Rejected: moves the scan-spec wire types and Arrow type mapping across the boundary, giving the crate a second responsibility and a far larger blast radius than the issue asks |

### Consequences

The catalog crate's manifest declares no `arrow`, `parquet`, `datafusion`, `object_store`, or `roaring` dependency, checked by a boundary test. The scan-spec wire format and Arrow type mapping stay in one crate.
