# Decisions: add-glue-catalog-kind

## ADR: One catalog-declared Parquet reader and one Iceberg planner serve Glue

**ID:** one-catalog-declared-parquet-reader-and-one-iceberg-planner-serve-glue
**Plan:** add-glue-catalog-kind
**Status:** Accepted

### Context

A Glue database mixes Iceberg tables and Hive Parquet tables. Glue, Unity, and direct storage differ only in where types, files, and Iceberg metadata come from.

### Decision

Glue Parquet tables use the Unity Parquet reader, generalized over a type source (Unity `type_json` or Glue Hive string) and a file source (table directory or Glue partitions). A Hive parser feeds the shared Spark classifier. Glue Iceberg tables use the one Iceberg planner, fed from `metadata_location` instead of REST `loadTable`.

### Options Considered

| Option | Verdict |
|--------|---------|
| A `GlueParquetFormatReader` copy | Rejected: two readers drift on schema authority and refusal rules |
| A second Iceberg path for Glue | Rejected: duplicates delete handling, name mapping, and pruning |

### Consequences

The shared partition predicate compares under declared types, and direct storage passes utf8 and keeps string comparison. The shared listing takes a `FilePattern`: Unity and direct storage pass `ParquetAtAnyDepth`, and Glue passes `AnyDirectChild`. No predicate goes into Glue's `Expression`, because an error in a second translator returns wrong rows under full delegation. The plan adds no `ScanSpec`, `FileEntry`, or `LogicalField` field.

## ADR: The Glue client uses aws-sdk-glue without aws-config

**ID:** glue-client-uses-aws-sdk-glue-without-aws-config
**Plan:** add-glue-catalog-kind
**Status:** Accepted

### Context

The Glue client needs SigV4 signing, retry, error-code parsing, and clock-skew correction, and the `.so` should link one TLS stack.

### Decision

The client uses `aws-sdk-glue` with default features off, and the SDK features `rustls` and `legacy-https-client` stay off. `reqwest` moves to rustls with native roots, so the `.so` links no OpenSSL.

### Options Considered

| Option | Verdict |
|--------|---------|
| A hand-written client on `reqwest` plus `sigv4.rs` | Rejected: lacks retry, error-code parsing, and clock-skew correction |
| `iceberg-catalog-glue` | Rejected: requires `aws-config` and loads only Iceberg tables |

### Consequences

The MSRV rises to 1.94.1, so every CI compiler must be at least that. One TLS stack (rustls) serves the whole `.so`.
