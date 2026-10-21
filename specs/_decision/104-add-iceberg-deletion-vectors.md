# Decisions: add-iceberg-deletion-vectors

## ADR: Iceberg deletion vectors are decoded by iceberg-rust, not by `delta_kernel` or by hand

**ID:** iceberg-deletion-vectors-decoded-by-iceberg-rust
**Plan:** add-iceberg-deletion-vectors
**Status:** Accepted

### Context

An Iceberg `deletion-vector-v1` blob has the same byte layout as one Delta deletion vector, and the engine already decodes Delta vectors with `delta_kernel`.

Applying deletion vectors widens the support set that `unsupported-deletes-classified-from-planned-tasks` states in its context. That ADR's classification still decides what the adapter refuses.

### Decision

The scan decodes Iceberg deletion vectors with iceberg-rust's public deletion-vector decoder and passes it only the fetched blob bytes.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse the `delta_kernel` decoder | Rejected: it reads a Delta container whose version byte precedes the vector, so an Iceberg blob would need a synthesized Delta descriptor and container |
| Hand-decode the roaring bitmap | Rejected: duplicates a spec-conformant decoder the workspace already depends on |

### Consequences

- The scan fetches only the blobs' byte ranges, not the whole Puffin file. Ranges less than 1 MiB apart share one request, and each request holds its own limiter permit.
- A change to the Iceberg blob format arrives with an iceberg-rust upgrade.
