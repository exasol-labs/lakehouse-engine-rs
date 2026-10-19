# Decisions: add-glue-catalog-kind

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
