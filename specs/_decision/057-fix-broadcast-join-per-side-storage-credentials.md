# Decisions: fix-broadcast-join-per-side-storage-credentials

## ADR: Route by path inside one registered store per bucket, not by a custom object-store registry

**ID:** join-route-by-path-inside-one-store-per-bucket
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

A broadcast join must read each table with that table's own storage credential, and two tables of one Databricks catalog normally share a bucket. DataFusion keys its object-store registry on `scheme://host[:port]`, so one bucket has one registered store.

### Decision

The adapter registers one routing store per bucket, holding one inner store per join side, each built from that side's own credential. Every call routes by path to the owning side's inner store.

### Options Considered

| Option | Verdict |
|--------|---------|
| A custom DataFusion registry keyed with userinfo | Rejected: the registry never receives a path, so it cannot separate two S3 tables in one bucket |
| Refuse credential-divergent joins | Rejected: forfeits broadcast for the common Databricks case |
| Fall back to the N-scan renderer | Rejected: forfeits broadcast for the same case |

### Consequences

- One code path serves every join with no credential comparison.
- Two credentials live behind one registered store.
- Path routing does not replace the scan-time container/store collision guard, because two containers of one storage account can produce identical container-relative paths.

## ADR: Delete the plan-time backend guard; the store-URL collision is the only rule, and the scan owns it

**ID:** join-backend-guard-deleted-store-url-collision-is-the-rule
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The scan registers one store per derived store URL, so different backend types or ADLS accounts get different registry keys and only two containers of one account collide. A plan-time guard that compares backends or account names rejects configurations the scan serves and admits the one it cannot.

### Decision

The adapter has no plan-time backend guard. The scan-time guard owns the one real rule: two sides collapsing onto one registry key while needing different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-scope the plan-time guard to compare derived store URLs | Rejected: re-derives DataFusion's registry-key formula in the adapter and still errors on a collision the N-scan fallback serves |
| Keep the guard and widen or narrow its backend comparison | Rejected: backend identity is not what the scan's addressing depends on |
| Make the collision a broadcast disqualifier that falls to the N-scan fallback | Rejected: the best behavior, but it needs plan-time store-URL derivation and new eligibility rules |

### Consequences

- Cross-backend joins and two-account ADLS joins plan and run normally.
- Two containers of one storage account fail at scan time with a message naming both store URLs, unless the plan uses the N-scan fallback, which serves that shape.
