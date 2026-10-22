# Decisions: fix-broadcast-join-per-side-storage-credentials

## ADR: Route by path inside one registered store per bucket, not by a custom object-store registry

**ID:** join-route-by-path-inside-one-store-per-bucket
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

A broadcast join must read each table with that table's own storage credential. DataFusion keys its object-store registry on `scheme://host[:port]`, so one bucket has one registered store. Two tables of one Databricks catalog normally share a bucket.

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

## ADR: `validate_sides_share_one_store` stays; prefix routing does not subsume it

**ID:** join-keep-validate-sides-share-one-store-guard
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

DataFusion's registry key drops URL userinfo, which on `abfss://` is the container. Azure paths are container-relative, so two containers of one storage account can produce identical paths that path-based routing cannot distinguish.

### Decision

A scan-time guard stays. It refuses a scan whose sides share one registry key but need different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete it as subsumed by the router | Rejected: the router cannot separate two containers with identical relative paths |

### Consequences

- This is the only guard over the different-container case.

## ADR: Delete the plan-time backend guard; the store-URL collision is the only rule, and the scan owns it

**ID:** join-backend-guard-deleted-store-url-collision-is-the-rule
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The scan builds each side's store from that side's own backend and registers one store per derived store URL. Different backend types or different ADLS accounts give different registry keys, so the scan serves them. Two containers of one account collide. A plan-time backend guard compared only the account name, so it rejected configurations the scan serves and admitted the one it cannot.

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
- Two containers of one storage account fail at scan time, with a message naming both store URLs. Whether it errors depends on the chosen plan, since the N-scan fallback serves that shape.
- Acceptance no longer follows the configuration alone, because refusing at plan time would reject a query the fallback reads correctly.
