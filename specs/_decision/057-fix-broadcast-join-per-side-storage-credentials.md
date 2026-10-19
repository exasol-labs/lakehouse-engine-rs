# Decisions: fix-broadcast-join-per-side-storage-credentials

## ADR: Route by path inside one registered store per bucket, not by a custom object-store registry

**ID:** join-route-by-path-inside-one-store-per-bucket
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

A broadcast join read both tables through the fact side's storage credential (issue #294). DataFusion keys its object-store registry on `scheme://host[:port]` and rejects URLs with a path, so one bucket has one registered store. Databricks makes a shared metastore bucket the normal case for two tables of one catalog.

### Decision

The adapter registers one `PrefixRoutingObjectStore` per bucket, holding one inner store per join side, each built from that side's own storage backend. Every `ObjectStore` call with a `Path` routes to the owning side's inner store.

### Options Considered

| Option | Verdict |
|--------|---------|
| A custom DataFusion registry keyed with userinfo | Rejected: the registry never receives a path, so it cannot separate two S3 tables in one bucket, and adopting `object_store`'s userinfo-retaining registry is a larger change against a pluggable seam |
| Refuse credential-divergent joins | Rejected: forfeits broadcast for the common Databricks case |
| Fall back to the N-scan renderer | Rejected: forfeits broadcast for the same case |

### Consequences

One code path serves every join with no credential comparison. Two credentials live behind one registered store.

## ADR: `validate_sides_share_one_store` stays; prefix routing does not subsume it

**ID:** join-keep-validate-sides-share-one-store-guard
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

DataFusion's registry key drops URL userinfo, which on `abfss://` is the container. An Azure store is container-scoped and its paths are container-relative, so two containers of one storage account can produce identical paths that path-based routing cannot distinguish.

### Decision

The scan-time guard stays, and only its doc comment changes. It refuses a spec whose sides share one registry key but need different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete it as subsumed by the router | Rejected: the router cannot separate two ADLS containers with identical relative paths |

### Consequences

This guard is the only guard over the ADLS different-container case, because `join-backend-guard-deleted-store-url-collision-is-the-rule` deletes the plan-time comparison that never covered it.

## ADR: Delete the plan-time backend guard; the store-URL collision is the only rule, and the scan owns it

**ID:** join-backend-guard-deleted-store-url-collision-is-the-rule
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

Code review of PR #306 found the plan-time guard's scope inverted. The scan builds each side's store from that side's own backend and registers one store per derived store URL. A variant difference is served, because different schemes give different registry keys. An ADLS account difference is served, because two accounts are two hosts. The same-account, different-container case is not served, and the guard admitted it because it compares only `account_name`. The guard therefore rejected two configurations the scan serves, for example two Iceberg tables in different ADLS accounts, and admitted the one it cannot.

### Decision

The adapter deletes the plan-time backend guard and its identity helper. The scan-time `validate_sides_share_one_store` precondition is the only owner of the real rule: two sides collapsing onto one registry key while needing different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-scope the plan-time guard to compare derived store URLs | Rejected: re-derives DataFusion's registry-key formula in the adapter and still hard-errors a container collision the N-scan fallback serves |
| Keep the guard and widen or narrow its backend comparison | Rejected: backend identity is not what the scan's addressing depends on |
| Make the collision a broadcast disqualifier that falls to the N-scan fallback | Rejected for this plan: the best behavior, but it needs plan-time store-URL derivation and new eligibility and spec requirements, so it is recorded as the known improvement and needs its own tracked work |

### Consequences

Cross-backend joins and two-account ADLS joins plan and run normally. Two containers of one storage account fail at scan time, with a message naming both store URLs, instead of at plan time. Whether it errors depends on the chosen plan, since the N-scan fallback serves that shape. The superseded ADR's property that acceptance follows the configuration, not the data, is given up, because the alternative refuses a query the fallback reads correctly.
