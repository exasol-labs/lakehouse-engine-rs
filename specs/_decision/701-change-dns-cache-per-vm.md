# Decisions: change-dns-cache-per-vm

## ADR: One system-resolver DNS cache per UDF VM process serves every client

**ID:** one-dns-cache-per-udf-vm
**Plan:** change-dns-cache-per-vm
**Status:** Accepted

### Context

UDFs keep no state, so a cache owned by a client lasts one scan shard, and a statement over many tables or a CREATE over thousands of tables repeats the same lookups against the node's DNS server (#701). A resolver that reads `resolv.conf` itself skips the node's own name service setup.

### Decision

Each UDF VM process keeps one DNS cache for its whole life, the only state a UDF keeps across calls. It looks names up through the system resolver (`getaddrinfo`), keeps an answer for 30 seconds and a failure for 10 seconds, and sends one lookup per host at a time. Every client of the `.so` resolves through it, so the Iceberg `FileIO` runs on object_store clients instead of OpenDAL, whose process-wide HTTP client cannot take a resolver.

### Options Considered

| Option | Verdict |
|--------|---------|
| One process-wide cache over the system resolver | ✓ Chosen: one lookup per host per VM, and `/etc/hosts`, `nsswitch.conf`, and `resolv.conf` apply as for any process |
| hickory resolvers, one per client | ✗ Rejected: the cache lasts one client, and hickory sends DNS queries for names `/etc/hosts` lists |

### Consequences

- `lakehouse-catalog` depends on `object_store`, because it reads Iceberg metadata through the same clients the scan uses.
- A new HTTP client is built only in the factories that attach the cache; `clippy.toml` bans the other constructors.
- Each loaded table's FileIO opens its own object_store connection pool, so a CREATE or REFRESH over N tables makes at least N TCP and TLS handshakes per storage host, where OpenDAL's process-wide client reused pooled connections.
