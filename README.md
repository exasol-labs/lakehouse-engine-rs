<div align="center">

# lakehouse-engine-rs

**An in-place lakehouse query engine for Exasol — query Apache Iceberg and Databricks-managed
tables straight from SQL by running DataFusion inside Rust UDFs across the cluster. No caching,
no materialization, no data movement.**

</div>

---

## What this is

`lakehouse-engine-rs` is an Exasol **Virtual Schema** that does more than translate and plan: it
runs the [DataFusion](https://datafusion.apache.org/) engine on the node, in place. The VS stays
thin — query translation, pushdown analysis, parallelization planning, result-schema mapping —
while all execution happens in disposable, node-local DataFusion runtimes inside Rust UDFs. Files
are sharded across nodes, scanned in parallel, then merged in Exasol. Every query is stateless: it
starts from source metadata and leaves nothing behind. (Its sibling
[`strata-rs`](https://github.com/exasol-labs/strata-rs) prunes and **caches** Parquet; this engine
**executes** in place with no result reuse.)

## Quick start

Assuming the `.so`, Rust SLC, scripts, and catalog CONNECTION are already deployed (see
[**Install**](docs/install.md)), create a Virtual Schema over an Iceberg namespace and query it:

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'   -- catalog URI + S3 creds
  ICEBERG_NAMESPACE  = 'default'                   -- every table in the namespace is exposed
  SCAN_SCHEMA        = 'LHVS'                       -- schema holding the scan SET script
  ALLOW_HTTP         = 'true';                      -- plain-HTTP catalog/S3 (e.g. local MinIO)

-- Projection + filter + LIMIT are pushed down to the node-local scan
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

## Documentation

- [**docs/**](docs/index.md) — documentation index
- [Install & deploy](docs/install.md) — build the `.so`, register the SLC, create scripts + CONNECTION + VS
- [Capabilities](docs/capabilities.md) — projection / filter / LIMIT / aggregation pushdown matrix
- [`specs/`](specs/) — design source of truth (spec-driven development via the `speq` skill)

## Crates

| Crate | Purpose |
|-------|---------|
| `crates/lakehouse-engine` | VS adapter + DataFusion scan SET UDF (`cdylib` + `rlib`) |
| `crates/vs-expression` | SQL expression translator (`rlib`; designed to be shared with `strata-rs`) |

## License

License: TBD
