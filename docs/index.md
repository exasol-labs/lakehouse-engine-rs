[lakehouse-engine](../README.md) › Docs

---

# lakehouse-engine documentation

`lakehouse-engine` is an Exasol Virtual Schema that runs the
[DataFusion](https://datafusion.apache.org/) engine in place, inside Rust UDFs, to
query Apache Iceberg and Databricks-managed tables straight from Exasol SQL. The VS
stays thin (translation, pushdown analysis, parallelization planning, schema mapping);
all execution happens in disposable, node-local DataFusion runtimes. Every query is
stateless — no caching, no materialization, no data movement.

## Guides

| Doc | What it covers |
|-----|----------------|
| [Install](install.md) | Build the `.so`, register the Rust SLC, deploy the scripts + CONNECTION (local / Glue / Databricks) + Virtual Schema, run E2E |
| [Capabilities](capabilities.md) | Pushdown support matrix — what runs in DataFusion vs. Exasol |
| [Architecture](architecture.md) | Parallelism, sharding & how pushdown combines with parent-level Exasol execution |
| [Performance](performance.md) | Benchmark results, tuning recommendations & outlook |
| [Tuning](tuning.md) | Parameters reference & telemetry |
