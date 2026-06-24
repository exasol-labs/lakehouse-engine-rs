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
| [Install](install.md) | Build the `.so`, register the Rust SLC, deploy the scripts + CONNECTION + Virtual Schema |
| [Capabilities](capabilities.md) | Pushdown support matrix — what runs in DataFusion vs. Exasol |

## Design & specs

Spec-driven development via the `speq` skill; the spec library is the design source of truth:

- [`specs/mission.md`](../specs/mission.md) — purpose, problem statement, tech stack
- [`specs/decision-log.md`](../specs/decision-log.md) — architecture decision records
- [`specs/vs-adapter/`](../specs/vs-adapter/) — create-virtual-schema, pushdown planning
- [`specs/datafusion-scan/`](../specs/datafusion-scan/) — scan execution, aggregation, type mapping
- [`specs/parallelism/`](../specs/parallelism/) — work-unit sharding
- [`specs/sql-comprehension/`](../specs/sql-comprehension/) — the `vs-expression` translator
- [`specs/packaging/`](../specs/packaging/) — single-`.so` two-entry-points layout, E2E harness
