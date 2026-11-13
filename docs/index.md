[lakehouse-engine](../README.md) › Docs

---

# lakehouse-engine documentation

`lakehouse-engine` is an Exasol Virtual Schema that queries Iceberg, Delta, and Parquet tables
straight from Exasol SQL. The tables sit behind an Iceberg REST catalog, Unity Catalog, AWS Glue, or
direct storage, and include Databricks-managed tables. It runs the
[DataFusion](https://datafusion.apache.org/) engine in place, inside Rust UDFs on the
Exasol nodes. Scans therefore run where the cluster already is. The VS layer stays
thin: it does query translation, pushdown analysis, parallelization planning, and schema
mapping. All execution happens in disposable, node-local DataFusion runtimes. The adapter
reuses a table's metadata within one statement and keeps nothing across statements. It
does no materialization and copies no data out.

After you deploy it, you query lakehouse tables like any other Exasol schema:

```sql
SELECT l_returnflag, SUM(l_quantity)
FROM my_lakehouse.lineitem
WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag;
```

## Documentation

| Guide | What it covers |
|-------|----------------|
| [Install](install.md) | One command installs the engine on any Exasol deployment: SaaS, Exasol AsApp, Docker, or on-premise. It uploads the `.so`, registers the Rust SLC, and creates the scripts. Build-from-source and fully manual paths are covered too, as appendices. |
| [Catalogs](catalogs.md) | Connect to Iceberg REST (including Lakekeeper), Unity Catalog, AWS Glue, and direct storage: CONNECTION objects, credentials, and object-storage access. |
| [Security](security.md) | Who needs which grant: a reader needs only `SELECT`, and the Virtual Schema's owner holds the script and CONNECTION grants. Also what a re-install drops, the error each missing grant gives, who can bypass the adapter, the Lakekeeper per-user permission check, the sealed vended-credential envelope, and rotation. |
| [Export](export.md) | **In development, not ready for use.** `EXPORT ... INTO SCRIPT` writes an Exasol table or a query on virtual tables into a new Iceberg table through an Iceberg REST catalog: the statement and its options, the type mapping, how writers commit, `SINGLE_COMMIT`, what a failed export leaves behind, the CONNECTION it needs, and the export role. |
| [Benchmark](benchmark.md) | The benchmark query set and how to run it yourself. |
| [Architecture](architecture.md) | How cluster and DataFusion parallelism combine: file sharding, `GROUP BY shard_key` fan-out, and how pushdown meets parent-level Exasol execution. |
| [Capabilities](capabilities.md) | Pushdown support matrix: what runs in DataFusion versus Exasol. |
| [Capability matrix](capability-matrix.md) | Every Exasol Virtual Schema capability, and whether the adapter advertises it. |
| [Tuning](tuning.md) | Configuration parameters reference and runtime telemetry. |
| [Debugging pushdown](debugging-pushdown.md) | Use `EXPLAIN VIRTUAL` to inspect what the adapter pushes down for a query. |
| [Semantic differences](semantic-differences.md) | Known cases where a pushed-down query returns a different result than native Exasol. |

## Start here

- **Deploying for the first time?** Read [Install](install.md). Then read [Catalogs](catalogs.md) to point the VS at your data, and [Security](security.md) to give the Virtual Schema's owner its grants before creating it.
- **Evaluating the approach?** Read [Architecture](architecture.md) and [Benchmark](benchmark.md).
- **Tuning a running deployment?** Read [Capabilities](capabilities.md) and [Tuning](tuning.md).
- **A query does not push down the way you expect?** Read [Debugging pushdown](debugging-pushdown.md).
- **A result differs from the same query on a native table?** Read [Semantic differences](semantic-differences.md).
