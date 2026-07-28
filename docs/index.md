[lakehouse-engine](../README.md) › Docs

---

# lakehouse-engine documentation

`lakehouse-engine` is an Exasol Virtual Schema that queries Apache Iceberg and
Databricks-managed tables straight from Exasol SQL. It runs the
[DataFusion](https://datafusion.apache.org/) engine in place, inside Rust UDFs on the
Exasol nodes. Scans therefore run where the cluster already is. The VS layer stays
thin: it does query translation, pushdown analysis, parallelization planning, and schema
mapping. All execution happens in disposable, node-local DataFusion runtimes. Every query
is stateless: no caching, no materialization, no data copied out.

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
| [Install](install.md) | A one-command install for Exasol SaaS, an automated two-command path for self-managed Exasol, and a manual curl/SQL fallback for restricted networks. Deploy the `.so`. Register the Rust SLC. Create the scripts. Point a Virtual Schema at your data. |
| [Catalogs](catalogs.md) | Connect to Iceberg REST, AWS Glue, and Lakekeeper catalogs: CONNECTION objects, credentials, and object-storage access. |
| [Benchmark](benchmark.md) | The benchmark query set and how to run it yourself. |
| [Architecture](architecture.md) | How cluster and DataFusion parallelism combine: file sharding, `GROUP BY shard_key` fan-out, and how pushdown meets parent-level Exasol execution. |
| [Capabilities](capabilities.md) | Pushdown support matrix: what runs in DataFusion versus Exasol. |
| [Tuning](tuning.md) | Configuration parameters reference and runtime telemetry. |
| [Debugging pushdown](debugging-pushdown.md) | Use `EXPLAIN VIRTUAL` to inspect what the adapter pushes down for a query. |

## Start here

- **Deploying for the first time?** Read [Install](install.md). Then read [Catalogs](catalogs.md) to point the VS at your data.
- **Evaluating the approach?** Read [Architecture](architecture.md) and [Benchmark](benchmark.md).
- **Tuning a running deployment?** Read [Capabilities](capabilities.md) and [Tuning](tuning.md).
- **A query does not push down the way you expect?** Read [Debugging pushdown](debugging-pushdown.md).
