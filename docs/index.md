[lakehouse-engine](../README.md) › Docs

---

# lakehouse-engine documentation

`lakehouse-engine` is an Exasol Virtual Schema that queries Apache Iceberg and
Databricks-managed tables straight from Exasol SQL. It runs the
[DataFusion](https://datafusion.apache.org/) engine in place, inside Rust UDFs on the
Exasol nodes, so scans execute where the cluster already is. The VS layer stays thin
(query translation, pushdown analysis, parallelization planning, schema mapping); all
execution happens in disposable, node-local DataFusion runtimes. Every query is
stateless: no caching, no materialization, no data copied out.

Once it is deployed, you query lakehouse tables like any other Exasol schema:

```sql
SELECT l_returnflag, SUM(l_quantity)
FROM my_lakehouse.lineitem
WHERE l_shipdate <= DATE '1998-09-01'
GROUP BY l_returnflag;
```

## Documentation

| Guide | What it covers |
|-------|----------------|
| [Install](install.md) | One-command install for Exasol SaaS, an automated two-command path for self-managed Exasol, and a manual curl/SQL fallback for restricted networks. Deploy the `.so`, register the Rust SLC, create the scripts, then point a Virtual Schema at your data. |
| [Catalogs](catalogs.md) | Connect to Iceberg REST, AWS Glue, and Lakekeeper catalogs: CONNECTION objects, credentials, and object-storage access. |
| [Benchmark](benchmark.md) | The benchmark query set and how to run it yourself. |
| [Architecture](architecture.md) | How cluster and DataFusion parallelism combine: file sharding, `GROUP BY shard_key` fan-out, and how pushdown meets parent-level Exasol execution. |
| [Capabilities](capabilities.md) | Pushdown support matrix: what runs in DataFusion versus Exasol. |
| [Tuning](tuning.md) | Configuration parameters reference and runtime telemetry. |
| [Debugging pushdown](debugging-pushdown.md) | Inspect exactly what the adapter pushes down for a query, using `EXPLAIN VIRTUAL`. |

## Start here

- **Deploying for the first time?** Follow [Install](install.md), then [Catalogs](catalogs.md) to point the VS at your data.
- **Evaluating the approach?** Read [Architecture](architecture.md) and [Benchmark](benchmark.md).
- **Tuning a running deployment?** See [Capabilities](capabilities.md) and [Tuning](tuning.md).
- **A query isn't pushing down the way you expect?** See [Debugging pushdown](debugging-pushdown.md).
