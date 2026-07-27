<div align="center">

<img src="docs/assets/logo.svg" width="128" height="128" alt="lakehouse-engine-rs logo">

# lakehouse-engine-rs

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml)
[![spec|driven](https://img.shields.io/badge/spec-driven-blueviolet.svg)](specs/)
[![Exasol|database](https://img.shields.io/badge/Exasol-database-blue.svg)](https://www.exasol.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

**In-place lakehouse query engine for Exasol — DataFusion in Rust UDFs, querying Iceberg and
Databricks tables straight from SQL.**

</div>

---

## Quick start

Once deployed (see [Install](docs/install.md)), create a Virtual Schema and query it:

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default';

SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

---

## What this is

`lakehouse-engine-rs` is a query engine for lakehouses, delivered as an Exasol Virtual Schema.
[Apache DataFusion](https://datafusion.apache.org/) executes inside stateless Rust UDFs — one
session per invocation, discarded on completion. The engine resolves the Iceberg file list once
per query and splits it into sharded work units that Exasol distributes across nodes and
multiplexes onto each node's cores, so cluster parallelism and DataFusion's vectorized execution
compound; no node scans another node's files. Pushed-down projection, filter, LIMIT, Top-N,
aggregation, and broadcast-eligible inner equi-joins keep each scan lean, reaching Apache Iceberg
and Databricks-managed Iceberg through the same path. Every query starts from source metadata,
with nothing materialized or copied out.

## Documentation

Start at the [documentation index](docs/index.md), or go straight to a guide:

| Guide | What it covers |
|-------|----------------|
| [Install](docs/install.md) | One-command install for Exasol SaaS, an automated two-command path for self-managed Exasol, and a manual curl/SQL fallback for restricted networks. Deploy the `.so`, register the Rust SLC, create the scripts, then point a Virtual Schema at your data. |
| [Catalogs](docs/catalogs.md) | Connect to Iceberg REST, AWS Glue, and Lakekeeper catalogs: CONNECTION objects, credentials, and object-storage access. |
| [Benchmark](docs/benchmark.md) | The benchmark query set and how to run it yourself. |
| [Architecture](docs/architecture.md) | How cluster and DataFusion parallelism combine: file sharding, `GROUP BY shard_key` fan-out, and how pushdown meets parent-level Exasol execution. |
| [Capabilities](docs/capabilities.md) | Pushdown support matrix: what runs in DataFusion versus Exasol. |
| [Tuning](docs/tuning.md) | Configuration parameters reference and runtime telemetry. |
| [Debugging pushdown](docs/debugging-pushdown.md) | Inspect exactly what the adapter pushes down for a query, using `EXPLAIN VIRTUAL`. |

## License

Free and open-source. Community-supported. Licensed under [MIT](LICENSE).

---

<div align="center">

Built with Rust 🦀 for Exasol. Maintained by [Exasol Labs 🧪](https://github.com/exasol-labs/).

</div>
