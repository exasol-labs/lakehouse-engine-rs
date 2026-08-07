<div align="center">

<img src="docs/assets/logo.svg" width="128" height="128" alt="lakehouse-engine-rs logo">

# lakehouse-engine-rs

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=exasol-labs_lakehouse-engine-rs&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=exasol-labs_lakehouse-engine-rs)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=exasol-labs_lakehouse-engine-rs&metric=coverage)](https://sonarcloud.io/summary/new_code?id=exasol-labs_lakehouse-engine-rs)
[![spec|driven](https://img.shields.io/badge/spec-driven-blueviolet.svg)](specs/)
[![Exasol|database](https://img.shields.io/badge/Exasol-database-blue.svg)](https://www.exasol.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

**In-place lakehouse query engine for Exasol. DataFusion runs in Rust UDFs and queries Iceberg
and Databricks tables straight from SQL.**

</div>

---

## Quick start

After you deploy the engine (see [Install](docs/install.md)), create a Virtual Schema. Then query
the schema:

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default';

SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

---

## What this is

`lakehouse-engine-rs` is a query engine for lakehouses. The engine is an Exasol Virtual Schema.

- **Execution model.** [Apache DataFusion](https://datafusion.apache.org/) runs inside stateless
  Rust UDFs. Each invocation creates one session, and the engine discards that session on
  completion.
- **Sharding.** The engine resolves the Iceberg file list once per query and splits it into
  sharded work units. Exasol distributes the work units across the nodes and multiplexes them onto
  the cores of each node. Cluster parallelism and the vectorized execution of DataFusion therefore
  compound. No node scans the files of another node.
- **Pushdown.** Pushed-down projection, filter, LIMIT, Top-N, aggregation, and broadcast-eligible
  inner equi-joins keep each scan lean. The same path reaches Apache Iceberg and
  Databricks-managed Iceberg.
- **No materialization.** Every query starts from source metadata. The engine materializes nothing
  and copies nothing out.

## Documentation

Start at the [documentation index](docs/index.md), or go straight to a guide:

| Guide | What it covers |
|-------|----------------|
| [Install](docs/install.md) | One command installs the engine on any Exasol deployment: SaaS, Exasol AsApp, Docker, or on-premise. It uploads the `.so`, registers the Rust SLC, and creates the scripts. Build-from-source and fully manual paths are covered too, as appendices. |
| [Catalogs](docs/catalogs.md) | How to connect to Iceberg REST, AWS Glue, and Lakekeeper catalogs. Covers CONNECTION objects, credentials, and object-storage access. |
| [Benchmark](docs/benchmark.md) | The benchmark query set and how to run it yourself. |
| [Architecture](docs/architecture.md) | How cluster and DataFusion parallelism combine: file sharding, `GROUP BY shard_key` fan-out, and how pushdown meets parent-level Exasol execution. |
| [Capabilities](docs/capabilities.md) | Pushdown support matrix: what runs in DataFusion versus Exasol. |
| [Tuning](docs/tuning.md) | Configuration parameters reference and runtime telemetry. |
| [Debugging pushdown](docs/debugging-pushdown.md) | How to see exactly what the adapter pushes down for a query, with `EXPLAIN VIRTUAL`. |

## License

Free and open-source. Community-supported. Licensed under [MIT](LICENSE).

---

<div align="center">

Built with Rust 🦀 and made with ❤️. Maintained by [Exasol Labs 🧪](https://github.com/exasol-labs/).

</div>
