<div align="center">

# lakehouse-engine-rs

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/exasol-labs/lakehouse-engine-rs/actions/workflows/ci.yml)
[![spec|driven](https://img.shields.io/badge/spec%7Cdriven-6f42c1.svg)](specs/)
[![Exasol|database](https://img.shields.io/badge/Exasol%7Cdatabase-004977.svg)](https://www.exasol.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

**In-place lakehouse query engine for Exasol — DataFusion in Rust UDFs, querying Iceberg and
Databricks tables straight from SQL. No caching, no data movement.**

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

`lakehouse-engine-rs` is an Exasol **Virtual Schema** that does more than translate and plan: it
runs the [DataFusion](https://datafusion.apache.org/) engine on the node, in place. The VS stays
thin — query translation, pushdown analysis, parallelization planning, result-schema mapping —
while all execution happens in disposable, node-local DataFusion runtimes inside Rust UDFs. Files
are sharded across nodes, scanned in parallel, then merged in Exasol. Every query is stateless: it
starts from source metadata and leaves nothing behind.

## Documentation

- [**docs/**](docs/index.md) — documentation index
- [Install & deploy](docs/install.md) — build the `.so`, register the SLC, create scripts + CONNECTION + VS. If `exapump`/curl can't reach BucketFS directly (e.g. Exasol SaaS), see [Install](docs/install.md) for a fully manual path — curl/UI upload plus hand-run SQL, no Docker required
- [Capabilities](docs/capabilities.md) — projection / filter / LIMIT / aggregation pushdown matrix
- [`specs/`](specs/) — design source of truth (spec-driven development via the `speq` skill)

## Crates

| Crate | Purpose |
|-------|---------|
| `crates/lakehouse-engine` | VS adapter + DataFusion scan SET UDF (`cdylib` + `rlib`) |
| `crates/vs-expression` | SQL expression translator (`rlib`) |

## License

Licensed under [MIT](LICENSE).

---

<div align="center">

Built with Rust 🦀 for Exasol.

Community-supported, maintained by [Exasol Labs 🧪](https://github.com/exasol-labs/).

</div>
