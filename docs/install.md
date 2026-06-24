[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

Build the UDF `.so`, register the Rust SLC, upload the binary, then create the scripts,
the catalog CONNECTION, and the Virtual Schema. The `make` targets below automate the
build/upload steps against the bundled Docker stack; the SQL steps run on any Exasol
cluster.

## Prerequisites

- **Docker** — the `.so` is built inside `rust:1.92-bookworm` (glibc 2.36) to match the SLC.
- **Rust toolchain** — host unit tests only (`cargo test`, debug). Never `cargo build --release` on the host: a host-glibc `.so` fails to load inside Exasol.
- **[`exapump`](https://github.com/exasol-labs/exapump)** — Exasol/BucketFS CLI used by the `make` targets.
- **An Exasol cluster + BucketFS**, an **Iceberg REST catalog**, and **S3-compatible storage**.
- All DSNs include `validateservercertificate=0` (self-signed Docker cert).

## 0. Local stack (optional)

For E2E or a throwaway environment, `docker-compose.yml` brings up Exasol + MinIO + an Iceberg REST catalog:

```sh
docker compose up -d
```

Default host ports (override via env): Exasol SQL `28563`, BucketFS `22581`, MinIO `19000`, Iceberg REST `18181`.

## 1. Build the `.so`

```sh
make cross-musl-udf-build      # → target/release/liblakehouse_engine.so
```

Rebuilds only when crate sources/manifests/lock change. One `.so` exports **both** entry points (VS adapter + scan SET UDF).

## 2. Register the Rust SLC

```sh
make install-slc               # downloads lc-rust 0.14.0, uploads to BucketFS, sets the RUST alias
```

Uploads the SLC to BucketFS `/default/slc/` and registers `SCRIPT_LANGUAGES` with a `RUST=` alias (replacing any existing one).

## 3. Upload the `.so`

```sh
make bucketfs-upload-so         # → BucketFS /default/udf/liblakehouse_engine.so
```

## 4. Create the scripts

Both entry points come from the one `.so`; the SLC dispatches by script name.

```sql
CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SET SCRIPT LHVS.LAKEHOUSE_SCAN(spec VARCHAR(2000000))
EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/
```

`EMITS (...)` is a placeholder — the adapter supplies concrete output columns per query.

## 5. Create the catalog CONNECTION

Catalog URI goes in `TO`; S3 + warehouse credentials go in the `IDENTIFIED BY` JSON password.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://iceberg-rest:8181'
  USER ''
  IDENTIFIED BY '{
    "warehouse":   "s3://warehouse/",
    "endpoint":    "http://minio:9000",
    "region":      "us-east-1",
    "access_key":  "minioadmin",
    "secret_key":  "minioadmin",
    "path_style":  true
  }';
```

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` (or `wh`) | yes | Iceberg warehouse location (`s3://…`) |
| `endpoint` | yes | S3 endpoint URL |
| `region` | yes | S3 region |
| `access_key` / `secret_key` | yes | S3 credentials |
| `session_token` | no | Temporary-credential token |
| `path_style` | no | Path-style S3 addressing (`true` for MinIO) |
| `use_vended_credentials` | no | Take S3 credentials vended by the catalog instead |

## 6. Create the Virtual Schema

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default'
  SCAN_SCHEMA        = 'LHVS'
  ALLOW_HTTP         = 'true';
```

| Property | Required | Meaning |
|---|---|---|
| `CATALOG_CONNECTION` | yes | Name of the CONNECTION object from step 5 |
| `ICEBERG_NAMESPACE` | yes | Iceberg namespace; **every table in it** is exposed as a virtual table |
| `SCAN_SCHEMA` | yes | Schema holding the `LAKEHOUSE_SCAN` SET script |
| `ALLOW_HTTP` | no | `'true'` to allow plain-HTTP catalog/S3 (e.g. local MinIO) |
| `PARALLELISM_FACTOR` | no | Work-unit oversubscription multiplier (G = node_count × factor, capped 300) |
| `DATAFUSION_TARGET_PARTITIONS` | no | DataFusion target partition count per UDF |
| `DATAFUSION_THREADS_PER_UDF` | no | DataFusion worker threads per UDF instance |
| `MEMORY_POOL_FRACTION` | no | Fraction of the per-instance memory limit given to the DataFusion pool |
| `INSTANCE_OVERHEAD_MB` | no | Reserved non-pool overhead per instance, in MB |
| `CONNECTION_NAME` | no | Connect-back CONNECTION used to capture the cluster node count |

## 7. Query

```sql
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

Projection, filter predicates, `LIMIT`, and aggregation are pushed down. See
[Capabilities](capabilities.md) for the full matrix.

## Addressing note

The adapter UDF runs **inside** the Exasol container, so every address in the CONNECTION
and the VS properties must resolve from there — use internal hostnames (e.g.
`iceberg-rest`, `minio`), not `localhost` or the Docker host gateway.
