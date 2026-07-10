[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

Build the UDF `.so`, register the Rust SLC, upload the binary, then create the scripts,
the catalog CONNECTION, and the Virtual Schema. The `make` targets below automate the
build/upload steps against the bundled Docker stack; the SQL steps run on any Exasol
cluster.

## Prerequisites

- **Docker** — the `.so` is built inside `rust:1.94-bookworm` (glibc 2.36) to match the SLC.
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

Rebuilds only when crate sources/manifests/lock change. One `.so` exports **both** entry points (VS adapter + scan SCALAR UDF). The `LAKEHOUSE_DISTRIBUTE_FILES` distributor is a separate LUA SET script created by plain DDL — no `.so` symbol.

## 2. Register the Rust SLC

```sh
make install-slc               # downloads lc-rust 0.19.1, uploads to BucketFS, sets the RUST alias
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

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(spec VARCHAR(2000000))
EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE LUA SET SCRIPT LHVS.LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/
```

`EMITS (...)` is a placeholder — the adapter supplies concrete output columns per query.
`LAKEHOUSE_DISTRIBUTE_FILES` is a pure passthrough LUA SET script (not a Rust entry point) that
does the cross-node `GROUP BY shard_key` fan-out of the per-shard file lists ahead of the scalar
scan.

## 5. Create the catalog CONNECTION

Catalog URI goes in `TO`; S3 + warehouse credentials go in the `IDENTIFIED BY` JSON password.
The fields are the same across backends; only their values and a few flags differ.

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` (or `wh`) | yes | Iceberg warehouse location (`s3://…`, or an AWS account id for Glue) |
| `endpoint` | yes | S3 endpoint URL |
| `region` | yes | S3 region |
| `access_key` / `secret_key` | yes | S3 credentials |
| `session_token` | no | Temporary STS token |
| `path_style` | no | Path-style S3 addressing (`true` for MinIO; `false` for AWS S3) |
| `use_sigv4` | no | SigV4-sign the catalog REST requests (`true` for AWS Glue) |
| `use_vended_credentials` | no | Take short-lived S3 credentials vended by the catalog (Glue) |

Credential values never appear in error messages or logs, and are passed to the scan UDF
inside the per-query scan spec — never stored in VS properties.

### Local (MinIO + Iceberg REST)

For the bundled Docker stack. Note the internal hostnames and `path_style: true`.

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

### Production (AWS Glue + S3)

The validated production path. Catalog is the Glue Iceberg REST endpoint; `warehouse` is the
AWS **account id** (not an `s3://` path); SigV4 and vended credentials are on.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'https://glue.us-east-1.amazonaws.com/iceberg'
  USER ''
  IDENTIFIED BY '{
    "warehouse":              "123456789012",
    "endpoint":               "https://s3.us-east-1.amazonaws.com",
    "region":                 "us-east-1",
    "access_key":             "AKIA...",
    "secret_key":             "...",
    "session_token":          "...",
    "path_style":             false,
    "use_sigv4":              true,
    "use_vended_credentials": true
  }';
```

Field differences at a glance:

| Field | Local MinIO | AWS Glue |
|---|---|---|
| `TO` (catalog URI) | `http://iceberg-rest:8181` | `https://glue.<region>.amazonaws.com/iceberg` |
| `warehouse` | `s3://warehouse/` | AWS account id, e.g. `123456789012` |
| `endpoint` | `http://minio:9000` | `https://s3.<region>.amazonaws.com` |
| `path_style` | `true` | `false` |
| `use_sigv4` | `false` (omit) | `true` |
| `use_vended_credentials` | `false` (omit) | `true` |

### Databricks-managed Iceberg

Databricks-managed tables are reached through the same Iceberg REST path: point `TO` at the
Databricks Unity Catalog Iceberg REST endpoint and supply its auth in place of Glue's. The
exact endpoint/auth shape for a Databricks workspace is not yet exercised by the test suite in
this repo — treat the Glue recipe as the template and adjust the catalog URI and credential
flags to the Databricks endpoint.

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
| `SCAN_SCHEMA` | yes | Schema holding the `LAKEHOUSE_SCAN` scalar script and the `LAKEHOUSE_DISTRIBUTE_FILES` distributor |
| `ALLOW_HTTP` | no | `'true'` to allow plain-HTTP catalog/S3 (e.g. local MinIO) |
| `PARALLELISM_FACTOR` | no | Work-unit oversubscription multiplier (G = node_count × factor, capped 300) |
| `DATAFUSION_TARGET_PARTITIONS` | no | DataFusion target partition count per UDF |
| `DATAFUSION_THREADS_PER_UDF` | no | DataFusion worker threads per UDF instance |
| `MEMORY_POOL_FRACTION` | no | Fraction of the per-instance memory limit given to the DataFusion pool |
| `INSTANCE_OVERHEAD_MB` | no | Reserved non-pool overhead per instance, in MB |

## 7. Query

```sql
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

Projection, filter predicates, `LIMIT`, and aggregation are pushed down. See
[Capabilities](capabilities.md) for the full matrix.

## End-to-end tests

`make test-e2e` builds the `.so`, then runs the Rust E2E suite against the bundled stack
(Exasol + MinIO + Iceberg REST from `docker-compose.yml`). It seeds Iceberg tables
in-process, runs serially (`--test-threads=1`), and **fails — never skips — if no Exasol is
reachable**.

```sh
docker compose up -d
make test-e2e
```

Port overrides (host side; defaults match `docker-compose.yml`):

| Env var | Default | Service |
|---|---|---|
| `LH_EXASOL_PORT` | `28563` | Exasol SQL |
| `LH_BUCKETFS_PORT` | `22581` | BucketFS |
| `LH_MINIO_PORT` | `19000` | MinIO S3 |
| `LH_REST_PORT` | `18181` | Iceberg REST |

## Addressing note

The adapter UDF runs **inside** the Exasol container, so every address in the CONNECTION
and the VS properties must resolve from there — use internal hostnames (e.g.
`iceberg-rest`, `minio`), not `localhost` or the Docker host gateway.
