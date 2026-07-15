[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

Build the UDF `.so`, register the Rust SLC, upload the binary, then create the scripts,
the catalog CONNECTION, and the Virtual Schema. The `make` targets below automate the
build/upload steps against the bundled Docker stack; the SQL steps run on any Exasol
cluster.

| Path | When to use |
|---|---|
| Automated (steps below) | `exapump`/curl has direct network access to both BucketFS and the DB SQL port (e.g. the bundled Docker stack, or an on-prem cluster reachable from your machine). One command per artifact. |
| [Manual](#manual-install) | No direct BucketFS network access — e.g. Exasol SaaS, which exposes only a BucketFS upload UI and REST API, not the raw BucketFS ports. Every step is a `curl`/SQL command or a UI action, no Docker or `exapump` BucketFS access required. |

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
make install-slc               # downloads lc-rust 0.21.0, uploads to BucketFS, sets the RUST alias
```

Uploads the SLC to BucketFS `/default/slc/` and registers `SCRIPT_LANGUAGES` with a `RUST=` alias (replacing any existing one).

## 3. Upload the `.so`

```sh
make bucketfs-upload-so         # → BucketFS /default/udf/liblakehouse_engine.so
```

## 4. Create the scripts

All four entry points come from the one `.so` (three RUST scripts) plus one plain LUA passthrough
script; the SLC dispatches the RUST ones by script name.

```sql
CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_DISTINCT_MERGE_COUNT(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
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

`LAKEHOUSE_SCAN` takes two `VARCHAR` arguments: `common` is the shard-invariant scan-spec blob
(shared across all shards) and `files` is the per-shard file list; `EMITS (...)` is a placeholder —
the adapter supplies concrete output columns per query.
`LAKEHOUSE_DISTINCT_MERGE_COUNT` is the merge step for single-group `COUNT(DISTINCT)`: it takes the
JSON array-of-arrays of per-shard local distinct sets and returns the global distinct cardinality.
`LAKEHOUSE_DISTRIBUTE_FILES` is a pure passthrough LUA SET script (not a Rust entry point) that
does the cross-node `GROUP BY shard_key` fan-out of the per-shard file lists ahead of the scalar
scan.

All three RUST scripts and `LAKEHOUSE_DISTRIBUTE_FILES` MUST be created in the same schema as
`LAKEHOUSE_ADAPTER` (here, `LHVS`) — the adapter qualifies its calls to them using its own
running-script schema, not a configured property.

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
  ALLOW_HTTP         = 'true';
```

| Property | Required | Meaning |
|---|---|---|
| `CATALOG_CONNECTION` | yes | Name of the CONNECTION object from step 5 |
| `ICEBERG_NAMESPACE` | yes | Iceberg namespace; **every table in it** is exposed as a virtual table |
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

## Manual install

Use this path when `exapump`/curl can't reach BucketFS directly — e.g. **Exasol SaaS**, which
exposes only a BucketFS upload UI and a presigned-URL REST API, never the raw BucketFS ports.
Every step below is a plain `curl`/SQL command (or a UI action) — no Docker or `exapump` BucketFS
access required. It replaces steps 2-3 above (SLC registration + `.so` upload); steps 5-7
(catalog CONNECTION, Virtual Schema, query) are unchanged and unaffected by which path you took.

### Step 1 — Package the artifacts

The SLC tarball is already a `.tar.gz` — download it straight from GitHub Releases (same URL
`make install-slc` uses; `SLC_VERSION` must be pinned to match the `exasol-udf-sdk`/
`exasol-udf-macros` version in `Cargo.toml` — a mismatch fails the fingerprint smoke test in
step 5 below):

```bash
curl -fsSL -o rustslc.tar.gz \
  https://github.com/exasol-labs/language-container-rs/releases/download/v<SLC_VERSION>/lc-rust-<SLC_VERSION>.tar.gz
```

The `.so` is built the same way as step 1 above (`make cross-musl-udf-build` →
`target/release/liblakehouse_engine.so`) — building never needs BucketFS/network access. Some
upload channels (the SaaS file API, confirmed against the staging cluster) reject raw
extension-less binaries, so package it into a tarball first:

```bash
mkdir -p pkg/udf
cp target/release/liblakehouse_engine.so pkg/udf/
chmod 755 pkg/udf/liblakehouse_engine.so
tar -czf lakehouse-engine.tar.gz -C pkg udf
```

This `udf/liblakehouse_engine.so` layout is what determines the extracted path referenced in
step 4's `%udf_object` below — keep it as-is unless you have a reason to rename.

### Step 2 — Upload both tarballs to BucketFS

Pick whichever channel your platform exposes:

#### a) BucketFS upload UI

Any platform with a BucketFS file browser (e.g. Exasol SaaS's "Files" tab): drop `rustslc.tar.gz`
and `lakehouse-engine.tar.gz` at the bucket root. BucketFS auto-extracts recognized archives on
upload, so there's no separate "extract" step.

#### b) Raw HTTP PUT

For an on-prem/Docker BucketFS that's reachable over the network, but without `exapump` or Docker
installed locally — this is the same mechanism `make install-slc`/`bucketfs-upload-so` use under
the hood, given here Makefile-independent:

```bash
curl -X PUT -T rustslc.tar.gz \
  "https://w:<BFS_WRITE_PASSWORD>@<HOST>:<BUCKETFS_PORT>/default/slc/rustslc.tar.gz" --insecure

curl -X PUT -T lakehouse-engine.tar.gz \
  "https://w:<BFS_WRITE_PASSWORD>@<HOST>:<BUCKETFS_PORT>/default/udf/lakehouse-engine.tar.gz" --insecure
```

`w` is the fixed BucketFS write-username; `--insecure` covers the self-signed Docker-db cert. Read
the write password from `EXAConf` as the Makefile does, or from your platform's admin UI.

#### c) Exasol SaaS REST API

SaaS doesn't expose the raw BucketFS ports at all, so on SaaS this is the only path that isn't the
UI. A couple of SaaS-specific things to know first:

- Auth is `Authorization: Bearer <PAT>` — a SaaS personal access token, from the SaaS web console.
- The API needs your SaaS `accountID` and the target `databaseID` — there is **no** endpoint to
  discover them; get `accountID` from the SaaS console, then confirm `databaseID` by listing
  databases in that account and matching by name:
  ```bash
  curl -H "Authorization: Bearer <PAT>" \
    https://cloud.exasol.com/api/v1/accounts/<accountID>/databases
  ```
  Use `cloud-staging.exasol.com` instead of `cloud.exasol.com` on the staging environment.

Upload is a two-step presigned-URL dance, once per tarball:

```bash
curl -X POST -H "Authorization: Bearer <PAT>" \
  "https://cloud.exasol.com/api/v1/accounts/<accountID>/databases/<databaseID>/files/lakehouse-engine.tar.gz"
# → {"url": "<presigned PUT URL>"}

curl -X PUT --upload-file lakehouse-engine.tar.gz "<presigned PUT URL>"
```

Repeat for `rustslc.tar.gz`. The presigned URL expires in ~600s and is signed for `host` only —
don't add extra headers, and run both commands back-to-back.

Verify with `GET .../files` (both tarballs should be listed). **Extracted path differs from the
on-prem default**: SaaS lands an uploaded `<name>.tar.gz` at
`/buckets/uploads/default/<name>/...` — e.g. `lakehouse-engine.tar.gz` extracts to
`/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so` — instead of the on-prem
default bucket's `/buckets/bfsdefault/default/...`.

### Step 3 — Register the SLC via plain SQL

This step only needs DB SQL access — any SQL client works (`exapump sql`, JDBC/ODBC, etc.), and
it's unaffected by which BucketFS channel you used in step 2. Read the current value first so you
don't clobber another alias sharing the same system variable — **some platforms (including some
Exasol SaaS instances) may already have a `RUST=` alias pre-provisioned and shared**, so check
before overwriting it:

```sql
SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES';
```

Then set it, preserving any other language aliases and pointing the `RUST=` entry at wherever
step 2 uploaded the SLC (SaaS path shown; swap in the on-prem `bfsdefault` path if you used
channel a/b):

```sql
ALTER SYSTEM SET SCRIPT_LANGUAGES = '<preserved aliases...> RUST=localzmq+protobuf:///uploads/default/rustslc?lang=rust#buckets/uploads/default/rustslc/exaudf/exaudfclient';
```

### Step 4 — Create the scripts

Same DDL as [step 4 above](#4-create-the-scripts), just point `%udf_object` at your platform's
extracted path — SaaS example:

```sql
CREATE SCHEMA IF NOT EXISTS LHVS;

CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_DISTINCT_MERGE_COUNT(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object /buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so
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

### Step 5 — Fingerprint smoke test

No catalog credentials needed — this alone proves the `.so` loaded and its `exasol-udf-sdk`/rustc
build matches the SLC:

```sql
SELECT LHVS.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1);
```

- `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected <sdk>:rustc_<ver>, found <sdk>:rustc_<ver>`
  → the SLC and the `exasol-udf-sdk` version this `.so` was built against don't match; re-check
  the `SLC_VERSION` pin against `Cargo.toml`'s `exasol-udf-sdk`/`exasol-udf-macros` version.
- Any other error (e.g. a scan-spec deserialization error) → a match — the placeholder arguments
  just aren't a valid scan spec, which is expected.

From here, continue with **steps 5-7 above** (catalog CONNECTION, Virtual Schema, query) —
unchanged regardless of how the SLC/`.so` got onto BucketFS.

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
