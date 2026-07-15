[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

With the Rust SLC already installed and registered (see [Prerequisites](#prerequisites)),
build the UDF `.so`, upload the binary, then create the scripts, the catalog CONNECTION, and
the Virtual Schema. The `make` targets below automate the build/upload steps against the
bundled Docker stack; the SQL steps run on any Exasol cluster.

| Path | When to use |
|---|---|
| Automated (steps below) | `exapump`/curl has direct network access to both BucketFS and the DB SQL port (e.g. the bundled Docker stack, or an on-prem cluster reachable from your machine). One command per artifact. |
| [Manual](#manual-install) | No direct BucketFS network access — e.g. Exasol SaaS, which exposes only a BucketFS upload UI and REST API, not the raw BucketFS ports. Every step is a `curl`/SQL command or a UI action, no Docker or `exapump` BucketFS access required. |

## Prerequisites

- **Docker** — the `.so` is built inside `rust:1.94-bookworm` (glibc 2.36) to match the SLC.
- **Rust toolchain** — host unit tests only (`cargo test`, debug). Never `cargo build --release` on the host: a host-glibc `.so` fails to load inside Exasol.
- **[`exapump`](https://github.com/exasol-labs/exapump)** — Exasol/BucketFS CLI used by the `make` targets.
- **The Rust SLC installed and registered** — see
  [language-container-rs](https://github.com/exasol-labs/language-container-rs) for install
  instructions. The installed SLC version must match this project's `exasol-udf-sdk` /
  `exasol-udf-macros` version (see `Cargo.toml`) — a mismatch fails the fingerprint smoke test
  below.
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

## 2. Upload the `.so`

```sh
make bucketfs-upload-so         # → BucketFS /default/udf/liblakehouse_engine.so
```

## 3. Create the scripts

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

## 4. Create the catalog CONNECTION

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
AWS **account id** (not an `s3://` path); SigV4 and vended credentials are on. The adapter
derives Glue's `catalogs/{account-id}` REST prefix internally — supply only the bare account id.

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

## 5. Create the Virtual Schema

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default'
  ALLOW_HTTP         = 'true';
```

| Property | Required | Meaning |
|---|---|---|
| `CATALOG_CONNECTION` | yes | Name of the CONNECTION object from step 4 |
| `ICEBERG_NAMESPACE` | yes | Iceberg namespace; **every table in it** is exposed as a virtual table |
| `ALLOW_HTTP` | no | `'true'` to allow plain-HTTP catalog/S3 (e.g. local MinIO) |
| `PARALLELISM_FACTOR` | no | Work-unit oversubscription multiplier (G = node_count × factor, capped 300) |
| `DATAFUSION_TARGET_PARTITIONS` | no | DataFusion target partition count per UDF |
| `DATAFUSION_THREADS_PER_UDF` | no | DataFusion worker threads per UDF instance |
| `MEMORY_POOL_FRACTION` | no | Fraction of the per-instance memory limit given to the DataFusion pool |
| `INSTANCE_OVERHEAD_MB` | no | Reserved non-pool overhead per instance, in MB |

## 6. Query

```sql
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

Projection, filter predicates, `LIMIT`, and aggregation are pushed down. See
[Capabilities](capabilities.md) for the full matrix.

## Manual install

Use this path when `exapump`/curl can't reach BucketFS directly — e.g. **Exasol SaaS**, which
exposes only a BucketFS upload UI and a presigned-URL REST API, never the raw BucketFS ports.
No Docker, no Rust toolchain, no local build — every step below is either downloading a prebuilt
release artifact, a plain `curl`/SQL command, or a UI action. It replaces steps 1-2 above (build +
`.so` upload); steps 4-6 (catalog CONNECTION, Virtual Schema, query) are unchanged and unaffected
by which path you took. The Rust SLC itself is a [prerequisite](#prerequisites) — install and
register it via [language-container-rs](https://github.com/exasol-labs/language-container-rs)
before starting here.

### Step 1 — Download the release tarball

Every [GitHub Release](https://github.com/exasol-labs/lakehouse-engine-rs/releases) ships a
prebuilt `lakehouse-engine.tar.gz`, already laid out as `udf/liblakehouse_engine.so` (executable
bit set) — download it as-is, no repackaging needed:

```bash
curl -fsSL -o lakehouse-engine.tar.gz \
  https://github.com/exasol-labs/lakehouse-engine-rs/releases/download/v<VERSION>/lakehouse-engine.tar.gz
```

Pin `<VERSION>` to the release you intend to run — e.g. `0.26.1` for the version this checkout's
`Cargo.toml` is at. The `udf/liblakehouse_engine.so` layout inside the tarball is what determines
the extracted path referenced in step 3's `%udf_object` below.

### Step 2 — Upload the tarball to BucketFS

Pick whichever channel your platform exposes:

#### a) BucketFS upload UI

Any platform with a BucketFS file browser (e.g. Exasol SaaS's "Files" tab): drop
`lakehouse-engine.tar.gz` at the bucket root. BucketFS auto-extracts recognized archives on
upload, so there's no separate "extract" step.

#### b) Raw HTTP PUT

For an on-prem/Docker BucketFS that's reachable over the network, but without `exapump` or Docker
installed locally — this is the same mechanism `bucketfs-upload-so` uses under the hood, given
here Makefile-independent:

```bash
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

Upload is a two-step presigned-URL dance:

```bash
curl -X POST -H "Authorization: Bearer <PAT>" \
  "https://cloud.exasol.com/api/v1/accounts/<accountID>/databases/<databaseID>/files/lakehouse-engine.tar.gz"
# → {"url": "<presigned PUT URL>"}

curl -X PUT --upload-file lakehouse-engine.tar.gz "<presigned PUT URL>"
```

The presigned URL expires in ~600s and is signed for `host` only — don't add extra headers, and
run both commands back-to-back.

Verify with `GET .../files` (the tarball should be listed). **Extracted path differs from the
on-prem default**: SaaS lands an uploaded `<name>.tar.gz` at
`/buckets/uploads/default/<name>/...` — e.g. `lakehouse-engine.tar.gz` extracts to
`/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so` — instead of the on-prem
default bucket's `/buckets/bfsdefault/default/...`.

### Step 3 — Create the scripts

Same DDL as [step 3 above](#3-create-the-scripts), just point `%udf_object` at your platform's
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

### Step 4 — Fingerprint smoke test

No catalog credentials needed — this alone proves the `.so` loaded and its `exasol-udf-sdk`/rustc
build matches the SLC:

```sql
SELECT LHVS.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1);
```

- `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected <sdk>:rustc_<ver>, found <sdk>:rustc_<ver>`
  → the registered SLC and this project's `exasol-udf-sdk`/`exasol-udf-macros` version (see
  `Cargo.toml`) don't match; re-check the SLC version installed per the
  [Prerequisites](#prerequisites).
- Any other error (e.g. a scan-spec deserialization error) → a match — the placeholder arguments
  just aren't a valid scan spec, which is expected.

From here, continue with **steps 4-6 above** (catalog CONNECTION, Virtual Schema, query) —
unchanged regardless of how the `.so` got onto BucketFS.

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
