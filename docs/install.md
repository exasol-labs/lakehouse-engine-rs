[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

Installing means getting the engine `.so` onto BucketFS and registering its scripts, then
pointing a Virtual Schema at your data. Pick the path that matches where your Exasol runs:

| You run on… | Path | What you run |
|---|---|---|
| **Exasol SaaS** | [One-command install](#exasol-saas-one-command-install) | One `curl … \| bash` |
| **Self-managed Exasol reachable from your machine** (BucketFS + SQL ports open) | [Automated build and upload](#self-managed-automated-build-and-upload) | Two `make` commands, then [create the scripts](#create-the-scripts) |
| **Restricted network** (no direct BucketFS access) | [Manual upload](#restricted-networks-manual-upload) | Download a release tarball, upload it, then [create the scripts](#create-the-scripts) |

Every path ends the same way: [point the VS at your data](#point-the-vs-at-your-data), then
[query](#query). The catalog `CONNECTION` and the `CREATE VIRTUAL SCHEMA` statement are always
manual, because they are specific to your dataset.

All paths assume the **Rust SLC is already installed and registered**, at the version this
project's `exasol-udf-sdk` / `exasol-udf-macros` targets (see `Cargo.toml`). A version mismatch
fails the fingerprint smoke test. See
[language-container-rs](https://github.com/exasol-labs/language-container-rs) for SLC install.
The SaaS one-command path registers the SLC for you.

## Exasol SaaS: one-command install

```bash
curl -fsSL https://github.com/exasol-labs/lakehouse-engine-rs/releases/download/v<VERSION>/install-saas.sh \
  | bash -s -- --account-id <ACCOUNT_ID> --database-id <DATABASE_ID> --profile <PROFILE>
```

This authenticates to Exasol SaaS and automates everything up to a query-ready install: it
registers the Rust SLC, uploads the engine tarball over a presigned URL, runs the create-scripts
DDL, and verifies the load with the fingerprint smoke test. It is idempotent (`CREATE OR REPLACE`,
`CREATE SCHEMA IF NOT EXISTS`, and an in-place `SCRIPT_LANGUAGES` swap), so re-running it upgrades
a prior install cleanly.

| Flag | Value |
|---|---|
| `--account-id` | SaaS account id, from the SaaS web console |
| `--database-id` | SaaS database id, from the SaaS web console |
| `--profile` | An `exapump` named profile; its `password` supplies the SaaS access token |

When it finishes, skip straight to [point the VS at your data](#point-the-vs-at-your-data). The
script stops before the catalog `CONNECTION` and `CREATE VIRTUAL SCHEMA`, which stay manual.

## Self-managed: automated build and upload

Use this when `exapump`/curl can reach both BucketFS and the DB SQL port directly, including the
[bundled Docker stack](#local-dev-stack).

```sh
make cross-musl-udf-build      # → target/release/liblakehouse_engine.so
make bucketfs-upload-so        # → BucketFS /default/udf/liblakehouse_engine.so
```

The build runs inside `rust:1.94-bookworm` (glibc 2.36, matching the SLC) and rebuilds only when
crate sources, manifests, or the lockfile change. One `.so` exports **both** RUST entry points
(VS adapter + scan SCALAR UDF). Requires Docker and [`exapump`](https://github.com/exasol-labs/exapump).

Then [create the scripts](#create-the-scripts).

## Restricted networks: manual upload

Use this when `exapump`/curl cannot reach the raw BucketFS ports. Every step is a download, a
`curl`, or a UI action. No Docker, no Rust toolchain, no local build.

### 1. Download the release tarball

Every [GitHub Release](https://github.com/exasol-labs/lakehouse-engine-rs/releases) ships a
prebuilt `lakehouse-engine.tar.gz`, already laid out as `udf/liblakehouse_engine.so`:

```bash
curl -fsSL -o lakehouse-engine.tar.gz \
  https://github.com/exasol-labs/lakehouse-engine-rs/releases/download/v<VERSION>/lakehouse-engine.tar.gz
```

Pin `<VERSION>` to the release you intend to run. The `udf/liblakehouse_engine.so` layout inside
the tarball determines the `%udf_object` path in [create the scripts](#create-the-scripts).

### 2. Upload the tarball to BucketFS

Pick whichever channel your platform exposes. BucketFS auto-extracts recognized archives on
upload, so there is no separate extract step.

**a) BucketFS upload UI.** Any platform with a file browser (e.g. Exasol SaaS's "Files" tab):
drop `lakehouse-engine.tar.gz` at the bucket root. Lands at
`buckets/bfsdefault/default/udf/liblakehouse_engine.so`.

**b) Raw HTTP PUT.** For an on-prem/Docker BucketFS reachable over the network but without
`exapump` or Docker locally. This is what `bucketfs-upload-so` does under the hood:

```bash
curl -X PUT -T lakehouse-engine.tar.gz \
  "https://w:<BFS_WRITE_PASSWORD>@<HOST>:<BUCKETFS_PORT>/default/udf/lakehouse-engine.tar.gz" --insecure
```

`w` is the fixed BucketFS write-username; `--insecure` covers the self-signed Docker-db cert. Read
the write password from `EXAConf` or your platform's admin UI. Lands at
`buckets/bfsdefault/default/udf/liblakehouse_engine.so`.

**c) Exasol SaaS REST API.** SaaS does not expose the raw BucketFS ports, so this is the only
non-UI channel there. (The [one-command install](#exasol-saas-one-command-install) does this
automatically; use this manual form only if you need the individual steps.) Auth is
`Authorization: Bearer <PAT>`, a SaaS personal access token from the web console. The API needs
your `accountID` and `databaseID`; there is no endpoint to discover them, so read `accountID` from
the console and match `databaseID` by name:

```bash
curl -H "Authorization: Bearer <PAT>" \
  https://cloud.exasol.com/api/v1/accounts/<accountID>/databases
```

Use `cloud-staging.exasol.com` on staging. Upload is a two-step presigned-URL exchange:

```bash
curl -X POST -H "Authorization: Bearer <PAT>" \
  "https://cloud.exasol.com/api/v1/accounts/<accountID>/databases/<databaseID>/files/lakehouse-engine.tar.gz"
# → {"url": "<presigned PUT URL>"}

curl -X PUT --upload-file lakehouse-engine.tar.gz "<presigned PUT URL>"
```

The presigned URL expires in ~600s and is signed for `host` only, so add no extra headers and run
both commands back-to-back. **SaaS lands the archive at a different path** than the other channels:
`/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`. Use that path in the next
step.

Then [create the scripts](#create-the-scripts).

## Create the scripts

The [SaaS one-command install](#exasol-saas-one-command-install) already did this. The two other
paths need it once, after the `.so` is on BucketFS.

One `.so` supplies both RUST entry points; the SLC dispatches them by script name. A third,
plain-LUA passthrough script fans the file lists out across nodes. Set `%udf_object` to wherever
your `.so` landed in the upload step:

- Automated, or manual via UI / raw PUT: `buckets/bfsdefault/default/udf/liblakehouse_engine.so`
- Manual via SaaS REST API: `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`

```sql
CREATE SCHEMA IF NOT EXISTS LHVS;

CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))
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

`LAKEHOUSE_SCAN` takes two `VARCHAR` arguments: `common` is the shard-invariant scan-spec blob and
`files` is the per-shard file list. `EMITS (...)` is a placeholder; the adapter supplies concrete
output columns per query. `LAKEHOUSE_DISTRIBUTE_FILES` is a pure passthrough that does the
cross-node `GROUP BY shard_key` fan-out ahead of the scalar scan.

All three scripts MUST live in the same schema as `LAKEHOUSE_ADAPTER` (here `LHVS`); the adapter
qualifies its calls using its own running-script schema, not a configured property.

### Fingerprint smoke test (optional)

This needs no catalog credentials. It alone proves the `.so` loaded and its build matches the SLC,
which is worth doing after a manual upload:

```sql
SELECT LHVS.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1);
```

- `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected <sdk>:rustc_<ver>, found <sdk>:rustc_<ver>`
  means the registered SLC and this project's `exasol-udf-sdk` / `exasol-udf-macros` version do not
  match; re-check the installed SLC.
- Any other error (e.g. a scan-spec deserialization error) is a match: the placeholder arguments
  just are not a valid scan spec, which is expected.

## Point the VS at your data

Two statements finish the install: a catalog `CONNECTION`, then the Virtual Schema over it. Here
is a complete local (MinIO + Iceberg REST) example:

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://iceberg-rest:8181'
  USER ''
  IDENTIFIED BY '{
    "warehouse":  "s3://warehouse/",
    "endpoint":   "http://minio:9000",
    "region":     "us-east-1",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "path_style": true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default'
  ALLOW_HTTP         = 'true';
```

`ICEBERG_NAMESPACE` exposes **every table in that namespace** as a virtual table. `ALLOW_HTTP =
'true'` permits plain-HTTP catalog/S3 access (needed for local MinIO).

- **AWS Glue, Databricks, and the full credential-JSON reference** are in [Catalogs](catalogs.md).
- **Tuning properties** (`PARALLELISM_FACTOR`, memory pool sizing, DataFusion partitions/threads,
  and more) are in [Tuning](tuning.md).

## Query

```sql
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

Projection, filter predicates, `LIMIT`, and aggregation are pushed down. See
[Capabilities](capabilities.md) for the full matrix.

## Addressing

The adapter UDF runs **inside** the Exasol container, so every address in the CONNECTION and the
VS properties must resolve from there. Use internal hostnames (e.g. `iceberg-rest`, `minio`),
never `localhost` or the Docker host gateway.

## Local dev stack

For E2E or a throwaway environment, `docker-compose.yml` brings up Exasol + MinIO + an Iceberg
REST catalog:

```sh
docker compose up -d
```

Default host ports (override via env): Exasol SQL `28563`, BucketFS `22581`, MinIO `19000`,
Iceberg REST `18181`. This local Exasol is reachable from your machine, so deploy to it with the
[automated path](#self-managed-automated-build-and-upload).

## End-to-end tests

`make test-e2e` builds the `.so`, then runs the Rust E2E suite against the bundled stack. It seeds
Iceberg tables in-process, runs serially (`--test-threads=1`), and **fails, never skips, if no
Exasol is reachable**:

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
</content>
