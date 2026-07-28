[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

The install puts the engine `.so` on BucketFS and registers its scripts. You then point a Virtual
Schema at your data. Pick the path that matches where your Exasol runs:

| You run on… | Path | What you run |
|---|---|---|
| **Exasol SaaS** | [One-command install](#exasol-saas-one-command-install) | One `curl … \| bash` |
| **Self-managed Exasol reachable from your machine** (BucketFS + SQL ports open) | [Automated build and upload](#self-managed-automated-build-and-upload) | Two `make` commands, then [create the scripts](#create-the-scripts) |
| **Restricted network** (no direct BucketFS access) | [Manual upload](#restricted-networks-manual-upload) | Download a release tarball, upload it, then [create the scripts](#create-the-scripts) |

Every path ends the same way. First [point the VS at your data](#point-the-vs-at-your-data), then
[query](#query). The catalog `CONNECTION` and the `CREATE VIRTUAL SCHEMA` statement are always
manual. They are specific to your dataset.

All paths assume that the **Rust SLC is already installed and registered**. The SLC version must
match the version this project's `exasol-udf-sdk` / `exasol-udf-macros` target (see `Cargo.toml`).
A version mismatch fails the fingerprint smoke test. For SLC install, see
[language-container-rs](https://github.com/exasol-labs/language-container-rs). The SaaS
one-command path registers the SLC for you.

## Exasol SaaS: one-command install

```bash
curl -fsSL https://github.com/exasol-labs/lakehouse-engine-rs/releases/download/v<VERSION>/install-saas.sh \
  | bash -s -- --account-id <ACCOUNT_ID> --database-id <DATABASE_ID> --profile <PROFILE>
```

This command authenticates to Exasol SaaS. It then automates every step up to a query-ready
install:

- It registers the Rust SLC.
- It uploads the engine tarball over a presigned URL.
- It runs the create-scripts DDL.
- It checks the load with the fingerprint smoke test.

The command is idempotent: it uses `CREATE OR REPLACE`, `CREATE SCHEMA IF NOT EXISTS`, and an
in-place `SCRIPT_LANGUAGES` swap. A second run therefore upgrades a prior install cleanly.

| Flag | Value |
|---|---|
| `--account-id` | SaaS account id, from the SaaS web console |
| `--database-id` | SaaS database id, from the SaaS web console |
| `--profile` | An `exapump` named profile. Its `password` supplies the SaaS access token |

After the command finishes, go to [point the VS at your data](#point-the-vs-at-your-data). The
script stops before the catalog `CONNECTION` and `CREATE VIRTUAL SCHEMA`. These two statements
stay manual.

## Self-managed: automated build and upload

If `exapump` and curl can reach both BucketFS and the DB SQL port directly, use this path. This
includes the [bundled Docker stack](#local-dev-stack).

```sh
make cross-musl-udf-build      # → target/release/liblakehouse_engine.so
make bucketfs-upload-so        # → BucketFS /default/udf/liblakehouse_engine.so
```

The build runs inside `rust:1.94-bookworm` (glibc 2.36, which matches the SLC). It rebuilds only
when crate sources, manifests, or the lockfile change. One `.so` exports **both** RUST entry
points (VS adapter + scan SCALAR UDF). This path requires Docker and
[`exapump`](https://github.com/exasol-labs/exapump).

Then [create the scripts](#create-the-scripts).

## Restricted networks: manual upload

If `exapump` and curl cannot reach the raw BucketFS ports, use this path. Every step is a
download, a `curl` command, or a UI action. This path needs no Docker, no Rust toolchain, and no
local build.

### 1. Download the release tarball

Every [GitHub Release](https://github.com/exasol-labs/lakehouse-engine-rs/releases) includes a
prebuilt `lakehouse-engine.tar.gz`. The archive contains the file as
`udf/liblakehouse_engine.so`:

```bash
curl -fsSL -o lakehouse-engine.tar.gz \
  https://github.com/exasol-labs/lakehouse-engine-rs/releases/download/v<VERSION>/lakehouse-engine.tar.gz
```

Pin `<VERSION>` to the release you intend to run. The `udf/liblakehouse_engine.so` layout inside
the tarball determines the `%udf_object` path in [create the scripts](#create-the-scripts).

### 2. Upload the tarball to BucketFS

Pick the channel that your platform exposes. BucketFS extracts known archives automatically on
upload. There is no separate extract step.

**a) BucketFS upload UI.** Use this channel on any platform with a file browser, for example the
"Files" tab in Exasol SaaS. Put `lakehouse-engine.tar.gz` at the bucket root. The file lands at
`buckets/bfsdefault/default/udf/liblakehouse_engine.so`.

**b) Raw HTTP PUT.** Use this channel for an on-prem or Docker BucketFS that you can reach over
the network. This channel needs no local `exapump` and no Docker.

```bash
curl -X PUT -T lakehouse-engine.tar.gz \
  "https://w:<BFS_WRITE_PASSWORD>@<HOST>:<BUCKETFS_PORT>/default/udf/lakehouse-engine.tar.gz" --insecure
```

`w` is the fixed BucketFS write-username. `--insecure` covers the self-signed Docker-db
certificate. Read the write password from `EXAConf` or from your platform's admin UI. The file
lands at `buckets/bfsdefault/default/udf/liblakehouse_engine.so`.

**c) Exasol SaaS REST API.** SaaS does not expose the raw BucketFS ports. This is therefore the
only non-UI channel on SaaS. The [one-command install](#exasol-saas-one-command-install) runs
these steps for you. If you need the individual steps, use this manual form instead. Authentication
uses `Authorization: Bearer <PAT>`, a SaaS personal access token from the web console. The API
needs your `accountID` and `databaseID`. No endpoint returns them. Read `accountID` from the
console, then match `databaseID` by name:

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

The presigned URL expires in ~600s. It is signed for `host` only. Add no extra headers. Run both
commands back-to-back. **SaaS puts the archive at a different path** than the other channels:
`/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`. Use that path in the next
step.

Then [create the scripts](#create-the-scripts).

## Create the scripts

The [SaaS one-command install](#exasol-saas-one-command-install) already ran this step. The two
other paths need it once, after the `.so` is on BucketFS.

One `.so` supplies both RUST entry points. The SLC dispatches them by script name. A third
script, a plain-LUA passthrough, fans the file lists out across nodes. Set `%udf_object` to the
path where your `.so` landed in the upload step:

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

`LAKEHOUSE_SCAN` takes two `VARCHAR` arguments:

- `common` is the shard-invariant scan-spec blob.
- `files` is the per-shard file list.

`EMITS (...)` is a placeholder. The adapter supplies concrete output columns per query.
`LAKEHOUSE_DISTRIBUTE_FILES` is a pure passthrough. It does the cross-node `GROUP BY shard_key`
fan-out before the scalar scan.

All three scripts MUST be in the same schema as `LAKEHOUSE_ADAPTER`, here `LHVS`. The adapter
qualifies its calls with its own running-script schema, not with a configured property.

### Fingerprint smoke test (optional)

This test needs no catalog credentials. It checks that the `.so` loaded and that its build matches
the SLC. After a manual upload, run this test:

```sql
SELECT LHVS.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1);
```

- `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected <sdk>:rustc_<ver>, found <sdk>:rustc_<ver>`
  means that the registered SLC and this project's `exasol-udf-sdk` / `exasol-udf-macros` version
  do not match. Check the installed SLC again.
- Any other error, for example a scan-spec deserialization error, means that the versions match.
  The placeholder arguments are not a valid scan spec. This error is expected.

## Point the VS at your data

Two statements finish the install: a catalog `CONNECTION`, then the Virtual Schema over it. The
following example is a complete local setup with MinIO and an Iceberg REST catalog:

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
'true'` permits plain-HTTP catalog and S3 access. Local MinIO needs this property.

- **AWS Glue, Lakekeeper, and the full credential-JSON reference** are in [Catalogs](catalogs.md).
- **Tuning properties** (`PARALLELISM_FACTOR`, memory pool sizing, DataFusion partitions/threads,
  and more) are in [Tuning](tuning.md).

## Query

```sql
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

The engine pushes down projection, filter predicates, `LIMIT`, and aggregation. For the full
matrix, see [Capabilities](capabilities.md).

## Addressing

The adapter UDF runs **inside** the Exasol container. Every address in the CONNECTION and in the
VS properties must resolve from there. Use internal hostnames, for example `iceberg-rest` and
`minio`. Never use `localhost` or the Docker host gateway.

## Local dev stack

For an evaluation or a throwaway environment, use `docker-compose.yml`. It starts Exasol, MinIO,
and an Iceberg REST catalog:

```sh
docker compose up -d
```

These are the default host ports. Environment variables override them.

- Exasol SQL `28563`
- BucketFS `22581`
- MinIO `19000`
- Iceberg REST `18181`

You can reach this local Exasol from your machine. Deploy to it with the
[automated path](#self-managed-automated-build-and-upload).

## End-to-end tests

`make test-e2e` builds the `.so`. It then runs the test suite against the bundled stack. Use it to
check your build and environment before you point the engine at real data. The suite seeds Iceberg
tables in-process and runs serially. If no Exasol is reachable, it **fails, never skips**:

```sh
docker compose up -d
make test-e2e
```

Port overrides (host side). The defaults match `docker-compose.yml`:

| Env var | Default | Service |
|---|---|---|
| `LH_EXASOL_PORT` | `28563` | Exasol SQL |
| `LH_BUCKETFS_PORT` | `22581` | BucketFS |
| `LH_MINIO_PORT` | `19000` | MinIO S3 |
| `LH_REST_PORT` | `18181` | Iceberg REST |
