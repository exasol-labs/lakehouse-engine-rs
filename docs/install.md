[lakehouse-engine](../README.md) › [Docs](index.md) › Install

---

# Install & Deploy

`deploy/scripts/install.sh` installs the engine on any Exasol deployment. This includes Exasol
SaaS, Exasol AsApp, Docker, and on-premise. Exasol AsApp, Docker, and on-premise all use the same
BucketFS interface.

One command detects your target, registers the Rust SLC, uploads the engine, creates the scripts,
and runs a smoke test. After the command finishes, you point a Virtual Schema at your data. That
last step stays manual because it is specific to your dataset.

The [build-from-source](#appendix-build-from-source) and [fully manual](#appendix-manual-install-on-a-restricted-network)
paths still work. If the one-line command does not fit your environment, use them instead. One
example is a network with no path to GitHub.

## Prerequisites

- `curl` and `tar` on the machine that runs the install command.
- [`exapump`](https://github.com/exasol-labs/exapump), with at least one profile already
  configured (`exapump profile add <name> --host <host> --user <user> --password <password>`,
  or `exapump profile init`). This is true for BucketFS targets even when you connect with
  `--dsn` or `--host`. The reason: `exapump bucketfs cp` always reads its connection from a
  profile, plus any `--bfs-*` overrides you give.
- A GitHub token with read access to this repo (private) and to
  [`language-container-rs`](https://github.com/exasol-labs/language-container-rs) (public). The
  install script uses one token for both.

## Install with one command

Because this repo is private, download the script through the authenticated GitHub contents
API. Do not use a plain `releases/download/...` URL: that URL does not work on a private repo.

### Exasol SaaS

```bash
export GITHUB_TOKEN=<your-token>

curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.raw" \
  https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh \
| bash -s -- --account-id <ACCOUNT_ID> --database-id <DATABASE_ID> --profile <PROFILE>
```

Give both `--account-id` and `--database-id` to select the SaaS target. Get both values from the
SaaS web console.

### Exasol AsApp, Docker, or on-premise (BucketFS)

Give neither `--account-id` nor `--database-id` to select the BucketFS target. This target
covers the [bundled Docker stack](#local-dev-stack) too.

With a profile that already has `bfs_host` and `bfs_write_password` set:

```bash
export GITHUB_TOKEN=<your-token>

curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.raw" \
  https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh \
| bash -s -- --profile <PROFILE>
```

With direct connection flags instead of a profile's connection fields, give the BucketFS flags
explicitly:

```bash
export GITHUB_TOKEN=<your-token>

curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.raw" \
  https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh \
| bash -s -- --host <host:port> --user <user> --password <password> \
    --bfs-host <bfs-host> --bfs-write-password <bfs-write-password>
```

You still need a configured `exapump` profile for this second form. See
[Prerequisites](#prerequisites).

## What the command does

1. It reads your flags and picks the SaaS target or the BucketFS target.
2. Unless you give `--skip-slc`, it downloads and registers the Rust SLC. It updates the list of
   registered languages in place, so every other language you already registered stays intact.
3. It downloads the `lakehouse-engine.tar.gz` release, pinned or latest, and uploads the engine
   file. On a BucketFS target it uses `exapump bucketfs cp`. On a SaaS target it uses a
   presigned-URL exchange.
4. It creates the schema (`LHVS` by default) and its three scripts: `LAKEHOUSE_ADAPTER`,
   `LAKEHOUSE_SCAN`, and `LAKEHOUSE_DISTRIBUTE_FILES`.
5. It runs a fingerprint smoke test. The test checks that the uploaded file matches the
   registered SLC.
6. It stops there and prints a `CONNECTION` and `CREATE VIRTUAL SCHEMA` template for you to edit.
   It creates no dataset-specific object itself.

The command is idempotent. It uses `CREATE OR REPLACE`, `CREATE SCHEMA IF NOT EXISTS`, and an
in-place language-list update. Run it again on a prior install to upgrade it.

## Flags

| Flag | Meaning |
|---|---|
| `--profile <name>` | An `exapump` named profile. One of three connectivity flags. Give exactly one. |
| `--dsn <dsn>` | A direct `exapump` DSN. You can set `EXAPUMP_DSN` instead. |
| `--host <host:port> --user <u> --password <p>` | A direct connection. `--host` must include the port. There is no separate `--port` flag. |
| `--github-token <token>` | Required in both targets. You can set `GITHUB_TOKEN` instead. |
| `--account-id <id>` | SaaS target only. SaaS account ID, from the SaaS web console. |
| `--database-id <id>` | SaaS target only. SaaS database ID, from the SaaS web console. |
| `--staging` | SaaS target only. Targets `cloud-staging.exasol.com` instead of `cloud.exasol.com`. |
| `--bfs-host <host>` | BucketFS target only. Default: the profile's `bfs_host`, else its host. |
| `--bfs-port <port>` | BucketFS target only. Default: the profile's `bfs_port`, else `2581`. |
| `--bfs-bucket <name>` | BucketFS target only. Default: `default`. |
| `--bfs-write-password <p>` | BucketFS target only. Default: the profile's `bfs_write_password`. |
| `--target <saas\|bucketfs>` | Both targets. Asserts the target you expect. If it does not match the detected target, the command stops with an error. |
| `--schema <name>` | Both targets. Default: `LHVS`. |
| `--lakehouse-version <v>` | Both targets. Pins the engine version. Default: latest release. |
| `--slc-version <v>` | Both targets. Pins the SLC version. Default: latest release. |
| `--skip-slc` | Both targets. Skips the SLC download and registration. Every other step still runs. |
| `--help` | Prints this reference. Needs no network access and no credentials. |

`--account-id` and `--database-id` together select the SaaS target. Neither flag selects the
BucketFS target. One flag without the other is an error: the command names both flags and stops.

Run `bash deploy/scripts/install.sh --help` to see the same reference from the script itself.

## Pinning a version

Pin the engine and the SLC to matching versions with `--lakehouse-version` and `--slc-version`:

```bash
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.raw" \
  "https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/contents/deploy/scripts/install.sh?ref=v0.9.0" \
| bash -s -- --profile <PROFILE> --lakehouse-version 0.9.0 --slc-version <slc-version>
```

The `?ref=v0.9.0` part of the URL pins the script itself to that tag. Pass matching
`--lakehouse-version` and `--slc-version` flags too, so the script and the artifacts it
downloads agree.

## Skip the SLC step

If the SLC is already registered, or if your account has no `ALTER SYSTEM` privilege on a
restrictive tenant, add `--skip-slc`. Every other step still runs: engine upload, script
creation, and the smoke test.

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

You can reach this local Exasol from your machine. Install to it with the
[BucketFS one-line command](#exasol-asapp-docker-or-on-premise-bucketfs).

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

---

## Appendix: build from source

If you develop the engine, or if the one-line command does not cover your build, use this path.

```sh
make cross-musl-udf-build      # → target/release/liblakehouse_engine.so
make bucketfs-upload-so        # → BucketFS udf/liblakehouse_engine.so
```

The build runs inside `rust:1.94-bookworm` (glibc 2.36, which matches the SLC). It rebuilds only
when crate sources, manifests, or the lockfile change. One `.so` exports **both** RUST entry
points (VS adapter + scan SCALAR UDF). This path needs Docker and
[`exapump`](https://github.com/exasol-labs/exapump). It also needs the Rust SLC already
registered, because it uploads only the engine file, not the SLC. Before you use this path,
register the SLC once. Run the [one-line command](#install-with-one-command) without `--skip-slc`,
or follow [language-container-rs](https://github.com/exasol-labs/language-container-rs) for a
standalone SLC install.

Then go to [Create the scripts by hand](#appendix-create-the-scripts-by-hand).

## Appendix: manual install on a restricted network

If `exapump` and `curl` cannot reach the raw BucketFS ports, and the GitHub API is also
unreachable, use this path. Every step is a download, a `curl` command, or a UI action.
This path needs no Docker, no Rust toolchain, and no local build.

### 1. Download the release tarball

Every [GitHub Release](https://github.com/exasol-labs/lakehouse-engine-rs/releases) includes a
prebuilt `lakehouse-engine.tar.gz`. This repo is private, so a plain `releases/download/...` URL
does not work. Download the asset through the authenticated GitHub API instead, in two steps.

First, look up the release and find the asset ID for `lakehouse-engine.tar.gz`:

```bash
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
  "https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/releases/tags/v<VERSION>"
```

Pin `<VERSION>` to the release you install. Find the `id` field of the asset named
`lakehouse-engine.tar.gz` in the response. Then download that asset by ID:

```bash
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/octet-stream" \
  "https://api.github.com/repos/exasol-labs/lakehouse-engine-rs/releases/assets/<ASSET_ID>" \
  -o lakehouse-engine.tar.gz
```

The archive contains the file at `udf/liblakehouse_engine.so`.

### 2. Upload the tarball to BucketFS

Pick the channel that your platform exposes. BucketFS extracts an uploaded `X.tar.gz`
automatically, into a new sibling directory named `X`. There is no separate extract step. This
extraction adds one directory level. Upload `lakehouse-engine.tar.gz` inside a directory named
`udf/`. It extracts to `udf/lakehouse-engine/`. The `.so` inside that directory then lands
at `udf/lakehouse-engine/udf/liblakehouse_engine.so`.

**a) BucketFS upload UI.** Use this channel on any platform with a file browser, for example the
"Files" tab in Exasol SaaS. Put `lakehouse-engine.tar.gz` inside the `udf/` directory at the
bucket root. The file lands at
`buckets/bfsdefault/default/udf/lakehouse-engine/udf/liblakehouse_engine.so`.

**b) Raw HTTP PUT.** Use this channel for an on-premise or Docker BucketFS that you can reach
over the network. This channel needs no local `exapump` and no Docker.

```bash
curl -X PUT -T lakehouse-engine.tar.gz \
  "https://w:<BFS_WRITE_PASSWORD>@<HOST>:<BUCKETFS_PORT>/default/udf/lakehouse-engine.tar.gz" --insecure
```

`w` is the fixed BucketFS write username. `--insecure` accepts the self-signed Docker database
certificate. Read the write password from `EXAConf`, or from your platform's admin UI. The file
lands at the same path as channel (a):
`buckets/bfsdefault/default/udf/lakehouse-engine/udf/liblakehouse_engine.so`.

**c) Exasol SaaS REST API.** SaaS does not expose the raw BucketFS ports. This is the only
non-UI channel on SaaS. The [one-line command](#install-with-one-command) runs these steps for
you. If you need the individual steps, use this manual form instead. Authentication uses
`Authorization: Bearer <PAT>`, a SaaS personal access token from the web console. The API needs
your `accountID` and `databaseID`. No endpoint returns them together. Read `accountID` from the
console. Then match `databaseID` by name:

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

The presigned URL expires in about 600 seconds. It is signed for `host` only. Add no extra
headers. Run both commands one after the other. **SaaS puts the archive at a different path**
than channels (a) and (b): `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`.
Use that path in the next step.

Then go to [Create the scripts by hand](#appendix-create-the-scripts-by-hand).

## Appendix: create the scripts by hand

The [one-line command](#install-with-one-command) already runs this step for you. Run it
yourself only after [build from source](#appendix-build-from-source) or a
[manual upload](#appendix-manual-install-on-a-restricted-network).

One `.so` supplies both RUST entry points. The SLC dispatches them by script name. A third
script, a plain-LUA passthrough, fans the file lists out across nodes. Set `%udf_object` to the
path where your `.so` landed in the upload step:

- Build from source, or manual upload via UI or raw PUT:
  `buckets/bfsdefault/default/udf/lakehouse-engine/udf/liblakehouse_engine.so`
- Manual upload via the SaaS REST API:
  `/buckets/uploads/default/lakehouse-engine/udf/liblakehouse_engine.so`

```sql
CREATE SCHEMA IF NOT EXISTS LHVS;

CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object buckets/bfsdefault/default/udf/lakehouse-engine/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SCALAR SCRIPT LHVS.LAKEHOUSE_SCAN(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/lakehouse-engine/udf/liblakehouse_engine.so
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

## Appendix: fingerprint smoke test by hand

The [one-line command](#install-with-one-command) already runs this test for you. Run it
yourself only after [create the scripts by hand](#appendix-create-the-scripts-by-hand).

This test needs no catalog credentials. It checks that the `.so` loaded and that its build
matches the SLC. After a manual upload, run this test:

```sql
SELECT LHVS.LAKEHOUSE_SCAN('x', 'y') EMITS (r VARCHAR(2000000)) FROM (SELECT 1);
```

- `F-UDF-CL-RUST-9001: Fingerprint mismatch: expected <sdk>:rustc_<ver>, found <sdk>:rustc_<ver>`
  means that the registered SLC and this project's `exasol-udf-sdk` / `exasol-udf-macros` version
  do not match. Check the installed SLC again.
- Any other error, for example a scan-spec deserialization error, means that the versions match.
  The placeholder arguments are not a valid scan spec. This error is expected.
