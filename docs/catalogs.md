[lakehouse-engine](../README.md) › [Docs](index.md) › Catalogs

---

# Catalogs

The adapter reaches a catalog through one of four catalog kinds, selected by the VS property `CATALOG_KIND`:

- An **Iceberg REST catalog** for Iceberg tables. This is the default. Select it by leaving `CATALOG_KIND` absent. The literal `'ICEBERG_REST'` is not a recognized value.
- A **native Unity Catalog** (`CATALOG_KIND = 'UNITY_CATALOG'`) for Delta and Parquet tables.
- The **AWS Glue Data Catalog** (`CATALOG_KIND = 'GLUE'`) for Iceberg and Hive Parquet tables.
- **Direct storage** (`CATALOG_KIND = 'DIRECT_STORAGE'`) for raw Parquet files with no catalog.

Each catalog kind has its own client. Within the Iceberg REST kind, every backend is the SAME client; the backends differ only by the auth mode that you turn on in the CONNECTION password JSON. Three Iceberg REST auth modes exist:

- no auth, for a local stack
- AWS SigV4, for the Glue Iceberg REST endpoint
- a static bearer token or OAuth2 client credentials, for a generic secured REST catalog

Lakekeeper is a concrete instance of the last mode. The native Unity Catalog kind reuses the same
`token` / `client_id`+`client_secret` catalog-auth fields (Databricks OAuth machine-to-machine, in
that case) but never SigV4 — see [Unity Catalog](#unity-catalog-delta-and-parquet-tables) below.

Find the row that matches your catalog. Then copy its recipe.

## What is supported today

| Catalog | Catalog kind | Auth mode | Status |
|---|---|---|---|
| [Local / generic Iceberg REST (no auth)](#local--generic-iceberg-rest-no-auth) | Iceberg REST | none | Supported |
| [AWS Glue Iceberg REST](#aws-glue-iceberg-rest-sigv4) | Iceberg REST | SigV4 | Supported |
| [AWS Glue Data Catalog (Iceberg and Hive Parquet tables)](#aws-glue-data-catalog-catalog_kind--glue) | Glue | SigV4 with static IAM credentials | Supported |
| [Generic REST with token / OAuth2](#generic-rest-with-static-token-or-oauth2) | Iceberg REST | bearer token or OAuth2 | Supported |
| [Lakekeeper](#lakekeeper-oidc-via-keycloak--seaweedfs) | Iceberg REST | OAuth2 client-credentials (OIDC) | Supported |
| [Unity Catalog (Delta and Parquet tables)](#unity-catalog-delta-and-parquet-tables) | Unity Catalog | none, PAT, or Databricks OAuth M2M | Supported |
| [Direct storage (raw Parquet, no catalog)](#direct-storage-raw-parquet-no-catalog) | Direct storage | none (storage credentials only) | Supported |

The steps here cover only the catalog CONNECTION and the Virtual Schema. They are the
[Point the VS at your data](install.md#point-the-vs-at-your-data) step of [Install](install.md).
First copy the `.so` to BucketFS and create the scripts.

## Connection fields

The catalog URI goes in the `TO` clause of the CONNECTION. Every credential field and behavior field goes in the `IDENTIFIED BY` JSON password object. The field set is identical for all backends. Only the values and a few flags change.

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` | yes under `ICEBERG_REST`; optional under `GLUE`; not used under `UNITY_CATALOG` | Catalog routing identifier — an AWS account id under Glue, a warehouse **name** under Lakekeeper, or whatever identifier a generic Iceberg REST catalog registered — never read as a storage location, so a URI-shaped value (as the bundled `iceberg-rest`/MinIO stack below uses) is still only an identifier. A native Unity Catalog is addressed by `catalog.schema.table` instead, so this field is not required (or read) under `CATALOG_KIND = 'UNITY_CATALOG'`. Under `CATALOG_KIND = 'GLUE'` it is the optional Glue `CatalogId` (an AWS account id) |
| `endpoint` | yes, unless `use_sigv4` or vended credentials | S3 endpoint URL |
| `region` | yes, unless `use_sigv4` and the address is a standard AWS Glue endpoint (`https://glue.<region>.amazonaws.com`), or vended credentials | S3 region — also the SigV4 signing region unless a standard Glue endpoint supplies its own; state it whenever a scan reads with static S3 keys |
| `access_key` | yes, unless `use_sigv4` or vended credentials | S3 access key |
| `secret_key` | yes, unless `use_sigv4` or vended credentials | S3 secret key |
| `session_token` | no | STS session token |
| `aws_assume_role_arn` | no | IAM role to assume; see [Assuming an AWS IAM role](#assuming-an-aws-iam-role) |
| `aws_external_id` | no; requires `aws_assume_role_arn` | `ExternalId` for a role whose trust policy requires one |
| `aws_sts_endpoint` | no; requires `aws_assume_role_arn` | STS endpoint override; see [Assuming an AWS IAM role](#assuming-an-aws-iam-role) |
| `path_style` | required if `endpoint` is set and vending is off; otherwise no, default `false` | Path-style S3 addressing: `true` for SeaweedFS or Ceph, `false` for real AWS S3 (virtual-hosted, the default) |
| `use_sigv4` | no, default `false`; implied under `GLUE` | SigV4-sign the catalog REST requests (AWS Glue). The `GLUE` kind always signs, and it rejects `false` |
| `use_vended_credentials` | no, default `false`; rejected under `GLUE` | Request short-lived S3 credentials from the `load_table` call of the catalog (Glue Iceberg REST, Lakekeeper) |
| `token` | no; rejected under `GLUE` | Static bearer token for generic REST catalog auth |
| `client_id` | no; rejected under `GLUE` | OAuth2 client id. Must appear together with `client_secret` |
| `client_secret` | no; rejected under `GLUE` | OAuth2 client secret. Must appear together with `client_id` |
| `oauth2_server_uri` | no; rejected under `GLUE` | OAuth2 token endpoint override |
| `scope` | no; rejected under `GLUE` | OAuth2 scope string |

**Mutual exclusivity:** you cannot combine `use_sigv4` with `token`, `client_id`, or `client_secret`. The adapter rejects a CONNECTION that sets both. SigV4 signs the catalog requests itself. A separate catalog token or OAuth2 flow conflicts with it. `use_sigv4` is rejected outright under `CATALOG_KIND = 'UNITY_CATALOG'` — a native Unity Catalog authenticates with a bearer token or Databricks OAuth, never AWS SigV4.

With `CATALOG_KIND` absent (Iceberg REST, the default), `warehouse` is always required. If you turn `use_sigv4` on, `access_key` and `secret_key` become required, and `region` becomes required too unless the CONNECTION's address is a standard, commercial AWS Glue endpoint of the form `https://glue.<region>.amazonaws.com` — such an endpoint supplies its own SigV4 signing region. These fields sign the catalog request, and `endpoint` stays optional. `region` also places the S3 store independently of whatever region signs the catalog request, so state it whenever a scan reads data with static S3 keys, even against a standard Glue endpoint. If you turn `use_vended_credentials` on without SigV4, you can omit all static S3 fields. The catalog then vends short-lived credentials from `load_table`. Under `CATALOG_KIND = 'UNITY_CATALOG'`, the same static-vs-vended S3 field choice applies, but `warehouse` is never required.

An unstated `path_style` resolves to `false` on a non-vended CONNECTION, which discards `endpoint` and derives a virtual-hosted AWS host from `region` instead. If you configure a non-vended `endpoint` (SeaweedFS, Ceph, or any other self-hosted S3-compatible store), you must state `path_style` explicitly — the adapter rejects a CONNECTION that sets `endpoint` without it. On a vended CONNECTION (`use_vended_credentials` on, or a catalog that vends storage credentials automatically), a stated `path_style` wins over the value the catalog response vends; omit the field there to keep the vended value.

Credential values never appear in error messages, logs, or debug output. The per-query scan spec carries a REFERENCE to the CONNECTION (its name) for a static credential, and an AES-GCM-sealed envelope for a vended one — no credential value travels in the scan spec itself. The adapter never stores them in Virtual Schema properties. See [Security](security.md) for the CONNECTION-access privilege model this relies on, and for exactly what a `SELECT`-only Virtual Schema user can and cannot read back.

The Virtual Schema then names the CONNECTION. [Install: Point the VS at your data](install.md#point-the-vs-at-your-data) and [Tuning](tuning.md) document its properties: `CATALOG_KIND`, `NAMESPACE`, `ALLOW_HTTP`, and the tuning properties. This page repeats only the properties that every recipe needs.

### Assuming an AWS IAM role

A CONNECTION can name an AWS IAM role instead of using its static key pair directly. Set
`aws_assume_role_arn` to the role ARN; `access_key` and `secret_key` (plus `session_token`, if
stated) remain required as the **base identity** that signs the STS `AssumeRole` call — the adapter
reads no ambient AWS credential (no environment variable, instance profile, or web-identity token)
for this. `aws_external_id` and `aws_sts_endpoint` are each accepted only alongside
`aws_assume_role_arn`; stating either one without a role is a rejected CONNECTION naming the field.

The session credentials `AssumeRole` returns then replace `access_key`, `secret_key`, and
`session_token` everywhere those fields are read: they sign SigV4 catalog requests (Glue Iceberg REST
and the native `CATALOG_KIND = 'GLUE'`), and they
are the storage credential for non-vended S3 access. A role leaves credential vending unchanged —
with `use_vended_credentials` on, storage is still resolved by vending (the `load_table` response
under Iceberg REST, or Unity Catalog temporary table credentials under `CATALOG_KIND =
'UNITY_CATALOG'`), exactly as for the same CONNECTION without a role; the session credentials sign
only the catalog requests in that case. The adapter sends exactly one `AssumeRole` call per request
— whatever the shard count — and does not renew the session mid-query; the STS session lasts a fixed
3600 seconds (`DurationSeconds` is not configurable).

**Endpoint and region.** The STS endpoint is `aws_sts_endpoint` when stated, else
`https://sts.<region>.amazonaws.com` for the signing region (the same region
[SigV4 catalog signing](#aws-glue-iceberg-rest-sigv4) resolves — a standard AWS Glue endpoint's own
region, else the stated `region`), else the `us-east-1` regional endpoint when no region
resolves. Sending the `AssumeRole` request to a plaintext `http://` `aws_sts_endpoint` requires
`ALLOW_HTTP = 'true'` on the Virtual Schema, because the response carries the session secret — it is
otherwise a rejected CONNECTION. A **China-region** deployment must state `aws_sts_endpoint`
explicitly: AWS's China STS endpoints live under `.amazonaws.com.cn`, which the default resolution
does not construct.

For local testing, the STS endpoint MUST evaluate `RoleArn`. The bundled SeaweedFS stack does. It does not enforce `aws_external_id`, which needs real AWS STS.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'https://glue.us-east-1.amazonaws.com/iceberg'
  USER ''
  IDENTIFIED BY '{
    "warehouse":           "123456789012",
    "region":              "us-east-1",
    "access_key":          "AKIA...",
    "secret_key":          "...",
    "use_sigv4":           true,
    "aws_assume_role_arn": "arn:aws:iam::123456789012:role/lakehouse-role",
    "aws_external_id":     "..."
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default';
```

`access_key` and `secret_key` above are the base identity's own key pair — the one that holds only
`sts:AssumeRole` permission on the named role — not the role's credentials. `aws_external_id` is
optional; omit it for a role whose trust policy requires none. `region` is stated explicitly, as
every recipe on this page recommends, since it also places the S3 store independently of whatever
region signs the catalog request.

## Local / generic Iceberg REST (no auth)

Use this recipe for the bundled Docker stack or any plain, unauthenticated Iceberg REST catalog. It uses static S3 credentials, `path_style: true` for SeaweedFS, and no catalog auth.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://iceberg-rest:8181'
  USER ''
  IDENTIFIED BY '{
    "warehouse":  "s3://warehouse/",
    "endpoint":   "http://seaweedfs:8333",
    "region":     "us-east-1",
    "access_key": "lhadmin",
    "secret_key": "lhadminsecret123",
    "path_style": true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default'
  ALLOW_HTTP         = 'true';
```

`ALLOW_HTTP = 'true'` is required here because both the catalog and SeaweedFS use plain HTTP. Use internal hostnames (`iceberg-rest`, `seaweedfs`) that resolve from inside the Exasol container. Never use `localhost`.

## AWS Glue Iceberg REST (SigV4)

The catalog URI is the Glue Iceberg REST endpoint. `warehouse` is the AWS **account id**, not an `s3://` path. The adapter derives the Glue REST prefix `catalogs/{account-id}` from it automatically. Turn `use_sigv4` on. `access_key` and `secret_key` then become required, and they sign the catalog requests. When the CONNECTION's address is a standard, commercial AWS Glue endpoint of the form `https://glue.<region>.amazonaws.com` — as below — that endpoint supplies its own SigV4 signing region, so `region` may be omitted for signing purposes; any other Glue endpoint (AWS GovCloud, AWS China, FIPS, a VPC interface endpoint, a private or proxy host) still requires a stated `region`. `region` also places the S3 store, independently of whatever region signs the catalog request, so state it whenever a scan reads data with static S3 keys — even against a standard endpoint. Omit `endpoint`. With `path_style: false`, the S3 client derives the standard AWS endpoint from `region`. The account id below is a placeholder, so substitute your own.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'https://glue.us-east-1.amazonaws.com/iceberg'
  USER ''
  IDENTIFIED BY '{
    "warehouse":     "123456789012",
    "region":        "us-east-1",
    "access_key":    "AKIA...",
    "secret_key":    "...",
    "session_token": "...",
    "path_style":    false,
    "use_sigv4":     true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default';
```

`session_token` is optional. For a long-lived key pair, remove it. To let the catalog vend short-lived S3 credentials for data access instead of the static pair, add `"use_vended_credentials": true`. The static `access_key` and `secret_key` are still required to sign the first `load_table` call, and `region` is too unless the CONNECTION's address is a standard AWS Glue endpoint, which supplies its own signing region. If you omit `region` against a standard endpoint under vending, store placement then depends entirely on whatever `client.region` Glue's vended response carries — no test in this repository asserts that Glue actually vends that key, so verify it against a live Glue catalog before relying on it, or state `region` explicitly for a guaranteed store placement. `ALLOW_HTTP` is absent because Glue and AWS S3 use HTTPS.

## Generic REST with static token or OAuth2

**Static bearer token.** Supply `token`. S3 access still uses the static key pair. If the catalog vends credentials, omit the S3 fields.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO '<rest-catalog-uri>'
  USER ''
  IDENTIFIED BY '{
    "warehouse": "<warehouse>",
    "token":     "<bearer-token>"
  }';
```

**OAuth2 client credentials.** Supply `client_id` and `client_secret` together. The adapter rejects one without the other. `oauth2_server_uri` and `scope` are optional overrides.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO '<rest-catalog-uri>'
  USER ''
  IDENTIFIED BY '{
    "warehouse":         "<warehouse>",
    "client_id":         "<id>",
    "client_secret":     "<secret>",
    "oauth2_server_uri": "<optional>",
    "scope":             "<optional>"
  }';
```

For both modes, create the Virtual Schema the same way:

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default';
```

If the catalog or its storage uses plain HTTP, add `ALLOW_HTTP = 'true'`. If not, omit this property.

## Lakekeeper (OIDC via Keycloak + SeaweedFS)

[Lakekeeper](https://github.com/lakekeeper/lakekeeper) is a widely used open-source Iceberg REST catalog. It needs no new adapter code and no new CONNECTION field. The adapter reaches it through the same OAuth2 client-credentials fields as the generic recipe above: `client_id`, `client_secret`, and `oauth2_server_uri`. Lakekeeper authenticates against **Keycloak** and uses **SeaweedFS** for S3 storage. Keycloak is the documented reference IdP of Lakekeeper, and any OIDC-compatible IdP works the same way. Two things are specific to Lakekeeper:

- **Base path.** Lakekeeper serves its REST API under the `/catalog` base path, so `TO` must include it, for example `http://lakekeeper:8181/catalog`. The adapter negotiates this base path automatically from the `GET /v1/config?warehouse=` response of the catalog. No other configuration is necessary.
- **Warehouse is a name, not a path.** Lakekeeper supports many warehouses. `warehouse` is the warehouse **name** that you register with the management API of Lakekeeper, for example `lakehouse_static`. It is not an `s3://` location. The `warehouse` field of Glue uses the same shape with an account id.

The adapter supports both credential modes below. They differ only in the Lakekeeper warehouse and the CONNECTION fields that you use.

**Static credentials** (`sts-enabled: false` on the Lakekeeper warehouse). The adapter reads SeaweedFS directly with the static key pair, like any other backend:

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://lakekeeper:8181/catalog'
  USER ''
  IDENTIFIED BY '{
    "warehouse":         "lakehouse_static",
    "client_id":         "lakehouse",
    "client_secret":     "<secret>",
    "oauth2_server_uri": "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
    "endpoint":          "http://seaweedfs:8333",
    "region":            "us-east-1",
    "access_key":        "lhadmin",
    "secret_key":        "lhadminsecret123",
    "path_style":        true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  NAMESPACE          = 'default'
  ALLOW_HTTP         = 'true';
```

**Vended (STS/AssumeRole) credentials** (`sts-enabled: true` on the Lakekeeper warehouse). The recipe is the same, with three differences:

- Point `warehouse` at the vended-credential warehouse.
- Add `"use_vended_credentials": true`.
- Remove the static S3 fields `endpoint`, `region`, `access_key`, and `secret_key`.

The adapter then requests short-lived credentials from the `load_table` response of Lakekeeper. In this stack these credentials are SeaweedFS STS AssumeRole credentials, scoped to the bucket of the warehouse.

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://lakekeeper:8181/catalog'
  USER ''
  IDENTIFIED BY '{
    "warehouse":               "lakehouse_vended",
    "client_id":               "lakehouse",
    "client_secret":           "<secret>",
    "oauth2_server_uri":       "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
    "use_vended_credentials": true
  }';
```

Create the Virtual Schema exactly as in the static example above. Use the same `NAMESPACE` and `ALLOW_HTTP`, and name the vended CONNECTION.

## Unity Catalog (Delta and Parquet tables)

Set `CATALOG_KIND = 'UNITY_CATALOG'` to reach a **native** Unity Catalog — self-hosted OSS or
Databricks-managed — instead of an Iceberg REST catalog. This is a genuinely different catalog
client, not another Iceberg REST auth mode: it enumerates `catalog.schema` and returns one virtual
table per **base table** (a `MANAGED` or `EXTERNAL` table whose `data_source_format` is `DELTA` or
`PARQUET`; views and tables in any other format are excluded and warned). `NAMESPACE` is the `catalog.schema` to
expose, and `warehouse` is not required — a native Unity Catalog is addressed by
`catalog.schema.table`, with no separate warehouse identifier. `TO` is the bare Unity Catalog REST
address; the adapter derives the `/api/2.1/unity-catalog` path itself.

Catalog authentication reuses the generic REST fields above — `token` for a static bearer, or
`client_id`/`client_secret` for OAuth2 client credentials — but never `use_sigv4`, which this kind
rejects. Object storage can be static S3 credentials or `use_vended_credentials`, exactly as the
other recipes.

**Parquet tables.** A `PARQUET` table is planned from its catalog entry. Its files supply only the
file list and the partition values. Its storage is resolved exactly as for a Delta table, static or
vended.

- The columns and their types are the catalog's own declaration, read from each column's
  `type_json`. A `struct`, `array`, or `map` column surfaces as JSON `VARCHAR(2000000)`. A `binary`
  or `variant` column, or one with no readable `type_json`, is refused by name, as for a Delta table
  (see [Binary columns](#binary-columns)).
  Every column is nullable, so a file that lacks a column reads NULL for it.
- The partition columns are the columns the catalog declares with a `partition_index`, in that
  order. Each file's values come from the `key=value` directory segments of its own path, matched
  to a partition column ignoring letter case. A `__HIVE_DEFAULT_PARTITION__` or empty value, or a
  missing segment, reads NULL. A segment naming no partition column is a plain directory. A value
  that the column's type cannot represent fails the query, naming the column, the type, and the value.
- A table with Databricks partition metadata logging enabled is still read from its directories.
  The reader does not consult the partitions that log registers.
- A data file needs the `.parquet` suffix. Every such object under the table's storage location is
  read, at any depth, unless a path segment starts with `_` or `.`. A zero-length object is never
  a data file.
- No Parquet footer is read at plan time. A predicate on a partition column prunes files under the
  column's declared type. String, integer, decimal, date, timestamp, and boolean partition columns
  compare in their own type, so `year < 10` keeps `year=9` and prunes `year=10`. A partition
  column of any other declared type prunes no file. A literal of another type than the column's, or
  one the type cannot represent exactly, also prunes no file. A predicate on a data column narrows
  the rows returned but not the files read.
- A data-file column binds to the catalog column whose name it matches ignoring letter case. A
  data-file type outside identity and the supported widening set (integer, floating-point, decimal,
  and date widening, a timestamp stored at the same or a coarser unit, and a binary value under a
  `STRING` column) fails every query that reads that column. The error names the column, both
  types, and the table's storage location. A column a file stores with the Parquet `UNKNOWN`
  (all-NULL) type reads as NULL under every catalog type.

> A Databricks-managed Delta table is also reachable through the **Iceberg REST** kind, via its
> UniForm Iceberg metadata — use the AWS Glue or generic-REST recipes above for that route instead.
> The two routes have different correctness dependencies (Iceberg deletes vs. Delta deletion
> vectors); pick one deliberately rather than mixing them for the same table.

**Self-hosted / OSS, no catalog auth** (matches the bundled Docker stack, whose Unity Catalog server
has auth disabled and whose Delta files sit in the same SeaweedFS bucket the Iceberg recipes use; the
stack's seed script registers its fixture tables under the `unity.delta_e2e` catalog/schema):

```sql
CREATE OR REPLACE CONNECTION UNITY_CATALOG_CREDS
  TO 'http://unitycatalog:8080'
  USER ''
  IDENTIFIED BY '{
    "endpoint":   "http://seaweedfs:8333",
    "region":     "us-east-1",
    "access_key": "lhadmin",
    "secret_key": "lhadminsecret123",
    "path_style": true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'UNITY_CATALOG_CREDS'
  CATALOG_KIND       = 'UNITY_CATALOG'
  NAMESPACE          = 'unity.delta_e2e'
  ALLOW_HTTP         = 'true';
```

**Databricks-managed**, with a personal access token and Databricks-vended storage credentials
(the common case — Databricks Unity Catalog vends short-lived credentials per table rather than
handing out a static key pair):

```sql
CREATE OR REPLACE CONNECTION UNITY_CATALOG_CREDS
  TO 'https://<workspace-host>.cloud.databricks.com'
  USER ''
  IDENTIFIED BY '{
    "token":                  "dapi...",
    "use_vended_credentials": true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'UNITY_CATALOG_CREDS'
  CATALOG_KIND       = 'UNITY_CATALOG'
  NAMESPACE          = 'main.sales';
```

To authenticate with a Databricks OAuth machine-to-machine service principal instead of a personal
access token, replace `token` with `client_id` and `client_secret`; the adapter mints and caches the
bearer itself, minting again a minute ahead of its stated expiry, and defaults `oauth2_server_uri`
to `{catalog-uri}/oidc/v1/token` and `scope` to `all-apis` when you omit them.

## AWS Glue Data Catalog (`CATALOG_KIND = 'GLUE'`)

Set `CATALOG_KIND = 'GLUE'` to read the AWS Glue Data Catalog through its native API. The adapter calls `GetTables`, `GetTable`, and `GetPartitions`. It does not use the Glue Iceberg REST endpoint. One Glue database is one virtual schema. The kind reads Iceberg tables and Hive Parquet tables.

The [AWS Glue Iceberg REST](#aws-glue-iceberg-rest-sigv4) recipe above is a different route. It reads Iceberg tables only. A CONNECTION that addresses the Glue Iceberg REST endpoint resolves the Iceberg REST kind unless `CATALOG_KIND = 'GLUE'` is set.

**CONNECTION shape.** `TO` is the Glue endpoint. A standard `https://glue.<region>.amazonaws.com` address supplies the SigV4 signing region. Any other address needs a stated `region`. The password carries static IAM credentials. The same `access_key`, `secret_key`, and optional `session_token` sign the Glue requests and read S3. With `aws_assume_role_arn`, the role's session replaces them for both (see [Assuming an AWS IAM role](#assuming-an-aws-iam-role)). State `region` so the S3 store is placed. `warehouse` is optional and names the Glue `CatalogId`. If you omit it, Glue uses the AWS account of the credentials, which is the role's account when a role is assumed. The account id below is a placeholder.

```sql
CREATE OR REPLACE CONNECTION GLUE_CATALOG_CREDS
  TO 'https://glue.us-east-1.amazonaws.com'
  USER ''
  IDENTIFIED BY '{
    "region":     "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "..."
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'GLUE_CATALOG_CREDS'
  CATALOG_KIND       = 'GLUE'
  NAMESPACE          = 'sales';
```

- `NAMESPACE` names exactly one Glue database. A value such as `sales.eu` fails.
- The adapter rejects `use_sigv4` set to `false`. The Glue kind always signs with AWS SigV4.
- The adapter rejects `use_vended_credentials` set to `true`. The Glue kind has no native credential vending.
- The adapter rejects `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope`. One error names every supplied field.
- The adapter signs only with the CONNECTION's credentials, or the session of the role it names. It reads no credential or region from the environment, a profile file, or instance metadata.

**Required IAM actions.** The credentials, or the assumed role, need these actions:

- `glue:GetTables`
- `glue:GetTable`
- `glue:GetPartitions`
- `s3:ListBucket` on the table buckets
- `s3:GetObject` on the table objects

**Table routing.** The adapter routes each Glue table by its declared table type before it reads the storage descriptor. A skipped table appears in `SKIPPED_TABLES` with the Glue value that decided the skip (see [Reading SKIPPED_TABLES](#reading-skipped_tables)).

| Glue table | Result |
|---|---|
| `Parameters.table_type` is `ICEBERG` (any letter case) with a non-empty `metadata_location` | Read as an Iceberg table |
| `table_type` is `ICEBERG` with an absent or empty `metadata_location` | Skipped |
| Any other `table_type`, for example `delta` | Skipped, naming the value |
| No `table_type`, and `InputFormat` is `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat` | Read as a Parquet table |
| Parquet table with `Parameters."projection.enabled"` set to `true` | Skipped: partition projection registers no partition in Glue, so a read would return zero rows |
| No `table_type`, and any other `InputFormat` (ORC, text, JSON, Avro, symlink) | Skipped, naming the input format |
| `TableType` is `VIRTUAL_VIEW` | Skipped |

**Iceberg tables.** The adapter reads the current `metadata.json` that `Parameters.metadata_location` names, through the CONNECTION's static S3 credentials. The metadata file is the schema authority. The adapter never reads the Glue columns of an Iceberg table. The same Iceberg planner plans the table as plans an Iceberg REST table, so delete handling, name mapping, and file pruning apply. `metadata_location` names the same snapshot as a REST `loadTable`. A metadata file the adapter cannot read fails the listing and names the table.

**Parquet tables.** The Glue columns, in declared order, followed by the partition keys, are the table's columns. Each column is nullable. The adapter reads no Parquet footer at plan time. A data file column binds to the Glue column whose name it matches ignoring letter case. A partition key that a data file also stores as a column reads the partition value.

| Hive type in Glue | Exasol type |
|---|---|
| `tinyint`, `smallint`, `int` or `integer`, `bigint` | `DECIMAL(3,0)`, `DECIMAL(5,0)`, `DECIMAL(10,0)`, `DECIMAL(20,0)` |
| `float`, `double` | `DOUBLE PRECISION` |
| `boolean` | `BOOLEAN` |
| `string`, `varchar(n)`, `char(n)` | `VARCHAR(2000000)` |
| `date` | `DATE` |
| `timestamp` | The catalog-declared timestamp type: `TIMESTAMP(6)` on Exasol 2025.x and later, bare `TIMESTAMP` on 8.x |
| `decimal(p,s)`, or `decimal` for `decimal(10,0)` | `DECIMAL(p,s)`, or `VARCHAR(2000000)` when the precision or scale is outside Exasol's `DECIMAL` domain |
| `array<...>`, `map<...>`, `struct<...>` | JSON `VARCHAR(2000000)` |
| `binary`, an unrecognized type, or a malformed type string | Declared `VARCHAR(2000000)`, and refused when a query reads it |

A refused column fails only the queries that read or emit it. The refusal reason quotes the Hive type string. A table whose every column is refused is refused as a whole. A `binary` column, or a nested type with a `binary` member, follows the rule in [Binary columns](#binary-columns).

**Partition values.** The adapter takes each partition's values from `GetPartitions`, in the order of the table's partition keys. It never parses a value from the location path. The Hive literal `__HIVE_DEFAULT_PARTITION__` reads NULL. A partition whose value count differs from the partition-key count fails the query and names the partition's location. Every query reads the Glue metadata again. It issues one `GetTable`, the paginated `GetPartitions`, and one object listing per kept partition. The adapter caches nothing.

**Data files.** The adapter lists each kept partition's location by its raw object key, without percent-decoding. A value `a b/c` is stored at the literal key `p_str=a b%2Fc/`, and the listing finds it. An unpartitioned table is listed at its table location. The file rule is the any-direct-child pattern:

- A data file is a direct child of the location, with any name. Trino writes files without an extension, so a `.parquet` suffix is not required.
- An object below a subdirectory is not read.
- An object whose name starts with `_` or `.`, such as `_SUCCESS`, is not read.
- A zero-length object is not read.
- A partition outside the table location in the same bucket is read, and its files carry absolute paths. The scheme `s3a://` addresses the same store as `s3://`.

**Partition pruning.** The adapter prunes partitions on their Glue values before it lists their locations. The shared partition predicate compares each value under the partition key's declared type. String, integer, decimal, date, timestamp, and boolean keys compare in their own type. A key of any other type, a literal of another type than the key's, and a literal the type cannot represent exactly prune nothing. The adapter does not send the predicate to Glue in the `Expression` of `GetPartitions`. The full predicate still applies above the scan, so pruned and unpruned queries return the same rows.

**Failures.** A kept partition that the adapter cannot read faithfully fails the query. The error names the partition's values and location and states the cause. The adapter never skips such a partition, because a skipped partition returns wrong rows.

- A kept partition with an ORC input format fails with `its input format '...' is not Parquet`.
- A kept partition in a bucket other than the table's bucket fails and names both buckets.
- A pruned partition never fails the query, because it contributes no row.
- A table dropped from Glue after the virtual schema was created fails and states that it does not exist.
- A table re-registered in an unsupported format fails with the reason its listing would skip it.
- Glue errors name the operation, the service error code, and the service message. `EntityNotFoundException` names the missing database or table.
- The adapter retries throttling and server errors up to 5 attempts per call, and each call ends within 30 seconds.
- If the signing time differs from the AWS time, the error states that the clock of the Exasol node differs from AWS time.

**Scoped exceptions.** These Glue cases are deliberate limits:

- A Hive table with partition projection is skipped, because Glue registers no partition for it.
- Every binary type is refused (see [Binary columns](#binary-columns)).
- An ORC, Delta (Athena), text, JSON, Avro, or symlink table is skipped.
- A `warehouse` that names another account's `CatalogId` is untested (#TBD). To read another account's catalog, assume a role in that account and omit `warehouse`.
- Lake Formation grants are not evaluated (see [Security](security.md#aws-glue-credentials-and-lake-formation)).

## Direct storage (raw Parquet, no catalog)

Set `CATALOG_KIND = 'DIRECT_STORAGE'` to read a plain directory tree of Parquet files with **no
catalog service at all** — no Iceberg REST, no Unity Catalog. Every first-level directory under the
CONNECTION address (joined with `NAMESPACE` when set) becomes one table; every `*.parquet` file
found anywhere below it, at any depth, is that table's data. A path segment that starts with `_` or
`.` (for example `_delta_log/`, `_SUCCESS`, `.spark-staging`) is excluded from both directory
discovery and file eligibility, so a loose file directly under the base path serves no table, and a
directory with no eligible file is skipped rather than served empty.

The file rule is the `**/*.parquet` pattern. An object whose name lacks the `.parquet` suffix is
never read. A zero-length object is never a data file, even with the suffix.

**CONNECTION shape.** The address is the storage base path itself — an `s3://`/`s3a://` bucket
prefix, or an `abfss://<container>@<account>.dfs.core.windows.net/<prefix>` container prefix — not a
catalog URI. The password carries ONLY storage credentials (the same S3 or Azure fields every other
recipe on this page uses): no catalog-auth field is accepted, and supplying one is a rejected
CONNECTION rather than a silently ignored one. `warehouse`, `token`, `client_id`, `client_secret`,
`oauth2_server_uri`, `scope`, `use_sigv4`, and `use_vended_credentials` (S3-vending; Azure vending
does not apply here) are all rejected under this kind. The address scheme must agree with the
credential shape: `s3`/`s3a` for S3 credentials, `abfss` only for Azure credentials — `abfs` (without
the trailing `s`) is rejected, naming `abfss` as the accepted spelling.

```sql
CREATE OR REPLACE CONNECTION DIRECT_STORAGE_CREDS
  TO 's3://warehouse/events'
  USER ''
  IDENTIFIED BY '{
    "endpoint":   "http://seaweedfs:8333",
    "region":     "us-east-1",
    "access_key": "lhadmin",
    "secret_key": "lhadminsecret123",
    "path_style": true
  }';

CREATE VIRTUAL SCHEMA MY_RAW_PARQUET
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'DIRECT_STORAGE_CREDS'
  CATALOG_KIND       = 'DIRECT_STORAGE'
  ALLOW_HTTP         = 'true';
```

**The three properties.**

| Property | Default | Meaning |
|---|---|---|
| `NAMESPACE` | absent (the CONNECTION address alone is the base path) | Narrows discovery to a subtree: joined onto the CONNECTION address with a single `/` to form the storage base path. Not a `catalog.schema` reference — there is no catalog to resolve it against |
| `MERGE_SCHEMA` | `TRUE` | `TRUE` reads every listed file's footer and folds them into one declared schema (widening a narrower numeric/date type into a wider one, unioning columns present in only some files). `FALSE` samples only the lexicographically first file's footer and declares that schema alone — cheaper, but a column or a wider type that only a later file carries is invisible, and a file whose column type is wider than the sampled type fails every query that reads the column, even when its values fit (a narrower one is read widened). `FALSE` takes the partition keys from the sampled file's path alone too, so it requires every file to share one partition layout: a key only other files carry is ignored, and a file lacking a sampled key reads NULL for it |
| `HIVE_PARTITIONING` | `TRUE` | `TRUE` declares each `key=value` directory segment below a table's root as a `VARCHAR` partition column and prunes files on it when a query is planned (see *Partition columns* below). `FALSE` reads such segments as plain directories: no partition column and no pruning |

An unparseable `MERGE_SCHEMA` or `HIVE_PARTITIONING` value is rejected rather than defaulted, since a
typo that silently selected the opposite mode would return a narrower schema instead of an error.

**Partition columns.** Under the default `HIVE_PARTITIONING = 'TRUE'`, a directory segment below a
table's root of the form `key=value`, at any depth and in any order (never the file name itself),
declares the partition column `KEY` as `VARCHAR(2000000)`: a directory value carries no type, and a
type inferred from the values seen would change whenever a new directory appeared. Partition columns
follow the Parquet columns, in the order each key first appears in the listing. The value is
percent-decoded (`region=a%2Fb` reads `a/b`), and `__HIVE_DEFAULT_PARTITION__` or an empty value
reads NULL. Under `MERGE_SCHEMA = 'TRUE'` a table's partition columns are the union of every file's
keys, and a file whose path lacks one of them reads NULL for it. Keys compare by their uppercase
form, the name they are declared under, which gives three rules:

- Two keys that differ only in case (`Year=` and `year=`) fail `CREATE VIRTUAL SCHEMA` and `REFRESH`,
  naming both spellings, since neither directory encoding is preferred over the other. This rule
  holds across the table under `MERGE_SCHEMA = 'TRUE'`; under `'FALSE'` it covers only the sampled
  file's own keys.
- A key that names a Parquet column (a `k=` segment over a file storing `K`) overrides it: `K` is
  declared once, as the `VARCHAR` partition column, and every row reads the directory value, never
  the file's stored one.
- A table where some but not all of the files storing such a column carry the key's segment fails
  the statement, naming the column, the key, and a file lacking the segment (a
  `k=__HIVE_DEFAULT_PARTITION__/` or empty `k=/` segment counts as present, and reads NULL). Under
  `MERGE_SCHEMA = 'TRUE'` any such file fails it; under `'FALSE'` only the sampled file itself is
  checked, and an unsampled file with the same problem stays undetected.

Before this release a `key=value` directory was read as a plain directory; set
`HIVE_PARTITIONING = 'FALSE'` to keep that behavior across the upgrade.

**Iceberg or Delta directory caveat.** Pointing this catalog kind at a directory that is actually an
Iceberg table or a Delta table is a supported but almost always wrong choice: direct storage knows
nothing about snapshots, manifests, or the transaction log, so it reads every physical Parquet file
under the directory, including files a real Iceberg snapshot or Delta log would mark deleted,
tombstoned, or superseded — the `_delta_log/` directory itself is silently skipped by the `_`-prefix
exclusion rule above, not read. This returns wrong rows, not an error. If you actually want a
catalog-aware, delete-correct read of an Iceberg or Delta table, use the matching kind instead: leave
`CATALOG_KIND` absent (Iceberg REST, the default) or set it to `'UNITY_CATALOG'` — see the recipes
above.

**Plan-time footer cost.** Every `CREATE VIRTUAL SCHEMA` and every `REFRESH` lists the directory tree
and reads Parquet file footers directly from object storage — there is no manifest, snapshot, or log
to consult instead, unlike the Iceberg REST and Unity Catalog kinds. Planning a query lists the table
again and reads the footers of the files it keeps. `MERGE_SCHEMA = 'FALSE'` bounds this to one footer
read per table; the default `'TRUE'` reads every kept file's footer. Partition pruning runs before
any footer is read, so a pruned file's footer is never read: a filter comparing a partition column
with a string literal by equality (`=`, `<>`), `IN`, a NULL check (`IS NULL`, `IS NOT NULL`), or a
range (`<`, `<=`, `>`, `>=`, `BETWEEN`), combined by `AND`, `OR`, and `NOT`, drops every file whose
partition values cannot satisfy it. Range and `BETWEEN` compare partition values as strings, the
order Exasol applies to `VARCHAR`, so `month=10` sorts before `month=9`. A filter on any other
column, or one that applies a function to a partition column, prunes no file: there is no
footer-statistics pruning (#412), and nothing bounds how many files a table may hold (#419).

**Limitation: mixed timestamp units do not fold.** The type-widening rules this kind applies when
folding schemas cover integer, float, and date widening, but carry no timestamp-to-timestamp rule.
Two files that declare the same column as `TIMESTAMP` at different units (for example microsecond in
one file, nanosecond in another) fail to fold under the default `MERGE_SCHEMA = 'TRUE'`. Set
`MERGE_SCHEMA = 'FALSE'` to work around it — the sampled file's declared unit then wins, and a file
is read only when its unit equals the sampled unit or is coarser. A file at a finer unit fails every
query that reads the column.

## Binary columns

Every binary type is refused on every source until issue #351 defines a faithful rendering for binary. The listing still declares the column as `VARCHAR(2000000)`, so the virtual table lists every column. A query that reads or emits a refused column fails at plan time. The error names the column, its declared type, and #351. A query that reads other columns still succeeds, and so does `SELECT COUNT(*)`. A table whose every column is refused is refused as a whole.

| Source | Refused types |
|---|---|
| Iceberg REST and Glue Iceberg | `binary`, `fixed(L)`, and `uuid`, at any depth |
| Delta, Unity Catalog, and Glue Parquet | `binary` |
| Direct storage | An unannotated `BYTE_ARRAY` (`binary`), an unannotated `FIXED_LEN_BYTE_ARRAY` (`fixed(L)`), `UUID` (`uuid`), `BSON` (`bson`), `GEOMETRY` (`geometry`), and `GEOGRAPHY` (`geography`) |

A catalog-declared source decides by the declared type. A Glue column declared `string` over a data file that stores a `BYTE_ARRAY` without the string annotation reads as text. A value that is not valid UTF-8 fails the query.

Direct storage declares no table type, so it decides by what each Parquet file declares. A top-level Parquet `ENUM` column reads as text. A nested `ENUM` member is refused (#TBD).

Two cases break a query that worked before:

- An Iceberg `binary` column that holds UTF-8 text is now refused.
- A direct-storage column that a legacy writer such as Impala or Hive stored as an unannotated `BYTE_ARRAY` string is now refused. Rewrite the file with the Parquet `STRING` annotation to read it.

## Reading SKIPPED_TABLES

Every catalog kind records the entries its listing skips. The adapter writes one warning line per skip. It also stores the skips in the `SKIPPED_TABLES` entry of the virtual schema's adapter notes. Read them with:

```sql
SELECT ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'MY_LAKEHOUSE';
```

`ADAPTER_NOTES` holds a JSON object. Its `SKIPPED_TABLES` value is an array with one object per skipped entry, in listing order. `table` is the catalog identifier. `reason` states why the entry was skipped.

| Catalog kind | `reason` |
|---|---|
| Iceberg REST | `catalog reported it is not a loadable Iceberg table` |
| Unity Catalog and Glue | The catalog value that decided the skip, for example `table_type=VIEW` or `InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat` |
| Direct storage | `holds no data file` |

A listing that skips nothing records an empty array, and a refresh replaces the previous array. A namespace whose every entry is skipped still creates an empty virtual schema.

Exasol rejects an `ADAPTER_NOTES` value longer than 2,000,000 bytes. The adapter therefore keeps the longest prefix of `SKIPPED_TABLES`, in listing order, that fits. It records the number of dropped entries in `SKIPPED_TABLES_OMITTED`, a string. The entry exists only when the count is above zero, and a refresh removes it once every skip fits. The warning lines still name every skip. The cap never applies to `TABLE_MAP`.

## Addressing

The adapter UDF runs **inside** the Exasol container. Every address in the CONNECTION must resolve from there. Use internal hostnames, for example `iceberg-rest`, `seaweedfs`, `lakekeeper`, `keycloak`, or `unitycatalog`. Never use `localhost` or the Docker host gateway. A Databricks-managed Unity Catalog is a public HTTPS endpoint, so it needs no internal hostname — just network egress from the Exasol node.
