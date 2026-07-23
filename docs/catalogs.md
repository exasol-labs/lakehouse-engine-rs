[lakehouse-engine](../README.md) › [Docs](index.md) › Catalogs

---

# Catalogs

Every catalog is reached the same way: as an **Iceberg REST catalog**. There is one REST catalog
client, and the backends differ only by the auth mode you turn on in the CONNECTION password JSON,
not by a separate catalog implementation. No auth for a local stack, AWS SigV4 for Glue, a static
bearer token or OAuth2 client credentials for a generic secured REST catalog. Pick the row that
matches your catalog, then copy its recipe.

## What is supported today

| Catalog | Auth mode | Test status |
|---|---|---|
| [Local / generic Iceberg REST (no auth)](#local--generic-iceberg-rest-no-auth) | none | **CI E2E**: `make test-e2e` on every push |
| [AWS Glue Iceberg REST](#aws-glue-iceberg-rest-sigv4) | SigV4 | **Validated, not in CI**: opt-in cloud test against real Glue |
| [Generic REST with token / OAuth2](#generic-rest-with-static-token-or-oauth2) | bearer token or OAuth2 | **Exercised, not in CI**: same opt-in cloud test |
| [Databricks Unity Catalog Iceberg REST](#databricks-unity-catalog-iceberg-rest) | token/OAuth2 (in theory) | **Untested**: template only, no Databricks-specific code |

The steps here cover only the catalog CONNECTION and the Virtual Schema — the
[Point the VS at your data](install.md#point-the-vs-at-your-data) step of [Install](install.md). Get
the `.so` onto BucketFS and create the scripts first.

## Connection fields

The catalog URI goes in the CONNECTION's `TO` clause. Every credential and behavior field goes in
the `IDENTIFIED BY` JSON password object. The field set is identical across all backends; only the
values and a few flags change. Field parsing and validation live in
`crates/lakehouse-engine/src/adapter/connection.rs`.

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` | yes | Iceberg warehouse location: an `s3://…` path normally, or an AWS account id under Glue |
| `endpoint` | yes, unless `use_sigv4` | S3 endpoint URL |
| `region` | yes, unless `use_sigv4` | S3 region (also required when `use_sigv4` is on) |
| `access_key` | yes, unless `use_sigv4` | S3 access key (also required when `use_sigv4` is on) |
| `secret_key` | yes, unless `use_sigv4` | S3 secret key (also required when `use_sigv4` is on) |
| `session_token` | no | STS session token |
| `path_style` | no, default `true` | Path-style S3 addressing: `true` for MinIO, `false` for real AWS S3 |
| `use_sigv4` | no, default `false` | SigV4-sign the catalog REST requests (AWS Glue) |
| `use_vended_credentials` | no, default `false` | Request short-lived S3 credentials from the catalog's `load_table` (Glue) |
| `token` | no | Static bearer token for generic REST catalog auth |
| `client_id` | no | OAuth2 client id; must appear together with `client_secret` |
| `client_secret` | no | OAuth2 client secret; must appear together with `client_id` |
| `oauth2_server_uri` | no | OAuth2 token endpoint override |
| `scope` | no | OAuth2 scope string |

**Mutual exclusivity:** `use_sigv4` cannot be combined with any of `token`, `client_id`, or
`client_secret`. `validate_creds()` rejects a CONNECTION that sets both. SigV4 signs the catalog
requests itself, so a separate catalog token or OAuth2 flow would conflict.

Note the SigV4 asymmetry. With `use_sigv4` off, all four S3 fields (`endpoint`, `region`,
`access_key`, `secret_key`) are the S3 credentials and are expected. With `use_sigv4` on,
`endpoint` becomes optional (derived from `region`) while `region`, `access_key`, and `secret_key`
stay required, because they sign the catalog request.

Credential values never appear in error messages, logs, or `Debug` output. They travel to the scan
UDF inside the per-query scan spec and are never stored in Virtual Schema properties.

The Virtual Schema then names the CONNECTION. Its properties (`ICEBERG_NAMESPACE`, `ALLOW_HTTP`,
and the tuning knobs) are documented in
[Install: Point the VS at your data](install.md#point-the-vs-at-your-data) and [Tuning](tuning.md);
this page repeats only the two properties every recipe needs.

## Local / generic Iceberg REST (no auth)

> **Tested, CI E2E.** Covered by `make test-e2e` (`.github/workflows/ci.yml`) against the bundled
> `docker-compose.yml` MinIO + Iceberg REST stack on every push. This is the most-exercised path.

For the bundled Docker stack or any plain, unauthenticated Iceberg REST catalog. Static S3
credentials, `path_style: true` for MinIO, no catalog auth.

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

`ALLOW_HTTP = 'true'` is required here because both the catalog and MinIO speak plain HTTP. Use
internal hostnames (`iceberg-rest`, `minio`) reachable from inside the Exasol container, never
`localhost`.

## AWS Glue Iceberg REST (SigV4)

> **Tested against real AWS Glue, but not in CI.** This is the validated production path. It is
> exercised by an opt-in cloud test (`crates/lakehouse-engine/tests/cloud_e2e_test.rs`, cargo
> feature `cloud-e2e`) that runs against a real AWS Glue account. That test does not run in CI.
> CI runs only the unit tests (`cargo test --workspace`, opt-in features off) and the local E2E
> stack. Do not read this as CI-tested.

The catalog URI is the Glue Iceberg REST endpoint. `warehouse` is the AWS **account id**, not an
`s3://` path; the adapter derives Glue's `catalogs/{account-id}` REST prefix internally. Turn
`use_sigv4` on: `region`, `access_key`, and `secret_key` become required and sign the catalog
requests. `endpoint` is omitted; with `path_style: false` the S3 client derives the standard AWS
endpoint from `region`. The account id below is a placeholder, so substitute your own.

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
  ICEBERG_NAMESPACE  = 'default';
```

`session_token` is optional; drop it for a long-lived key pair. To let the catalog vend
short-lived S3 credentials instead of using the static pair for data access, add
`"use_vended_credentials": true`; the static `access_key`/`secret_key`/`region` are still required
to sign the initial `load_table` call. `ALLOW_HTTP` is omitted because Glue and AWS S3 are HTTPS.

## Generic REST with static token or OAuth2

> **Implemented and exercised by the same opt-in `cloud_e2e_test.rs`, not CI-gated.** Use for a
> secured Iceberg REST catalog that authenticates with a bearer token or the OAuth2
> client-credentials flow. SigV4 must be off (they are mutually exclusive).

**Static bearer token.** Supply `token`. S3 access is still the static key pair (or omit the S3
fields if the catalog vends credentials).

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO '<rest-catalog-uri>'
  USER ''
  IDENTIFIED BY '{
    "warehouse": "<warehouse>",
    "token":     "<bearer-token>"
  }';
```

**OAuth2 client credentials.** Supply `client_id` and `client_secret` together
(`validate_creds()` rejects one without the other). `oauth2_server_uri` and `scope` are optional
overrides.

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

Either way, the Virtual Schema is created the same:

```sql
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default';
```

Add `ALLOW_HTTP = 'true'` only if the catalog or its storage is reachable over plain HTTP.

## Databricks Unity Catalog Iceberg REST

> **Untested. Template only.** There is no Databricks-specific code path: no special token refresh,
> no custom Databricks headers. Databricks-managed Iceberg is reachable only in theory, because
> Unity Catalog exposes the same Iceberg REST protocol. It is not yet exercised by this repo's test
> suite.

Point `TO` at the Databricks Unity Catalog Iceberg REST endpoint and use the token or OAuth2 auth
described above. Treat the [AWS Glue recipe](#aws-glue-iceberg-rest-sigv4) as the template and
adjust the catalog URI and credential flags to the Databricks endpoint. There is no worked example
here because the shape has not been validated.

## Addressing

The adapter UDF runs **inside** the Exasol container, so every address in the CONNECTION must
resolve from there. Use internal hostnames (e.g. `iceberg-rest`, `minio`), never `localhost` or the
Docker host gateway.
