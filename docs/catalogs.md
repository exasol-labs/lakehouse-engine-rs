[lakehouse-engine](../README.md) › [Docs](index.md) › Catalogs

---

# Catalogs

Every catalog is reached the same way: as an **Iceberg REST catalog**. There is one REST catalog client, and the backends differ only by the auth mode you turn on in the CONNECTION password JSON, not by a separate catalog implementation. No auth for a local stack, AWS SigV4 for Glue, a static bearer token or OAuth2 client credentials for a generic secured REST catalog — Lakekeeper is a concrete instance of that last mode. Pick the row that matches your catalog, then copy its recipe.

## What is supported today

| Catalog | Auth mode | Status |
|---|---|---|
| [Local / generic Iceberg REST (no auth)](#local--generic-iceberg-rest-no-auth) | none | Supported |
| [AWS Glue Iceberg REST](#aws-glue-iceberg-rest-sigv4) | SigV4 | Supported |
| [Generic REST with token / OAuth2](#generic-rest-with-static-token-or-oauth2) | bearer token or OAuth2 | Supported |
| [Lakekeeper](#lakekeeper-oidc-via-keycloak--minio) | OAuth2 client-credentials (OIDC) | Supported |

The steps here cover only the catalog CONNECTION and the Virtual Schema — the
[Point the VS at your data](install.md#point-the-vs-at-your-data) step of [Install](install.md). Get
the `.so` onto BucketFS and create the scripts first.

## Connection fields

The catalog URI goes in the CONNECTION's `TO` clause. Every credential and behavior field goes in the `IDENTIFIED BY` JSON password object. The field set is identical across all backends; only the values and a few flags change.

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` | yes | Iceberg warehouse location: an `s3://…` path normally, an AWS account id under Glue, or a warehouse **name** under Lakekeeper |
| `endpoint` | yes, unless `use_sigv4` or vended credentials | S3 endpoint URL |
| `region` | yes, unless `use_sigv4` or vended credentials | S3 region |
| `access_key` | yes, unless `use_sigv4` or vended credentials | S3 access key |
| `secret_key` | yes, unless `use_sigv4` or vended credentials | S3 secret key |
| `session_token` | no | STS session token |
| `path_style` | no, default `true` | Path-style S3 addressing: `true` for MinIO, `false` for real AWS S3 |
| `use_sigv4` | no, default `false` | SigV4-sign the catalog REST requests (AWS Glue) |
| `use_vended_credentials` | no, default `false` | Request short-lived S3 credentials from the catalog's `load_table` (Glue, Lakekeeper) |
| `token` | no | Static bearer token for generic REST catalog auth |
| `client_id` | no | OAuth2 client id; must appear together with `client_secret` |
| `client_secret` | no | OAuth2 client secret; must appear together with `client_id` |
| `oauth2_server_uri` | no | OAuth2 token endpoint override |
| `scope` | no | OAuth2 scope string |

**Mutual exclusivity:** `use_sigv4` cannot be combined with any of `token`, `client_id`, or `client_secret`. The adapter rejects a CONNECTION that sets both. SigV4 signs the catalog requests itself, so a separate catalog token or OAuth2 flow would conflict.

Only `warehouse` is unconditionally required. With `use_sigv4` on, `region`/`access_key`/`secret_key` become required instead (they sign the catalog request) and `endpoint` stays optional. With `use_vended_credentials` on and no SigV4, the static S3 fields can be omitted entirely — the catalog vends short-lived credentials from `load_table` instead.

Credential values never appear in error messages, logs, or debug output. They travel to the scan UDF inside the per-query scan spec and are never stored in Virtual Schema properties.

The Virtual Schema then names the CONNECTION. Its properties (`ICEBERG_NAMESPACE`, `ALLOW_HTTP`, and the tuning knobs) are documented in [Install: Point the VS at your data](install.md#point-the-vs-at-your-data) and [Tuning](tuning.md); this page repeats only the two properties every recipe needs.

## Local / generic Iceberg REST (no auth)

For the bundled Docker stack or any plain, unauthenticated Iceberg REST catalog. Static S3 credentials, `path_style: true` for MinIO, no catalog auth.

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

`ALLOW_HTTP = 'true'` is required here because both the catalog and MinIO speak plain HTTP. Use internal hostnames (`iceberg-rest`, `minio`) reachable from inside the Exasol container, never `localhost`.

## AWS Glue Iceberg REST (SigV4)

The catalog URI is the Glue Iceberg REST endpoint. `warehouse` is the AWS **account id**, not an `s3://` path; the adapter derives Glue's `catalogs/{account-id}` REST prefix from it automatically. Turn `use_sigv4` on: `region`, `access_key`, and `secret_key` become required and sign the catalog requests. `endpoint` is omitted; with `path_style: false` the S3 client derives the standard AWS endpoint from `region`. The account id below is a placeholder, so substitute your own.

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

`session_token` is optional; drop it for a long-lived key pair. To let the catalog vend short-lived S3 credentials instead of using the static pair for data access, add `"use_vended_credentials": true`; the static `access_key`/`secret_key`/`region` are still required to sign the initial `load_table` call. `ALLOW_HTTP` is omitted because Glue and AWS S3 are HTTPS.

## Generic REST with static token or OAuth2

**Static bearer token.** Supply `token`. S3 access is still the static key pair (or omit the S3 fields if the catalog vends credentials).

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO '<rest-catalog-uri>'
  USER ''
  IDENTIFIED BY '{
    "warehouse": "<warehouse>",
    "token":     "<bearer-token>"
  }';
```

**OAuth2 client credentials.** Supply `client_id` and `client_secret` together (the adapter rejects one without the other). `oauth2_server_uri` and `scope` are optional overrides.

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

## Lakekeeper (OIDC via Keycloak + MinIO)

[Lakekeeper](https://github.com/lakekeeper/lakekeeper) is a widely-used open-source Iceberg REST catalog. It needs no new adapter code and no new CONNECTION field — it is reached through the same `client_id`/`client_secret`/`oauth2_server_uri` OAuth2 client-credentials fields as the generic recipe above, authenticating against **Keycloak** (Lakekeeper's documented reference IdP; any OIDC-compatible IdP works the same way) and backed by **MinIO** for S3 storage. Two things are specific to Lakekeeper and worth calling out:

- **Base path.** Lakekeeper serves its REST API under a `/catalog` base path, so `TO` must include it (e.g. `http://lakekeeper:8181/catalog`). The adapter negotiates this automatically from the catalog's `GET /v1/config?warehouse=` response, so you don't need to configure anything else for it.
- **Warehouse is a name, not a path.** Lakekeeper is multi-warehouse: `warehouse` is the warehouse **name** registered with Lakekeeper's management API (e.g. `lakehouse_static`), not an `s3://` location — the same shape Glue's account-id `warehouse` field already uses.

Both credential modes below are fully supported; the difference is just which Lakekeeper warehouse and CONNECTION fields you use:

**Static credentials** (`sts-enabled: false` on the Lakekeeper warehouse) — the adapter reads MinIO directly with the static key pair, same as any other backend:

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://lakekeeper:8181/catalog'
  USER ''
  IDENTIFIED BY '{
    "warehouse":         "lakehouse_static",
    "client_id":         "lakehouse",
    "client_secret":     "lakehouse-engine-secret",
    "oauth2_server_uri": "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
    "endpoint":          "http://minio:9000",
    "region":            "us-east-1",
    "access_key":        "minioadmin",
    "secret_key":        "minioadmin",
    "path_style":        true
  }';

CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_CONNECTION = 'LAKEHOUSE_CATALOG_CREDS'
  ICEBERG_NAMESPACE  = 'default'
  ALLOW_HTTP         = 'true';
```

**Vended (STS/AssumeRole) credentials** (`sts-enabled: true` on the Lakekeeper warehouse) — same recipe, three differences: point `warehouse` at the vended-credential warehouse, add `"use_vended_credentials": true`, and drop the static S3 fields entirely (`endpoint`, `region`, `access_key`, `secret_key`) — the adapter requests short-lived credentials from Lakekeeper's `load_table` response instead, which in this stack are MinIO STS AssumeRole credentials scoped to the warehouse's bucket:

```sql
CREATE OR REPLACE CONNECTION LAKEHOUSE_CATALOG_CREDS
  TO 'http://lakekeeper:8181/catalog'
  USER ''
  IDENTIFIED BY '{
    "warehouse":               "lakehouse_vended",
    "client_id":               "lakehouse",
    "client_secret":           "lakehouse-engine-secret",
    "oauth2_server_uri":       "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
    "use_vended_credentials": true
  }';
```

The Virtual Schema is created identically to the static example above (same `ICEBERG_NAMESPACE` and `ALLOW_HTTP`, just naming the vended CONNECTION).

## Addressing

The adapter UDF runs **inside** the Exasol container, so every address in the CONNECTION must resolve from there. Use internal hostnames (e.g. `iceberg-rest`, `minio`, `lakekeeper`, `keycloak`), never `localhost` or the Docker host gateway.
