[lakehouse-engine](../README.md) › [Docs](index.md) › Catalogs

---

# Catalogs

The adapter reaches every catalog the same way: as an **Iceberg REST catalog**. There is one REST catalog client. The backends differ only by the auth mode that you turn on in the CONNECTION password JSON, not by a separate catalog implementation. Three auth modes exist:

- no auth, for a local stack
- AWS SigV4, for Glue
- a static bearer token or OAuth2 client credentials, for a generic secured REST catalog

Lakekeeper is a concrete instance of the last mode.

Find the row that matches your catalog. Then copy its recipe.

## What is supported today

| Catalog | Auth mode | Status |
|---|---|---|
| [Local / generic Iceberg REST (no auth)](#local--generic-iceberg-rest-no-auth) | none | Supported |
| [AWS Glue Iceberg REST](#aws-glue-iceberg-rest-sigv4) | SigV4 | Supported |
| [Generic REST with token / OAuth2](#generic-rest-with-static-token-or-oauth2) | bearer token or OAuth2 | Supported |
| [Lakekeeper](#lakekeeper-oidc-via-keycloak--minio) | OAuth2 client-credentials (OIDC) | Supported |

The steps here cover only the catalog CONNECTION and the Virtual Schema. They are the
[Point the VS at your data](install.md#point-the-vs-at-your-data) step of [Install](install.md).
First copy the `.so` to BucketFS and create the scripts.

## Connection fields

The catalog URI goes in the `TO` clause of the CONNECTION. Every credential field and behavior field goes in the `IDENTIFIED BY` JSON password object. The field set is identical for all backends. Only the values and a few flags change.

| JSON field | Required | Meaning |
|---|---|---|
| `warehouse` | yes | Catalog routing identifier — an AWS account id under Glue, a warehouse **name** under Lakekeeper, or whatever identifier a generic Iceberg REST catalog registered — never read as a storage location, so a URI-shaped value (as the bundled `iceberg-rest`/MinIO stack below uses) is still only an identifier |
| `endpoint` | yes, unless `use_sigv4` or vended credentials | S3 endpoint URL |
| `region` | yes, unless `use_sigv4` or vended credentials | S3 region |
| `access_key` | yes, unless `use_sigv4` or vended credentials | S3 access key |
| `secret_key` | yes, unless `use_sigv4` or vended credentials | S3 secret key |
| `session_token` | no | STS session token |
| `path_style` | no, default `true` | Path-style S3 addressing: `true` for MinIO, `false` for real AWS S3 |
| `use_sigv4` | no, default `false` | SigV4-sign the catalog REST requests (AWS Glue) |
| `use_vended_credentials` | no, default `false` | Request short-lived S3 credentials from the `load_table` call of the catalog (Glue, Lakekeeper) |
| `token` | no | Static bearer token for generic REST catalog auth |
| `client_id` | no | OAuth2 client id. Must appear together with `client_secret` |
| `client_secret` | no | OAuth2 client secret. Must appear together with `client_id` |
| `oauth2_server_uri` | no | OAuth2 token endpoint override |
| `scope` | no | OAuth2 scope string |

**Mutual exclusivity:** you cannot combine `use_sigv4` with `token`, `client_id`, or `client_secret`. The adapter rejects a CONNECTION that sets both. SigV4 signs the catalog requests itself. A separate catalog token or OAuth2 flow conflicts with it.

Only `warehouse` is always required. If you turn `use_sigv4` on, `region`, `access_key`, and `secret_key` become required instead. These three fields sign the catalog request, and `endpoint` stays optional. If you turn `use_vended_credentials` on without SigV4, you can omit all static S3 fields. The catalog then vends short-lived credentials from `load_table`.

Credential values never appear in error messages, logs, or debug output. They travel to the scan UDF inside the per-query scan spec. The adapter never stores them in Virtual Schema properties.

The Virtual Schema then names the CONNECTION. [Install: Point the VS at your data](install.md#point-the-vs-at-your-data) and [Tuning](tuning.md) document its properties: `ICEBERG_NAMESPACE`, `ALLOW_HTTP`, and the tuning properties. This page repeats only the two properties that every recipe needs.

## Local / generic Iceberg REST (no auth)

Use this recipe for the bundled Docker stack or any plain, unauthenticated Iceberg REST catalog. It uses static S3 credentials, `path_style: true` for MinIO, and no catalog auth.

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

`ALLOW_HTTP = 'true'` is required here because both the catalog and MinIO use plain HTTP. Use internal hostnames (`iceberg-rest`, `minio`) that resolve from inside the Exasol container. Never use `localhost`.

## AWS Glue Iceberg REST (SigV4)

The catalog URI is the Glue Iceberg REST endpoint. `warehouse` is the AWS **account id**, not an `s3://` path. The adapter derives the Glue REST prefix `catalogs/{account-id}` from it automatically. Turn `use_sigv4` on. `region`, `access_key`, and `secret_key` then become required, and they sign the catalog requests. Omit `endpoint`. With `path_style: false`, the S3 client derives the standard AWS endpoint from `region`. The account id below is a placeholder, so substitute your own.

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

`session_token` is optional. For a long-lived key pair, remove it. To let the catalog vend short-lived S3 credentials for data access instead of the static pair, add `"use_vended_credentials": true`. The static `access_key`, `secret_key`, and `region` are still required to sign the first `load_table` call. `ALLOW_HTTP` is absent because Glue and AWS S3 use HTTPS.

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
  ICEBERG_NAMESPACE  = 'default';
```

If the catalog or its storage uses plain HTTP, add `ALLOW_HTTP = 'true'`. If not, omit this property.

## Lakekeeper (OIDC via Keycloak + MinIO)

[Lakekeeper](https://github.com/lakekeeper/lakekeeper) is a widely used open-source Iceberg REST catalog. It needs no new adapter code and no new CONNECTION field. The adapter reaches it through the same OAuth2 client-credentials fields as the generic recipe above: `client_id`, `client_secret`, and `oauth2_server_uri`. Lakekeeper authenticates against **Keycloak** and uses **MinIO** for S3 storage. Keycloak is the documented reference IdP of Lakekeeper, and any OIDC-compatible IdP works the same way. Two things are specific to Lakekeeper:

- **Base path.** Lakekeeper serves its REST API under the `/catalog` base path, so `TO` must include it, for example `http://lakekeeper:8181/catalog`. The adapter negotiates this base path automatically from the `GET /v1/config?warehouse=` response of the catalog. No other configuration is necessary.
- **Warehouse is a name, not a path.** Lakekeeper supports many warehouses. `warehouse` is the warehouse **name** that you register with the management API of Lakekeeper, for example `lakehouse_static`. It is not an `s3://` location. The `warehouse` field of Glue uses the same shape with an account id.

The adapter supports both credential modes below. They differ only in the Lakekeeper warehouse and the CONNECTION fields that you use.

**Static credentials** (`sts-enabled: false` on the Lakekeeper warehouse). The adapter reads MinIO directly with the static key pair, like any other backend:

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

**Vended (STS/AssumeRole) credentials** (`sts-enabled: true` on the Lakekeeper warehouse). The recipe is the same, with three differences:

- Point `warehouse` at the vended-credential warehouse.
- Add `"use_vended_credentials": true`.
- Remove the static S3 fields `endpoint`, `region`, `access_key`, and `secret_key`.

The adapter then requests short-lived credentials from the `load_table` response of Lakekeeper. In this stack these credentials are MinIO STS AssumeRole credentials, scoped to the bucket of the warehouse.

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

Create the Virtual Schema exactly as in the static example above. Use the same `ICEBERG_NAMESPACE` and `ALLOW_HTTP`, and name the vended CONNECTION.

## Addressing

The adapter UDF runs **inside** the Exasol container. Every address in the CONNECTION must resolve from there. Use internal hostnames, for example `iceberg-rest`, `minio`, `lakekeeper`, or `keycloak`. Never use `localhost` or the Docker host gateway.
