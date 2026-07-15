# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

* SigV4 signing and credential vending are opt-in per CONNECTION (`use_sigv4`,
  `use_vended_credentials`); both default to false so existing MinIO/REST stacks
  behave exactly as before.
* On the SigV4/Glue path the adapter derives the REST catalog prefix by
  unconditionally prepending `catalogs/` to the configured bare-account-id
  `warehouse`, because AWS Glue's Iceberg REST catalog requires the prefix in the
  `catalogs/{catalogId}` form. The user-facing `warehouse` remains the bare AWS
  account id; the adapter appends `catalogs/` so the user never supplies it. This
  derivation extends the deliberate SigV4 `/v1/config` short-circuit already
  recorded in `specs/_decision/001-migrate-legacy-decision-log.md`; the
  `catalogs/{id}` shape is a Glue-proprietary convention, not an Apache Iceberg
  REST spec requirement. The derivation applies only to Glue — `CatalogAuth::Sigv4`
  is today exclusively the Glue path — and does NOT generalize to other SigV4-style
  catalogs such as S3 Tables (#123).
* **`use_vended_credentials` is orthogonal to catalog authentication.** Vended S3
  credential extraction is gated SOLELY on `use_vended_credentials`, never on the
  catalog-auth mode. It applies identically across all four auth modes: no-auth,
  static bearer `token`, OAuth2 client-credentials (`client_id` + `client_secret`),
  and AWS SigV4. The catalog-auth mode selects only how the table-load request is
  authenticated; it never gates whether vended creds are extracted.
* The adapter resolves the table once per query via a single self-issued
  `loadTable` GET whose authentication is chosen by the catalog-auth mode (SigV4
  signature | `Authorization: Bearer <token>` | OAuth2-grant-derived bearer |
  none). The raw `LoadTableResult` from that one request feeds BOTH Iceberg file
  planning AND vended-credential extraction. `iceberg-catalog-rest` 0.9.1's
  `RestCatalog::load_table` returns only a `Table` and drops the response
  `config`/`storage_credentials`, so it cannot surface vended creds — the
  self-issued GET is required for vending on every mode.
* When `use_vended_credentials` is false, no `loadTable` response field is read for
  credentials and the static storage credentials flow through unchanged on every
  auth mode.
* Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS
  tokens) MUST NEVER appear in any returned SQL string or error message.
* See `vs-adapter/pushdown-planning` for the base pushdown planning scenarios and
  `vs-adapter/rest-catalog-oauth-auth` for the catalog-auth modes.

## Scenarios

### Scenario: Catalog REST requests to Glue are SigV4-signed when enabled

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and supply `region`, `access_key`, and `secret_key`
* *AND* a query that requires resolving the Iceberg snapshot and file list from an AWS Glue Iceberg REST catalog endpoint
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL sign every outbound catalog HTTP request with an AWS SigV4 signature computed from the credentials, the configured `region`, and the `glue` signing service name
* *AND* the adapter SHALL resolve the data-file list through the signed catalog requests
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message

### Scenario: Unsigned catalog path is unchanged when SigV4 and vending are both disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_sigv4` or set it to false AND omit `use_vended_credentials` or set it to false (the existing MinIO / local REST case)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve the file list with unsigned catalog requests exactly as before
* *AND* the adapter MUST NOT read any vended credentials from the `loadTable` response
* *AND* each per-shard scan-spec storage block SHALL carry the static `access_key`, `secret_key`, and optional `session_token` from the CONNECTION
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-feature behaviour

### Scenario: Vended S3 credentials override static credentials regardless of catalog auth mode

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true under ANY catalog-auth mode (no-auth, static bearer token, OAuth2 client-credentials, or SigV4)
* *AND* a `loadTable` response that carries short-lived vended S3 credentials (access key, secret key, and session token) in either its `storage-credentials` block or its flat `config` map
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL extract the vended S3 access key, secret key, and session token from the `loadTable` response exactly once per query in the planning layer, gated solely on `use_vended_credentials` and never depending on which catalog-auth mode authenticated the request
* *AND* the adapter SHALL place the vended credentials (not the static ones) into the storage block of every per-shard scan spec, preserving the static `endpoint`, `region` (when no vended region is present), `path_style`, and `allow_http`
* *AND* the vended credentials MUST NOT appear in any returned SQL string or error message

### Scenario: Vended credentials are extracted on the static bearer-token catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *AND* a `loadTable` response whose flat `config` map carries vended S3 credentials (the Databricks Unity Catalog shape, where `storage-credentials` is empty)
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL authenticate the self-issued `loadTable` GET with an `Authorization: Bearer <token>` header
* *AND* the adapter SHALL extract the vended S3 access key, secret key, and session token from the response `config` map and place them into every per-shard scan spec storage block
* *AND* the `token` value and the vended credentials MUST NOT appear in any returned SQL string or error message

### Scenario: Vended credentials are extracted on the OAuth2 client-credentials catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL perform the OAuth2 client-credentials grant to obtain a bearer token and authenticate the self-issued `loadTable` GET with that token
* *AND* the adapter SHALL extract the vended S3 credentials from the `loadTable` response and place them into every per-shard scan spec storage block
* *AND* the `client_secret` value, the obtained bearer token, and the vended credentials MUST NOT appear in any returned SQL string or error message

### Scenario: Vended-credentials request advertises access delegation and adopts the vended region

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *WHEN* the adapter issues the `loadTable` request to fetch vended credentials
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` request header so spec-compliant catalogs return vended credentials
* *AND* when the `loadTable` response config carries a `client.region` value, the adapter SHALL set the per-shard scan-spec storage `region` to that vended region
* *AND* when no `client.region` is present, the adapter SHALL preserve the static `region` from the CONNECTION

### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place the static `access_key`, `secret_key`, and optional `session_token` from the CONNECTION into each scan spec storage block
* *AND* the adapter MUST NOT attempt to read vended credentials from the `loadTable` response on any catalog-auth mode

### Scenario: SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and set `warehouse` to a bare AWS account id (e.g. `123456789012`)
* *WHEN* the adapter issues a self-issued catalog HTTP request under SigV4 — the `loadTable` GET that resolves the file list during `pushdown`, or the namespace/table list GETs that enumerate tables during `createVirtualSchema`
* *THEN* the adapter SHALL address the catalog under the REST prefix `catalogs/{warehouse}`, derived by unconditionally prepending `catalogs/` to the configured `warehouse`, so account id `123456789012` yields the path segment `catalogs/123456789012`
* *AND* the adapter SHALL apply this identical derived prefix on both the `loadTable` path and the namespace/table enumeration path, from one shared derivation
* *AND* the adapter MUST NOT contact the `/v1/config` endpoint to resolve the prefix on the SigV4/Glue path
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message
