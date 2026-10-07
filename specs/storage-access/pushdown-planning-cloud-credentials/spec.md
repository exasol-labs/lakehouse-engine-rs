# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer. The adapter signs catalog requests with AWS SigV4 when enabled and derives the Glue REST catalog prefix. When vending is enabled, it extracts short-lived vended credentials from the `loadTable` response on every catalog-authentication mode and seals them into every per-shard scan spec. When vending is disabled, each scan spec carries a reference to the CONNECTION instead.

## Background

* SigV4 signing and credential vending are opt-in per CONNECTION (`use_sigv4`,
  `use_vended_credentials`); both default to false so existing SeaweedFS/REST stacks
  behave exactly as before.
* On the SigV4/Glue path the adapter derives the REST catalog prefix `catalogs/{warehouse}`
  by unconditionally prepending `catalogs/` to the configured bare-account-id `warehouse`,
  because AWS Glue's Iceberg REST catalog requires that form; the user-facing `warehouse`
  stays the bare AWS account id. The derivation applies only to Glue — `CatalogAuth::Sigv4`
  is exclusively the Glue path today — and does NOT generalize to other SigV4-style
  catalogs such as S3 Tables (tracked exception: #123).
* **`use_vended_credentials` is orthogonal to catalog authentication.** Vended S3
  credential extraction is gated SOLELY on `use_vended_credentials`, identically across all
  four catalog-auth modes (no-auth, static bearer `token`, OAuth2 client-credentials, AWS
  SigV4); the catalog-auth mode selects only how the table-load request authenticates.
* The adapter resolves the table once per query via a single self-issued `loadTable` GET,
  authenticated per the catalog-auth mode. Its raw `LoadTableResult` feeds BOTH Iceberg file
  planning AND vended-credential extraction — `iceberg-catalog-rest` 0.9.1's
  `RestCatalog::load_table` drops the response `config`/`storage_credentials`, so it cannot
  surface vended creds on its own.
* When `use_vended_credentials` is false, no `loadTable` response field is read for
  credentials and the static storage credentials flow through unchanged on every auth mode.
* See `pushdown/pushdown-planning` for the base pushdown planning scenarios and
  `catalog/rest-catalog-oauth-auth` for the catalog-auth modes.
* **A catalog MUST NOT downgrade the transport on its own authority.** A resolved plaintext
  address — a vended or CONNECTION-configured `http://` S3 endpoint, or an `abfs://` ADLS
  location — is honoured only when the `ALLOW_HTTP` virtual-schema property is true (default
  false); otherwise it is a clear plan-time error, regardless of which source supplied the
  address. `ALLOW_HTTP` is resolved once outside the catalog crate and threaded in as a plain
  boolean; it names no credential and cannot supply one. The non-vended `storage_block` path
  carries no such gate.
* **Effective scan storage under vending splits credentials from addressing.** CREDENTIALS
  (the S3 key pair and session token, or the ADLS SAS) come from the selected `loadTable`
  credential source ALONE — never backfilled from the CONNECTION — and an unmet credential
  request is a plan-time error. ADDRESSING (`endpoint`, `region`) comes from the CONNECTION
  when the CONNECTION states a non-empty value, else from the selected source, resolved
  independently per field; when neither states one, that field is legal-empty and resolves to
  the AWS default chain. `path_style` resolves in three ordered steps: the CONNECTION's stated
  value, else the source's `s3.path-style-access`, else whether an `endpoint` was resolved at
  all (the last-resort step stays because `register_side_store` gates `endpoint` on
  `path_style`). The backend variant still comes from the table location's scheme alone,
  regardless of any of this. Both catalog kinds share this precedence through one function,
  `s3_backend`. A Glue CONNECTION that states `region` places the store with that value (it
  also signs the catalog request, per `connection/connection-credentials`); a Glue CONNECTION
  that omits `region` places the store from Glue's vended `client.region` alone, which stays
  UNVERIFIED — the opt-in cloud E2E suite (`specs/testing.md`) reports that key without asserting it.
* The static `access_key` and `secret_key` a SigV4 CONNECTION supplies, and a `region` it
  states, are CATALOG-authentication inputs (`connection/connection-credentials`) and keep
  their existing guard independent of vending: they sign the `loadTable` request before any
  credential is vended, and no longer reach the scan's storage as credentials once vending is
  requested (the stated `region` still reaches it, as addressing).
* Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS tokens, a vended
  SAS) MUST NEVER appear in any returned SQL string or error message. `redact_credentials`
  matches the `adls.sas-token` label by case-insensitive substring, so its host-suffixed wire
  spelling is redacted too. Every new error path returns `Result`, never panics — a panic
  inside a UDF is an abnormal VM exit that SIGKILLs every sibling VM of the statement part —
  and every such error names the location's storage host, scheme, or expected config key,
  never a credential value.
* See `storage-access/pushdown-planning-cloud-credentials-vended-storage` for how the effective scan storage is resolved from a `loadTable` response under vending.

## Scenarios

### Scenario: Catalog REST requests to Glue are SigV4-signed when enabled

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and supply `region`, `access_key`, and `secret_key`
* *AND* a query that requires resolving the Iceberg snapshot and file list from an AWS Glue Iceberg REST catalog endpoint
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL sign every outbound catalog HTTP request with an AWS SigV4 signature computed from the credentials, the configured `region`, and the `glue` signing service name
* *AND* the adapter SHALL resolve the data-file list through the signed catalog requests
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message

### Scenario: Unsigned catalog path is unchanged when SigV4 and vending are both disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_sigv4` or set it to false AND omit `use_vended_credentials` or set it to false (the existing SeaweedFS / local REST case)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve the file list with unsigned catalog requests exactly as before
* *AND* the adapter MUST NOT read any vended credentials from the `loadTable` response
* *AND* the shard-invariant common scan-spec argument SHALL carry a REFERENCE to the CONNECTION that supplies the static `access_key`, `secret_key`, and optional `session_token`, rather than those values (issue #135), unless the CONNECTION names `aws_assume_role_arn`, whose block is SEALED instead (`storage-access/scan-spec-credential-reference`)
* *AND* the referenced credentials SHALL be resolved by the scan UDF under `storage-access/scan-spec-credential-reference`, so the credential set reaching object storage is field-for-field what the CONNECTION supplies
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-feature behaviour, changing only the content of the `storage` block of the common argument

### Scenario: Vended S3 credentials are the sole storage source regardless of catalog auth mode

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true under any catalog-auth mode (no-auth, static bearer token, OAuth2 client-credentials, or SigV4)
* *AND* a `loadTable` response for an `s3://` table that carries short-lived vended S3 credentials (access key, secret key, and session token) in either its `storage-credentials` block or its flat `config` map
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL derive the effective storage from that `loadTable` response exactly once per query in the planning layer, gated solely on `use_vended_credentials` and never depending on which catalog-auth mode authenticated the request — while the PLANNING outcome for the no-auth mode is the refusal `storage-access/scan-spec-credential-reference` specifies, raised downstream of this derivation by the one variant-selection function
* *AND* on the three auth modes carrying secret material the adapter SHALL place the resolved backend — vended access key, secret key, and session token included — into the storage block of every per-shard scan spec ONLY inside the sealed envelope of `storage-access/scan-spec-credential-reference`, and MUST NOT emit a bare connection reference there (no CONNECTION name identifies a credential the catalog vended for one table) and MUST NOT emit a plaintext inline backend
* *AND* the adapter MUST NOT read `access_key`, `secret_key`, or `session_token` from the CONNECTION for this storage block, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled and its absence is an error rather than a silent static read
* *AND* the adapter SHALL resolve the store `endpoint` and `region` for this storage block from the CONNECTION when the CONNECTION states a non-empty value and from the response otherwise, taking each of the two independently
* *AND* the adapter SHALL set `allow_http` from the `ALLOW_HTTP` virtual-schema property, so a resolved plain-`http://` endpoint is honoured only with the operator's consent and a catalog cannot downgrade the transport on its own authority
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — they appear there only as the sealed envelope's ciphertext, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause whose SQL half was FALSE before this plan

### Scenario: Vended credentials are extracted on the static bearer-token catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *AND* a `loadTable` response whose flat `config` map carries vended S3 credentials (the Databricks Unity Catalog shape, where `storage-credentials` is empty)
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL authenticate the self-issued `loadTable` GET with an `Authorization: Bearer <token>` header
* *AND* the adapter SHALL extract the vended S3 access key, secret key, and session token from the response `config` map and place them into every per-shard scan spec storage block, sealed under `storage-access/scan-spec-credential-reference`'s envelope
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message, because a catalog-auth secret never crosses the UDF boundary as a parsed value — it contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped the `token` and the vended credentials under one prohibition, which now holds in the plaintext sense for both

### Scenario: Vended credentials are extracted on the OAuth2 client-credentials catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL perform the OAuth2 client-credentials grant to obtain a bearer token and authenticate the self-issued `loadTable` GET with that token
* *AND* the adapter SHALL extract the vended S3 credentials from the `loadTable` response and place them into every per-shard scan spec storage block, sealed under `storage-access/scan-spec-credential-reference`'s envelope
* *AND* the `client_secret` value and the obtained bearer token MUST NOT appear in any returned SQL string or error message, because neither crosses the UDF boundary as a parsed value — the `client_secret` contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped all three under one prohibition, which now holds in the plaintext sense for all three

### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place a REFERENCE to the CONNECTION into each scan spec storage block, and MUST NOT place the static `access_key`, `secret_key`, or `session_token` value there, unless the CONNECTION names `aws_assume_role_arn`, whose block is SEALED instead (`storage-access/scan-spec-credential-reference`)
* *AND* the adapter MUST NOT attempt to read vended credentials from the `loadTable` response on any catalog-auth mode
* *AND* the credentials the scan reads SHALL be the CONNECTION's own, so this scenario's observable storage behaviour is unchanged and only the transport of the credential changes

### Scenario: SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and set `warehouse` to a bare AWS account id (e.g. `123456789012`)
* *WHEN* the adapter issues a self-issued catalog HTTP request under SigV4 — the `loadTable` GET that resolves the file list during `pushdown`, or the namespace/table list GETs that enumerate tables during `createVirtualSchema`
* *THEN* the adapter SHALL address the catalog under the REST prefix `catalogs/{warehouse}`, derived by unconditionally prepending `catalogs/` to the configured `warehouse`, so account id `123456789012` yields the path segment `catalogs/123456789012`
* *AND* the adapter SHALL apply this identical derived prefix on both the `loadTable` path and the namespace/table enumeration path, from one shared derivation
* *AND* the adapter MUST NOT contact the `/v1/config` endpoint to resolve the prefix on the SigV4/Glue path
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message
