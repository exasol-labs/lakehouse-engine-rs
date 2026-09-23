# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

<!-- DELTA:CHANGED -->
## Background

* SigV4 signing and credential vending are opt-in per CONNECTION (`use_sigv4`,
  `use_vended_credentials`); both default to false so existing MinIO/REST stacks
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
* See `vs-adapter/pushdown-planning` for the base pushdown planning scenarios and
  `vs-adapter/rest-catalog-oauth-auth` for the catalog-auth modes.
* **Iceberg REST compliance, quoted from `apache/iceberg` `open-api/rest-catalog-open-api.yaml`
  (main), verified against the fetched file.** `StorageCredential.prefix`: "Indicates a
  storage location prefix where the credential is relevant. Clients should choose the most
  specific prefix (by selecting the longest prefix) if several credentials of the same type
  are available." `LoadTableResult`, under `## Storage Credentials`: "Credentials for ADLS /
  GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first
  check whether the respective credentials exist in the `storage-credentials` field before
  checking the `config` for credentials." The `## AWS Configurations` section enumerates
  `client.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`,
  `s3.remote-signing-enabled`, and `s3.cross-region-access-enabled`, and the document
  enumerates no ADLS key at all (checked by searching the fetched file for `adls.`). The
  host-suffixed `adls.sas-token.<host>` spelling is the Iceberg Java `AzureProperties`
  convention that Lakekeeper emits, live-verified against a real Lakekeeper response. Both
  maps stay readable under the spec's `additionalProperties: string` allowance, so reading a
  key the spec doesn't enumerate (`s3.endpoint`, `s3.path-style-access`, the ADLS SAS key) is
  not a deviation, and neither spec constrains a client's own precedence between a stated
  value and a vended one for `endpoint`, `region`, or `path_style`.
* ONE credential-source selection serves both backends: the longest `storage_credentials`
  entry whose non-empty `prefix` prefixes the table location, else the flat `config` map. A
  matched entry is authoritative for the WHOLE credential set — a key it omits does NOT fall
  back to the flat `config` map, because the REST rule above is read per credential set, not
  per key. The prefix comparison lowercases the URI scheme on both sides and compares
  everything after `://` byte-exactly.
* **The storage backend variant is selected from the table location's URI scheme alone, and
  the mapping is closed.** `s3://`/`s3a://` select the S3 backend; `abfss://`/`abfs://` select
  the ADLS backend; every other scheme (or a scheme-less location) is an error naming the
  scheme; there is no default backend.
* **Iceberg table-spec grounding for reading the scheme off the table location**, quoted from
  `apache/iceberg` `format/spec.md` (main). Table metadata field `location` is "The table's
  base location...", marked `_required_` in v1, v2, and v3; v4 makes it `_optional_` but "Must
  be an absolute path when present". The spec defines a "Relative path" as one that does not
  start with a URI scheme, so an absolute location by definition starts with one — a
  scheme-less or absent location is a malformed catalog response.
* **The vended ADLS SAS is recovered by host, and the account name is derived from that same
  host.** The vended key is host-suffixed on the wire (`adls.sas-token.<host>`) but flat in
  the backend (`adls.sas-token`), because the downstream reader
  (`iceberg-0.10.0/src/io/storage/config/azdls.rs`) accepts only the flat form. The host
  comparison against the table location's host is case-insensitive (RFC 3986 §3.2.2); the
  account name is taken VERBATIM (not case-folded) from that host's first dot-separated
  label, because `iceberg-storage-opendal` compares it byte-exactly against the account in
  each file URI as a wrong-account guard (`adls.account-name`).
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
  also signs the catalog request, per `vs-adapter/connection-credentials`); a Glue CONNECTION
  that omits `region` places the store from Glue's vended `client.region` alone, which stays
  UNVERIFIED — `e2e-harness/cloud-e2e-harness` reports that key without asserting it.
* The static `access_key` and `secret_key` a SigV4 CONNECTION supplies, and a `region` it
  states, are CATALOG-authentication inputs (`vs-adapter/connection-credentials`) and keep
  their existing guard independent of vending: they sign the `loadTable` request before any
  credential is vended, and no longer reach the scan's storage as credentials once vending is
  requested (the stated `region` still reaches it, as addressing).
* Remote signing (`s3.remote-signing-enabled`, `remote-signing-config`) is not implemented and
  is read nowhere in this engine; a warehouse configured for it vends no access key, so it
  fails loud at plan time rather than silently falling through to static credentials.
* **Per-side credential and backend resolution for joins.** Each side of a broadcast-eligible
  or fallback join resolves its own effective storage independently, and its own backend
  travels with it into the scan spec; the adapter never compares sides' resolved backends at
  plan time. A backend-variant difference (`s3://` vs `abfss://`) and an ADLS storage-account
  difference are both served, because each yields a distinct DataFusion registry key. The one
  shape the scan cannot serve — two sides needing different stores under one registry key
  (two ADLS containers of one account) — is refused by the scan's own store-registration
  precondition, not restated at plan time.
* Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS tokens, a vended
  SAS) MUST NEVER appear in any returned SQL string or error message. `redact_credentials`
  matches the `adls.sas-token` label by case-insensitive substring, so its host-suffixed wire
  spelling is redacted too. Every new error path returns `Result`, never panics — a panic
  inside a UDF is an abnormal VM exit that SIGKILLs every sibling VM of the statement part —
  and every such error names the location's storage host, scheme, or expected config key,
  never a credential value.
* An ABSENT table location is rejected by `vs-adapter/pushdown-planning-file-resolution`
  before the vended/static split, so this feature's vended and static resolution paths never
  see one; that rule is path-independent (vending disabled, vending enabled, every join side)
  and is owned there, not restated here.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Catalog REST requests to Glue are SigV4-signed when enabled

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and supply `region`, `access_key`, and `secret_key`
* *AND* a query that requires resolving the Iceberg snapshot and file list from an AWS Glue Iceberg REST catalog endpoint
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL sign every outbound catalog HTTP request with an AWS SigV4 signature computed from the credentials, the configured `region`, and the `glue` signing service name
* *AND* the adapter SHALL resolve the data-file list through the signed catalog requests
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message
