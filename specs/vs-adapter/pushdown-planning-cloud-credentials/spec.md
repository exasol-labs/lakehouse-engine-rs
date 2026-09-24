# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

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
* *AND* the shard-invariant common scan-spec argument SHALL carry a REFERENCE to the CONNECTION that supplies the static `access_key`, `secret_key`, and optional `session_token`, rather than those values — SUPERSEDING the recorded clause that required each per-shard scan-spec storage block to carry them, which described the exposure of issue #135
* *AND* the referenced credentials SHALL be resolved by the scan UDF under `vs-adapter/scan-spec-credential-reference`, so the credential set reaching object storage is field-for-field what the CONNECTION supplies
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-feature behaviour, changing only the content of the `storage` block of the common argument

### Scenario: Vended S3 credentials are the sole storage source regardless of catalog auth mode

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true under any catalog-auth mode (no-auth, static bearer token, OAuth2 client-credentials, or SigV4)
* *AND* a `loadTable` response for an `s3://` table that carries short-lived vended S3 credentials (access key, secret key, and session token) in either its `storage-credentials` block or its flat `config` map
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL derive the effective storage from that `loadTable` response exactly once per query in the planning layer, gated solely on `use_vended_credentials` and never depending on which catalog-auth mode authenticated the request — while the PLANNING outcome for the no-auth mode is the refusal `vs-adapter/scan-spec-credential-reference` specifies, raised downstream of this derivation by the one variant-selection function
* *AND* on the three auth modes carrying secret material the adapter SHALL place the resolved backend — vended access key, secret key, and session token included — into the storage block of every per-shard scan spec ONLY inside the sealed envelope of `vs-adapter/scan-spec-credential-reference`, and MUST NOT emit a bare connection reference there (no CONNECTION name identifies a credential the catalog vended for one table) and MUST NOT emit a plaintext inline backend
* *AND* the adapter MUST NOT read `access_key`, `secret_key`, or `session_token` from the CONNECTION for this storage block, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled and its absence is an error rather than a silent static read
* *AND* the adapter SHALL resolve the store `endpoint` and `region` for this storage block from the CONNECTION when the CONNECTION states a non-empty value and from the response otherwise, taking each of the two independently
* *AND* the adapter SHALL set `allow_http` from the `ALLOW_HTTP` virtual-schema property, so a resolved plain-`http://` endpoint is honoured only with the operator's consent and a catalog cannot downgrade the transport on its own authority
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — they appear there only as the sealed envelope's ciphertext, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause whose SQL half was FALSE before this plan

### Scenario: Vended credentials are extracted on the static bearer-token catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *AND* a `loadTable` response whose flat `config` map carries vended S3 credentials (the Databricks Unity Catalog shape, where `storage-credentials` is empty)
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL authenticate the self-issued `loadTable` GET with an `Authorization: Bearer <token>` header
* *AND* the adapter SHALL extract the vended S3 access key, secret key, and session token from the response `config` map and place them into every per-shard scan spec storage block, sealed under `vs-adapter/scan-spec-credential-reference`'s envelope
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message, because a catalog-auth secret never crosses the UDF boundary as a parsed value — it contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped the `token` and the vended credentials under one prohibition, which now holds in the plaintext sense for both

### Scenario: Vended credentials are extracted on the OAuth2 client-credentials catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL perform the OAuth2 client-credentials grant to obtain a bearer token and authenticate the self-issued `loadTable` GET with that token
* *AND* the adapter SHALL extract the vended S3 credentials from the `loadTable` response and place them into every per-shard scan spec storage block, sealed under `vs-adapter/scan-spec-credential-reference`'s envelope
* *AND* the `client_secret` value and the obtained bearer token MUST NOT appear in any returned SQL string or error message, because neither crosses the UDF boundary as a parsed value — the `client_secret` contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped all three under one prohibition, which now holds in the plaintext sense for all three

### Scenario: Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *WHEN* the adapter issues the `loadTable` request to fetch vended credentials for an `s3://` table
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` request header so spec-compliant catalogs return vended credentials
* *AND* the adapter SHALL resolve the per-shard scan-spec storage `region` from the CONNECTION's `region` when that value is non-empty, and from the selected credential source's `client.region` otherwise; when NEITHER states one, the `region` SHALL be empty
* *AND* the adapter SHALL resolve the per-shard scan-spec storage `endpoint` from the CONNECTION's `endpoint` when that value is non-empty, and from the selected credential source's `s3.endpoint` otherwise; when NEITHER states one, the `endpoint` SHALL be empty
* *AND* the two SHALL be resolved INDEPENDENTLY, so a CONNECTION stating only a `region` beside a response stating only an `s3.endpoint` yields BOTH values rather than one source winning wholesale
* *AND* an S3 storage block whose `endpoint` and `region` are BOTH empty SHALL be produced successfully and MUST NOT be refused, because Databricks AWS vends short-lived credentials with no endpoint and no region field at all and the AWS default chain places that store
* *AND* the adapter SHALL resolve `path_style` in THREE ordered steps: the CONNECTION's `path_style` when the CONNECTION STATES one, else the selected credential source's `s3.path-style-access` when the response states a value parseable as a boolean, else whether a store `endpoint` was RESOLVED at all under the two rules above — SUPERSEDING the recorded clause "the adapter MUST NOT read the CONNECTION's `path_style` here, because that field is a plain boolean with a `true` default and cannot express 'unstated'" (#130)
* *AND* "STATES one" SHALL mean the CONNECTION JSON carried a `path_style` key with a boolean value, so an ABSENT key SHALL fall through to the second step and a stated `false` SHALL WIN over a vended `true`
* *AND* the third step SHALL remain the LAST resort rather than being deleted, because `register_side_store` treats `path_style` as the gate on whether `endpoint` reaches the S3 builder
* *AND* `path_style` SHALL be resolved INDEPENDENTLY of `endpoint` and `region`
* *AND* a CONNECTION stating `path_style: false` beside a RESOLVED non-empty `endpoint` SHALL be honoured and MUST NOT be refused
* *AND* the vended region, endpoint, path-style, and keys SHALL be read from the SAME single credential source — the longest-matching `storage_credentials` entry's config, falling back to the flat `config` map only when no entry's non-empty `prefix` prefixes the location — so this delta changes what OVERRIDES a vended value and never which response map a vended value is read from

### Scenario: The storage backend under vending is selected from the table location's URI scheme

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *AND* a `loadTable` response whose table metadata `location` is an absolute URI
* *WHEN* the adapter resolves the effective scan storage for that table
* *THEN* the adapter SHALL select the storage backend from that location's URI scheme ALONE, mapping `s3://` and `s3a://` to the S3 backend and `abfss://` and `abfs://` to the ADLS backend
* *AND* the adapter MUST NOT consult the CONNECTION credential shape, the backend `storage_block` selected, or any virtual-schema property to make this selection, so a CONNECTION carrying static Azure credentials and a CONNECTION carrying static S3 credentials resolve to the SAME backend for the same table location
* *AND* for any other scheme — and for a location that carries no `<scheme>://` prefix at all — the adapter SHALL return a `UdfError::User` naming the unsupported scheme and MUST NOT fall back to a default backend
* *AND* an ABSENT table location SHALL never reach this selection, because `vs-adapter/pushdown-planning-file-resolution` rejects a `loadTable` response carrying no location BEFORE the vended/static split — a path-independent rule this feature references rather than restates, so the prohibition on substituting the CONNECTION's `warehouse` binds the non-vended path too
* *AND* that error MUST NOT contain any credential value
* *AND* the mapping SHALL be a TOTAL function over its input: every one of the four accepted schemes yields a backend and EVERY other input, including the empty string, yields a `UdfError::User` — so the catch-all branch is REQUIRED here rather than forbidden, because the match is over a scheme string and not over a `StorageBackend`
* *AND* because this site does not match on `StorageBackend`, adding a THIRD variant to that enum SHALL NOT break this site's build, and this clause states that plainly rather than claiming a compile-time guarantee it cannot deliver
* *AND* a source-level probe in `crates/lakehouse-catalog/tests/catalog_public_surface.rs` SHALL therefore EXTRACT the variant list from `storage.rs`'s `enum StorageBackend` source — already reachable there through the `CATALOG_SOURCES` `include_str!` table — and assert that every extracted variant name appears in `vended.rs`, so a third variant left unreachable from vending fails that probe instead of becoming a silent gap
* *AND* a probe holding a HARDCODED variant list SHALL NOT satisfy the preceding clause, because such a list keeps passing after a third variant is added, which is the exact silent gap the probe exists to prevent
* *AND* when the selected scheme is a plaintext one — `abfs://`, or `s3://`/`s3a://` resolving to a vended plain-`http://` endpoint — the adapter SHALL honour it only when the `ALLOW_HTTP` virtual-schema property is true, and otherwise SHALL return a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property with no credential value

### Scenario: A vended-credentials request the catalog does not satisfy is a clear error

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *WHEN* the adapter resolves the effective scan storage from a `loadTable` response whose selected credential source carries no usable credential for the location's backend — for an S3 location, an absent or empty-string `s3.access-key-id` or `s3.secret-access-key`; for an ADLS location, no `adls.sas-token.<host>` key whose recovered host equals the location's host under case-insensitive comparison
* *THEN* the adapter SHALL return a `UdfError::User` stating that vended credentials were requested and the catalog returned none for that location
* *AND* the adapter MUST NOT fall back to any static CONNECTION credential, because a silent fallback reads through credentials the operator believed were unused
* *AND* a usable key pair beside an EMPTY store address SHALL NOT be an error: the recorded clause requiring a `UdfError::User` naming `client.region` and `s3.endpoint` when the source carries neither is SUPERSEDED and that error is DELETED, because the CONNECTION or the AWS default chain now places the store and refusing it would reject legal Databricks AWS tables
* *AND* every such error SHALL name the location's storage host or scheme and MUST NOT contain any credential value or any vended secret
* *AND* the ADLS variant of that error SHALL name the anchor's host in a position NOT preceded by an `adls.sas-token` label, because `redact_credentials` truncates everything from the end of that label to the next delimiter (`crates/lakehouse-catalog/src/redaction.rs:52` and `:69-76`) — so an error whose only mention of the host sits inside the expected key `adls.sas-token.<host>` reaches the operator as `adls.sas-token[REDACTED]` with both the key and the host destroyed
* *AND* that truncation of the key label SHALL be treated as intended rather than worked around, and the ADLS error text MUST still contain the anchor host AFTER redaction runs
* *AND* every such error MUST be returned as a `Result`, never raised as a panic, because a panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part

### Scenario: A vended Azure SAS is selected by host and carries a consistent account name

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *AND* a table whose location is `abfss://<container>@<account>.dfs.core.windows.net/<path>`
* *AND* a `loadTable` response whose selected credential source carries one or more host-suffixed `adls.sas-token.<host>` keys
* *WHEN* the adapter resolves the effective scan storage for that table
* *THEN* the adapter SHALL recover `<host>` from each such key and SHALL select the ONE key whose recovered host equals the host of the table location, read as the segment after any `<container>@` userinfo and before the next `/`
* *AND* that host comparison SHALL be CASE-INSENSITIVE, because RFC 3986 §3.2.2 makes a URI host case-insensitive — the same rule this feature already applies to the scheme — so a catalog spelling the account differently from the table location names the same storage account and MUST NOT be reported as the catalog having vended none
* *AND* when the source carries both an exact-case spelling of the location's host and a case-variant one, the adapter SHALL select the EXACT spelling; when it carries only case-variant spellings, the adapter SHALL select the lexicographically smallest key — so a payload carrying case-variant keys resolves deterministically rather than by hash-map iteration order in either case
* *AND* the KEY LABEL `adls.sas-token.` SHALL still be matched exactly, because unlike the host it is a protocol key spelling with no documented case rule and the S3 arm reads its own keys exactly — relaxing one arm alone would make the two arms disagree about what a vended key is
* *AND* the adapter SHALL resolve the ADLS backend's `account_name` from that same recovered host — its first dot-separated label, which for the `<account>.dfs.core.windows.net` form is the label before `.dfs.` — so the account name and the SAS always describe one storage account
* *AND* the adapter SHALL take that label VERBATIM from the table location and MUST NOT case-fold it, because the guard it feeds compares it byte-exactly against the account parsed out of each file URI (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`) — a normalised account name would fire the wrong-account guard on the very locations it was derived from, which is why the host comparison above is relaxed while this derivation is not
* *AND* the adapter MUST NOT read `account_name`, `account_key`, or `sas_token` from the CONNECTION for this storage block, so a vended-only Azure CONNECTION that supplies none of them resolves successfully and a CONNECTION that supplies a static account key has that key ignored
* *AND* the adapter SHALL place the selected SAS into the ADLS backend's SAS credential state, so the flat `adls.sas-token` config key the iceberg ADLS reader accepts is emitted by the existing `catalog_storage_props` mapping without a second key spelling
* *AND* when the recovered host carries no dot-separated label from which an account name can be read, the adapter SHALL return a `UdfError::User` naming the host and MUST NOT emit an empty `account_name`, because `adls.account-name` is the wrong-account guard and an empty value disarms it
* *AND* the selected SAS MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that forbade both

### Scenario: A join whose sides resolve to different storage backends is planned, not rejected

* *GIVEN* a broadcast-eligible or fallback join under ONE CONNECTION whose credentials set `use_vended_credentials` to true
* *AND* two or more involved tables whose locations do not all select the same storage backend — an `s3://` fact with an `abfss://` dimension, or two `abfss://` sides naming DIFFERENT storage accounts
* *WHEN* the adapter has resolved every side's file list and effective storage
* *THEN* the adapter SHALL plan the join and MUST NOT compare the sides' resolved storage backends at all, because each side's own backend travels with that side into the scan spec and is read through a store built from that backend (`datafusion-scan/scan-execution-join`)
* *AND* the adapter MUST NOT reject a join for a backend VARIANT difference, because the two schemes yield two DataFusion registry keys and each side's store is built by its own backend's arm — no S3 builder is ever handed an `abfss://` URI
* *AND* the adapter MUST NOT reject a join for an ADLS storage-ACCOUNT difference, because two accounts are two hosts and therefore two registry keys, each store carrying its own account name and SAS
* *AND* a join whose sides all select the SAME backend variant SHALL keep its current plan, whether or not the two sides' credentials are equal
* *AND* the ONE shape the scan cannot serve — two sides collapsing onto ONE registry key while needing DIFFERENT stores, i.e. two ADLS CONTAINERS of one storage account — SHALL be refused by the scan's own store-registration precondition and MUST NOT be restated at plan time, because that collapse is a property of the DERIVED store URLs and a plan-time copy would re-derive DataFusion's registry-key formula in a second place
* *AND* an unserveable spec SHALL therefore fail with the scan's `UdfError::User` naming both derived store URLs, and MUST NOT contain any credential value, vended secret, or SAS token

### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place a REFERENCE to the CONNECTION into each scan spec storage block, and MUST NOT place the static `access_key`, `secret_key`, or `session_token` value there — SUPERSEDING the recorded clause that required those values in the block
* *AND* the adapter MUST NOT attempt to read vended credentials from the `loadTable` response on any catalog-auth mode
* *AND* the credentials the scan reads SHALL be the CONNECTION's own, so this scenario's observable storage behaviour is unchanged and only the transport of the credential changes

### Scenario: SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and set `warehouse` to a bare AWS account id (e.g. `123456789012`)
* *WHEN* the adapter issues a self-issued catalog HTTP request under SigV4 — the `loadTable` GET that resolves the file list during `pushdown`, or the namespace/table list GETs that enumerate tables during `createVirtualSchema`
* *THEN* the adapter SHALL address the catalog under the REST prefix `catalogs/{warehouse}`, derived by unconditionally prepending `catalogs/` to the configured `warehouse`, so account id `123456789012` yields the path segment `catalogs/123456789012`
* *AND* the adapter SHALL apply this identical derived prefix on both the `loadTable` path and the namespace/table enumeration path, from one shared derivation
* *AND* the adapter MUST NOT contact the `/v1/config` endpoint to resolve the prefix on the SigV4/Glue path
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message

### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the vended sequence written out at its single call site — select the credential source for the location, then build the storage backend that source describes — whose steps `select_credential_source` and the shared per-backend construction functions are the mechanism
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the location anchor, the resolved `ALLOW_HTTP` value, and the CONNECTION's configured store address, and returning `Result<StorageBackend, UdfError>`
* *AND* `resolve_vended_storage` MUST NOT take a storage backend, a `ConnectionCreds`, or any other value carrying a credential field as a parameter, so "no CONNECTION CREDENTIAL is read under vending" stays enforced by what its parameters CAN carry
* *AND* the store-address parameter SHALL be a type declaring EXACTLY the CONNECTION's `endpoint`, `region`, and `path_style`, with exactly one conversion from `ConnectionCreds` declared beside it, and a source-level probe SHALL assert that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password` — SUPERSEDING the recorded clause naming EXACTLY `endpoint` and `region` (#130)
* *AND* the type SHALL carry `path_style` as an OPTION of boolean, because the resolution chain branches on whether the CONNECTION stated a value
* *AND* every field of the type SHALL stay non-`pub` and SHALL be read through an accessor, and the existing probe asserting that non-`pub` property SHALL cover the added field without being rewritten to enumerate fields by name
* *AND* the `ALLOW_HTTP` parameter SHALL NOT be read as an exception to that rule: it carries one virtual-schema boolean resolved outside this crate, names no credential, and cannot supply one
* *AND* `resolve_vended_storage` SHALL be the ONLY Iceberg-path vended entry point reachable from outside the `lakehouse-catalog` crate, and EVERY mechanism step — the per-catalog wire extraction and the shared policy and construction functions alike — SHALL stay crate-private
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call, SHALL be the SAME scheme-agnostic selection for both backends, and SHALL supply every value the resolved backend carries
* *AND* that prefix comparison SHALL be made with the URI SCHEME of both the location and the entry `prefix` lowercased, and with everything after `://` compared byte-exactly, because the backend variant is selected from a CASE-INSENSITIVE scheme (RFC 3986 §3.1): a response spelling the location's scheme differently from an entry's `prefix` would otherwise miss the entry that governs that location and silently read the flat `config` map instead, while a bucket, container, or object key stays case-sensitive because two buckets differing only in case are two buckets
* *AND* a matched `storage_credentials` entry SHALL remain authoritative for the whole credential set: a key that entry omits MUST NOT fall back to the flat `config` map, because the Iceberg REST rule is read per credential SET rather than per key
* *AND* `anchor` SHALL be the table's own location — that is what `storage_credentials[*].prefix` matches against AND what the backend variant is selected from — so an HTTPS catalog URI passed as the anchor is rejected as an unsupported scheme rather than silently selecting the flat `config` map
* *AND* the `use_vended_credentials` gate SHALL stay at the call site rather than becoming a parameter of `resolve_vended_storage`, because a boolean that switches a function between "do the work" and "return the input" is a decision the function declined to make
* *AND* that same gate SHALL also select the scan-spec storage wire variant — the SEALED envelope under vending when the sealing key exists, the named plan-time refusal under vending when it does not (no-auth), a connection reference otherwise — through the ONE pure selection function `vs-adapter/scan-spec-credential-reference` specifies, so the variant can never disagree with the resolver that produced its payload and no site chooses it independently
* *AND* the format readers' own vended/static split SHALL be UNCHANGED and MUST NOT return that wrapper, because each reader uses the concrete backend immediately for its own plan-time manifest or log read
* *AND* the catalog-auth secrets and any minted bearer value MUST NOT appear in any returned SQL string or error message; the vended STS keys, the vended session token, the vended SAS, and any static Azure account key or SAS token MUST NOT appear in any error message, and the VENDED values among them appear in the returned SQL string ONLY as the sealed envelope's ciphertext — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan, never plaintext — SUPERSEDING the recorded clause that forbade all of them in both places, which now holds in the plaintext sense throughout
