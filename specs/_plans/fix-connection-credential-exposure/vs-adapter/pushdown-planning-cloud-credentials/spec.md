# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode. A credential the CONNECTION supplies is REFERENCED by connection name in the per-shard scan spec and resolved by the scan UDF; a credential the catalog vends is embedded in that spec, because no name identifies it.

## Background

* **This delta changes WHERE a resolved storage credential travels, not how it is resolved. It is issue #135.** Every selection rule of this feature — the SigV4 gate, the `use_vended_credentials` gate, the single credential-source selection, the longest-`prefix` match, the `storage-credentials`-before-`config` ordering, the CONNECTION-wins address rule, the scheme-driven backend selection, the plaintext consent gates — is UNCHANGED. What changes is the wire form the resolved result takes: a CONNECTION-supplied credential becomes a reference, specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES rather than restates.
* **SUPERSEDES the unconditional clause "Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS tokens) MUST NEVER appear in any returned SQL string or error message."** That sentence was aspirational for the storage half and FALSE against the implemented tree: `crates/lakehouse-engine/src/adapter/pushdown/support.rs:441` serialized the storage block into a SQL literal with no encoding, and the committed golden fixtures contain `"access_key"` and `"secret_key"` in plaintext. The replacement splits it by what can be referenced. Signing keys, bearer tokens, and OAuth2 client secrets never crossed the UDF boundary and still do not. A CONNECTION-supplied STORAGE credential now genuinely does not reach the SQL. A VENDED storage credential still does, tracked as issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378).
* **The scoping is deliberate and the unscoped claim is not merely reworded.** A security spec asserting "no credential appears in any returned SQL" while one class of credential demonstrably does is worse than one that names the exception, because the next reader trusts it and stops looking.
* **The residual is not a re-scoping of an old exception but a NEW one, and it is narrower than what closes.** A vended credential expires and is scoped to the storage prefix the catalog vended it for; a CONNECTION `secret_key` is long-lived and account-wide. Issue #378 records why the same mechanism cannot close it: the Exasol pushdown response carries exactly one string field, so a value the planning layer resolves per query and the UDF may not re-derive has no unobservable path to the UDF.
* **The SigV4 clauses become TRUE rather than being edited.** `use_sigv4` requires a static `access_key`, `secret_key`, and `region` on the CONNECTION, and those are exactly the values the reference now defers, so "the SigV4 signing keys MUST NOT appear in any returned SQL string" holds for the first time. Those two scenarios are therefore UNCHANGED, and this bullet records that their unchanged text is now satisfied by mechanism rather than by intent.
* **Under vending the adapter still resolves the credential itself, and no scenario of this feature moves work to the UDF.** The `loadTable` request, the credential-source selection, and the backend construction stay in the planning layer, run once per query. Only the vending-DISABLED path defers, and it defers a CONNECTION read, not a catalog read.
* No dependency is added and no dependency version changes. `StorageProps`, `StorageBackend`, `AdlsCred`, `VendedS3`, `StaticStoreAddress`, `select_credential_source`, `resolve_vended_storage`, and `resolve_uc_vended_storage` are UNEDITED.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Unsigned catalog path is unchanged when SigV4 and vending are both disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_sigv4` or set it to false AND omit `use_vended_credentials` or set it to false (the existing MinIO / local REST case)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve the file list with unsigned catalog requests exactly as before
* *AND* the adapter MUST NOT read any vended credentials from the `loadTable` response
* *AND* the shard-invariant common scan-spec argument SHALL carry a REFERENCE to the CONNECTION that supplies the static `access_key`, `secret_key`, and optional `session_token`, rather than those values — SUPERSEDING the recorded clause that required each per-shard scan-spec storage block to carry them, which described the exposure of issue #135
* *AND* the referenced credentials SHALL be resolved by the scan UDF under `vs-adapter/scan-spec-credential-reference`, so the credential set reaching object storage is field-for-field what the CONNECTION supplies
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-feature behaviour, changing only the content of the `storage` block of the common argument
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place a REFERENCE to the CONNECTION into each scan spec storage block, and MUST NOT place the static `access_key`, `secret_key`, or `session_token` value there — SUPERSEDING the recorded clause that required those values in the block
* *AND* the adapter MUST NOT attempt to read vended credentials from the `loadTable` response on any catalog-auth mode
* *AND* the credentials the scan reads SHALL be the CONNECTION's own, so this scenario's observable storage behaviour is unchanged and only the transport of the credential changes
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Vended S3 credentials are the sole storage source regardless of catalog auth mode

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true under ANY catalog-auth mode (no-auth, static bearer token, OAuth2 client-credentials, or SigV4)
* *AND* a `loadTable` response for an `s3://` table that carries short-lived vended S3 credentials (access key, secret key, and session token) in either its `storage-credentials` block or its flat `config` map
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL derive the effective storage from that `loadTable` response exactly once per query in the planning layer, gated solely on `use_vended_credentials` and never depending on which catalog-auth mode authenticated the request
* *AND* the adapter SHALL place the vended access key, secret key, and session token INLINE into the storage block of every per-shard scan spec, and MUST NOT emit a connection reference there, because no CONNECTION name identifies a credential the catalog vended for one table
* *AND* the adapter MUST NOT read `access_key`, `secret_key`, or `session_token` from the CONNECTION for this storage block, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled and its absence is an error rather than a silent static read
* *AND* the adapter SHALL resolve the store `endpoint` and `region` for this storage block from the CONNECTION when the CONNECTION states a non-empty value and from the response otherwise, taking each of the two independently
* *AND* the adapter SHALL set `allow_http` from the `ALLOW_HTTP` virtual-schema property, so a resolved plain-`http://` endpoint is honoured only with the operator's consent and a catalog cannot downgrade the transport on its own authority
* *AND* the vended credentials MUST NOT appear in any error message, and DO appear in the returned SQL string — the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), SUPERSEDING the recorded clause that forbade both, which was FALSE for the SQL half
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are extracted on the static bearer-token catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *AND* a `loadTable` response whose flat `config` map carries vended S3 credentials (the Databricks Unity Catalog shape, where `storage-credentials` is empty)
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL authenticate the self-issued `loadTable` GET with an `Authorization: Bearer <token>` header
* *AND* the adapter SHALL extract the vended S3 access key, secret key, and session token from the response `config` map and place them into every per-shard scan spec storage block
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message, because a catalog-auth secret never crosses the UDF boundary
* *AND* the vended credentials MUST NOT appear in any error message, and DO appear in the returned SQL string under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that grouped the `token` and the vended credentials under one prohibition, which now holds for the `token` alone
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are extracted on the OAuth2 client-credentials catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL perform the OAuth2 client-credentials grant to obtain a bearer token and authenticate the self-issued `loadTable` GET with that token
* *AND* the adapter SHALL extract the vended S3 credentials from the `loadTable` response and place them into every per-shard scan spec storage block
* *AND* the `client_secret` value and the obtained bearer token MUST NOT appear in any returned SQL string or error message, because neither crosses the UDF boundary
* *AND* the vended credentials MUST NOT appear in any error message, and DO appear in the returned SQL string under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that grouped all three under one prohibition
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
* *AND* the selected SAS MUST NOT appear in any error message, and DOES appear in the returned SQL string under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the vended sequence written out at its single call site — select the credential source for the location, then build the storage backend that source describes — whose steps `select_credential_source` and the shared per-backend construction functions are the mechanism
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the location anchor, the resolved `ALLOW_HTTP` value, and the CONNECTION's configured store address, and returning `Result<StorageBackend, UdfError>`
* *AND* `resolve_vended_storage` MUST NOT take a storage backend, a `ConnectionCreds`, or any other value carrying a credential field as a parameter, so "no CONNECTION CREDENTIAL is read under vending" stays enforced by what its parameters CAN carry
* *AND* the store-address parameter SHALL be a type declaring EXACTLY the CONNECTION's `endpoint` and `region`, with exactly one conversion from `ConnectionCreds` declared beside it, and a source-level probe SHALL assert that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`
* *AND* the `ALLOW_HTTP` parameter SHALL NOT be read as an exception to that rule: it carries one virtual-schema boolean resolved outside this crate, names no credential, and cannot supply one
* *AND* `resolve_vended_storage` SHALL be the ONLY Iceberg-path vended entry point reachable from outside the `lakehouse-catalog` crate, and EVERY mechanism step — the per-catalog wire extraction and the shared policy and construction functions alike — SHALL stay crate-private
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call, SHALL be the SAME scheme-agnostic selection for both backends, and SHALL supply every value the resolved backend carries
* *AND* that prefix comparison SHALL be made with the URI SCHEME of both the location and the entry `prefix` lowercased, and with everything after `://` compared byte-exactly, because the backend variant is selected from a CASE-INSENSITIVE scheme (RFC 3986 §3.1): a response spelling the location's scheme differently from an entry's `prefix` would otherwise miss the entry that governs that location and silently read the flat `config` map instead, while a bucket, container, or object key stays case-sensitive because two buckets differing only in case are two buckets
* *AND* a matched `storage_credentials` entry SHALL remain authoritative for the whole credential set: a key that entry omits MUST NOT fall back to the flat `config` map, because the Iceberg REST rule is read per credential SET rather than per key
* *AND* `anchor` SHALL be the table's own location — that is what `storage_credentials[*].prefix` matches against AND what the backend variant is selected from — so an HTTPS catalog URI passed as the anchor is rejected as an unsupported scheme rather than silently selecting the flat `config` map
* *AND* the `use_vended_credentials` gate SHALL stay at the call site rather than becoming a parameter of `resolve_vended_storage`, because a boolean that switches a function between "do the work" and "return the input" is a decision the function declined to make
* *AND* that same gate SHALL also select the scan-spec storage wire variant — inline under vending, a connection reference otherwise — through the ONE pure selection function `vs-adapter/scan-spec-credential-reference` specifies, so the variant can never disagree with the resolver that produced its payload and no site chooses it independently
* *AND* the format readers' own vended/static split SHALL be UNCHANGED and MUST NOT return that wrapper, because each reader uses the concrete backend immediately for its own plan-time manifest or log read
* *AND* the catalog-auth secrets and any minted bearer value MUST NOT appear in any returned SQL string or error message; the vended STS keys, the vended session token, the vended SAS, and any static Azure account key or SAS token MUST NOT appear in any error message, and the VENDED values among them DO appear in the returned SQL string under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade all of them in both places
<!-- /DELTA:CHANGED -->
