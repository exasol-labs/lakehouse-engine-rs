# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode in code path, with ONE combination refused at plan time (no-auth + vending). A credential the CONNECTION supplies is REFERENCED by connection name in the per-shard scan spec and resolved by the scan UDF; a credential the catalog vends is embedded in that spec ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference`, because no name identifies the vended value itself.

## Background

* **This delta changes WHERE a resolved storage credential travels, not how it is resolved. It is issues #135 and #378.** Every selection rule of this feature — the SigV4 gate, the `use_vended_credentials` gate, the single credential-source selection, the longest-`prefix` match, the `storage-credentials`-before-`config` ordering, the CONNECTION-wins address rule, the scheme-driven backend selection, the plaintext consent gates — is UNCHANGED. What changes is the wire form the resolved result takes: a CONNECTION-supplied credential becomes a reference, and a vended credential becomes a SEALED envelope, both specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES rather than restates. One combination gains a plan-time refusal: no-auth + vending, where the envelope's key material would not exist — see the superseded orthogonality bullet below.
* **SUPERSEDES the recorded orthogonality bullet's "It applies identically across all four auth modes" for the PLANNING outcome, not for the gating.** The recorded bullet (`specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:21-27`) states a code-path gating rule — extraction is gated solely on `use_vended_credentials`, and "the catalog-auth mode selects only how the table-load request is authenticated" — not an endorsement of every combination as a deployment shape. The gating stays exactly that. The planning OUTCOME now differs for one combination: a no-auth catalog with vending enabled is refused at plan time under `vs-adapter/scan-spec-credential-reference`'s refusal scenario, because the sealed envelope's key is derived from the CONNECTION's catalog-auth secret material and no-auth supplies none. The refusal breaks no tested deployment: vending is exercised only against authenticated stacks, and the no-auth `iceberg-rest-fixture` stack pins `use_vended_credentials: false` (`crates/lakehouse-engine/tests/common/seed.rs:279`, `tests/common/stack.rs:410`).
* **SUPERSEDES the unconditional clause "Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS tokens) MUST NEVER appear in any returned SQL string or error message."** That sentence was aspirational for the storage half and FALSE against the implemented tree: `crates/lakehouse-engine/src/adapter/pushdown/support.rs:441` serialized the storage block into a SQL literal with no encoding, and the committed golden fixtures contain `"access_key"` and `"secret_key"` in plaintext. The replacement splits it by what can be referenced. Signing keys, bearer tokens, and OAuth2 client secrets never crossed the UDF boundary and still do not. A CONNECTION-supplied STORAGE credential now genuinely does not reach the SQL. A VENDED storage credential reaches it ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — never in plaintext.
* **The scoping is deliberate and the unscoped claim is not merely reworded.** A security spec asserting "no credential appears in any returned SQL" while one class of credential demonstrably does is worse than one that names the exception, because the next reader trusts it and stops looking.
* **The vended exposure is CLOSED by a different mechanism than the static one, with a BOUNDED guarantee.** The Exasol pushdown response carries exactly one string field, and a value the planning layer resolves per query has no name the UDF could re-derive it by — so the vended value crosses that field as AES-GCM ciphertext under a key both sides derive from the same CONNECTION. `vs-adapter/scan-spec-credential-reference` owns the envelope, the bound (defeats a plaintext read of the SQL surfaces, not offline cryptanalysis — acceptable because vended values expire and are scoped to the prefix the catalog vended them for), and the no-auth refusal; this feature CITES it.
* **The SigV4 clauses become TRUE rather than being edited.** `use_sigv4` requires a static `access_key`, `secret_key`, and `region` on the CONNECTION, and those are exactly the values the reference now defers, so "the SigV4 signing keys MUST NOT appear in any returned SQL string" holds for the first time. Those two scenarios are therefore UNCHANGED, and this bullet records that their unchanged text is now satisfied by mechanism rather than by intent.
* **Under vending the adapter still resolves the credential itself, and no scenario of this feature moves resolution work to the UDF.** The `loadTable` request, the credential-source selection, and the backend construction stay in the planning layer, run once per query; the sealing of the result is one further plan-time step. What the UDF gains on the sealed path is a CONNECTION read and an AEAD open — the same grant-gated read the reference path performs, never a catalog read.
* The sealing dependencies this plan adds land in `crates/lakehouse-engine`, where the envelope lives (plan.md § Dependencies); `lakehouse-catalog` gains no dependency. `StorageProps`, `StorageBackend`, `AdlsCred`, `VendedS3`, `StaticStoreAddress`, `select_credential_source`, `resolve_vended_storage`, and `resolve_uc_vended_storage` are UNEDITED.

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

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true under any catalog-auth mode (no-auth, static bearer token, OAuth2 client-credentials, or SigV4)
* *AND* a `loadTable` response for an `s3://` table that carries short-lived vended S3 credentials (access key, secret key, and session token) in either its `storage-credentials` block or its flat `config` map
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL derive the effective storage from that `loadTable` response exactly once per query in the planning layer, gated solely on `use_vended_credentials` and never depending on which catalog-auth mode authenticated the request — while the PLANNING outcome for the no-auth mode is the refusal `vs-adapter/scan-spec-credential-reference` specifies, raised downstream of this derivation by the one variant-selection function
* *AND* on the three auth modes carrying secret material the adapter SHALL place the resolved backend — vended access key, secret key, and session token included — into the storage block of every per-shard scan spec ONLY inside the sealed envelope of `vs-adapter/scan-spec-credential-reference`, and MUST NOT emit a bare connection reference there (no CONNECTION name identifies a credential the catalog vended for one table) and MUST NOT emit a plaintext inline backend
* *AND* the adapter MUST NOT read `access_key`, `secret_key`, or `session_token` from the CONNECTION for this storage block, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled and its absence is an error rather than a silent static read
* *AND* the adapter SHALL resolve the store `endpoint` and `region` for this storage block from the CONNECTION when the CONNECTION states a non-empty value and from the response otherwise, taking each of the two independently
* *AND* the adapter SHALL set `allow_http` from the `ALLOW_HTTP` virtual-schema property, so a resolved plain-`http://` endpoint is honoured only with the operator's consent and a catalog cannot downgrade the transport on its own authority
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — they appear there only as the sealed envelope's ciphertext, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause whose SQL half was FALSE before this plan
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are extracted on the static bearer-token catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *AND* a `loadTable` response whose flat `config` map carries vended S3 credentials (the Databricks Unity Catalog shape, where `storage-credentials` is empty)
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL authenticate the self-issued `loadTable` GET with an `Authorization: Bearer <token>` header
* *AND* the adapter SHALL extract the vended S3 access key, secret key, and session token from the response `config` map and place them into every per-shard scan spec storage block, sealed under `vs-adapter/scan-spec-credential-reference`'s envelope
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message, because a catalog-auth secret never crosses the UDF boundary as a parsed value — it contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped the `token` and the vended credentials under one prohibition, which now holds in the plaintext sense for both
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Vended credentials are extracted on the OAuth2 client-credentials catalog path

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, do not enable `use_sigv4`, and set `use_vended_credentials` to true
* *WHEN* the adapter resolves the file list
* *THEN* the adapter SHALL perform the OAuth2 client-credentials grant to obtain a bearer token and authenticate the self-issued `loadTable` GET with that token
* *AND* the adapter SHALL extract the vended S3 credentials from the `loadTable` response and place them into every per-shard scan spec storage block, sealed under `vs-adapter/scan-spec-credential-reference`'s envelope
* *AND* the `client_secret` value and the obtained bearer token MUST NOT appear in any returned SQL string or error message, because neither crosses the UDF boundary as a parsed value — the `client_secret` contributes to the envelope key only as unparsed HKDF input
* *AND* the vended credentials MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that grouped all three under one prohibition, which now holds in the plaintext sense for all three
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
* *AND* the selected SAS MUST NOT appear in any error message, and MUST NOT appear in PLAINTEXT in the returned SQL string — sealed-envelope ciphertext only, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause that forbade both
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
* *AND* that same gate SHALL also select the scan-spec storage wire variant — the SEALED envelope under vending when the sealing key exists, the named plan-time refusal under vending when it does not (no-auth), a connection reference otherwise — through the ONE pure selection function `vs-adapter/scan-spec-credential-reference` specifies, so the variant can never disagree with the resolver that produced its payload and no site chooses it independently
* *AND* the format readers' own vended/static split SHALL be UNCHANGED and MUST NOT return that wrapper, because each reader uses the concrete backend immediately for its own plan-time manifest or log read
* *AND* the catalog-auth secrets and any minted bearer value MUST NOT appear in any returned SQL string or error message; the vended STS keys, the vended session token, the vended SAS, and any static Azure account key or SAS token MUST NOT appear in any error message, and the VENDED values among them appear in the returned SQL string ONLY as the sealed envelope's ciphertext — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan, never plaintext — SUPERSEDING the recorded clause that forbade all of them in both places, which now holds in the plaintext sense throughout
<!-- /DELTA:CHANGED -->
