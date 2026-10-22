# Feature: Storage Backend Enum

A `StorageBackend` enum names the storage backend, so other modules ask the enum for what they need instead of deciding which backend they talk to. The feature covers the enum's S3 and ADLS variants and methods, the tagged scan-spec wire encoding, the `Debug` redaction of the storage credential types, and the scan's object-store registration, which names no backend.

## Background

* `StorageBackend::Adls { account_name: String, cred: AdlsCred }` carries an account name and exactly one credential, so an Azure backend with two credentials or none is unrepresentable. `S3` wraps the unchanged `StorageProps`, and its serde encoding stays field-for-field identical.
* `AdlsCred` redacts its payload in `Debug`, as `ConnectionCreds` does for its secret fields.
* The enum declares `secret_values`, `catalog_storage_props`, and `file_io`. Object-store registration is one backend-dispatching function in `crates/lakehouse-engine/src/scan/object_store.rs`, because `lakehouse-catalog` must not depend on `object_store` or `datafusion` (`architecture.md`).
* `catalog_storage_props` is the single source of the iceberg storage config keys. The S3 keys are `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, and `S3_PATH_STYLE_ACCESS`. The first four are inserted only when non-empty, the session token only when `Some`, and path-style always. The ADLS keys are `iceberg::io::ADLS_ACCOUNT_NAME`, `ADLS_ACCOUNT_KEY`, and `ADLS_SAS_TOKEN` (`adls.account-name`, `adls.account-key`, `adls.sas-token`). Its `HashMap` return is order-safe, because both consumers insert by key.
* The Azure arm must not emit `adls.connection-string`, because `iceberg-storage-opendal` fails `FileIO` construction when that key is present. `OpenDalStorageFactory::Azdls` is a unit variant.
* `adls.account-name` is a deliberate wrong-account guard. `iceberg-storage-opendal` compares it with the account in each URI and raises `DataInvalid` on a mismatch, and `validate_creds` requires `account_name` for every Azure CONNECTION, so the guard is always armed.
* The container comes from the URI userinfo and the endpoint from the URI host, so one `FileIO` serves every container in the account. Both `abfss://` and `abfs://` are accepted.
* `MicrosoftAzureBuilder::with_url` sets both account and container, so the built store is container-scoped. DataFusion keys its registry by scheme and `host:port` only, which excludes userinfo, so two containers of one account collapse onto one registered store. `ListingTableUrl::object_store()` includes userinfo, and `validate_uniform_object_store_files` compares it per file, so a file list mixing containers within one side is already rejected. The across-side case needs its own precondition.
* In the `object_store` crate a shared account key is set with `with_access_key`, not `with_account_key`. A raw SAS string is set with `with_config(AzureConfigKey::SasKey, …)`, not `with_sas_authorization`. An access key beats a SAS in `build()`, which `AdlsCred` makes unreachable.
* `with_url` accepts an `abfss://` host with exactly one of four suffixes (`dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com`) and a single-label account segment. Any other host returns `UrlNotRecognised`. That is an upstream limit the arm surfaces, not one it works around.
* `FileEntry::path` is an absolute S3 or s3a URI. `get_store` matches `scheme://host:port` exactly, so the store-key derivation returns the scheme verbatim, and an `s3a://` file list registers under `s3a://<bucket>`.
* The store-key derivation does not use `Url::host_str()` alone, because on an `abfss://` URI it yields the storage account, not the container.
* The Iceberg table spec defines `file_path` (field 100) as "Full URI for the file with FS scheme" and resolves a path that starts with a URI scheme as absolute. Both rules are scheme-agnostic, and `relativize_path_to_root` and `reconstruct_abs_uri` implement them.
* `CommonScanSpec.storage` is serialized into the scan-driving SQL literal. The same `.so` produces and consumes the spec within one deploy, so the wire tag needs no cross-version compatibility. The tag is externally tagged and lowercase. `#[serde(untagged)]` must not be used.
* The S3 unit, integration, and E2E suites are the characterization gate that shows the S3 path unchanged.
* See `storage-access/storage-backend-enum-selection` for how consumers carry a backend, the sites that select a backend from input, and the modules permitted to name a variant.

## Scenarios

### Scenario: One enum names the storage backend and answers every backend-specific question

* *GIVEN* the `StorageBackend` enum and its three methods `secret_values`, `catalog_storage_props`, and `file_io`
* *WHEN* a consumer asks the backend for its secret values, its iceberg storage config, or a `FileIO`
* *THEN* `lakehouse-catalog` SHALL declare exactly two variants, `S3(StorageProps)` and `Adls { account_name: String, cred: AdlsCred }`, and a `pub` enum `AdlsCred` with EXACTLY TWO states, an account key and a SAS token, each carrying its secret as a `String`
* *AND* `AdlsCred` SHALL implement `Debug` MANUALLY so the wrapped secret is replaced by a redaction marker, matching what `ConnectionCreds` already does for `secret_key`, `session_token`, `token`, and `client_secret`
* *AND* `AdlsCred` SHALL derive `Clone`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`, because `StorageBackend` derives all five and stops compiling without them
* *AND* all three methods SHALL have one arm per variant, and NO method SHALL have a catch-all `_` arm, so a third backend is a build failure at every site with a decision to make rather than a silent fall-through
* *AND* `catalog_storage_props` SHALL emit `iceberg::io::ADLS_ACCOUNT_NAME` when `account_name` is non-empty, and EXACTLY ONE of `iceberg::io::ADLS_ACCOUNT_KEY` and `iceberg::io::ADLS_SAS_TOKEN` chosen by the `AdlsCred` state, and SHALL emit NO other key, in particular no `adls.connection-string`, which fails `FileIO` construction hard, and no `s3.*` key
* *AND* `catalog_storage_props` SHALL name those keys through the `iceberg::io::ADLS_*` constants rather than string literals, so this module stays the SINGLE source of every iceberg storage config key it emits, as it is for the six S3 keys
* *AND* `file_io` SHALL select the unit factory variant `OpenDalStorageFactory::Azdls` and SHALL configure it from `catalog_storage_props` alone, so the Azure props map and the Azure `FileIO` cannot drift
* *AND* `secret_values` SHALL return EXACTLY ONE value for the Azure variant, the account key or the SAS token, and SHALL return an EMPTY list when that value is the empty string, because every returned value is fed to literal-value error redaction and redacting `""` would strip every character of the error it guards
* *AND* `secret_values` MUST NOT return `account_name`, because a storage account name is not a secret and appears in every `abfss://` URI the engine handles
* *AND* the S3 arm of all three methods SHALL depend on `StorageProps` alone, so the six S3 config keys, their inclusion conditions, and the S3 secret list do not depend on the Azure variant

### Scenario: The scan registers its object store without naming the backend

* *GIVEN* the scan's object-store registration, which builds one store per distinct store URL across the spec's sides
* *WHEN* the scan builds its session context
* *THEN* ONE backend-agnostic derivation (`side_store_url`) SHALL return the side's store URL as the `scheme://userinfo@host:port` slice of that side's first reconstructed file URI, the SAME slice `ListingTableUrl::object_store()` takes, so registration and validation agree by construction
* *AND* that one derivation SHALL be the ONLY store-key derivation in the module, with each arm reading what its builder needs OUT of the returned URL, the S3 arm the host as the bucket name, the Azure arm the whole URL, and NO arm re-parsing the file URI its own way
* *AND* the `Url` that derivation returns for an `s3://` file list MUST equal `s3://<bucket>`
* *AND* for an `s3a://` file list, the OTHER absolute scheme `FileEntry::path` documents, the derivation SHALL return `s3a://<bucket>`, because the registry lookup through `ListingTableUrl::parse(...).object_store()` preserves the `s3a` scheme and `get_store` is an exact match on `scheme://host:port` with no scheme fallback
* *AND* an `s3a://` scan SHALL therefore find its registered store and MUST NOT fail with "No suitable object store found for s3a://…"
* *AND* the Azure arm SHALL pass that URL to `MicrosoftAzureBuilder::with_url`, which sets the account and the container from it, and SHALL pass the account host through VERBATIM rather than reconstructing `<account>.dfs.core.windows.net`, because reconstruction would discard whatever host the file URI actually names
* *AND* the arm MUST NOT compensate for `MicrosoftAzureBuilder`'s OWN host restriction: `with_url` accepts an `abfss://` host bearing exactly one of four suffixes, `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com`, and returns `UrlNotRecognised` for every other host, so a sovereign-cloud host (`*.dfs.core.chinacloudapi.cn`), a private endpoint, or any other Azure-compatible host SHALL surface that build error through the arm's redaction, naming no credential value
* *AND* the Azure arm SHALL apply the resolved connection budget through the SAME `ClientOptions` helper the S3 arm uses, so the warm-connection ceiling is one decision for both backends
* *AND* the Azure arm SHALL set the account key via `with_access_key` OR the SAS token via the SAS config key, selected by the `AdlsCred` state, and SHALL set NEITHER when the other is set
* *AND* the Azure arm SHALL redact the backend's `secret_values` out of any builder error before surfacing it, through the SAME two-step redaction the S3 arm applies, so a malformed account key or SAS never reaches an error message
* *AND* the Azure arm SHALL wrap the built store in the spec-sized HEAD decorator and register it under the derived URL once per distinct store URL, the same contract the S3 arm has
* *AND* a whole-spec precondition SHALL reject a scan spec whose sides resolve to DIFFERENT object stores that DataFusion would register under the SAME registry key, checked ONCE before any registration and skipping a join whose dimension file list is empty
* *AND* that precondition SHALL compare, per side, the registry key DataFusion computes (scheme + `host:port`, EXCLUDING userinfo) against the store URL the side actually needs (scheme + userinfo + `host:port`), and SHALL return a `UdfError::User` naming both conflicting store URLs when two sides share the former and differ in the latter
* *AND* that precondition SHALL be BACKEND-AGNOSTIC and MUST NOT match on a storage-backend variant, because it states a property of DataFusion's registry key rather than of any one backend; an `s3://` URI carries no userinfo, so the two notions coincide and the check can never fire for an S3 spec
* *AND* `validate_uniform_object_store_files` SHALL reject a file list mixing containers WITHIN one side, for data files and for their associated delete files alike, because `ListingTableUrl::object_store()` includes the userinfo the registry key drops, so the whole-spec precondition covers only the ACROSS-side case

### Scenario: The scan-spec wire carries the backend as a tagged variant

* *GIVEN* the externally-tagged lowercase encoding, under which the S3 backend serializes as `{"s3": {…}}`
* *WHEN* a storage backend is serialized into the shard-invariant common blob
* *THEN* the backend value SHALL be `{"s3": {…}}` or `{"adls": {"account_name": …, "cred": {…}}}`, whose single lowercase variant key names the backend and whose `cred` value is itself a tagged object naming the account-key or SAS state
* *AND* that backend value SHALL appear on the wire ONLY inside the scan-spec storage wrapper specified by `storage-access/scan-spec-credential-reference`, so the `storage` key of the common blob no longer holds a bare backend: the `connection` variant carries a reference and no credential at all; the `sealed` variant carries the connection name and this same backend encoding as AES-GCM ciphertext; and the `inline` variant carries it in plaintext for host-test spec construction ONLY, the adapter's selection function cannot return it
* *AND* the backend value's OWN encoding MUST be the same whether or not a wrapper encloses it: the eighteen credential-bearing golden SQL fixtures carry the wrapper in their `storage` value, and the six `empty_*` golden fixtures carry no `storage` value
* *AND* the encoding MUST round-trip: deserializing a serialized backend SHALL yield an equal value, including which `AdlsCred` state it holds
* *AND* the backend enum MUST NOT be declared `untagged`, and the wrapper enum MUST NOT be either, and the decode-side test SHALL pin that `azure` is not the ADLS variant key (`adls` is), that an S3-shaped payload under `adls` is rejected rather than resolved by trial deserialization, and that a connection reference and an inline S3 backend, both JSON objects, are never told apart by field-name coincidence
* *AND* the wrapper MUST NOT expose any method returning secret values or a credential payload, so a scan-side site left reading the unresolved wire value fails to compile rather than yielding an empty secret set
* *AND* a scan-spec deserialization failure MUST NOT contain any credential value, no storage access key, secret key, session token, Azure account key, or SAS token, while the connection NAME MAY appear, because it is not a secret and the resolution error specified by `storage-access/scan-spec-credential-reference` names it; a sealed payload that fails base64 decoding or AEAD authentication SHALL likewise produce an error naming the connection and the failed operation, carrying neither the payload nor any plaintext

### Scenario: No storage credential type prints its payload through Debug

* *GIVEN* the storage types that hold a credential value, `StorageProps`, `StorageBackend`, `AdlsCred`, and `ConnectionCreds`
* *WHEN* any of them is formatted with the `Debug` formatter
* *THEN* the rendered text MUST NOT contain the value of `access_key`, `secret_key`, `session_token`, `token`, `client_secret`, `account_key`, `sas_token`, or an `AdlsCred` payload, and SHALL render a fixed placeholder in each such position instead
* *AND* the guard SHALL sit on the TYPE rather than on each formatting site, because a use site added later cannot be reviewed by a test that does not yet exist
* *AND* the non-secret fields of those types, `endpoint`, `region`, `path_style`, `allow_http`, `warehouse`, `account_name`, `client_id`, `oauth2_server_uri`, `scope`, `use_sigv4`, and `use_vended_credentials`, SHALL still render their values, so `Debug` stays useful for diagnosing a misconfigured store address
* *AND* the label-based redaction SHALL additionally cover the exact literal `"sas":`, the serialized `AdlsCred::Sas` wire key, and MUST NOT be given a bare `sas` pattern, because the matcher redacts from a matched label to the next delimiter and would destroy unrelated text
* *AND* every production site that composes the two redaction passes SHALL apply the VALUE pass before the LABEL pass, the order this crate's own redaction module documents as required for a SAS token, so no site keeps the inverted composition

### Scenario: The S3 path stays independent of the Azure backend variant

* *GIVEN* the unit, integration, and S3 E2E suites
* *WHEN* the S3 path runs beside the Azure variant, its three enum arms, its object-store arm, and the whole-spec store precondition
* *THEN* every S3 test MUST pass, and no S3 assertion MUST be weakened, disabled, or deleted to accommodate the Azure variant
* *AND* a test site that is S3-only SHALL panic on an `Adls` arm (or use `let … else`) that names the site as S3-only, and MUST NOT use a `_` catch-all, so the site stays a build failure for a THIRD backend
* *AND* a `ConnectionCreds` struct literal SHALL supply the three Azure fields (`account_name`, `account_key`, `sas_token`), and the `Debug`-redaction test in `crates/lakehouse-catalog/src/creds.rs` SHALL assert that the two secret fields are redacted
* *AND* NO test edit SHALL change, weaken, or delete an assertion about S3 behavior, so the gate proves the S3 path independent of the Azure variant
* *AND* the scan-driving SQL generated for a given S3 pushdown request SHALL differ from that of another backend only in the `storage` VALUE, because an S3 CONNECTION yields the S3 backend and `storage-access/scan-spec-credential-reference` replaces the value with a connection reference or a sealed envelope
* *AND* the props map `build_rest_catalog` builds, the `FileIO` an S3 backend produces, the file list resolved for a given table, and the vended-versus-static storage resolved from a given `loadTable` response MUST each depend on the S3 backend alone
* *AND* the store registration for an S3 spec SHALL build one store per distinct bucket: one for a single-table scan, one for a join whose two sides share a bucket, two for a join whose sides span two buckets, and one for a join whose dimension file list is empty
* *AND* the S3 E2E suite MUST pass against a `.so` built from the current source, because that suite is this feature's declared characterization gate and a stale `.so` would make the gate vacuous
* *AND* no storage credential, catalog secret, live bearer token, Azure account key, or static SAS token SHALL appear in any returned SQL string or error message, and no VENDED STS key and no VENDED SAS SHALL appear there in PLAINTEXT, because a vended value travels only inside the AES-GCM-sealed envelope of `storage-access/scan-spec-credential-reference` and a static credential is referenced by name
* *AND* making that true for Azure requires BOTH redaction layers, because neither alone covers the manifest-read path: `redact_credentials` SHALL carry the Azure labels (`account_key`, `sas_token`, `adls.account-key`, `adls.sas-token`) beside the AWS ones, and every error site that calls it SHALL first apply value-based `redact_secret_values` against the effective backend's `secret_values()`, as `redact_error` in the adapter does
* *AND* the object-store arm's redaction alone MUST NOT be treated as discharging that requirement, because the Azure props feed `adls.account-key` and `adls.sas-token` into `FileIO` through `catalog_storage_props`, so an Azure secret can surface from a manifest-list or manifest-load failure that never reaches the object-store arm
