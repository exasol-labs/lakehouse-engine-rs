# Feature: Storage Backend Enum

Gives the storage-backend decision exactly one home: a `StorageBackend` enum whose variant IS the backend, so every other module asks the enum for what it needs instead of deciding for itself which backend it is talking to.

## Background

* **This delta is issue #135. It amends ONE scenario, adds ONE, and changes no backend selection rule.** `StorageBackend`'s variants, `AdlsCred`'s states, `secret_values()`, `catalog_storage_props`, the backend-dispatching store registration, the three-selector dispatch, and every source probe over them are UNCHANGED. What changes is that the `storage` value on the scan-spec wire is no longer a bare backend, and that the S3 arm stops printing its credentials in `Debug`.
* **DISCHARGES this feature's own `#135` citation.** The recorded `AdlsCred` bullet reads: "Doing the same on the wire type costs six lines and leaves issue #135 (credentials in cleartext in query plans) with strictly less to fix rather than strictly more." This delta is that fix. The `AdlsCred` redacting `Debug` it describes is unchanged and is now the pattern the S3 arm follows.
* **The S3 arm is the asymmetry that bullet left standing, and it is a live exposure of the same class.** `StorageProps` derives `Debug` (`crates/lakehouse-catalog/src/creds.rs:181-182`) and so prints `access_key`, `secret_key`, and `session_token` verbatim, and `StorageBackend` derives `Debug` (`crates/lakehouse-catalog/src/storage.rs:80`) and so prints the S3 arm through it. Only the `Adls` arm is protected. Any `{:?}` on either type — in a log line, a `panic!`, an `unwrap` message, or a future error path — leaks the credential, and no test can see it because no such site exists today. The guard belongs on the type rather than on each use site, which is what `AdlsCred` already established.
* **`ConnectionCreds` prints `access_key` in the clear too, and that is corrected in the same change.** Its manual `Debug` (`creds.rs:61-92`) redacts `secret_key`, `session_token`, `token`, `client_secret`, `account_key`, and `sas_token` but not `access_key`. An AWS access key id is an identifier rather than a secret on its own, yet it is half of a credential pair and is exactly the value issue #135's reporter quotes (`AKIA...`).
* **`redact_credentials`' label list omits the `AdlsCred::Sas` wire key, and the literal to add is `"sas":` rather than `sas`.** `serde(rename_all = "snake_case")` makes that state serialize under the key `sas`, and the list (`crates/lakehouse-catalog/src/redaction.rs:31-59`) carries `sas_token` and `adls.sas-token` but not it. The matcher redacts everything from a matched label to the next delimiter and matches plain substrings, so a bare `sas` would destroy unrelated text while `"sas":` matches only the serialized wire key. A non-JSON rendering such as `Debug`'s `Sas("…")` is NOT covered by the label pass and is covered instead by the manual `Debug` impl this delta adds.
* **Two call sites compose the redaction passes in the order this crate documents as broken.** `crates/lakehouse-catalog/src/auth.rs:93` and `:160` apply `redact_secret_values(&redact_credentials(msg), …)`, and `crates/lakehouse-catalog/src/redaction.rs:91-97` records why the value pass must run FIRST for a SAS token. Both become the documented composition, which `redact_error_text` already is.
* **The wire wrapper is specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES rather than restates.** That feature owns what the reference means, how it is resolved, and which gate selects the variant. This feature owns only the consequence for the enum: the backend still appears on the wire, now under an inline variant, and the enum itself gains nothing.
* **The wrapper deliberately exposes NO secret accessor.** Five scan-side sites build a redaction secret set by reading the spec's storage block today. Giving the wrapper a `secret_values()` that returned empty for the reference variant would make all five compile and silently disarm value-based redaction; omitting it makes them fail to compile. `vs-adapter/scan-spec-credential-reference` owns that requirement and enumerates the sites.
* **The inline encoding of a backend stays byte-identical, and that is what keeps this delta bounded.** Nesting the existing `{"s3": …}` / `{"adls": …}` object under an inline variant changes what encloses it, never its own bytes, so `catalog_storage_props`, the decode-side variant-key pins, and the round-trip guarantee all hold on the inner value without edit.
* No dependency is added and no dependency version changes.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The scan-spec wire carries the backend as a tagged variant

* *GIVEN* the externally-tagged lowercase encoding slice B landed, under which the S3 backend serializes as `{"s3": {…}}`
* *WHEN* a storage backend is serialized into the shard-invariant common blob
* *THEN* the backend value SHALL be `{"s3": {…}}` or `{"adls": {"account_name": …, "cred": {…}}}`, whose single lowercase variant key names the backend and whose `cred` value is itself a tagged object naming the account-key or SAS state
* *AND* that backend value SHALL appear on the wire ONLY inside the inline variant of the scan-spec storage wrapper specified by `vs-adapter/scan-spec-credential-reference`, so the `storage` key of the common blob no longer holds a bare backend — its OTHER variant carries a connection reference and no credential at all
* *AND* the backend value's OWN encoding MUST be byte-identical to before this delta, so nesting changes what encloses it and never its own bytes
* *AND* the encoding MUST round-trip: deserializing a serialized backend SHALL yield an equal value, including which `AdlsCred` state it holds
* *AND* neither the backend enum nor the wrapper enum MUST be declared `untagged`, and the decode-side test that pins this SHALL be KEPT: its `{"azure": …}` case still proves that `azure` is not the ADLS variant key, and its S3-shaped-payload-under-`adls` case still proves that a payload naming a variant it does not match is rejected rather than resolved by trial deserialization — a connection reference and an inline S3 backend are both JSON objects, so trial deserialization would decide between them by field-name coincidence
* *AND* the wrapper MUST NOT expose any method returning secret values or a credential payload, so a scan-side site left reading the unresolved wire value fails to compile rather than yielding an empty secret set
* *AND* a scan-spec deserialization failure MUST NOT contain any credential value — no storage access key, secret key, session token, Azure account key, or SAS token — while the connection NAME MAY appear, because it is not a secret and the resolution error specified by `vs-adapter/scan-spec-credential-reference` names it
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: No storage credential type prints its payload through Debug

* *GIVEN* the storage types that hold a credential value — `StorageProps`, `StorageBackend`, `AdlsCred`, and `ConnectionCreds`
* *WHEN* any of them is formatted with the `Debug` formatter
* *THEN* the rendered text MUST NOT contain the value of `access_key`, `secret_key`, `session_token`, `token`, `client_secret`, `account_key`, `sas_token`, or an `AdlsCred` payload, and SHALL render a fixed placeholder in each such position instead
* *AND* the guard SHALL sit on the TYPE rather than on each formatting site, because a use site added later cannot be reviewed by a test that does not yet exist
* *AND* the non-secret fields of those types — `endpoint`, `region`, `path_style`, `allow_http`, `warehouse`, `account_name`, `client_id`, `oauth2_server_uri`, `scope`, `use_sigv4`, and `use_vended_credentials` — SHALL still render their values, so `Debug` stays useful for diagnosing a misconfigured store address
* *AND* the label-based redaction SHALL additionally cover the exact literal `"sas":`, the serialized `AdlsCred::Sas` wire key, and MUST NOT be given a bare `sas` pattern, because the matcher redacts from a matched label to the next delimiter and would destroy unrelated text
* *AND* every production site that composes the two redaction passes SHALL apply the VALUE pass before the LABEL pass, the order this crate's own redaction module documents as required for a SAS token, so no site keeps the inverted composition
<!-- /DELTA:NEW -->
