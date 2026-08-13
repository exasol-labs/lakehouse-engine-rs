# Feature: Unity Catalog Vended Credentials

Requests per-table, short-lived, scoped storage credentials from the Unity Catalog Temporary Table Credentials API and terminates them in a `StorageBackend` value.

## Background

* **This delta closes two policy gaps and is issue #330.** It SUPERSEDES this feature's Background sentence that `resolve_uc_vended_storage` "MUST NOT read any CONNECTION-derived value": under the credentials/addressing split it reads a CONNECTION-configured store `endpoint` and `region` for ADDRESSING, and still reads no CONNECTION CREDENTIAL. The distinction is load-bearing and is stated rather than implied — see `vs-adapter/storage-backend-enum` § "The vended selectors take a store address that cannot carry a credential".
* **Gap 1 — this selector accepted `abfs://` with no operator consent.** `classify_vended_scheme` maps both `abfs` and `abfss` to the ADLS kind; the ADLS arm took no `allow_http` parameter at all, so a plaintext `abfs://` location was always accepted and read over HTTPS instead — a silent scheme upgrade. The Iceberg vended selector rejected it for exactly that reason. This feature specified the consent gate for a vended S3 endpoint only, so spec and code agreed; the gap was between the two selectors, and the fix is ONE shared gate both reach rather than a second per-kind copy.
* **Gap 2 — this selector could build an S3 store with no address, and the Iceberg selector rejected the same state.** Real Databricks AWS vends short-lived credentials with NO endpoint, and the vended AWS credential type carries no region field at all, so this selector produced `StorageBackend::S3` with both `region` and `endpoint` empty. Sharing the policy alone would have imported the Iceberg selector's "store address undetermined" hard error and made a legal Databricks table fail. The address rule replaces that error instead: an empty address is LEGAL and resolves to the AWS default chain.
* **Precedence, decided in the plan interview: the CONNECTION wins when set.** For `endpoint` and `region` independently, a non-empty CONNECTION value takes precedence over whatever the catalog vends; vended addressing fills in only when the CONNECTION is silent. An operator who configured a store address means it.
* **The `abfs://` gate and the vended-plaintext-endpoint gate are the SAME rule at two places in one URI**, so both live in the shared home: `abfs` names plaintext transport on the location itself, and an `http://` endpoint names it on the store the location is read through. Neither backend can be downgraded to plaintext without the operator saying so.
* This vending path still has NO production caller — Delta scan execution reaches it in #319/#320 — so both gaps are latent, and this delta lands before that wiring so the shared policy is in place when the first live caller arrives.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An S3 vended response terminates in an S3 storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `aws_temp_credentials` with an access key id, a secret access key, and a session token, and a storage location whose scheme is `s3`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the S3 variant of `StorageBackend` carrying the vended access key, secret key, and session token
* *AND* the selector MUST NOT read an access key, a secret key, or a session token from the CONNECTION, so a credential the response does not carry is an error and never a static fallback
* *AND* the selector SHALL resolve the store `endpoint` and `region` through the ONE shared store-address rule both vended selectors call, taking each independently from the CONNECTION when the CONNECTION's value is non-empty and from the vended response otherwise
* *AND* the selector SHALL leave a field empty when NEITHER source states it, and an S3 backend whose `endpoint` and `region` are BOTH empty SHALL be returned successfully rather than refused, because Databricks AWS vends no endpoint and no region and the AWS default chain places that store
* *AND* the vended access key, secret key, and session token MUST NOT appear in any error message, returned SQL, or log line
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A vended plaintext endpoint is honored only with operator consent

* *GIVEN* an S3 Unity Catalog temporary-credentials response and a resolved store `endpoint` whose scheme is plaintext `http`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend with the resolved `ALLOW_HTTP` consent value
* *THEN* the selector SHALL honor that endpoint only when `ALLOW_HTTP` is true and otherwise SHALL return an error naming the plaintext endpoint, the table location, and the `ALLOW_HTTP` property
* *AND* the gate SHALL apply to the RESOLVED endpoint whichever source supplied it — the CONNECTION's when set, the vended one otherwise — because the gate is on the transport the store will actually use and not on where the value came from
* *AND* the gate SHALL be the ONE shared gate `resolve_vended_storage` also reaches, with one error text naming no catalog kind, so the two selectors cannot disagree about what plaintext consent means
* *AND* the error message MUST NOT contain any credential value or vended secret
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A plaintext abfs:// location is honored only with operator consent

* *GIVEN* a Unity Catalog temporary-credentials response carrying a usable `azure_user_delegation_sas`, and a storage location whose scheme is the plaintext `abfs` rather than the TLS `abfss`
* *WHEN* either vended selector resolves the storage backend with the resolved `ALLOW_HTTP` consent value
* *THEN* the selector SHALL honor that location only when `ALLOW_HTTP` is true, and otherwise SHALL return an error naming the `abfs://` scheme, the table location, and the `ALLOW_HTTP` property
* *AND* the refusal SHALL state WHY a silent acceptance is wrong: `abfs` names plaintext transport, this engine has no plaintext Azure path, so accepting it would read the location over HTTPS instead — a silent scheme upgrade the operator never authorised
* *AND* that gate SHALL live in the ONE shared ADLS construction function both vended selectors call, so NEITHER selector can construct an ADLS backend without it having run, and a per-kind copy of the gate SHALL NOT be added
* *AND* this scenario SHALL REPLACE the Iceberg-only coverage of the same rule rather than duplicate it, so one scenario governs both catalog kinds and a future third kind inherits it by calling the shared function
* *AND* an `abfss` location SHALL stay ungated, because it names TLS transport and needs no consent
* *AND* the error message MUST NOT contain any credential value or vended secret
<!-- /DELTA:NEW -->
