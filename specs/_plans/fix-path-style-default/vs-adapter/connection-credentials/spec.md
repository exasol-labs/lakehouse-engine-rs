# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION selects WHICH storage backend the scan reads through whenever `use_vended_credentials` is false, selects whether requests to the catalog are AWS SigV4-signed, and selects whether the engine requests short-lived vended credentials at table-load time. Under vending, the CONNECTION's storage CREDENTIALS are ignored while its configured store `endpoint`, `region`, and `path_style` participate in ADDRESSING; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage and the precedence between the two sources. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.
The Azure Data Lake Storage Gen2 credential shape is specified by the sibling feature
`connection-credentials-azure`. Parameterizing validation by the resolved `CatalogKind`
and the Unity Catalog reuse of these auth fields is specified by the sibling feature
`connection-credentials-unity-catalog`.

## Background

<!-- DELTA:NEW -->
* **This delta widens `path_style` to a tri-state and flips its unstated meaning from `true` to `false` (#130).** `ConnectionCreds.path_style` and `StorageCreds.path_style` become `Option<bool>`: absent = unstated, supplied = the operator chose. AMENDS three scenarios, ADDS one, SUPERSEDES two Background bullets.
* **SUPERSEDES "path_style is NOT admitted, and the reason is a type limitation".** The field CAN now express "unstated" and an explicitly stated value DOES participate in the CONNECTION-wins rule (`vs-adapter/pushdown-planning-cloud-credentials`).
* **SUPERSEDES "The vending-DISABLED path is untouched".** The `true` default is the defect #130 reports. An unstated `path_style` now resolves to `false`, matching AWS S3 client convention.
* **A resolved `false` DISCARDS a configured endpoint** (`build_undecorated_store`, `object_store.rs:226-231`, passes `endpoint` to `AmazonS3Builder` only inside `if storage.path_style`). This forces the new guard: a non-vended CONNECTION with an `endpoint` and no stated `path_style` is rejected rather than silently reaching the wrong host.
* **`StorageProps` keeps its `bool` and `true` serde default.** It is the resolved wire type; the adapter always serializes the field. Decision [5] in the decision-log records the rationale.
* **Neither the Iceberg table spec nor the Delta protocol constrain this choice.** `s3.path-style-access` appears nowhere in the Iceberg REST spec; Delta's `PROTOCOL.md` contains no `path-style`/`path_style`/`virtual-hosted`. Decision [9] records the evidence.
<!-- /DELTA:NEW -->

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI; the resolved `ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is the only unconditionally-required field.

## Scenarios

### Scenario: One storage-credential projection and one selector serve both readers

* *GIVEN* the two readers of a CONNECTION on the vending-disabled path — the adapter at plan time, and the scan UDF at execution time under `vs-adapter/scan-spec-credential-reference`
* *WHEN* either reader turns a resolved CONNECTION password into a storage backend
* *THEN* exactly ONE storage-credential projection type — declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token` — and exactly ONE selector over it SHALL serve both readers, and neither reader SHALL carry its own copy of either
<!-- DELTA:CHANGED -->
* *AND* the projection SHALL carry `path_style` as an OPTION of boolean, preserving whether the CONNECTION stated a value, and the ONE selector SHALL be the single place that resolves an unstated value to `false`, so the two readers cannot resolve the same absent field differently
* *AND* the parse step SHALL NOT resolve that value, because a parse that already substituted `false` would destroy the distinction the vended path reads
<!-- /DELTA:CHANGED -->
* *AND* that pair SHALL live in the crate that already owns `ConnectionCreds`, `StorageProps`, and `StorageBackend`, while `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL ALL STAY in the adapter module where `vs-adapter/catalog-crate-structure` pins them, so the scan path depends inward on a credential type and no function interpreting the Exasol CONNECTION object crosses the crate boundary
* *AND* the backend the two readers derive from one CONNECTION password and one `allow_http` value SHALL be field-for-field EQUAL, asserted by a test over a password carrying every storage field, over one carrying empty strings, and over one omitting fields
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line from either reader

### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
<!-- DELTA:CHANGED -->
* *AND* the adapter SHALL treat an omitted `path_style` as UNSTATED rather than as a value, and `storage_block` SHALL resolve an unstated `path_style` to `false`, so a CONNECTION naming a `region` and no `endpoint` addresses its bucket virtual-hosted exactly as an AWS S3 client does — SUPERSEDING the recorded clause "the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)", whose `true` default is the defect issue [#130](https://github.com/exasol-labs/lakehouse-engine-rs/issues/130) reports
* *AND* a CONNECTION that supplies `path_style` SHALL have that exact value applied on this path, so a path-style store is reached by stating `path_style: true` rather than by relying on a default
<!-- /DELTA:CHANGED -->
* *AND* the backend `storage_block` builds from this CONNECTION SHALL be the S3 variant, unchanged from before this delta, because S3 stays the no-static-storage-fields default and a CONNECTION that names no Azure field describes no Azure backend
* *AND* this SHALL hold whether or not `use_vended_credentials` is set, so an existing vended-S3 CONNECTION that supplies no static storage field at all yields exactly the backend it yielded before
* *AND* this clause SHALL be read as constraining `storage_block`'s output ONLY, and MUST NOT be read as constraining the EFFECTIVE scan storage: when `use_vended_credentials` is true the effective backend is selected from the table location's URI scheme and `storage_block`'s output is never read, so the same CONNECTION resolves to an ADLS scan backend for an `abfss://` table

<!-- DELTA:NEW -->
### Scenario: A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and a non-empty `endpoint`, omits `path_style`, and omits `use_vended_credentials` or sets it to false
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `path_style` and stating that a CONNECTION configuring a store `endpoint` MUST state `path_style`
* *AND* the error message SHALL state that `true` reaches a path-style store such as MinIO or Ceph and `false` addresses the bucket virtual-hosted, so the operator can choose without reading the source
* *AND* the adapter SHALL accept the same CONNECTION once it states `path_style`, under EITHER value
* *AND* the adapter MUST NOT apply this guard when `use_vended_credentials` is true or when the CONNECTION supplies no `endpoint`
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:NEW -->

### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, and supplies one credential set — either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `access_key`, `secret_key`, `session_token`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled
<!-- DELTA:CHANGED -->
* *AND* the adapter SHALL read the CONNECTION's `endpoint`, `region`, and `path_style` into the effective scan storage as ADDRESSING when the CONNECTION states them, under the ONE precedence rule specified in `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" — SUPERSEDING the recorded clause that named `endpoint` and `region` alone, and SUPERSEDING the recorded clause "the adapter MUST NOT read the CONNECTION's `path_style`... because that field is a plain boolean with a `true` default and cannot express 'unstated'", whose reason no longer holds (#130)
* *AND* "states them" SHALL mean non-empty for `endpoint` and `region` and PRESENT for `path_style`, because an absent boolean and an empty string are the two spellings of unstated that these field types admit
<!-- /DELTA:CHANGED -->
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still require `access_key`, `secret_key`, and `region` when `use_sigv4` is true, because those sign the catalog `load_table` request rather than reaching object storage
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
