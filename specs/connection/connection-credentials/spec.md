# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog address and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties. The CONNECTION selects the storage backend, whether catalog requests are AWS SigV4-signed, and whether the engine requests vended credentials at table-load time.

## Background

* The VS property `CATALOG_CONNECTION` names the CONNECTION, and the adapter resolves it with `ctx.connection(name)`. The resolved `address` is the catalog URI, and the resolved `password` is a JSON object string with the credential fields.
* Both adapter entry points (`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through the same path.
* The resolved password never appears in any error message, returned SQL, or log line.
* `warehouse` is required under every catalog kind except `DIRECT_STORAGE`, which rejects it (`connection/connection-credentials-direct-storage`). `endpoint` stays optional.
* Catalog authentication and S3 storage credentials are independent: any combination is valid (`connection/connection-credentials-catalog-auth`). The SigV4 requirement is in `connection/connection-credentials-sigv4`.
* Under vending (`use_vended_credentials` true), storage credentials are ignored, not rejected. A CONNECTION can carry a static key pair and `region` for SigV4 catalog signing while vending its storage credentials.
* Under vending, `endpoint`, `region`, and `path_style` are read as addressing. `storage-access/pushdown-planning-cloud-credentials-vended-storage` owns the precedence between the CONNECTION and the catalog response.
* The Azure-and-S3 mixed-fields rejection applies under vending too, because a CONNECTION that supplies both sets declares two incompatible intents.
* A vended-only CONNECTION supplies no Azure field. `storage_block` returns an S3 backend with every field empty and the vended resolution never reads it. The table location's `abfss://` scheme selects the ADLS backend.
* `path_style` is optional: absent means unstated. `storage_block` resolves an unstated value to `false`, which addresses the bucket virtual-hosted as an AWS S3 client does.
* A resolved `path_style` of `false` discards a configured `endpoint`, because the object-store builder takes `endpoint` only when `path_style` is true. A non-vended CONNECTION with an `endpoint` and no stated `path_style` is therefore rejected.
* The storage-backend selection rule: Azure when `account_name` is present with exactly one of `account_key` and `sas_token`, S3 otherwise.

## Scenarios

### Scenario: Adapter reads catalog and storage credentials from a CONNECTION object

* *GIVEN* a `CREATE VIRTUAL SCHEMA` that supplies `CATALOG_CONNECTION = '<conn_name>'`
* *AND* an Exasol CONNECTION named `<conn_name>` whose address is the Iceberg REST catalog URI and whose password is a JSON object holding `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter handles a `createVirtualSchema` or `pushdown` request
* *THEN* the adapter SHALL call `ctx.connection('<conn_name>')` to obtain the credentials
* *AND* the adapter SHALL use the resolved address as the catalog URI and the parsed JSON password to build the catalog and storage configuration
* *AND* the adapter SHALL carry `<conn_name>` itself on the resolved configuration, so the pushdown planning layer can reference the CONNECTION by name under `storage-access/scan-spec-credential-reference` instead of embedding the credentials it supplied
* *AND* the adapter MUST NOT read `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `CATALOG_URI`, `S3_ENDPOINT`, or `S3_REGION` from plain VS properties when `CATALOG_CONNECTION` is present
* *AND* neither the resolved password text nor any credential parsed out of it SHALL appear in any error message or log line, and neither SHALL appear in PLAINTEXT in the returned SQL under either setting of `use_vended_credentials`, the vended path carries only the sealed envelope of `storage-access/scan-spec-credential-reference`

### Scenario: One storage-credential projection and one selector serve both readers

* *GIVEN* the readers of a CONNECTION on the vending-disabled path, the adapter at plan time, the scan UDF at execution time under `storage-access/scan-spec-credential-reference`, and the adapter's table-enumeration path under `CatalogKind::DirectStorage`, which opens a store before any table exists
* *WHEN* any reader turns a resolved CONNECTION password into a storage backend
* *THEN* exactly ONE storage-credential projection type, declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token`, and exactly ONE selector over it SHALL serve EVERY reader, and no reader SHALL carry its own copy of either
* *AND* the projection SHALL carry `path_style` as an OPTION of boolean, preserving whether the CONNECTION stated a value, and the ONE selector SHALL be the single place that resolves an unstated value to `false`, so no two readers resolve the same absent field differently
* *AND* the parse step SHALL NOT resolve that value, because a parse that already substituted `false` would destroy the distinction the vended path reads
* *AND* that pair SHALL live in the crate that already owns `ConnectionCreds`, `StorageProps`, and `StorageBackend`, while `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` SHALL ALL STAY in the adapter module where `catalog/catalog-crate-structure` pins them, so the scan path depends inward on a credential type and no function interpreting the Exasol CONNECTION object crosses the crate boundary
* *AND* the adapter's own CONNECTION-to-backend entry point SHALL reach that selector through the projection rather than re-implementing the selection, and the nine storage field spellings SHALL have exactly ONE reader, so no two readers normalise an empty or absent field differently
* *AND* the backend the readers derive from one CONNECTION password and one `allow_http` value SHALL be field-for-field EQUAL, asserted by a test over a password carrying every storage field, over one carrying empty strings, and over one omitting fields
* *AND* the scan-side reader SHALL apply the DERIVATION only and MUST NOT re-run the acceptance validation the adapter applies, because that validation is parameterized by the resolved `CatalogKind` and answers a plan-time question the adapter already answered for this query
* *AND* that acceptance validation SHALL require `warehouse` under every CATALOG kind and SHALL REJECT it under `DIRECT_STORAGE`, which reaches no catalog; `connection/connection-credentials-direct-storage` owns that kind's required, rejected, and address-agreement rules
* *AND* the selection rule, Azure when `account_name` is present with exactly one of `account_key` and `sas_token`, S3 otherwise, SHALL be UNCHANGED, so a CONNECTION resolves to the same backend it resolved to before this delta
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line from any reader

### Scenario: Missing connection name is rejected with a clear, credential-safe error

* *GIVEN* a VS request whose properties do not include a non-empty `CATALOG_CONNECTION`
* *WHEN* the adapter handles the request
* *THEN* the adapter SHALL return an error stating that `CATALOG_CONNECTION` is required
* *AND* the error message MUST NOT contain any credential value

### Scenario: Malformed connection password is rejected without leaking the password

* *GIVEN* a CONNECTION whose password is not a parseable JSON object
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating the CONNECTION password is not a valid JSON object
* *AND* the error message MUST NOT contain the password text

### Scenario: Connection password missing required credential fields is rejected listing only the field names

* *GIVEN* a CONNECTION whose JSON password omits `warehouse`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `warehouse` as the missing required field
* *AND* the error message MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT report any of `endpoint`, `region`, `access_key`, or `secret_key` as missing, because those fields are optional

### Scenario: Static S3 credentials are optional regardless of catalog auth mode

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, does not enable `use_sigv4`, but omits `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept the password without reporting any of `endpoint`, `region`, `access_key`, or `secret_key` as missing
* *AND* the adapter SHALL treat each omitted S3 field as absent, independently of whether any catalog-auth field or `use_vended_credentials` is set

### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 SeaweedFS/REST stacks behave exactly as before
* *AND* the adapter SHALL treat an omitted `path_style` as UNSTATED rather than as a value, and `storage_block` SHALL resolve an unstated `path_style` to `false`, so a CONNECTION naming a `region` and no `endpoint` addresses its bucket virtual-hosted exactly as an AWS S3 client does
* *AND* a CONNECTION that supplies `path_style` SHALL have that exact value applied on this path, so a path-style store is reached by stating `path_style: true` rather than by relying on a default
* *AND* the backend `storage_block` builds from this CONNECTION SHALL be the S3 variant, unchanged from before this delta, because S3 stays the no-static-storage-fields default and a CONNECTION that names no Azure field describes no Azure backend
* *AND* this SHALL hold whether or not `use_vended_credentials` is set, so an existing vended-S3 CONNECTION that supplies no static storage field at all yields exactly the backend it yielded before
* *AND* this clause SHALL be read as constraining `storage_block`'s output ONLY, and MUST NOT be read as constraining the EFFECTIVE scan storage: when `use_vended_credentials` is true the effective backend is selected from the table location's URI scheme and `storage_block`'s output is never read, so the same CONNECTION resolves to an ADLS scan backend for an `abfss://` table

### Scenario: A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and a non-empty `endpoint`, omits `path_style`, and omits `use_vended_credentials` or sets it to false
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `path_style` and stating that a CONNECTION configuring a store `endpoint` MUST state `path_style`
* *AND* the error message SHALL state that `true` reaches a path-style store such as SeaweedFS or Ceph and `false` addresses the bucket virtual-hosted, so the operator can choose without reading the source
* *AND* the adapter SHALL accept the same CONNECTION once it states `path_style`, under EITHER value
* *AND* the adapter MUST NOT apply this guard when `use_vended_credentials` is true or when the CONNECTION supplies no `endpoint`
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, and supplies one credential set, either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `access_key`, `secret_key`, `session_token`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled
* *AND* the adapter SHALL read the CONNECTION's `endpoint`, `region`, and `path_style` into the effective scan storage as ADDRESSING when the CONNECTION states them, under the ONE precedence rule specified in `storage-access/pushdown-planning-cloud-credentials-vended-storage` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set"
* *AND* "states them" SHALL mean non-empty for `endpoint` and `region` and PRESENT for `path_style`, because an absent boolean and an empty string are the two spellings of unstated that these field types admit
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still apply the guard of `connection-credentials-sigv4` § "When SigV4 is enabled, access_key, secret_key, and a signing region are required" when `use_sigv4` is true, because those inputs sign the catalog `load_table` request rather than reaching object storage
* *AND* a `region` the CONNECTION states SHALL ALSO place the store under vending, while a signing region derived from the CONNECTION address SHALL place no store
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
