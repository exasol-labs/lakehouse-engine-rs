# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION also selects WHICH storage backend the scan reads through, whether requests to the catalog are AWS SigV4-signed, and whether the engine requests short-lived vended credentials at table-load time. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.

## Background

* **This delta adds the Azure credential shape and nothing else.** It implements issue #275, slice C of six (A-F) for Azure Data Lake Storage Gen2 (`abfss://`) support. Every existing scenario keeps its behaviour: a CONNECTION that supplies no Azure field is parsed, validated, and projected exactly as before, byte for byte.
* **Three new optional password fields, all read by the same `nonempty_str` rule as every existing field:** `account_name` (the Azure storage account, not a secret — it appears in every `abfss://` URI), `account_key` (a shared-key secret), and `sas_token` (a shared-access-signature secret; a SAS supplied inline in the CONNECTION is a secret exactly as an account key is). An empty-string value is "absent", the convention every other field already uses.
* **The credential SHAPE selects the backend; there is no `backend` field.** Adding one would be a second source of truth free to disagree with the credentials actually supplied. `vs-adapter/storage-backend-enum` already records that `storage_block` is the ONLY site that selects a backend from input; this delta is the first input that makes that selection observable, and it adds no second decision point.
* **The presence of ANY Azure field — not just `account_name` — makes the CONNECTION an Azure CONNECTION.** Keying selection on `account_name` alone would let a CONNECTION that supplies `account_key` and forgets `account_name` fall silently back to S3 with the key ignored. Keying on any-of-three turns that same input into a named-field error.
* **Exactly one credential, never two.** `AdlsCred` has an account-key state and a SAS state and no "both" state, so a CONNECTION supplying both describes a backend the type cannot represent. Rejecting is the only reading that does not silently pick one.
* **Mixing Azure and static S3 fields is rejected rather than resolved.** A credentials path MUST NOT resolve an ambiguous input silently. This is the one rule the issue text does not name; it is here because the alternative — an undeclared precedence between two credential sets — is exactly the silent misconfiguration the rest of this feature exists to prevent.
* **`use_sigv4` together with Azure fields is NOT given its own guard.** SigV4 already requires `access_key`, `secret_key`, and `region`, so such a CONNECTION is rejected either by the mixed-fields rule (when those are supplied) or by the existing SigV4 rule (when they are not). A third guard would add a second error for an input that already fails loud.
* **`allow_http` does not reach the Azure backend in this slice.** It arrives from the `ALLOW_HTTP` VS property and is consumed only by the S3 payload. Azurite-emulator support (plain-HTTP Azure endpoints) is out of scope here and is not silently half-wired: the Azure backend carries no HTTP-scheme knob at all.
* **Vended Azure credentials are out of scope (issue #276, slice D).** An Azure CONNECTION that also sets `use_vended_credentials` is accepted and reads with its STATIC credentials; `vs-adapter/pushdown-planning-cloud-credentials` carries that deferral as a tracked exception.
* Every error added by this delta names FIELD NAMES only. No `account_key`, `sas_token`, or any other supplied value appears in any error message, returned SQL, or log line.

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI; the resolved `ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is the only unconditionally-required field. Catalog
authentication and S3 storage credentials are fully orthogonal: any combination is valid,
including an unauthenticated catalog that vends S3 credentials and an OAuth-authenticated
catalog used with static S3 credentials (the catalog-auth modes themselves are specified in
`connection-credentials-catalog-auth`). The one conditional requirement is the AWS Glue SigV4
path: when `use_sigv4`
is true the static `access_key`, `secret_key`, and `region` are required (they sign the
catalog `load_table` request, ahead of any credential vending); `endpoint` stays optional.

## Scenarios

### Scenario: Azure account-key credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `account_key`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `sas_token`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and an account-key credential holding the supplied `account_key`
* *AND* the adapter MUST NOT produce an S3 backend for this CONNECTION
* *AND* the supplied `account_key` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: Azure inline-SAS credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `sas_token`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `account_key`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and a SAS credential holding the supplied `sas_token`
* *AND* the adapter SHALL treat the `sas_token` value as a secret on every path that treats `account_key` as one, because a SAS supplied inline in the CONNECTION grants the same access
* *AND* the supplied `sas_token` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: An Azure CONNECTION without exactly one account name and one credential is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and at least one of `account_name`, `account_key`, and `sas_token`, in one of three malformed shapes: `account_name` absent while a credential is present; `account_name` present while BOTH `account_key` and `sas_token` are present; or `account_name` present while NEITHER is present
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that an Azure CONNECTION requires `account_name` and exactly one of `account_key` and `sas_token`
* *AND* the error SHALL name the offending field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT fall back to the S3 backend for any of the three shapes, because a malformed Azure credential set is an error and not an absent one

### Scenario: A CONNECTION mixing Azure and static S3 credential fields is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, at least one of `account_name`, `account_key`, and `sas_token`, AND at least one of `endpoint`, `region`, `access_key`, `secret_key`, and `session_token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that Azure and S3 storage credentials cannot both be supplied on one CONNECTION
* *AND* the error SHALL name the supplied Azure field names and the supplied S3 field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT apply a precedence rule between the two credential sets, because an undeclared precedence resolves an ambiguous credentials input silently

### Scenario: Adapter reads catalog and storage credentials from a CONNECTION object

* *GIVEN* a `CREATE VIRTUAL SCHEMA` that supplies `CATALOG_CONNECTION = '<conn_name>'`
* *AND* an Exasol CONNECTION named `<conn_name>` whose address is the Iceberg REST catalog URI and whose password is a JSON object holding `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter handles a `createVirtualSchema` or `pushdown` request
* *THEN* the adapter SHALL call `ctx.connection('<conn_name>')` to obtain the credentials
* *AND* the adapter SHALL use the resolved address as the catalog URI and the parsed JSON password to build the catalog and storage configuration
* *AND* the adapter MUST NOT read `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `CATALOG_URI`, `S3_ENDPOINT`, or `S3_REGION` from plain VS properties when `CATALOG_CONNECTION` is present

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

### Scenario: When SigV4 is enabled, access_key, secret_key, and region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse` but omits one or more of `access_key`, `secret_key`, and `region`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the missing field(s) and stating they are required when SigV4 signing is enabled
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the static `access_key`, `secret_key`, and `region` sign the catalog `load_table` request before any vended credentials are used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `account_name`, `account_key`, and `sas_token` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
* *AND* the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)
* *AND* the resolved storage backend SHALL be the S3 variant, unchanged from before this delta, because S3 stays the no-static-storage-fields default and a CONNECTION that names no Azure field describes no Azure backend
* *AND* this SHALL hold whether or not `use_vended_credentials` is set, so an existing vended-S3 CONNECTION that supplies no static storage field at all resolves to exactly the backend it resolved to before
