<!-- DELTA:CHANGED -->
# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and object-storage credentials from a named Exasol CONNECTION object instead of plain VS properties, so credentials are managed and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`), and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue, Databricks) and its backing object storage. The credential set carried by the CONNECTION selects WHICH storage backend the scan reads through whenever `use_vended_credentials` is false, selects whether requests to the catalog are AWS SigV4-signed, and selects whether the engine requests short-lived vended credentials at table-load time. Under vending, the CONNECTION's storage CREDENTIALS are ignored while its configured store `endpoint`, `region`, and `path_style` participate in ADDRESSING; `vs-adapter/pushdown-planning-cloud-credentials` specifies the effective storage and the precedence between the two sources. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.
The Azure Data Lake Storage Gen2 credential shape is specified by the sibling feature
`connection-credentials-azure`. Parameterizing validation by the resolved `CatalogKind`
and the Unity Catalog reuse of these auth fields is specified by the sibling feature
`connection-credentials-unity-catalog`. The AWS Glue SigV4 catalog-signing requirement —
`access_key`, `secret_key`, a signing region, and the standard-AWS-Glue-endpoint region
derivation — is specified by the sibling feature `connection-credentials-sigv4`.
The sibling feature `connection-credentials-assume-role` specifies AWS IAM role assumption:
`aws_assume_role_arn`, `aws_external_id`, `aws_sts_endpoint`, and the assumed role's
precedence over vending.
<!-- /DELTA:CHANGED -->

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/connection-credentials/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and a non-empty `endpoint`, omits `path_style`, and whose storage credential source under `vs-adapter/connection-credentials-assume-role` is not vending: it omits `use_vended_credentials`, sets it to false, or names `aws_assume_role_arn`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `path_style` and stating that a CONNECTION configuring a store `endpoint` MUST state `path_style`
* *AND* the error message SHALL state that `true` reaches a path-style store such as MinIO or Ceph and `false` addresses the bucket virtual-hosted, so the operator can choose without reading the source
* *AND* the adapter SHALL accept the same CONNECTION once it states `path_style`, under EITHER value
* *AND* the adapter MUST NOT apply this guard when the storage credential source is vending, or when the CONNECTION supplies no `endpoint`
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Static storage credentials are ignored, not rejected, when vending is requested

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, sets `use_vended_credentials` to true, names no `aws_assume_role_arn`, and supplies one credential set — either static S3 storage fields, or `account_name` plus exactly one of `account_key` and `sas_token`
* *WHEN* the adapter resolves the connection and the pushdown path resolves the effective scan storage for a table
* *THEN* the adapter SHALL accept the CONNECTION and MUST NOT report an error for supplying storage credentials alongside `use_vended_credentials`
* *AND* the adapter MUST NOT read any of `access_key`, `secret_key`, `session_token`, `account_name`, `account_key`, or `sas_token` into the effective scan storage for that table, so a CREDENTIAL the response does not advertise is ABSENT rather than backfilled
* *AND* the adapter SHALL read the CONNECTION's `endpoint`, `region`, and `path_style` into the effective scan storage as ADDRESSING when the CONNECTION states them, under the ONE precedence rule specified in `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set"
* *AND* "states them" SHALL mean non-empty for `endpoint` and `region` and PRESENT for `path_style`, because an absent boolean and an empty string are the two spellings of unstated that these field types admit
* *AND* the adapter SHALL still apply the existing guard rejecting a CONNECTION that supplies BOTH Azure and static S3 storage fields, because that input declares two incompatible intents whether or not either is read
* *AND* the adapter SHALL still apply the guard of `connection-credentials-sigv4` § "When SigV4 is enabled, access_key, secret_key, and a signing region are required" when `use_sigv4` is true, because those inputs sign the catalog `load_table` request rather than reaching object storage
* *AND* a `region` the CONNECTION states SHALL ALSO place the store under vending, while a signing region derived from the CONNECTION address SHALL place no store
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line
<!-- /DELTA:CHANGED -->
