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
`aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint`.
<!-- /DELTA:CHANGED -->

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/connection-credentials/spec.md`.

## Scenarios

### Scenario: A non-vended CONNECTION configuring a store endpoint without stating path_style is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and a non-empty `endpoint`, omits `path_style`, and omits `use_vended_credentials` or sets it to false
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `path_style` and stating that a CONNECTION configuring a store `endpoint` MUST state `path_style`
* *AND* the error message SHALL state that `true` reaches a path-style store such as MinIO or Ceph and `false` addresses the bucket virtual-hosted, so the operator can choose without reading the source
* *AND* the adapter SHALL accept the same CONNECTION once it states `path_style`, under EITHER value
* *AND* the adapter MUST NOT apply this guard when `use_vended_credentials` is true or when the CONNECTION supplies no `endpoint`
* *AND* the error message MUST NOT contain any supplied credential value
