# Feature: Assume-Role E2E Suite

Proves on the local Exasol Docker stack that a CONNECTION whose base identity cannot read the warehouse bucket reads it after naming a role. The local STS server is SeaweedFS, which evaluates the role's trust policy and scopes the session to the role's attached policy. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **SeaweedFS does not evaluate `sts:ExternalId`.** `cloud-e2e/cloud-assume-role-e2e` covers `aws_external_id` enforcement against real AWS.
* **Every assume-role test FAILS, not skips,** when SeaweedFS or Exasol is unreachable, like every other local Docker E2E suite.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The base identity alone is denied the warehouse bucket

* *GIVEN* a table seeded into the local Iceberg REST catalog, and a CONNECTION that carries the base user's key pair and names no role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL fail with an error that reports the store's access-denied response
* *AND* the error MUST NOT contain the base user's secret key
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Naming the role reads the rows the base identity was denied

* *GIVEN* the same table, and a CONNECTION carrying the same base key pair plus `aws_assume_role_arn`, `aws_external_id`, and an `aws_sts_endpoint` naming SeaweedFS, with `ALLOW_HTTP` set on the virtual schema
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the unfiltered query SHALL return the seeded row count, and the filtered query SHALL return the seeded filtered row count
* *AND* the `EXPLAIN VIRTUAL` output SHALL carry the sealed storage envelope and MUST NOT contain the base secret key or the external id
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A wrong base secret fails CREATE VIRTUAL SCHEMA with HTTP 403

* *GIVEN* a CONNECTION that carries the base `access_key`, a wrong `secret_key`, and the role
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error reporting HTTP 403
* *AND* the error MUST NOT contain the wrong secret key or the base user's actual secret key
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A direct-storage CONNECTION naming the role lists and reads through the session

* *GIVEN* Parquet files under a direct-storage base path in the `warehouse` bucket, and a direct-storage CONNECTION carrying the base key pair and the role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries one of its tables
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL list the table
* *AND* the query SHALL return the rows the fixture wrote
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session

* *GIVEN* a Delta table seeded into the local Unity Catalog, and a Unity Catalog CONNECTION that does not set `use_vended_credentials` and carries the base user's key pair and the role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL return the rows the fixture holds, because the base user is denied every S3 request
<!-- /DELTA:REMOVED -->
