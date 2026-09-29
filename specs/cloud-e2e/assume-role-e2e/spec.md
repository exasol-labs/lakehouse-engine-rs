# Feature: Assume-Role E2E Suite

Proves end to end on the local Exasol Docker stack that a CONNECTION whose base identity cannot read the warehouse bucket reads it after naming a role. The engine reaches the role only through an `AssumeRole` call made by the official AWS SDK, and the local STS server is SeaweedFS's own implementation, not a hand-written stand-in. This is the spike of issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139), run against a live Exasol container.

## Background

* **No hand-written STS on either side.** The engine calls STS through `aws-sdk-sts`. The local server is SeaweedFS 4.48 (4.44 or later is required for static-user principal ARNs in trust policies), which verifies the SigV4 signature for service `sts` and evaluates the role's trust policy. A hand-written client tested against a hand-written server can drift from AWS together, so neither exists.
* **The role is configuration.** `seaweedfs-iam.json` defines the base user `lhassumebase` with no S3 access, the policy `WarehouseRead`, and the role `LakehouseReader`, whose trust policy admits only that user and whose attached policy is `WarehouseRead`. The session is scoped to the role's policy, so a read succeeds only if the session came from STS.
* **SeaweedFS does not evaluate `sts:ExternalId`.** Enforcement of `aws_external_id` belongs to the opt-in real-AWS suite `cloud-e2e/cloud-assume-role-e2e`. The local suite still sends an external id holding `+`, `=`, `/`, `:`, and `@` to exercise the encoding path.
* **Two CI jobs run the assume-role scenarios.** The `e2e` job runs `make test-e2e`, whose explicit `--test` list registers `e2e_assume_role_test`. The `e2e-unity` job runs the Unity Catalog suite `e2e_unity_test` on the `docker-compose.unity.yml` overlay, which reuses the base SeaweedFS and its base user.

## Scenarios

### Scenario: The assume-role binary is wired into the suite gate

* *GIVEN* the repository `Makefile`, whose `test-e2e` recipe enumerates its test binaries on one line
* *WHEN* the suite gate runs
* *THEN* the `test-e2e` recipe SHALL run `--test e2e_assume_role_test`
* *AND* every assume-role test SHALL FAIL, not skip, when SeaweedFS is unreachable, like every other local Docker E2E suite

### Scenario: The base identity alone is denied the warehouse bucket

* *GIVEN* a table seeded into the local Iceberg REST catalog, and a CONNECTION that carries the base user's key pair and names no role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL fail with an error that reports the store's access-denied response
* *AND* the error MUST NOT contain the base user's secret key

### Scenario: Naming the role reads the rows the base identity was denied

* *GIVEN* the same table, and a CONNECTION carrying the same base key pair plus `aws_assume_role_arn` naming `LakehouseReader`, an `aws_external_id` holding `+`, `=`, `/`, `:`, and `@`, and an `aws_sts_endpoint` naming SeaweedFS, with `ALLOW_HTTP` set on the virtual schema
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the unfiltered query SHALL return the seeded row count, and the filtered query SHALL return the seeded filtered row count
* *AND* the `EXPLAIN VIRTUAL` output SHALL carry the sealed storage envelope and MUST NOT contain the base secret key or the external id

### Scenario: An unknown role ARN fails CREATE VIRTUAL SCHEMA with AccessDenied

* *GIVEN* a CONNECTION that carries the base key pair and a role ARN no role defines
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `AccessDenied` code and the role ARN
* *AND* the error MUST NOT contain the base secret key

### Scenario: A user outside the trust policy fails CREATE VIRTUAL SCHEMA with AccessDenied

* *GIVEN* a CONNECTION that names `LakehouseReader` but carries the key pair of a user its trust policy does not admit
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `AccessDenied` code and the role ARN
* *AND* the error MUST NOT contain that user's secret key

### Scenario: A wrong base secret fails CREATE VIRTUAL SCHEMA with HTTP 403

* *GIVEN* a CONNECTION that carries the base `access_key`, a wrong `secret_key`, the role, and the external id
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error reporting HTTP 403, because SeaweedFS answers a bad signature with a bare S3-style error body that carries no STS error code
* *AND* the error MUST NOT contain the wrong secret key or the base user's actual secret key

### Scenario: An unreachable STS endpoint fails CREATE VIRTUAL SCHEMA

* *GIVEN* a CONNECTION that carries the base key pair, the role, and an `aws_sts_endpoint` nothing listens on
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the role ARN and the STS host
* *AND* the error MUST NOT contain the base secret key or the external id

### Scenario: A direct-storage CONNECTION naming the role lists and reads through the session

* *GIVEN* Parquet files under a direct-storage base path in the `warehouse` bucket, and a direct-storage CONNECTION carrying the base key pair and the role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries one of its tables
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL list the table, which proves that the store opened at create time used the session
* *AND* the query SHALL return the rows the fixture wrote

### Scenario: A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session

* *GIVEN* a Delta table seeded into the local Unity Catalog, and a Unity Catalog CONNECTION that does not set `use_vended_credentials` and carries the SeaweedFS endpoint, the base user's key pair, the role, its external id, and an `aws_sts_endpoint` naming SeaweedFS
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL return the rows the fixture holds, which proves that the plan-time Delta log read and the scan used the session, because the base user is denied every S3 request
