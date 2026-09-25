# Feature: Assume-Role E2E Suite

Proves end to end on the local Exasol Docker stack that a CONNECTION whose base identity cannot read the warehouse bucket reads it after naming a role. The engine reaches the role only through a signed STS `AssumeRole` call. This is the spike of issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139), run against a live Exasol container.

## Background

* **The stack models the role with two MinIO users and an STS stub.** `minio-init` creates a base user with no policy, so MinIO denies it every S3 request. It also creates a role user with read access to the `warehouse` bucket. The `sts-stub` service (`scripts/sts-stub/sts_stub.py`, Python standard library only) stands in for AWS STS at `http://sts-stub:8080`.
* **The stub checks the request from its own code, not the engine's.** It recomputes the SigV4 signature for the service `sts` with the base user's secret, independently of the engine's `aws-sigv4` signer. It compares `RoleArn` and `ExternalId` against the configured role. On a match it mints MinIO session credentials by calling MinIO's own `AssumeRole` as the role user, and returns them in the AWS `AssumeRoleResponse` shape. On a mismatch it returns the AWS `ErrorResponse` shape with HTTP 403 and code `AccessDenied` or `SignatureDoesNotMatch`.
* **The stub counts AssumeRole requests.** `GET /__requests` returns the count, so the suite observes whether the engine called STS.
* **Two CI jobs run the assume-role scenarios.** The `e2e` job runs `make test-e2e`, whose explicit `--test` list registers `e2e_assume_role_test`. The `e2e-unity` job runs the Unity Catalog suite `e2e_unity_test` on the `docker-compose.unity.yml` overlay, which reuses the base MinIO and its two users.

## Scenarios

### Scenario: The assume-role binary and its STS stub are wired into the suite gate

* *GIVEN* the repository `Makefile`, whose `test-e2e` recipe enumerates its test binaries on one line and whose `unity-up` target starts the Unity Catalog stack, and CI's `e2e` and `e2e-unity` jobs, which start the stack services they wait on and then run their suites
* *WHEN* the suite gate runs
* *THEN* the `test-e2e` recipe SHALL run `--test e2e_assume_role_test`, and the `e2e` job, the `e2e-unity` job, and the `unity-up` target SHALL each start `sts-stub` in their `--wait` service set
* *AND* every assume-role test SHALL FAIL, not skip, when the STS stub is unreachable, like every other local Docker E2E suite

### Scenario: The base identity alone is denied the warehouse bucket

* *GIVEN* a table seeded into the local Iceberg REST catalog, and a CONNECTION that carries the base user's key pair and names no role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL fail with an error that reports the store's access-denied response
* *AND* the STS stub SHALL record no `AssumeRole` request for that query
* *AND* the error MUST NOT contain the base user's secret key

### Scenario: Naming the role reads the rows the base identity was denied

* *GIVEN* the same table, and a CONNECTION carrying the same base key pair plus `aws_assume_role_arn`, an `aws_external_id` holding `+`, `=`, `/`, `:`, and `@`, and an `aws_sts_endpoint` naming the stub, with `ALLOW_HTTP` set on the virtual schema
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the unfiltered query SHALL return the seeded row count, and the filtered query SHALL return the seeded filtered row count
* *AND* the STS stub SHALL record at least one `AssumeRole` request
* *AND* the `EXPLAIN VIRTUAL` output SHALL carry the sealed storage envelope and MUST NOT contain the base secret key or the external id

### Scenario: A wrong external id fails CREATE VIRTUAL SCHEMA with the STS error

* *GIVEN* a CONNECTION that carries the base key pair and the role, with an `aws_external_id` the stub does not accept
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `AccessDenied` code and the role ARN
* *AND* the error MUST NOT contain the base secret key or the rejected external id

### Scenario: A wrong base secret fails CREATE VIRTUAL SCHEMA with the STS signature error

* *GIVEN* a CONNECTION that carries the base `access_key`, a wrong `secret_key`, the role, and the accepted external id
* *WHEN* the suite creates a virtual schema through that CONNECTION
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `SignatureDoesNotMatch` code
* *AND* the error MUST NOT contain the wrong secret key or the base user's actual secret key

### Scenario: A direct-storage CONNECTION naming the role lists and reads through the session

* *GIVEN* Parquet files under a direct-storage base path in the `warehouse` bucket, and a direct-storage CONNECTION carrying the base key pair and the role
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries one of its tables
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL list the table, which proves that the store opened at create time used the session
* *AND* the query SHALL return the rows the fixture wrote

### Scenario: A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session

* *GIVEN* a Delta table seeded into the local Unity Catalog, and a Unity Catalog CONNECTION that does not set `use_vended_credentials` and carries the MinIO endpoint, the base user's key pair, the role, its external id, and an `aws_sts_endpoint` naming the stub
* *WHEN* the suite creates a virtual schema through that CONNECTION and queries the table
* *THEN* the query SHALL return the rows the fixture holds, which proves that the plan-time Delta log read and the scan used the session, because MinIO denies the base user every S3 request
* *AND* the STS stub SHALL record at least one `AssumeRole` request
