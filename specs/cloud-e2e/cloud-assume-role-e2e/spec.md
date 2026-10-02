# Feature: Cloud Assume-Role E2E

An opt-in end-to-end test that proves a real AWS STS accepts the engine's signed `AssumeRole` request and that Glue and S3 accept the resulting session. Like `e2e-harness/cloud-e2e-harness`, the test skips cleanly when its AWS variables are absent. Tracked by issue [#139](https://github.com/exasol-labs/lakehouse-engine-rs/issues/139).

## Background

* **The test extends `e2e-harness/cloud-e2e-harness`.** It reuses that suite's `cloud-e2e` cargo feature, environment discovery, redacting WebSocket client, and seeded Glue table.
* **No credential value or external id appears in test output.**

## Scenarios

### Scenario: An assume-role CONNECTION reaches Glue and S3 through the assumed role

* *GIVEN* the Glue environment variables this suite already reads, plus `ASSUME_ROLE_BASE_ACCESS_KEY_ID`, `ASSUME_ROLE_BASE_SECRET_ACCESS_KEY`, `AWS_ASSUME_ROLE_ARN`, and `AWS_EXTERNAL_ID`, naming an IAM user that `deploy/data-stack` grants only `sts:AssumeRole` on that role, and a role whose trust policy requires that external id and whose permissions grant the Glue and S3 read the engine-reader user holds
* *WHEN* the test creates a virtual schema through a SigV4 CONNECTION carrying the base key pair, `region`, the role, and the external id, and runs the projection and filter query
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL list the Glue table and the query SHALL return rows consistent with the seeded data, which proves that real AWS STS accepted the engine's signed `AssumeRole` request and that Glue and S3 accepted its session
* *AND* when any of the four added variables is absent, the test SHALL skip with a message naming the absent variable, and MUST NOT fail
* *AND* the test output MUST NOT contain any credential value or the external id

### Scenario: The assume-role base identity alone is denied by Glue

* *GIVEN* the environment of § "An assume-role CONNECTION reaches Glue and S3 through the assumed role"
* *WHEN* the test creates a virtual schema through a SigV4 CONNECTION carrying the base key pair and `region` and naming no role
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail, because Glue denies the base identity
* *AND* the test SHALL skip under the same absent-variable condition, and its output MUST NOT contain any credential value

### Scenario: A wrong external id is denied by STS

* *GIVEN* the environment of § "An assume-role CONNECTION reaches Glue and S3 through the assumed role"
* *WHEN* the test creates a virtual schema through a SigV4 CONNECTION carrying the base key pair, the role, and an `aws_external_id` the role's trust policy does not accept
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `AccessDenied` code, which proves that real AWS STS enforces `sts:ExternalId`, the one condition the local SeaweedFS stack (`cloud-e2e/assume-role-e2e`) does not evaluate
* *AND* the test SHALL skip under the same absent-variable condition, and its output MUST NOT contain any credential value or the rejected external id
