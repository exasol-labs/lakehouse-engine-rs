# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse
query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog
loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4
catalog signing, and vended S3 credentials all work against the live cloud stack.
Unlike the local Docker harness (which must FAIL when its stack is down), this cloud
test is opt-in: it SKIPS cleanly when the AWS credentials are not configured, so it is
safe to run in CI or trigger manually without a permanently-attached cloud account.
The remote bench harness drives the same cloud path and now selects its catalog backend,
defaulting to Glue.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/e2e-harness/cloud-e2e-harness/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An assume-role CONNECTION reaches Glue and S3 through the assumed role

* *GIVEN* the Glue environment variables this suite already reads, plus `ASSUME_ROLE_BASE_ACCESS_KEY_ID`, `ASSUME_ROLE_BASE_SECRET_ACCESS_KEY`, `AWS_ASSUME_ROLE_ARN`, and `AWS_EXTERNAL_ID`, naming an IAM user that `deploy/data-stack` grants only `sts:AssumeRole` on that role, and a role whose trust policy requires that external id and whose permissions grant the Glue and S3 read the engine-reader user holds
* *WHEN* the test creates a virtual schema through a SigV4 CONNECTION carrying the base key pair, `region`, the role, and the external id, and runs the projection and filter query
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL list the Glue table and the query SHALL return rows consistent with the seeded data, which proves that real AWS STS accepted the engine's signed `AssumeRole` request and that Glue and S3 accepted its session
* *AND* when any of the four added variables is absent, the test SHALL skip with a message naming the absent variable, and MUST NOT fail
* *AND* the test output MUST NOT contain any credential value or the external id
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The assume-role base identity alone is denied by Glue

* *GIVEN* the environment of § "An assume-role CONNECTION reaches Glue and S3 through the assumed role"
* *WHEN* the test creates a virtual schema through a SigV4 CONNECTION carrying the base key pair and `region` and naming no role
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL fail, because Glue denies the base identity
* *AND* the test SHALL skip under the same absent-variable condition, and its output MUST NOT contain any credential value
<!-- /DELTA:NEW -->
