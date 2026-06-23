# Feature: Cloud E2E Harness (Glue + SigV4)

An opt-in end-to-end smoke and performance test that exercises the full lakehouse
query path against a real Exasol cluster and a real AWS Glue Iceberg REST catalog
loaded with meaningful data, verifying that CONNECTION-object credentials, SigV4
catalog signing, and vended S3 credentials all work against the live cloud stack.
Unlike the local Docker harness (which must FAIL when its stack is down), this cloud
test is opt-in: it SKIPS cleanly when the AWS credentials are not configured, so it is
safe to run in CI or trigger manually without a permanently-attached cloud account.

## Background

The test is gated behind a dedicated cargo feature distinct from `exasol-e2e` (e.g.
`cloud-e2e`), so the local-Docker suite's fail-when-down semantics are never changed.
The cloud test discovers its credentials and endpoints from environment variables. The
Exasol CONNECTION used by the virtual schema is created from those same environment
values. All DSN/connection strings include `validateservercertificate=0`. No credential
value is printed to test output.

## Scenarios

### Scenario: Cloud smoke test queries a real Glue-backed virtual schema

* *GIVEN* the AWS credential and endpoint environment variables are present
* *AND* an Exasol cluster reachable from the test, with the SLC and engine `.so` installed
* *AND* an Exasol CONNECTION whose address is the Glue Iceberg REST endpoint and whose password JSON enables SigV4 and supplies the region and keys
* *WHEN* the test creates a virtual schema over a Glue-managed Iceberg table loaded with meaningful data and runs a projection + filter query through it
* *THEN* the query SHALL return rows consistent with the seeded data (correct columns, predicate honoured)
* *AND* the test SHALL exercise the SigV4-signed catalog path and the S3 data-file read path end to end

### Scenario: Cloud test skips cleanly when AWS credentials are absent

* *GIVEN* the AWS credential environment variables are not set
* *WHEN* the cloud E2E test runs
* *THEN* the test SHALL skip without failing
* *AND* the test MUST NOT attempt any network call to AWS or Exasol
* *AND* the skip behaviour MUST be distinct from the local-Docker suite, which still FAILS when its stack is down

### Scenario: Cloud performance smoke records timing and row-count sanity

* *GIVEN* the AWS credentials are present and a Glue-backed virtual schema over a meaningfully-sized table
* *WHEN* the test runs an aggregate query (e.g. a grouped COUNT/SUM) through the virtual schema
* *THEN* the result row count and aggregate values SHALL be sane (non-zero, matching the seeded data shape)
* *AND* the test SHALL record the wall-clock query duration for manual inspection
* *AND* the test MUST NOT assert a hard latency threshold (timing is observational, not a pass/fail gate)

### Scenario: Vended credentials are exercised end to end against Glue

* *GIVEN* the AWS credentials are present and the CONNECTION enables `use_vended_credentials`
* *WHEN* the test runs a scan query whose data files are read using credentials vended by Glue's `load_table` response
* *THEN* the scan SHALL successfully read the data files using the vended credentials
* *AND* the test output MUST NOT contain any vended or static credential value
