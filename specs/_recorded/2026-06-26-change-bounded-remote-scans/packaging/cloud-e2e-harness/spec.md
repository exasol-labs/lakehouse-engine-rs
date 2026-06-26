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
value is printed to test output. The same cloud path is driven by the remote bench
harness (`bench/run.sh` with `BENCH_TARGET=remote`), which builds the
`CREATE VIRTUAL SCHEMA` statement against the live cluster from the bench environment.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Remote bench wires NR_OF_CORES and PARALLELISM_FACTOR into the virtual schema

* *GIVEN* the remote bench target running against a real Glue catalog and external Exasol cluster
* *AND* `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR` set in the bench environment
* *WHEN* the bench harness builds the `CREATE VIRTUAL SCHEMA` statement for the remote target
* *THEN* the harness SHALL pass `NR_OF_CORES` and `PARALLELISM_FACTOR` as virtual-schema properties on the remote target, just as the docker target already does
* *AND* the property values SHALL come from `BENCH_NR_OF_CORES` and `BENCH_PARALLELISM_FACTOR`, applying the same defaults the docker path uses when those variables are unset
* *AND* the remote path MUST NOT emit an empty extra-properties block that drops these parallelism knobs (the prior behaviour where the cluster ran at its built-in defaults)
<!-- /DELTA:NEW -->
