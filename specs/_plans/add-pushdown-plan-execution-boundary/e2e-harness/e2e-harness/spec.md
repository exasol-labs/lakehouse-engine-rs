# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`e2e-harness/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #402.** It adds ONE scenario and amends no recorded clause. The recorded least-privilege scenario asserts what the pushdown plan CONTAINS; the added scenario asserts what a reader can DO with it.
* **The denial is the enforcement mechanism behind every adapter-side pushdown decision.** A reader able to execute the plan could drop the `filter`, widen the `projection`, or swap the file list, making each adapter-injected predicate advisory rather than enforced.
* **`EXECUTE ON SCRIPT` is the boundary under test, and the reader's lack of it is ASSERTED rather than only arranged.** The shared provisioning grants `EXECUTE ON SCRIPT` for the adapter, scan, and distributor scripts to the VS owner alone. That arrangement is a fixture detail no test reads, so adding a reader to the grant loop breaks no test today. Asserting the reader's absent privilege turns a later convenience `GRANT EXECUTE` into a test failure.
* **No production code changes.** The scenario states and guards shipped behavior.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A least-privilege reader cannot execute the pushdown plan it can read

* *GIVEN* the provisioning of the recorded scenario "A least-privilege user queries the VS and recovers no credential from the plan": a virtual schema owned by a non-DBA holding `EXECUTE ON SCRIPT` for the adapter, scan, and distributor scripts, and a reader holding only `CREATE SESSION` plus `SELECT` on the virtual schema
* *AND* the reader's absent script-execute authority is ASSERTED, not assumed from the provisioning code: no `EXECUTE` object privilege on any of the three scripts and no `EXECUTE ANY SCRIPT` system privilege SHALL reach the reader, DIRECTLY or through any role it holds, read from the DBA object-privilege, system-privilege, and role-privilege views
* *WHEN* the reader runs `EXPLAIN VIRTUAL` over a single-table projection-and-filter query, captures the single adapter-generated pushdown statement isolated from the echoed adapter exchange that `EXPLAIN VIRTUAL` returns alongside it, and submits that isolated statement verbatim as its own SQL
* *THEN* the isolated statement SHALL be self-contained and schema-qualified: it SHALL name the scan and distributor scripts under the script schema, and SHALL carry the table root, the projection, the filter, and the per-shard file list as plaintext literals
* *AND* Exasol SHALL reject the submitted statement with an error naming insufficient privilege to CALL the script, rather than an unresolved object, a syntax fault, or a missing CONNECTION grant, and that error MUST NOT carry the CONNECTION's `access_key` or `secret_key` VALUES
* *AND* the reader's ordinary virtual-schema query SHALL still return the owner's rows after the rejection, so the denial SHALL be scoped to DIRECT script invocation and SHALL NOT disturb the delegated scan the engine performs for the reader
* *AND* the test MUST fail (not skip) when the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->
