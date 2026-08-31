# Feature: Unity Catalog E2E Harness

End-to-end coverage of the Virtual Schema against a native Unity Catalog OSS server backed by MinIO
and seeded with the vendored Delta fixtures, run through the shared harness so the script DDL is
byte-identical to every other E2E binary. The suite fails, never skips, when the stack is unavailable.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no fixture, seed, or assertion of the others.** The Unity Catalog bring-up, the Delta fixtures, the createVirtualSchema listing, the dual-credential-mode scan-spec resolution, and the fail-not-skip rule are all UNCHANGED.
* **The blanket leak clause is SUPERSEDED because its returned-SQL half was never true for a vended credential.** A resolved bearer token and an OAuth client secret still reach no returned SQL string, no error message, and no test output. A VENDED storage credential travels INLINE in the scan-spec storage block, tracked as issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), while a CONNECTION-supplied static storage credential now reaches the SQL as a REFERENCE only, under `vs-adapter/scan-spec-credential-reference`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The Unity Catalog E2E suite leaks no credential value

* *GIVEN* a createVirtualSchema request whose CONNECTION or resolved auth carries a value, on any failure path the suite exercises
* *WHEN* the suite surfaces an error or prints diagnostic output
* *THEN* no resolved bearer token and no OAuth client secret SHALL appear in any returned SQL string, error message, or test output, and no vended storage credential SHALL appear in any error message or test output
* *AND* a VENDED storage credential DOES appear in the returned SQL string under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), while a CONNECTION-supplied static storage credential MUST NOT, because it travels as a REFERENCE under `vs-adapter/scan-spec-credential-reference` — SUPERSEDING the recorded clause that grouped all three under one prohibition
<!-- /DELTA:CHANGED -->
