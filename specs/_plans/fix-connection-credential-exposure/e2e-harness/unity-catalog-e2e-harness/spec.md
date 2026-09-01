# Feature: Unity Catalog E2E Harness

End-to-end coverage of the Virtual Schema against a native Unity Catalog OSS server backed by MinIO
and seeded with the vendored Delta fixtures, run through the shared harness so the script DDL is
byte-identical to every other E2E binary. The suite fails, never skips, when the stack is unavailable.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no fixture, seed, or assertion of the others.** The Unity Catalog bring-up, the Delta fixtures, the createVirtualSchema listing, the dual-credential-mode scan-spec resolution, and the fail-not-skip rule are all UNCHANGED.
* **The blanket leak clause is SUPERSEDED because its returned-SQL half was never true for a vended credential before this plan.** A resolved bearer token and an OAuth client secret still reach no returned SQL string, no error message, and no test output. A VENDED storage credential now travels in the scan-spec storage block ONLY inside the AES-GCM-sealed envelope of `vs-adapter/scan-spec-credential-reference` — issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — while a CONNECTION-supplied static storage credential reaches the SQL as a REFERENCE only, under the same feature.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The Unity Catalog E2E suite leaks no credential value

* *GIVEN* a createVirtualSchema request whose CONNECTION or resolved auth carries a value, on any failure path the suite exercises
* *WHEN* the suite surfaces an error or prints diagnostic output
* *THEN* no resolved bearer token and no OAuth client secret SHALL appear in any returned SQL string, error message, or test output, and no vended storage credential SHALL appear in any error message or test output
* *AND* a VENDED storage credential MUST NOT appear in PLAINTEXT in the returned SQL string — it appears there only as the sealed envelope's ciphertext, issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — and a CONNECTION-supplied static storage credential MUST NOT appear at all, because it travels as a REFERENCE under `vs-adapter/scan-spec-credential-reference` — SUPERSEDING the recorded clause that grouped all three under one prohibition, which now holds in the plaintext sense throughout
<!-- /DELTA:CHANGED -->
