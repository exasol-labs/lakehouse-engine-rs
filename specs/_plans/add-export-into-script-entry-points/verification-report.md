# Verification Report: add-export-into-script-entry-points

## Verdict

| Result | Details |
|--------|---------|
| **PASS with one unverified scenario** | Every local check passes. The multi-node writer-count proof of #565 did not run, and the requester accepted shipping without it. |
| Code review | 6 findings, 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build`; the `.so` exports four entry-point symbols) |
| Tests | ✓ (`cargo test --workspace`, exit 0) |
| E2E (local) | ✓ (358 passed, 0 failed, includes `e2e_local::export`) |
| Installer tests | ✓ (`make test-install`: 537 passed, 0 failed) |
| Lint | ✓ |
| Format | ✓ |
| Coverage gate | ✓ (216 of 220 changed lines, 98.2%, threshold 90%) |
| Scenario Coverage | ✓ for every local scenario |
| Cloud E2E (task 1.13) | not run: no multi-node cluster |
| Manual Tests | not run by hand: `exapump` is not installed on the host; the same behaviors are asserted by passing E2E tests |

## Test Evidence

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | exit 0 | 0 failures | 0 |
| Integration (`make e2e-up test-e2e`) | 358 | 358 | 0 |
| Installer (`make test-install`) | 537 | 537 | 0 |

`export_refuses_unsupported_column_types` passed in 12 of 12 separate runs. The only uncovered changed lines are the four `#[exasol_udf]` declaration lines in `lib.rs`, which the SDK macro expands and the E2E suite exercises through the `.so`.

## Scenario Coverage

Every scenario of the plan's Scenario Coverage table has a passing test, except the multi-node half of the SINGLE_COMMIT and WRITERS_PER_NODE scenarios. `cloud_export_writer_counts` exists and passes its skip path only.

## Notes

- Multi-node writer-count proof: unverified. The requester accepted shipping without it. A follow-up issue tracks running `cloud_export_writer_counts` on a multi-node cluster (#TBD).
- An unsupported column type is refused with the named guard error or with Exasol's own `Column type not supported`, nondeterministically. The spec delta, plan, and decision log state this. The requester rewords #565.
