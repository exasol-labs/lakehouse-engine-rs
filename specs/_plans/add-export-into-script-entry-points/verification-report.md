# Verification Report: add-export-into-script-entry-points

## Verdict

| Result | Details |
|--------|---------|
| **INCOMPLETE** | Build, unit tests, local E2E, lint, and format pass. The installer tests, the multi-node cloud proof, the coverage gate, and the manual `exapump` steps did not run. |
| Code review | 6 findings, 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build` ran in the fix pass; the `.so` exports four entry-point symbols) |
| Tests | ✓ (`cargo test --workspace` exit 0) |
| E2E (local) | ✓ (358 passed, 0 failed, includes `e2e_local::export`) |
| Lint | ✓ |
| Format | ✓ |
| Installer tests (`make test-install`) | ✗ not run: `jq` is not installed on this host |
| Cloud E2E (task 1.13) | ✗ not run: no multi-node cluster |
| Coverage gate (90% changed lines) | ✗ not run |
| Scenario Coverage | ✓ for local scenarios, see Notes for the rest |
| Manual Tests | ✗ not run as written: `exapump` is not installed on this host |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | exit 0 | 0 failures | 0 |
| Integration (`make e2e-up test-e2e`) | 358 | 358 | 0 |

`export_refuses_unsupported_column_types` passed in 12 of 12 separate runs.

### Manual Tests

| Test | Result |
|------|--------|
| `nm -D` lists four `__exa_udf_entry_*` symbols, including `LAKEHOUSE_EXPORT` | ✓ |
| Export with `WRITERS_PER_NODE = '3'`, `TRUNCATE` refusal, GEOMETRY refusal, forged writer call, five-script listing, installer run | not run by hand; the same behaviors are asserted by passing E2E tests in `export.rs` |

## Scenario Coverage

All scenarios in the plan's Scenario Coverage table have a test at the listed location. Local E2E passes for every `export.rs` test and `so_exports_four_entry_point_symbols`. The installer scenarios (`test_five_scripts_ddl_saas_path_types`, `test_template_prints_export_role`, `test_docs_describe_version_verification`) and the CI step `Export script shape` have not run.

## Notes

- The multi-node half of the SINGLE_COMMIT and WRITERS_PER_NODE scenarios is unverified. `cloud_export_writer_counts` exists and passes its skip path only. The Open Question on shipping without it stays open (#TBD).
- Decision recorded in this run: an unsupported column type is refused with the named guard error or with Exasol's own `Column type not supported`, nondeterministically. The spec delta, plan, and decision log are amended. The requester rewords #565.
- Review fix 4.4 changed an installer message. `make test-install` must confirm no assertion depends on the old wording.
