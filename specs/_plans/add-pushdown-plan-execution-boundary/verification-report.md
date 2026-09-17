# Verification Report: add-pushdown-plan-execution-boundary

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Documentation states the plan visibility/execution split; a new E2E scenario guards it. All gates green. |
| Code review | 4 findings — 4 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit/Integration (`cargo test`, host) | 1 | all green, 0 failed | 0 |
| E2E (`make test-e2e`, 15 binaries, serial, live Docker Exasol/MinIO/Iceberg REST) | 15 | all green, 0 failed | 0 |
| E2E `e2e_credential_exposure_test` (isolated re-run, `--nocapture`) | 1 | 12 passed, 0 failed | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `docker compose up -d --wait minio exasol iceberg-rest && cargo test --features exasol-e2e --test e2e_credential_exposure_test -- --test-threads=1 --nocapture` | ✓ — 12 passed, 0 failed, including `the_reader_cannot_execute_the_pushdown_plan_it_captured` |
| `grep -n "EXECUTE ON SCRIPT\|EXECUTE ANY SCRIPT\|FOR SCRIPT\|#378" docs/security.md` | ✓ — new `## Plan visibility versus plan execution` section names the object grant, the system privilege, and the script-scoped connection grant as two independent gates; names table root, bucket layout, file names, byte sizes, and the CONNECTION name as readable; cross-references the sealed-envelope section |
| `grep -n "EXECUTE ON SCRIPT\|re-running the installer" docs/install.md` | ✓ — non-DBA note names all three necessary grants, states they precede `CREATE VIRTUAL SCHEMA`, and states re-running the installer drops them |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings          → exit 0, clean
cargo clippy --workspace --all-targets --features exasol-e2e -- -D warnings → exit 0, clean
```

### Formatter

```
cargo fmt --all -- --check → exit 0, no changes
```

### Build

```
make cross-udf-build → exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| e2e-harness | e2e-harness | A least-privilege reader cannot execute the pushdown plan it can read | `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` | `the_reader_cannot_execute_the_pushdown_plan_it_captured` | Pass |

## Notes

- No production code changed, per the plan's Non-Goals. Changes: one new test helper
  (`isolated_pushdown_statement` in `crates/lakehouse-engine/tests/common/e2e_harness.rs`), one new
  E2E test (`e2e_credential_exposure_test.rs`), and two documentation sections
  (`docs/security.md`, `docs/install.md`).
- Task 2.2 recorded five live-verified facts in `specs/_plans/add-pushdown-plan-execution-boundary/notes/A.md`,
  extended with a sixth during code-review fixups: `LAKEHOUSE_DISTRIBUTE_FILES` execute alone does
  not let a reader reach the distributor script, because every generated plan calls it only from
  inside the `LAKEHOUSE_SCAN` invocation's `FROM` clause. `docs/security.md` was corrected to state
  this precisely instead of implying the two script grants are independently sufficient.
- Code review found 4 standard findings (context-free panics, a magic number, a redundant
  conjunctive assertion duplicating an earlier check, and one doc sentence outdated by the live
  probe above); all 4 were fixed and re-verified live against the same running Docker stack.
- The plan's own Manual Testing table predicted `4 passed` for the credential-exposure binary;
  the actual, verified count is 12 passed (11 pre-existing plus the 1 new scenario) — the plan's
  count was a documentation estimate, not a gate, and the observed 0-failure result is what this
  report certifies.
- No installer change was made, per the plan's Non-Goals — `deploy/scripts/install.sh` issues no
  `GRANT EXECUTE` today and none was added.
