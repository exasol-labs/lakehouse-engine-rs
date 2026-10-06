# Tasks: reorg-e2e-specs-into-testing

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 2.1 Rewrite the 16 comment and doc lines in plan.md § Reference Edits › Path references: 5 lines that name a removed E2E spec (`.github/workflows/azure-orphan-sweep.yml:3`, `.github/workflows/glue-orphan-sweep.yml:3`, `crates/lakehouse-engine/tests/common/glue.rs:2` and `:844`, `deploy/README.md:385`) and 11 code comment lines that name a moved feature (`crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs:42`, `src/adapter/pushdown/joins/planning.rs:241`, `src/adapter/pushdown/joins/rendering.rs:206` and `:209`, `src/adapter/pushdown_surface_probe_tests.rs:4`, `src/scan/partition_values.rs:177`, `src/types/hive_type.rs:3`, `src/types/hive_type_cases_tests.rs:13` and `:36`, `tests/common/glue.rs:473`, `tests/pushdown_public_surface.rs:4`). Change comment and doc text only.
- [x] 2.2 Rewrite the `/// Scenario:` test doc lines in plan.md § Reference Edits › Scenario doc lines to the new line given there. Change no test name, body, or attribute.
- [x] 2.3 Apply the four edits in plan.md § Reference Edits › Pointer lines: the `specs/testing.md` pointer and the `scan-types/type-mapping` path in `AGENTS.md`, and the `scan-read-path/scan-execution-delta-deletion-vectors` path and the `specs/testing.md` sentence in `specs/mission.md`.

## Phase 3: Verification
- [x] 3.1 `cargo clippy --workspace --all-targets --all-features -- -D warnings`: exit 0 (compiles every feature-gated E2E test binary)
- [x] 3.2 `cargo fmt --all -- --check`: no changes
- [x] 3.3 `cargo test`: 0 failures
- [x] 3.4 `git diff -U0 -- crates .github deploy AGENTS.md specs/mission.md`: every changed line is a comment, a doc comment, or Markdown prose
- [x] 3.5 `speq plan validate reorg-e2e-specs-into-testing`: validation passed

## Phase 4: Review Fixes
- [x] 4.1 In `crates/lakehouse-engine/tests/e2e_glue_test.rs` line 432, change the doc line to `/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it`, and update the matching `New line` cell of the `e2e_glue_test.rs:432` row in plan.md § Reference Edits › Scenario doc lines. Comment text only.
- [x] 4.2 In `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` line 378, change the doc line to `/// Scenario: A narrow physical column binds to the current wider logical type and is cast per file` (capital A, no trailing period). Comment text only.

## Post-record (operator, after `/speq:implement`, in order)
- [ ] 5.1 Run `/speq:record reorg-e2e-specs-into-testing`. Expect `speq record` to exit 1 with one `No scenarios defined` failure per directory in plan.md § Dead Code Removal (decision-log.md [2]). No library threshold is exceeded after this plan; if the recorder reports one, stop and report it.
- [ ] 5.2 Copy `specs/_recorded/<NNN>-reorg-e2e-specs-into-testing/testing.md` to `specs/testing.md`. `<NNN>` is the number `speq record` assigned, `047` unless another plan records first.
- [ ] 5.3 Run `grep -L '^### Scenario' specs/*/*/spec.md | grep -v '^specs/_'`. Its output must list exactly the directories in plan.md § Dead Code Removal, each holding only `spec.md`. Delete those directories, then delete the six emptied domain directories `specs/e2e-harness`, `specs/azure-e2e`, `specs/unity-e2e`, `specs/lakekeeper-e2e`, `specs/glue-e2e`, and `specs/cloud-e2e`.
- [ ] 5.4 `speq feature validate` exits 0. `speq domain list` shows the domains of plan.md § Domain Layout. `speq search index` rebuilds. This prints nothing: `git grep -ohE '(vs-adapter|datafusion-scan|sql-comprehension|packaging|e2e-harness|azure-e2e|cloud-e2e|glue-e2e|lakekeeper-e2e|unity-e2e)/[a-z0-9-]*[a-z0-9]' -- ':!specs/_recorded' ':!specs/_decision' | sort -u | while read -r p; do [ -f "specs/$p/spec.md" ] || echo "$p"; done`
