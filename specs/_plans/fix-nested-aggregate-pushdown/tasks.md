# Tasks: fix-nested-aggregate-pushdown

Fix family selected (decision-log entry [4], from the Task 1 spike): family (a)
correct-parsing — extend `detect_group_by_aggregates` to recognize a
literal-only `selectList` (the "count the groups" pushdown shape) and still
emit a grouped scan, rather than falling back to row-scan (silently wrong on
duplicate-key tables).

## Phase 2: Implementation (Group A)
- [x] 1. Diagnostic spike — capture the real `pushdownRequest` JSON for the Q7 nested-aggregate shape. [expert]

## Phase 2: Implementation (Group B)
- [x] 2. Root-cause write-up + fix-family selection (formalize against decision-log entry [4]).

## Phase 2: Implementation (Group C)
- [x] 3. Implement the behavioral fix (family (a): preserve GROUP BY for literal-only selectList). [expert]

## Phase 2: Implementation (Group D)
- [ ] 4. Host unit test for the composed-request guard.
- [ ] 5. E2E regression test for the Q7 nested-aggregate shape (include duplicate-key case: `GROUP BY MOD(id,4)` expecting 4, not just the unique-id `events` case).

## Phase 3: Verification (Group E)
- [ ] 6. Verification gate (cargo test, clippy, fmt, cross-musl-udf-build, test-e2e).
