# Plan Review Findings: fix-substr-left-unicode-expressions-pushdown (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 1 (Blockers: 0, Advisory: 1)
- Intent Fidelity blockers: 0

## Intent Fidelity
No objection -- axis checked: the user asked to enable `unicode_expressions` and add a regression test, rejecting both capability withdrawal and a broader audit. The plan enables the feature (task 1.1), adds a host regression test (tasks 2.1-2.4), and adds an E2E test (tasks 3.1-3.4). Non-goals explicitly exclude the audit the user declined and any translator or capabilities.rs change.

## Feasibility

#### [EFFORT_MISESTIMATION] ADVISORY
- Location: plan.md, Implementation Tasks, task 3.2
- Issue: task 3.2 directs the implementer to add the test "in a new section after 8.16." The file has sections through 8.19 (`// 8.19  Issue #209 dialect-fix E2E parity + now-family withdrawal` at line 3430). Inserting after 8.16 places the new test between 8.16 and 8.17, in the middle of the file rather than at the end. The planning notes (`notes/planning.md:21`) read only lines 2430-2500, which stops inside section 8.15; sections 8.17-8.19 were not read.
- Fix: change "in a new section after 8.16" to "in a new section after 8.19 (the last section in the file)." Update the section number in the task to 8.20.

## Requirement Quality
No objection -- axis checked. Both spec deltas carry concrete GIVEN/WHEN/THEN scenarios with unambiguous pass/fail criteria. The scan delta specifies both select-list and filter positions with exact fragment strings. The capability-extensions delta requires a pushdown-proof assertion (`substr(` in the generated SQL). Neither delta conflicts with the existing scenarios in `datafusion-scan/scan-execution-expression-pushdown` (which covers general expression projection and statistical aggregates) or `vs-adapter/pushdown-planning-capability-extensions` (which covers arithmetic, CAST, ISO week, regexp, bitwise, now-family, and literal-projection capabilities). The `left(...)` clause in the scan delta correctly describes a non-regression guard, not a new behavior.

## Task Breakdown
No objection -- axis checked. Every spec delta has an implementing task (1.1) and test tasks (2.1-2.4 for the scan delta, 3.1-3.4 for the capability-extensions delta). Tasks 1.2 and 1.3 are plan-maintenance tasks with no spec delta, acknowledged as such in the Parallelization traceability paragraph. The single parallelization group is correct: all tasks share one `Cargo.toml` edit and one root cause. No task carries `[expert]`, appropriate for a one-line manifest change and two tests that copy existing harnesses.

## Design Depth
No objection -- axis checked. The change introduces no new module, interface, or boundary. The root-cause analysis correctly identifies the defect at the `ExprPlanner` registration level (the `UnicodeFunctionPlanner` behind the `unicode_expressions` cfg gate), distinguishes it from the scalar-function registry path that `left(...)` uses, and scopes the blast radius to two `ExprPlanner` hooks (`plan_position` and `plan_substring`). The member-manifest placement follows the repository's documented convention and avoids the cache-key hazard.

## Prose Quality
No objection -- axis checked. Active voice throughout, BLUF structure in every section, no em dashes, no contractions, no semicolons in prose, no process narration (evidence citations such as "A probe against the current workspace showed" support factual claims). Vocabulary is consistent (`fragment`, `plan`, `render`, `advertise`). No weak modals or unsupported claims.
