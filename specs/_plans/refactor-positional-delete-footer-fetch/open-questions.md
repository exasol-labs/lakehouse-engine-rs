# Open Questions: refactor-positional-delete-footer-fetch

speq-plan-pr could not complete this plan without human input. What's done so far is committed on this branch. Reply inline on the PR, or resume with `/speq:plan refactor-positional-delete-footer-fetch` locally, or re-run `/speq:plan-pr refactor-positional-delete-footer-fetch` after commenting.

## Round 1 (resolved)

- [x] Issue #165's proposed change asks to "guard against silent double-fetch if [the metadata cache] evicts." Task 1.7 measured cache reuse over K=64 tiny fixtures, too small to ever trigger eviction. **Answer: (a)** — scale the fixture to the cache limit and ship a runtime eviction observable as task 1.7b.

## Round 2 (blocks this plan)

The human's answer to round 1 was implemented, but round-2 adversarial review found 3 BLOCKER defects in the fix itself. These are fully specified fixes (each has a "Fix:" line in `review/round-2.md`), not open design choices — they route here only because the workflow caps automated review at 2 rounds. Reply "apply the round-2 fixes" to have `planner-agent` apply them and re-validate, or give different direction.

- [ ] **Eviction detector lives in the wrong place and can't pass its own test.** Task 1.7b puts re-fetch detection in `SpecSizedObjectStore`, a private struct only production code constructs — every host test registers its own decorator instead, so the planned test's assertion can never see a re-fetch. Fix (`review/round-2.md` Feasibility BLOCKER 1): detect re-fetches by diffing `FileMetadataCache::list_entries()` against the paths Phase B cached, not at the object-store layer.
- [ ] **The eviction counter's report path violates the scenario it implements.** Task 1.7b wires a per-re-fetch `debug_checkpoint()` call, which is ungated and does `flush()` + `sync_all()` per line — that emits stderr output and an fsync per re-fetch at the production default log level, breaking the new scenario's "MUST NOT emit output at the default level" clause. The counter is also never reset between scan invocations sharing one UDF process, so invocation 2 can report invocation 1's stale count. Fix (`review/round-2.md` Requirement Quality BLOCKER 1): drop `debug_checkpoint`, keep only the level-gated `udf_log!` at the report site, and reset the counter at the start of every scan invocation.
- [ ] **The new mixed-shard test (task 1.3) can't pass with the spec builder it's told to use.** `scan_spec` leaves `logical_schema` empty, so `register_file_list` infers from the first assigned file and fetches its footer before Phase B runs, breaking both of the test's request-count assertions regardless of file order. Fix (`review/round-2.md` Feasibility BLOCKER 2): add a `scan_spec_with_logical_schema` helper (mirroring task 1.1's `raw_spec_with_logical_schema`) and require both new task-1.3 tests, plus task 1.4, to build their spec with it.
