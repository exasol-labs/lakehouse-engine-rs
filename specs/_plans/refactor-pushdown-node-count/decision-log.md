# Decision Log: refactor-pushdown-node-count

## Interview

No live interview was run. This plan was authored headless via `/speq:plan-pr`, so the body of GitHub issue [#184](https://github.com/exasol-labs/lakehouse-engine-rs/issues/184) stands in for the interview. The issue supplied the intent, the current-state code references, the suggested approach, the out-of-scope boundary (`NR_OF_CORES`), the no-behaviour-change constraint, and one explicitly flagged open item.

**Q (from the issue, "Verify before removing"):** Does `ctx.node_count()` return the correct cluster size at *pushdown* time, not only at `createVirtualSchema`? The issue author states they have not run that check on the four-node staging cluster.
**A (resolved during planning, not escalated):** Settled at the code level for the mechanism, deferred to a mandatory manual gate for the database-side value. See decision [1].

**Q (from the issue, "Suggested approach"):** Does `createVirtualSchema` still need a locally computed `cluster_nodes` for its create-time derivations, for example the `PARALLELISM_FACTOR` default?
**A (corrected during planning):** No. The issue is mistaken on this point. See decision [4].

## Design Decisions

### [1] Do not escalate the unverified multi-node premise; gate it in Manual Testing instead

- **Decision:** Plan the refactor without escalating via `OPEN QUESTIONS:`, and make the four-node staging check a mandatory pre-merge gate in `plan.md` § Manual Testing, with an explicit "MUST NOT ship" failure condition.
- **Alternatives:** (a) Escalate as an irreducible open question and stop, persisting a partial plan. (b) Assume the premise silently and plan as a pure refactor with no extra verification. (c) Keep a defensive `adapterNotes` fallback so a wrong `node_count()` cannot degrade `G` (rejected separately as decision [5]).
- **Rationale:** The escalation bar in headless mode is irreversibility, a change in user-facing behaviour, an architectural fork, or security. This is none of those, because the mechanism is decidable by reading the SLC runtime. `exa-udf-runtime` decodes one `UdfMeta` per handshake; for a single-call script, `single_call.rs:31` builds `HandshakeMeta::from(meta)` and hands it to `SingleCallContext`, whose `node_count()` returns `self.handshake.node_count`. The VS adapter call is that single-call path, and the request type (`createVirtualSchema` vs `pushdown`) is a field in the JSON payload, not in the handshake, so the request type cannot vary the value. The load-bearing consequence: `CLUSTER_NODES` is already sourced from this exact call today, and the E2E test `create_vs_records_cluster_nodes_property` asserts it returns `≥ 1` against a live database. If `node_count()` were wrong at pushdown it would be equally wrong at create time, and current sharding would already be wrong. This refactor changes when the value is read, not what is read, so it introduces no new risk. What remains genuinely unverified is whether the *database* populates `numberOfNodes` as `4` on a four-node cluster, and that is a pre-existing property of the current code path, not a consequence of this change. Because the `0 => 1` floor makes a wrong value degrade silently rather than fail, the check earns a hard gate rather than an assumption. Option (b) was rejected for that reason; option (a) was rejected because escalating a question the current code already depends on would block a plan on a pre-existing condition.
- **Promotes to ADR:** yes

### [2] `adapterNotes` admits only create-time values a pushdown cannot recompute

- **Decision:** Adopt as a standing rule: `schemaMetadata.adapterNotes` carries a value only when the value is derived at create time and a pushdown cannot recompute it. `TABLE_MAP` qualifies (recomputing it costs a catalog namespace enumeration per query). Handshake metadata never qualifies.
- **Alternatives:** Treat `adapterNotes` as a general-purpose cache for anything convenient to have at pushdown time, which is the de facto status quo that produced `CLUSTER_NODES`.
- **Rationale:** The status quo duplicates one decision, where the node count comes from, across a writer and a reader that agree only through the untyped string key `"CLUSTER_NODES"`. That is back-door information leakage: two sites independently assume the same format with nothing enforcing the agreement. It also contradicts the mission's own rule that UDFs hold no cross-call state and that metadata is resolved per query. Stating the admission criterion explicitly is what stops the next convenient value from following `CLUSTER_NODES` in. Supersedes ADR `source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006`, whose decision to record the count as `CLUSTER_NODES` this rule reverses; the `UdfContext::node_count()` source and the `0 => 1` floor it established are retained.
- **Promotes to ADR:** yes

### [3] Capture the handshake read in `dispatch`; pass a value, never `ctx`, into async planning

- **Decision:** `dispatch`'s pushdown arm calls `cluster_nodes_from_context(ctx)` before `rt.block_on`, and `handle_pushdown_request` gains a plain `cluster_nodes: usize` parameter.
- **Alternatives:** Pass `&mut dyn UdfContext` into `handle_pushdown_request` and read `node_count()` there, which is the obvious shortcut given the function already receives a `request`.
- **Rationale:** Two independent reasons. Mechanically, `node_count()` is a synchronous handshake read that may block on the UDF host, and the arm's existing comment records that such reads must happen before the tokio runtime is entered; `resolve_connection_config` and `ctx.script_schema()` are already captured there for exactly this reason. Architecturally, injecting the resolved value keeps the async planning code free of ambient reads and free of any dependency on the UDF delivery mechanism, which is the dependency direction the project's layering already follows via the existing `script_schema: &str` parameter. The new capture joins two siblings rather than establishing a new pattern.
- **Promotes to ADR:** yes

### [4] Reduce `resolve_cluster_nodes` to `resolve_nr_of_cores(props)`, dropping both the node count and `ctx`

- **Decision:** Collapse `resolve_cluster_nodes(ctx: &mut dyn UdfContext, props: &Json) -> (u32, u32)` to `resolve_nr_of_cores(props: &Json) -> u32`.
- **Alternatives:** Follow the issue's suggested approach literally and keep computing `cluster_nodes` at create time, on the stated grounds that create-time derivations such as the `PARALLELISM_FACTOR` default still need it.
- **Rationale:** The issue's premise here is wrong, and the code disagrees with it. At `crates/lakehouse-engine/src/adapter/mod.rs:219-238` the create-time value `cluster_nodes` reaches exactly one consumer: the `build_adapter_notes` call that writes the note. `resolve_parallelism_factor(props, nr_of_cores)`, `resolve_df_threading(..., nr_of_cores, parallelism_factor)`, and `resolve_s3_max_connections(props, nr_of_cores, parallelism_factor)` all derive from `nr_of_cores`, never from the node count. Once the note is dropped, keeping the node count would leave dead code, and keeping `ctx` would leave a parameter the core-count path never needed, since `parse_nr_of_cores_override(props)` and `available_parallelism_or_0()` touch neither the context nor the handshake. The reduction is therefore part of the refactor, not scope creep.
- **Promotes to ADR:** no

### [5] No `adapterNotes` fallback when `node_count()` reports `0`

- **Decision:** Apply the `0 => 1` floor and nothing else. Do not consult a persisted `CLUSTER_NODES` value even when an older virtual schema still carries one.
- **Alternatives:** Read the note as a safety net when `node_count()` returns `0`, so a broken handshake on an existing schema keeps its previously recorded fan-out.
- **Rationale:** The fallback would restore precisely the writer/reader coupling this plan removes, and it would add a branch reachable only on a broken handshake against a pre-refactor schema, which no test in this repository can construct. It also buys nothing measurable: the `0 => 1` floor reproduces the pre-refactor behaviour for an absent or unparseable note exactly, because `handle_pushdown_request` already defaulted that read to `1`.
- **Promotes to ADR:** no

### [6] Accept a live node count as a deliberate behaviour change on a resized cluster

- **Decision:** Treat the shift from a create-time-frozen node count to a per-pushdown live one as intended, and state it in `plan.md` § Impact rather than suppressing it.
- **Alternatives:** Preserve the frozen semantics exactly, for instance by continuing to write the note and reading the context only to refresh it, so that `G` changes only on an explicit `REFRESH`.
- **Rationale:** The issue's constraint is that `G` be identical "for a given cluster", and it is: for a fixed node count the arithmetic is byte-identical. The divergence appears only when the cluster is resized, where the new behaviour is strictly more correct, since a schema created on one node currently keeps planning single-node fan-outs on a grown cluster until an operator refreshes it. Preserving the frozen semantics would mean deliberately keeping a stale cluster property, which contradicts the mission's stateless rule. The change is recorded in Impact because an operator watching shard counts across a resize will observe it.
- **Promotes to ADR:** yes

### [7] Edit the `NR_OF_CORES` recording scenario's key enumeration, without touching its behaviour

- **Decision:** Apply a `DELTA:CHANGED` to `vs-adapter/create-virtual-schema-adapter-notes` § "Adapter records the per-node core count in the virtual-schema adapterNotes", removing only `CLUSTER_NODES` from its "alongside `CLUSTER_NODES` and `PARALLELISM_FACTOR`" clause. Apply the same narrow edit to the five resource-note scenarios that enumerate `CLUSTER_NODES` as a sibling key.
- **Alternatives:** (a) Leave those scenarios untouched, honouring "`NR_OF_CORES` is out of scope" literally. (b) Rewrite the enumerations into a stable phrasing that lists no sibling keys, so future note changes stop rippling.
- **Rationale:** The out-of-scope boundary protects the `NR_OF_CORES` *behaviour*, and this edit changes none of it: the property override, the `available_parallelism()` auto-detect, and the `0`-when-unknown outcome are all unchanged, and no test covering them changes. What does change is a factual claim inside a `SHALL` clause. Leaving it would make six merged scenarios assert that `adapterNotes` carries a key it no longer carries, which is a spec that lies. Option (b) is the better long-term shape but rewrites six normative clauses in a pure refactor, so it is deliberately deferred; the narrow deletion keeps the reviewer's diff to the one word that became false.
- **Promotes to ADR:** no

### [8] Home the two new scenarios in `vs-adapter/pushdown-planning`

- **Decision:** Add "Pushdown reads the cluster node count from the UDF handshake" and "Pushdown node count falls back to one when the handshake reports none" to `vs-adapter/pushdown-planning`. Change `parallelism/work-unit-sharding` only where it names the node count's source.
- **Alternatives:** Put both scenarios in `parallelism/work-unit-sharding`, since the node count exists only to size `G`.
- **Rationale:** `pushdown-planning` is the feature that owns what the adapter reads while serving a `pushdown` request, and it already carries the sibling scenario for the other synchronous handshake read, "Scan-driving UDF invocations are schema-qualified from the running adapter script's schema" (`ctx.script_schema()`). Putting the node-count read next to it keeps one feature owning the handshake-read seam. `work-unit-sharding` owns the arithmetic that consumes the value, which is unchanged, so it needs only a corrected source attribution. This takes `vs-adapter/pushdown-planning` to 11 scenarios, crossing `/speq:spec-merge`'s >10 signal. Keep the feature intact rather than splitting it: `datafusion-scan/scan-execution` and `datafusion-scan/scan-execution-positional-deletes` already sit at 11, and the handshake-read seam belongs with `ctx.script_schema()`.
- **Promotes to ADR:** no

### [9] Prove the mechanism with unit tests and the regression with E2E; gate the multi-node value manually

- **Decision:** Map the `0 => 1` floor and the pass-through to unit tests on `cluster_nodes_from_context`, add two E2E tests (notes lack `CLUSTER_NODES`; the fan-out still renders without it), and route the four-node assertion to Manual Testing.
- **Alternatives:** Require an automated integration test that proves the node count came from the handshake rather than the note.
- **Rationale:** No test on the single-node Docker Exasol can distinguish a correctly read `1` from the `0 => 1` floor, because both produce `node_count = 1`. Claiming an automated test proves the premise would be false coverage. What the automated suite can prove, and now does, is that the note is gone and that the sharded pushdown path still renders its fan-out without it, which means the value reached `shard_count` from somewhere other than `adapterNotes`. The discriminating assertion needs a cluster with more than one node, so it is written as an exact staging command with an explicit fail condition instead of being hidden in a test that cannot fail for the right reason.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Dropping the note's insert does not remove the persisted key

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER (round 1, § Feasibility). `build_adapter_notes` opens with `let mut notes = parse_adapter_notes(request);`, so it merges into the notes Exasol round-trips on every `refresh` / `setProperties`. Deleting the `NOTE_CLUSTER_NODES` insert therefore never deletes the key: it would survive on every pre-existing schema forever, making `plan.md` § Impact item 3 and § Migration row 1 false and the new scenario's THEN unsatisfiable for the `refresh` / `setProperties` request types its own WHEN names.
- **Direction change:** Took the reviewer's branch (a) — remove the key actively — over branch (b) — narrow the scenario and document persistence-but-ignored. Branch (a) is the one consistent with decision [2]'s admission criterion and decision [3]'s rejection of a written-never-read note: a key nothing reads is exactly the dead state the plan set out to remove, and leaving it on every upgraded schema would keep the coupling's residue while claiming it was gone. Branch (b) would also have required rewriting two operator-facing claims into weaker ones. Concretely: task 1.3 now replaces the insert with `notes.remove(NOTE_CLUSTER_NODES);` and KEEPS the `NOTE_CLUSTER_NODES` constant as the removal key (its § Dead Code Removal row is replaced by a row retiring only the insert statement); the new scenario in `vs-adapter/create-virtual-schema-adapter-notes` gains a GIVEN for inherited notes and a `SHALL remove` clause; a second Background bullet states that not-recording and not-removing are different properties; task 1.3 adds the unit test `refresh_notes_drop_inherited_cluster_nodes`; § Impact item 3, § Migration row 1, § Summary, § Goals, the § Architecture diagram, and the § Consequences row all now name the active removal; and § Manual Testing gains a legacy-schema `REFRESH` check, since no automated test in this repo can build a pre-refactor persisted note.
- **Promotes to ADR:** no

### [plan-review] Two recorded specs still mandate preserving CLUSTER_NODES

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER (round 1, § Requirement Quality). `specs/vs-adapter/create-virtual-schema/spec.md:111` and `specs/vs-adapter/refresh-and-set-properties/spec.md:33` both enumerate `CLUSTER_NODES` among the notes the adapter preserves, and neither feature appeared in `plan.md` § Features. After `/speq:record` the library would both mandate and forbid the key on a `refresh` response.
- **Direction change:** Added both features to § Features and created both spec deltas — `vs-adapter/create-virtual-schema/spec.md` (`DELTA:CHANGED` on "Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes") and `vs-adapter/refresh-and-set-properties/spec.md` (`DELTA:CHANGED` on "Refresh rebuilds the table map and preserves other adapter notes"). Both go one clause beyond the reviewer's narrow-deletion instruction, and deliberately so: deleting the token alone would leave "preserve every other pre-existing entry" mandating preservation of the very key branch (a) removes, so each scenario now names `CLUSTER_NODES` as the single exception to preservation. The `create-virtual-schema` Feature description also drops the node count from its recorded-values list, logged in § Record Notes. Both scenarios map to existing tests whose `CLUSTER_NODES` assertions task 1.3 inverts (`create_vs_records_table_map_in_adapter_notes`, `table_map_merges_with_existing_notes`) — the reviewer's finding surfaced these two assertions, which the old task list never mentioned.
- **Promotes to ADR:** no

### [plan-review] Mission glossary still says the node count is captured at createVirtualSchema

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER (round 1, § Requirement Quality). `specs/mission.md:75` (Domain Glossary, IPROC / NPROC row) states the shard-count node number is "captured once at `createVirtualSchema`", which this plan makes false. No task edited it and no entry deferred it, so `/speq:audit` would report mission drift against four revised feature specs.
- **Direction change:** Added task 1.4 — reword that row to say the node count is read from `UdfContext::node_count()` per pushdown request, leaving its `NPROC()` / `IPROC()` clauses untouched — and listed `specs/mission.md:75` as a revised artifact in § Migration. The task number 1.4 was freed by the 1.3/1.4 merge below; the task is docs-only and independent of the code path, so it joins Group D rather than extending the critical path.
- **Promotes to ADR:** no

### [plan-review] Task 1.3 left the test target uncompilable until 1.4

- **Finding:** `[TASK_GRANULARITY]` BLOCKER (round 1, § Task Breakdown). Task 1.3 changed `build_adapter_notes`'s arity and deleted the `NOTE_CLUSTER_NODES` constant while the 18 test call sites and the test references to that constant were repaired only in 1.4. Between the two, `cargo test` could not build, so `/speq:implement`'s per-task gate could not pass and `plan.md` line 129's "each leaves the tree compiling and green" claim was false.
- **Direction change:** Merged 1.3 and 1.4 into a single task covering the signature change, the insert-to-remove swap, and the full test reconciliation, and collapsed § Parallelization to Group C = 1.3, Group D = 1.4 (mission) / 1.5 / 1.6 / 1.7. Rewrote the closing paragraph to state why 1.3 is indivisible — the test target breaks the moment the parameter goes — instead of claiming a per-task green that the split could not deliver. Chose the merge over the reviewer's alternative (retarget the tests first, then change the arity), because retargeting tests to assert absence before the production change would leave them failing for a whole task, which the TDD gate reads as a red step with no failing-test-first justification. Branch (a) above also shrinks the merged task: keeping the constant means the nine test references to it still compile.
- **Promotes to ADR:** no

### [plan-review] Two clauses asserted internal call sites no test can decide

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` ADVISORY (round 1, § Requirement Quality). The `dispatch`-before-runtime capture ordering and the "MUST NOT read `UdfContext::node_count()` while serving that request" clause are decidable only by reading the source; neither mapped test observes a capture site, an ordering, or a call count.
- **Direction change:** Demoted the capture-ordering clause out of the `vs-adapter/pushdown-planning` scenario's THEN — the § Background bullet in the same delta already states it as a design constraint, which is where a non-observable constraint belongs. Deleted the "MUST NOT read `node_count()`" clause from the `create-virtual-schema-adapter-notes` scenario rather than adding a call-counting `StubCtx` variant: the observable requirement (no `CLUSTER_NODES` in the notes) is already covered, and the create path's freedom from the read is enforced structurally by `resolve_nr_of_cores` losing its `ctx` parameter.
- **Promotes to ADR:** no

### [plan-review] Out-of-scenario delta edits had no defined merge action

- **Finding:** `[COMPLETENESS_GAP]` ADVISORY (round 1, § Requirement Quality). `/speq:spec-merge`'s marker table defines actions for scenarios only, so the revised Background bullets and Feature descriptions had no defined merge action — and `parallelism/work-unit-sharding`'s recorded Background would have kept naming `adapterNotes` as the node-count source directly above the merged scenario saying the opposite.
- **Direction change:** Added a § Record Notes section to `plan.md` listing every out-of-scenario edit by file and anchor (seven rows), with an explicit instruction that `recorder-agent` applies the checklist rather than inferring the edits from markers. Replaced § Features' closing paragraph with a pointer to it.
- **Promotes to ADR:** no

### [plan-review] The build_adapter_notes call-site count was wrong

- **Finding:** `[EFFORT_MISESTIMATION]` ADVISORY (round 1, § Task Breakdown). Task 1.3 said 18 call sites; `grep -c 'build_adapter_notes('` returns 20, one of which is the definition, leaving 19.
- **Direction change:** Task 1.3 now reads "all 19 `build_adapter_notes` call sites (1 production, 18 test)". Verified independently: 20 matches, definition at `adapter/mod.rs:662`, production call at `:265`.
- **Promotes to ADR:** no

### [plan-review] Scenario Coverage listed an unedited scenario and miscounted the rest

- **Finding:** `[TRACEABILITY_GAP]` ADVISORY (round 1, § Task Breakdown). The "Adapter records the DataFusion threading mode" row named a scenario no delta in this plan touches, and the accompanying sentence said "the five unchanged resource-note scenarios" where decision [7] counts six.
- **Direction change:** Deleted that row and rewrote the sentence as "the six scenarios whose deltas edit only a sibling-key enumeration", adding one sentence marking the two newly added features' rows as the exception, since their preserve-other-notes clause genuinely changes meaning.
- **Promotes to ADR:** no

### [plan-review] An unrelated Background clause was reworded with no marker

- **Finding:** `[SCOPE_CREEP]` ADVISORY (round 1, § Design Depth). The `parallelism/work-unit-sharding` delta silently changed the recorded "it Exasol hash-partitions groups (no longer balanced)" to "hash-partitions them", outside every `DELTA:*` marker and with no traceable need.
- **Direction change:** Restored "hash-partitions groups" verbatim, so that delta's Background touches only the node-count bullet inside its marker.
- **Promotes to ADR:** no

### [plan-review] Summary carried a third sentence of tracking metadata

- **Finding:** `[PROSE_BLOAT]` ADVISORY (round 1, § Prose Quality). Three sentences against `/speq:writing-guardrails`' two-sentence § Summary cap, the third carrying only the issue link, which § Interview already cites.
- **Direction change:** Deleted the third sentence and appended the issue link to the second, which the branch (a) rewrite also reworded to name the active removal.
- **Promotes to ADR:** no

### [plan-review] Rollback after a REFRESH silently degrades the shard fan-out

- **Finding:** `[NFR_IGNORED]` ADVISORY (round 2, § Feasibility). Branch (a)'s active note removal is one-way: after a `REFRESH` on the new adapter, `CLUSTER_NODES` is gone. If an operator then rolls the `.so` back to a pre-refactor build, that build reads the now-absent note and falls to `unwrap_or(1)`, so `G` silently collapses to `1 × PARALLELISM_FACTOR` until another `REFRESH` rewrites the note. No artifact stated this consequence, and § Impact item 3's "no breaking change" heading overstated the change against its own body.
- **Direction change:** Added a § Migration row documenting the rollback-after-`REFRESH` degradation: results stay correct, fan-out shrinks silently, resolved only by another `REFRESH`. Removed "no breaking change" from § Impact item 3's heading, which now reads "No migration."
- **Promotes to ADR:** no

### [plan-review] Decision log named no superseded ADR for the reversed CLUSTER_NODES rule

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER (round 2, § Requirement Quality). No decision entry named the ADR this plan overrides. The live ADR `source-cluster-node-count-from-udfcontext-node-count-not-a-connect-back-select-nproc-supersedes-adr-006` (`specs/_decision/001-migrate-legacy-decision-log.md:1304-1331`, Status Accepted) mandates recording the node count as `CLUSTER_NODES`; decisions [2] and [6] directly reverse that with no pointer between them, so `/speq:record` would publish two Accepted ADRs in direct contradiction with no way to repair the older one later.
- **Direction change:** Added a closing Rationale sentence to decision [2] naming the superseded ADR by slug, stating that this rule reverses recording the count as `CLUSTER_NODES` while retaining the `UdfContext::node_count()` source and the `0 => 1` floor that ADR established. Decision [6] deliberately carries no supersede reference, so `recorder-agent` sets `**Supersedes:**` on exactly one new ADR.
- **Promotes to ADR:** no

### [plan-review] Task 1.3's retarget instruction still targeted the wrong test

- **Finding:** `[TASK_GRANULARITY]` BLOCKER (round 2, § Task Breakdown). Task 1.3's retarget clause still named `build_adapter_notes_merges_existing` for a key-rename to `NOTE_PARALLELISM_FACTOR`, but that test has no `adapter_note` call to retarget — its probe is a `CLUSTER_NODES` survival assertion (`adapter/mod.rs:1502-1506`) that round 1's fix should have inverted instead. Left as written, retargeting the key while keeping the `"3"` expectation fails immediately, and the test that hid round 1's original defect keeps asserting the wrong thing.
- **Direction change:** Removed `build_adapter_notes_merges_existing` from the retarget clause, leaving only `adapter_note_absent_or_unparseable_yields_none` retargeted to `NOTE_PARALLELISM_FACTOR`. Added a separate instruction: keep the fixture's inherited `CLUSTER_NODES` entry, replace the `"3"` survival assertion with an assertion that the key is absent, and update the doc comment to state merge-not-clobber for every key except `CLUSTER_NODES`. Added `build_adapter_notes_merges_existing` to § Scenario Coverage under "createVirtualSchema adapterNotes omit the cluster node count".
- **Promotes to ADR:** no

### [plan-review] No test could fail on a re-introduced adapterNotes read

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` ADVISORY (round 2, § Requirement Quality). The "MUST NOT read the node count from `adapterNotes`" clauses had no discriminating test: `cluster_nodes_passes_through_reported_node_count` never sees a request, the E2E test's schema simply lacks a `CLUSTER_NODES` note at all, and `adapter_notes_carry_parallelism_factor` never enters the pushdown path. A note read could be re-introduced and every mapped test would still pass.
- **Direction change:** Added task 1.3 unit test `pushdown_ignores_persisted_cluster_nodes_note` — a `pushdown` request whose `adapterNotes` carry `CLUSTER_NODES = "9"`, called with `cluster_nodes = 1`, asserting the derived shard count is `1 × PARALLELISM_FACTOR`. Mapped it in § Scenario Coverage against both "Pushdown reads the cluster node count from the UDF handshake" and "Recorded parallelism factor drives later work-unit sharding".
- **Promotes to ADR:** no

### [plan-review] Fallback scenario compared against a reference implementation this plan deletes

- **Finding:** `[AMBIGUOUS_REQUIREMENT]` ADVISORY (round 2, § Requirement Quality). The fallback scenario's AND clause required the new shard count to match "the shard count the pre-refactor path produced from an absent or unparseable `CLUSTER_NODES` adapterNote" — once `adapter/mod.rs:373-376` is deleted, no reader or test can evaluate that comparison. It is migration rationale stated as a requirement, not a requirement.
- **Direction change:** Replaced the clause in `vs-adapter/pushdown-planning/spec.md` with the absolute statement it stands for: `G` SHALL be `min(1 × PARALLELISM_FACTOR, 300, file_count)` per `parallelism/work-unit-sharding`. The pre-refactor-equivalence argument already lives in decision [5]'s Rationale.
- **Promotes to ADR:** no

### [plan-review] Record Notes row 2 misdescribed its own edit

- **Finding:** `[COMPLETENESS_GAP]` ADVISORY (round 2, § Requirement Quality). § Record Notes row 2 named the wrong anchor (recorded bullets 1-6, when the marker spans only 1-5) and claimed the other four carried-through bullets were "unchanged," when two are reworded (the connect-back bullet drops the node count from its subject; the parallelism-factor bullet drops `CLUSTER_NODES` from its sibling list). A recorder honoring "unchanged" over the delta text would re-merge `CLUSTER_NODES` as a recorded sibling key — the exact contradiction round 1 blocked. § Features line 84 also undercounted the edit, saying both features "carry a single scenario edit" when `create-virtual-schema` also revises its Feature description.
- **Direction change:** Rewrote § Record Notes row 2's anchor to "bullets 1-5 (recorded numbering)" and its Edit column to state the marker's six bullets replace all five verbatim, naming which two change subject. Reworded § Features line 84 to note `create-virtual-schema` also revises its Feature description (see § Record Notes).
- **Promotes to ADR:** no

### [plan-review] Task 1.3 undercounted resolve_cluster_nodes's test call sites

- **Finding:** `[EFFORT_MISESTIMATION]` ADVISORY (round 2, § Task Breakdown). Task 1.3 named only one of three `resolve_cluster_nodes` test call sites (`nr_of_cores_from_available_parallelism_when_unavailable`); the other two (`nr_of_cores_property_overrides_auto_detect`, `nr_of_cores_property_falls_back_to_auto_detect`) also assert on the discarded node-count half of the old tuple and need those assertions deleted, not just a signature update. Task 1.2 also left the comment block above the deleted note-read (`adapter/mod.rs:370-372`) naming `CLUSTER_NODES` as note-carried, which the change makes false.
- **Direction change:** Task 1.3 now lists all three call sites by name and line, including the `assert_eq!(nodes, ...)` deletions each needs. Task 1.2 now also rewrites the `adapter/mod.rs:370-372` comment block to name only `PARALLELISM_FACTOR` and the other note-carried values.
- **Promotes to ADR:** no

### [plan-review] Task 1.1 and task 1.3 both claimed the same two tests under different names

- **Finding:** `[TRACEABILITY_GAP]` ADVISORY (round 2, § Task Breakdown). Task 1.1's "write its two unit tests first" and task 1.3's "retarget `cluster_nodes_defaults_to_one_when_node_count_zero` and `cluster_nodes_passes_through_reported_node_count` onto `cluster_nodes_from_context`" pointed at the same two behaviors. The existing tests already pass against `resolve_cluster_nodes`, so they cannot serve as task 1.1's TDD red step, forcing either duplicate coverage under new names or a naming collision.
- **Direction change:** Task 1.1 now names its two new tests explicitly (`cluster_nodes_from_context_defaults_to_one_when_node_count_zero`, `cluster_nodes_from_context_passes_through_reported_node_count`) and § Scenario Coverage rows 1 and 3 point to those names. Task 1.3 now deletes the two old tests, superseded by task 1.1's tests, instead of retargeting them, and both are added to § Dead Code Removal.
- **Promotes to ADR:** no

### [plan-review] Refresh scenario's changed clause had no test mapped, and the decision log claimed one that doesn't assert it

- **Finding:** `[TRACEABILITY_GAP]` ADVISORY (round 2, § Task Breakdown). § Scenario Coverage's last row mapped "Refresh rebuilds the table map and preserves other adapter notes" only to `refresh_rebuilds_table_map_preserves_notes`, which contains no `CLUSTER_NODES` assertion at all. The clause the delta actually changed — the inherited-key removal — is proven only by `refresh_notes_drop_inherited_cluster_nodes`, which was mapped to a different scenario. Compounding it, this decision log's own "[plan-review] Two recorded specs still mandate preserving CLUSTER_NODES" entry listed `refresh_rebuilds_table_map_preserves_notes` among the tests task 1.3 inverts, which is false.
- **Direction change:** Removed `refresh_rebuilds_table_map_preserves_notes` from that earlier Review Finding's list of inverted tests. Added `refresh_notes_drop_inherited_cluster_nodes` to § Scenario Coverage's "Refresh rebuilds the table map and preserves other adapter notes" row, alongside the existing test.
- **Promotes to ADR:** no

### [plan-review] Homing both new scenarios in pushdown-planning crosses the spec-merge split threshold

- **Finding:** `[HIDDEN_DEPENDENCY]` ADVISORY (round 2, § Task Breakdown). Adding both new scenarios to `vs-adapter/pushdown-planning` takes it from 9 to 11 recorded scenarios, crossing `/speq:spec-merge`'s "> 10 scenarios per spec" organization-review signal, which stops `speq record` before archiving to demand an unplanned split decision. Decision [8] weighed only where the seam belongs and never mentioned the count.
- **Direction change:** Added a closing sentence to decision [8] recording the threshold crossing and the precedent for keeping the feature intact: `datafusion-scan/scan-execution` and `datafusion-scan/scan-execution-positional-deletes` already sit at 11 scenarios each, and the handshake-read seam belongs with `ctx.script_schema()`.
- **Promotes to ADR:** no

### [plan-review] The NOTE_CLUSTER_NODES tombstone had no sunset condition

- **Finding:** `[TACTICAL_SHORTCUT]` ADVISORY (round 2, § Design Depth). Branch (a) leaves `NOTE_CLUSTER_NODES` and its `remove` call in `build_adapter_notes` permanently, with no condition recorded anywhere for when they can be deleted — an unexplained legacy eviction inside a builder whose stated job is recording current values.
- **Direction change:** Extended task 1.3's channel-rationale-comment instruction to state the removal condition in the comment itself: the constant and the `remove` call exist solely to evict a key persisted by pre-refactor adapter versions, and both can be deleted once every deployed virtual schema has refreshed on this version or later. Filed [#287](https://github.com/exasol-labs/lakehouse-engine-rs/issues/287) as the tombstone's tracked owner and named it in a new § Consequences sentence.
- **Promotes to ADR:** no

### [plan-review] Three governed sentences broke the 25-word cap

- **Finding:** `[PROSE_BLOAT]` ADVISORY (round 2, § Prose Quality). plan.md § Features line 84's second sentence ran 38 words and hung a bare "it" four clauses from its antecedent. The `create-virtual-schema-adapter-notes` Feature description closed with a separate 38-word sentence and, in doing so, narrowed its recorded subject from "cluster configuration" to just the core count, silently dropping the parallelism-factor, threading, and memory-budget entries the feature still records.
- **Direction change:** Split plan.md line 84's second sentence into two. Restored the `create-virtual-schema-adapter-notes` Feature description's opening subject to "the per-node core count, the resource budgets, and the Exasol-name to Iceberg-identifier map" and split its closing sentence at the semicolon.
- **Promotes to ADR:** no
