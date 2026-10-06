# Decision Log: reorg-e2e-specs-into-testing

## Interview

**Q:** What happens to the E2E scenarios that restate functional behavior?
**A:** Merge; remove duplications. Merge them into the matching existing feature specs (vs-adapter, datafusion-scan, sql-comprehension, and the others), and delete them when they duplicate an existing requirement.

**Q:** How should the plan record which test covers which requirement?
**A:** No per-requirement test list. Assume that each requirement has a test. Capture only the rules and specs for how the E2E tests are structured and organized.

**Q:** Where do the ops specs (`azure-orphan-container-sweep`, `glue-orphan-sweep`, `aws-lakekeeper-perf-catalog`) go?
**A:** Into the Ops section of `specs/testing.md`. Deduplicate the two sweeps into one contract.

**Q:** Should the plan run the adversarial plan-reviewer?
**A:** No. The orchestrator does only a sanity review of the reorganization.

**Q:** speq does not support a top-level `specs/testing.md`: `speq plan validate` ignores the file, `speq record` archives it without writing it, and the recorder's merge procedure has a manual step only for `architecture.md`. Where should the Testing and Ops content live?
**A:** Option A. Author the complete `testing.md` (Testing and Ops sections, plain rules prose, no delta markers, no requirement-to-test table) in the plan directory, and add a plan task that copies it to `specs/testing.md` after `speq record`.

**Q:** May the plan edit comments and docs outside `specs/` that point at the old spec paths or quote old scenario titles?
**A:** Yes. Comments and docs only: the four path references and the `/// Scenario:` test doc lines, as one plan task, with no behavior change.

**Q:** `speq record` leaves a feature whose scenarios are all removed as an empty `spec.md` that fails validation, and it does not delete the directory. How are the 21 features removed?
**A:** Follow the repository precedent (plan 003, `parallelism/iproc-sharding`): full `DELTA:REMOVED` deltas with scenario bodies, plus a task that deletes the 21 emptied directories after record.

**Q:** The first sanity review flagged an `AGENTS.md` pointer line beyond the listed outside-`specs/` edits, pre-existing library thresholds, and borderline duplicate calls. How should the plan proceed?
**A:** Revise this plan into one plan that cleans up the whole spec library. (1) Keep the pointer lines in `AGENTS.md` and `specs/mission.md`. (2) Split `vs-adapter` by feature area into domains of 8 features or fewer where it makes sense. (3) Move the packaging fixture specs, and any other spec that only describes E2E test fixtures, into the Fixtures section of `testing.md`, keeping product behavior in feature specs. (4) Also split `datafusion-scan` and `vs-adapter/pushdown-planning-cloud-credentials` to meet the 8-feature and 10-scenario limits where it makes sense. (5) For the borderline rows #136, #100, #90, and #92, keep the scenario in a feature spec rather than marking it a duplicate. Also review the feature specs that mention E2E or end-to-end and move any real E2E requirement to `testing.md`. Rename or move features through REMOVED and full new-spec deltas.

**Q:** The second revision kept 39 non-E2E test-rule passages (unit tests, golden SQL fixtures, crate-surface probes, regression rules, the arm64 unit-test CI job) in their feature specs, split `sql-comprehension` and five more oversized features beyond the named ones, and kept domain names that match source directories. How should the plan proceed?
**A:** Keep the extra splits. Add the 39 passages to this plan: move them to `testing.md`, and where a scenario is removed or rewritten, keep the product behavior in the feature spec. Keep the 8-feature and 10-scenario limits. Keep the domain names.

## Design Decisions

### [1] Harness and ops rules live in one non-feature file, `specs/testing.md`

- **Decision:** The plan authors `specs/_plans/reorg-e2e-specs-into-testing/testing.md` as a complete new file with two sections, Testing and Ops. A task copies it to `specs/testing.md` after `speq record`. The file states the coverage rule (every feature scenario has an integration or E2E test) and how the suites are built, run, and wired into Make and CI. It holds no requirement-to-test table and no feature behavior. `AGENTS.md` § Testing and `specs/mission.md` Core Capability 13 gain a one-line pointer to it, the same way `AGENTS.md` points to `specs/udf-context.md`.
- **Alternatives:** A `testing` domain of Gherkin feature specs (rejected by the user: harness rules must stop being feature specs, and the 10-scenario cap would split them into four to six features). Adding `testing.md` support to the speq CLI and recorder first (rejected: tool change outside this repository, blocks the plan).
- **Rationale:** The user chose this option after the CLI limitation was shown. `udf-context.md` sets the precedent for a hand-maintained top-level reference file. The rule that binds contributors ("read `specs/testing.md`") goes into `AGENTS.md`, which is where `/speq:adr-rules` rule 4 sends conventions.
- **Consequences:** speq does not validate or search-index `specs/testing.md`. Later edits to it are direct edits, like `mission.md`. `speq search` no longer returns harness scenarios, which removes the duplicate hits the orchestrator's search showed.
- **Promotes to ADR:** no

### [2] Whole-feature removal uses full REMOVED deltas plus a post-record directory deletion

- **Decision:** Each of the 21 E2E features gets a delta that keeps its description and Background unmarked and wraps every scenario, with its full body, in `DELTA:REMOVED`. After `speq record`, a task deletes the 21 feature directories and the six emptied domain directories.
- **Alternatives:** Heading-only REMOVED blocks (rejected: `speq plan validate` reports missing description, Background, and steps). Deleting the directories in the implementation phase without deltas (rejected: the removal would then be invisible to the plan's validated artifacts, and `speq record` of any later delta would fail on the missing target).
- **Rationale:** Repository precedent `specs/_recorded/003-add-group-by-and-sql-comprehension/parallelism/iproc-sharding`. Verified on a scratch copy with speq 0.25.0: full-body REMOVED deltas validate, and `speq record` then leaves an empty `spec.md` that `speq feature validate` rejects with `No scenarios defined`.
- **Consequences:** `speq record` exits non-zero on its post-record validation until the directories are deleted. The task order in `plan.md` places the deletion directly after record and before the final validation. A dry run of this plan on a scratch copy on 2026-10-06 confirmed the sequence: `speq record` exited 1 and archived the plan to `specs/_recorded/047-reorg-e2e-specs-into-testing/`, and after the six domain directories were deleted and `testing.md` was copied, `speq feature validate` exited 0.
- **Promotes to ADR:** no

### [3] Duplicate test: same observable product behavior at any tier

- **Decision:** An E2E scenario is a duplicate when an existing feature scenario states the same observable product behavior, at any test tier. Clauses about how a test runs are not product behavior and move to `testing.md` once, in general form: fail-not-skip, skip-when-absent for the opt-in suite, the comparison oracle (seeded data, unpushed or single-node result, key-first ordering), `EXPLAIN VIRTUAL` or captured SQL used as evidence that the scan UDF ran, fixture names and layouts, Make targets, and test-output redaction.
- **Alternatives:** Keep every E2E scenario as a feature scenario with an "end to end" title (rejected: restates the same requirement twice, which the user asked to remove).
- **Rationale:** The interview answer assumes every requirement has a test, so the tier of the test is not part of the requirement. Writing a test-run clause once in `testing.md` removes the five copies of "fails, never skips" and the six copies of "no credential in output".
- **Promotes to ADR:** no

### [4] The two orphan sweeps share one contract; per-cloud differences stay specific

- **Decision:** `testing.md` § Ops states one sweep contract (schedule, suite-owned selection, 24-hour retention floor by the cloud's own timestamp, scheduled runs delete, manual runs preview by default, empty run succeeds, fail loudly, no credential in the log) followed by Azure specifics and Glue specifics. A rule only one spec stated stays under that cloud's specifics: already-absent counts as deleted (Azure), and the per-delete print with a final count (Glue).
- **Alternatives:** Make every rule common (rejected: it would add obligations to a workflow that no spec required and no test checks).
- **Rationale:** The interview asked for one contract. Keeping the intersection common and the rest specific loses nothing and invents nothing.
- **Promotes to ADR:** no

### [5] The remote benchmark scenarios go to Ops, not Testing

- **Decision:** The four `bench/run.sh` scenarios of `e2e-harness/cloud-e2e-harness` (remote `PARALLELISM_FACTOR`, catalog selection, Lakekeeper CONNECTION password, run leaves the virtual schema in place) move to `testing.md` § Ops § Remote benchmark harness, next to the AWS Lakekeeper benchmark catalog that shares their environment and demo runbook.
- **Alternatives:** Keep them under Testing with the cloud E2E suite (rejected: `bench/run.sh` is operator tooling for benchmark and demo runs, not a test suite, and its demo runbook depends on `lakekeeper-up.sh`, which Ops already holds).
- **Rationale:** The two topics share one environment (`bench/.env`, `secrets.sh`, `BENCH_CATALOG`) and one runbook, so one section owns both.
- **Promotes to ADR:** no

### [6] Comment and doc references move with the specs

- **Decision:** One task edits comments and docs only. Four path references (`.github/workflows/azure-orphan-sweep.yml:3`, `.github/workflows/glue-orphan-sweep.yml:3`, `crates/lakehouse-engine/tests/common/glue.rs:2` and `:844`, `deploy/README.md:385`) point to `specs/testing.md`. Each of the 23 `/// Scenario:` test doc lines that quote a removed E2E title is rewritten, including the eight that quote it with a lowercase first letter, and the line that quotes the rewritten type-relaxation title follows its new title (decision [13]): a duplicate quotes the covering feature scenario's title, a moved clause quotes its new title, and a harness rule becomes a one-line `/// specs/testing.md § <section>` pointer.
- **Alternatives:** Leave the lines stale (rejected by the user in the interview).
- **Rationale:** `AGENTS.md` requires each test to quote its scenario title verbatim, so a stale title breaks traceability. No code behavior changes.
- **Promotes to ADR:** no

### [7] A clause that no test exercises, or that is stale, is not migrated

- **Decision:** Two E2E clauses are dropped instead of moved. First, `e2e-harness/e2e-harness` § "A least-privilege user queries the VS and recovers no credential from the plan" claims that granting the script-scoped CONNECTION access to the reader instead of the owner also denies the scan. `crates/lakehouse-engine/tests/e2e_credential_exposure_test.rs` revokes the owner's grant and never grants the reader, so no test exercises the claim. Second, `lakekeeper-e2e/lakekeeper-e2e-harness` § "End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows" takes the path-style flag from the vended response. The suite's vended CONNECTION states `path_style: true` (`crates/lakehouse-engine/tests/common/lakekeeper.rs`), and a stated CONNECTION value wins (`storage-access/pushdown-planning-cloud-credentials-vended-storage`), so that clause describes behavior the suite does not exercise.
- **Alternatives:** Move both clauses into feature specs (rejected: the plan changes no code, so a moved untested clause would break the coverage rule in `testing.md` on the day it lands, and the stale clause would contradict the recorded precedence rule).
- **Rationale:** The interview rule is that every requirement has a test. A clause with no test, or one that contradicts a recorded rule, cannot meet that rule without a code change, which this plan excludes.
- **Consequences:** If the reader-grant property matters, a later plan adds a test and the clause together. The mapping in `plan.md` names both dropped clauses.
- **Promotes to ADR:** no

### [8] Uncovered product clauses extend the closest existing scenario

- **Decision:** Of the 137 E2E scenarios, 58 hold only harness or ops rules and move to `testing.md`. The other 79 state product behavior. 67 of those are duplicates, and 12 move a clause into a feature scenario. `plan.md` § Scenario Mapping lists the destination of each scenario. Five of the twelve extend an existing scenario with an AND clause:
  - `catalog/rest-catalog-oauth-auth`: enumeration requests use the warehouse prefix
  - `storage-access/scan-spec-credential-reference`: the grant is checked against the owner
  - `pushdown-aggregates/pushdown-planning-nested-aggregate-fallback`: the composed query returns the single-node result
  - `direct-storage/direct-storage-table-discovery`: a fold failure fails the whole statement
  - `scan-read-path/scan-execution-positional-deletes-fanout`: the post-delete result does not depend on shard placement

  Four become new scenarios: the reader cannot execute the plan (`storage-access/scan-spec-credential-reference`), a storage denial reports the store's response (`file-planning/pushdown-planning-file-resolution`), and the two borderline rows of decision [14]. The remaining three carry live type coverage: `scan-types/type-mapping-live-coverage` gains the coverage of the deleted direct-storage and Unity scenarios as AND clauses on three of its own scenarios, because its Background already relies on those tables.
- **Alternatives:** A new feature per suite that holds its "end to end" scenarios (rejected: it rebuilds the E2E specs under a new name).
- **Rationale:** An AND clause on the scenario that already owns the behavior keeps one owner per requirement. No feature exceeds 10 scenarios afterwards, checked by a dry-run record on a scratch copy (decisions [10] and [11] resolve the limits the library exceeded before this plan).
- **Promotes to ADR:** no

### [9] No architecture delta

- **Decision:** The plan writes no `architecture.md` delta.
- **Alternatives:** none
- **Rationale:** The plan reorganizes the spec library and changes no component, boundary, interface, data flow, constraint, or external dependency. `specs/architecture.md` § Interfaces already lists the E2E Make targets, and they do not change. `specs/architecture.md` names `vs-adapter` and `datafusion-scan` as code components (`crates/lakehouse-engine/src/adapter/` and `src/scan/`), not as spec domains, and cites no spec path, so the domain split of decision [10] leaves it accurate.
- **Promotes to ADR:** no

### [10] Oversized domains split into top-level domains of 8 features or fewer, feature slugs unchanged

- **Decision:** `vs-adapter` (84 features), `datafusion-scan` (27), and `sql-comprehension` (11) split by feature area into 22 domains of at most 8 features each. Each existing domain keeps its 8 or fewer core features under its own name, and every other feature moves to a new domain with its slug unchanged. `plan.md` § Domain Layout lists the domains and § Feature Moves lists every old and new path. The new domains are top-level directories, not nested sub-domains.
- **Alternatives:** Nested sub-domains such as `vs-adapter/pushdown/<feature>` (rejected: on a scratch copy, speq 0.25.0 left a nested feature out of `speq feature list` and read its third path segment as a scenario name in `speq feature get`). Renaming feature slugs to drop prefixes such as `pushdown-planning-` (rejected: it adds a second rename on top of the domain move to every reference, and the slugs stay unique without it). Leaving `sql-comprehension` at 11 features (rejected: the user asked for a plan that cleans up the whole library, and the recorder stops on any domain above 8).
- **Rationale:** A domain is a feature area a reader can name. Keeping slugs makes every move a domain change only, so one table maps every old path to its new path, and every `/// Scenario:` line stays valid because no scenario title changes.
- **Consequences:** The library grows from 11 domains to 24. A reference to a slug family such as `vs-adapter/pushdown-planning*` becomes the bare glob `pushdown-planning*`, because that family now spans several domains.
- **Promotes to ADR:** no

### [11] A feature above 10 scenarios splits into two features; the first half keeps the slug

- **Decision:** The six features above 10 scenarios split into two features each: `connection-credentials-assume-role`, `pushdown-planning-cloud-credentials`, `storage-backend-enum`, `parquet-directory-seam`, `pushdown-planning-char-type-declaration`, and `scan-execution-field-id-projection`. Each scenario moves verbatim into exactly one half. The first half keeps the original slug, so an existing reference keeps resolving. A reference that names a scenario of the second half points to the second half. Each Background bullet goes to the half whose scenarios depend on it, and each half names its sibling.
- **Alternatives:** Split only `pushdown-planning-cloud-credentials`, the one oversized feature the first review reported (rejected: the other five exceed the same limit, and the recorder stops on each).
- **Rationale:** Splitting by sub-topic keeps each half readable and keeps scenario titles, and therefore test traceability, unchanged.
- **Promotes to ADR:** no

### [12] Fixture-only specs become `testing.md` § Fixtures rules

- **Decision:** `packaging/positional-delete-fixtures`, `packaging/int96-timestamp-fixture`, and `packaging/iceberg-type-promotion-fixture` are removed through full REMOVED deltas, and their rules become bullets in `testing.md` § Testing › Fixtures. None of the three states product behavior. The product behavior those fixtures exercise stays in the feature specs that already own it: positional deletes in `scan-read-path/scan-execution-positional-deletes`, INT96 decoding in `datafusion-scan/scan-execution-value-conversion`, and type promotion in `file-planning/iceberg-type-promotion`.
- **Alternatives:** Keep them as feature specs in `packaging` (rejected: they describe how a test fixture is written, which the user asked to move to `testing.md`).
- **Rationale:** Same rule as decision [3]: how a test is built is not product behavior.
- **Promotes to ADR:** no

### [13] Test rules move out of feature specs; product behavior stays

- **Decision:** Every remaining feature spec that mentions E2E or end-to-end was reviewed. Each passage that states only how a test is built, run, organized, or guarded moves to `testing.md`: the E2E requirements, and the unit-test, golden-fixture, crate-probe, regression, and arm64 unit-test CI rules found in the same specs. Where a clause mixes test method with product behavior, the product part stays in the scenario, restated as product behavior. A scenario whose product behavior would otherwise be lost is rewritten, not removed: `scan-types/type-relaxation` § "Every supported relaxation pair is proven castable rather than assumed" becomes § "Every supported relaxation pair casts without losing a value". `catalog/catalog-crate-structure` § "Behavior is unchanged across the extraction" keeps its title and states the unchanged behavior instead of the unchanged tests. Two scenarios that state no product behavior are removed: `packaging/aarch64-ci-build` § "arm64 CI job runs unit tests without coverage or E2E" and `catalog/catalog-crate-structure` § "Every moved module keeps its own tests". Product behavior, pointers, and plain facts stay. `plan.md` § Test-Rule Review lists every passage that moved, and `testing.md` gains the subsections Unit tests and Regression guards and probes for the non-E2E rules.
- **Alternatives:** Move only the E2E requirements (the first revision; superseded by the user, who asked to move the unit-test, golden-fixture, probe, regression, and CI unit-job rules too). Remove the type-relaxation scenario together with its test method (rejected: its castability rule is product behavior that no other scenario states in full).
- **Rationale:** `testing.md` states how tests prove behavior, and feature specs state the behavior. The coverage rule in `testing.md` already requires a test for every scenario, so a feature spec needs no clause that only demands a test.
- **Promotes to ADR:** no

### [14] Borderline E2E rows stay as feature scenarios

- **Decision:** Rows #100 and #136 of § Scenario Mapping, which the first review counted as duplicates, become new feature scenarios. #100 becomes `unity-catalog/unity-catalog-vended-credentials` § "A catalog that vends no storage endpoint or credential is read through the CONNECTION's storage fields". #136 becomes `connection/connection-credentials-assume-role-session-use` § "A catalog denial of a CONNECTION that names no role is a credential-safe error". Rows #90 and #92 already move their clauses into `scan-types/type-mapping-live-coverage` and stay as they are.
- **Alternatives:** Keep #100 and #136 as duplicates of looser covering scenarios (rejected by the user: when unsure, keep the scenario).
- **Rationale:** The covering scenarios matched only loosely: #136's cover speaks of an unreachable catalog, not a denying one, and #100's cover does not state that the scan reads through the CONNECTION's storage fields when the catalog vends none.
- **Promotes to ADR:** no

### [15] Accepted ADR fragments keep their old spec paths

- **Decision:** The 8 spec-path references in 7 ADR fragments under `specs/_decision/` stay unchanged.
- **Alternatives:** Rewrite the paths in place (rejected: `/speq:adr-rules` forbids editing an old fragment, and a path change is no reason to supersede a decision).
- **Rationale:** An ADR records the library as it stood when the decision was accepted. Feature slugs are unchanged, so each old path still names its feature, and `plan.md` § Feature Moves maps it to the new domain.
- **Promotes to ADR:** no
