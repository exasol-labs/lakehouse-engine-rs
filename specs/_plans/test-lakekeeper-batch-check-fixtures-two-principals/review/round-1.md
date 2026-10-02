# Plan Review Findings: test-lakekeeper-batch-check-fixtures-two-principals (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

1. PR #441 merges first and moves the e2e stack to SeaweedFS. The new CI job runs `docker wait` on `minio-init`, which no longer exists, and fails before any test runs. `speq record` then writes "(OIDC + MinIO)" back over the recorded harness title. Routed to the Feasibility BLOCKER.
2. #415 ships the README as the operator requirement. Every deployment grants the engine's service account warehouse-wide `manage_grants`, although namespace scope suffices. A later Lakekeeper upgrade enforces the OpenFGA v1.11 floor that upstream already documents, and the pinned stack stops working. The README also calls derivation unsafe on evidence that shows only that no proxy is present. Routed to the Feasibility and Requirement Quality advisories.
3. #416 runs "two Exasol users holding different grants ... against the environment from #414". Its `USER_MAPPING` template reaches only template-shaped ids, and the fixture grants only one such id. The overlay header says that the stack has no Exasol. #416 rebuilds the environment. Meanwhile the permanent spec states "the adapter calls no management API", which #415 makes false. Routed to the Intent Fidelity advisory and the Requirement Quality BLOCKER.

## Intent Fidelity

Apart from the finding below, the plan covers all five #414 scope bullets. Interview answer 1 settles the separate overlay and the `lakekeeper-authz-e2e` feature. For "the target deployment", § Context records that the AWS deployment runs `allowall`. Verified: `deploy/lakekeeper-stack/lakekeeper-userdata.sh.tftpl` sets no `AUTHZ_BACKEND`, and `main.tf:17` pins the same `v0.13.1` image.

#### [SCOPE_REDUCTION] ADVISORY
- Location: plan.md § Design (Goals and the findings table), task 1.1 (header comment), task 1.6; lakekeeper-authz-contract/spec.md § Background bullet 4
- Issue: #414 asks for "the reusable fixtures and environment the next two increments build on". #416 needs "two Exasol users holding different grants, through the full path, against the environment from #414". The #415 `USER_MAPPING` template (`'oidc~$lower($1)@corp'`) reaches only template-shaped ids. The plan's own finding states that no template derives the `lakehouse-reader-a` and `lakehouse-reader-b` ids (`oidc~<uuid>`). The fixture grants exactly one template-shaped id (`oidc~template-user@corp`, `select` on `authz_alpha`). Task 1.1's header comment states "the absence of Exasol". #416 must therefore add a second template id with a different grant, and must also add an Exasol layering.
- Fix: Add the never-logged-in id `oidc~template-user-b@corp`, holding `select` on `authz_beta` only, to Background bullet 4 and task 1.6. In task 1.1, replace "the absence of Exasol" with a header line that shows how to add `exasol` to the `up` list for #416.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Context (probe paragraph and the forces list), § Architecture diagram, tasks 1.1 and 3.6, § Manual Testing rows 1 and 6; decision-log.md [4] Rationale; lakekeeper-authz-contract/spec.md § Background bullet 4; lakekeeper-e2e-harness/spec.md title and description
- Issue: The plan targets the MinIO stack on `origin/main` (services `minio` and `minio-init`). The plan sits untracked on `feat/add-aws-assume-role-credentials`, which is open PR #441 (state: mergeable, review required). That branch replaces MinIO with `seaweedfs` (commit `a173738`). Its `docker-compose.yml` has no `minio` or `minio-init` service and creates the bucket through `-bucket=warehouse`. The recorded harness spec on this branch is titled "Lakekeeper E2E Harness (OIDC + SeaweedFS)". The delta's title says "(OIDC + MinIO)", and its description says "backed by MinIO". On this branch, or after #441 merges, task 3.6's `docker wait` on `minio-init` fails before any test runs. The `up --wait minio ...` set also fails. The delta's description contradicts the recorded spec. No artifact names #441 or the base that the plan builds on.
- Fix: Add PR #441 to plan.md § Dependencies as a merge-order prerequisite. Rewrite every MinIO reference listed in Location for the SeaweedFS stack: service `seaweedfs`, no storage one-shot in task 1.1's order or in task 3.6's `docker wait` list, and `seaweedfs` in the Manual Testing commands. Replace "static MinIO credentials" with "static S3 credentials". Set the harness delta's title and description to the recorded "(OIDC + SeaweedFS)" text.
- Escalation: MECHANICAL (the branch that holds the plan and the recorded spec on that branch decide which stack the plan must name)

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Context (probe paragraph), § Dependencies, task 1.1
- Issue: The plan pins `openfga/openfga:v1.8.16` because upstream's `examples/access-control-simple` uses `v1.8`. At tag `v0.13.1`, Lakekeeper's `docs/docs/authorization-openfga.md` states: "OpenFGA v1.11 or later is required ... Earlier versions will fail with `cannot write a tuple which already exists` during a re-bootstrap or reconcile run. We test against v1.14." At the same tag, `examples/access-control-advanced` runs the server on `openfga/openfga:v1.12`. The plan's flows avoid the idempotent-write paths, so the probe passes. Bootstrap runs once, and the plan writes no role assignments (`authorizer.rs:175-187`, `:948-983`). The contract that #415 inherits is still captured on a version that upstream does not support. The plan does not state this fact.
- Fix: In task 1.1 and § Dependencies, pin `openfga/openfga` to a `v1.14` patch, and re-check the probe facts on that version. If the plan keeps `v1.8.16`, quote the upstream minimum-version note in § Context. Also list it in the README's pinned-versions section as a known deviation.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md tasks 1.1, 1.2, 3.6
- Issue: Three wiring steps are missing.
  1. `common/lakekeeper.rs` imports `super::stack::{self, CatalogConnectionPassword, wait_for_url}`. `common/stack.rs` has its own `#![cfg(any(...))]`, and that gate does not list the new feature. Task 1.2 does not add the feature to that gate, so a build with `--features lakekeeper-authz-e2e` alone does not compile.
  2. Every service in `docker-compose.lakekeeper.yml` joins the `lakehouse` network, and `lakekeeper` joins no other network. Task 1.1 sets no network for `openfga-db`, `openfga-migrate`, or `openfga`. Those services then join `default`, and `lakekeeper` cannot resolve `openfga`.
  3. Task 3.6 runs `pull` with the three compose files and no service list. That pull downloads the Exasol and Spark images, which the job never starts. The `e2e-azure` job passes an explicit service list.
- Fix: In task 1.2, add `lakekeeper-authz-e2e` to the inner `cfg` of `tests/common/stack.rs`. In task 1.1, put the three new services on the `lakehouse` network. In task 3.6, pass the list of started services to `pull`.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md task 2.2 and the § Scenario Coverage row "The shared realm file carries no authorization-suite principal"; decision-log.md [2]
- Issue: Decision [2] is the plan's only ADR, and its guard protects the AWS deployment. Task 2.2 puts that guard in `e2e_lakekeeper_authz_test.rs`, behind the `lakekeeper-authz-e2e` feature. Only the new `e2e-lakekeeper-authz` job runs it, and task 3.6 marks that job "Not yet a required check." A PR that adds a test client to `scripts/keycloak-realm-iceberg.json` therefore passes every required check. The guard needs no running stack.
- Fix: Move `authz_principals_stay_out_of_the_shared_realm_file` to a test target that has no feature gate, for example `crates/lakehouse-engine/tests/realm_file_guard.rs`. The required `unit-tests` job (`cargo llvm-cov --workspace`) then runs it. Update the Scenario Coverage row.

## Requirement Quality

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: lakekeeper-authz-contract/spec.md § Feature description and § Background bullets 1, 3, and 4
- Issue: No GIVEN, WHEN, or THEN step in this delta depends on the following clauses.
  - Description: "It changes no adapter behaviour: the adapter calls no management API." After #415, the adapter calls `batch-check`, so the permanent spec would state a false fact.
  - Bullet 1: "with its own PostgreSQL store" and "Exasol is not part of this stack." No scenario cites the version pins `v1.8.16` or `v0.13.1` either.
  - Bullet 3: "at provisioning time through Keycloak's Admin REST API".
  - Bullet 4: "(static MinIO credentials)" and "metadata-only".
- Fix: Delete the description sentence "It changes no adapter behaviour: the adapter calls no management API." Delete "with its own PostgreSQL store", "Exasol is not part of this stack.", "at provisioning time through Keycloak's Admin REST API", "(static MinIO credentials)", and "metadata-only". If the version pins define the contract, move the Lakekeeper and OpenFGA pins into the GIVEN of "Committed batch-check fixtures match the live contract". Otherwise delete them. plan.md, decision [2], and the fixture README already carry these facts.
- Escalation: MECHANICAL (each clause was checked against this delta's own scenarios)

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Context (bullet on the guard), findings table row 2; decision-log.md [8] "Privilege"; task 3.4
- Issue: § Context says that, on a table, `can_read_assignments` "derives from `manage_grants`". The `v4.7` model at tag `v0.13.1` (`lakekeeper_table.fga`) defines it as any `can_grant_*` relation or `can_change_ownership`. It also defines `can_grant_select` as `manage_grants or (select and pass_grants)`. Two narrower grants therefore pass the guard too. Namespace `manage_grants` reaches the namespace's tables through `manage_grants from parent`. Table `pass_grants` plus an inherited `select` or `describe` passes for that one table, because table `pass_grants` does not inherit. Row 2 of the findings table drops the "across a warehouse" qualifier from decision [8]. It reads "The least standing privilege is `manage_grants` on the warehouse." #416 documents "the privilege boundary" from this finding.
- Fix: In § Context, quote the model's definitions of `can_read_assignments` and `can_grant_select`. Restate the finding as follows. Across a warehouse, the grant is warehouse `manage_grants`. Narrower options are namespace `manage_grants`, or table `pass_grants` with `select` on each table. In the task 3.4 README, add both narrower paths and label them as derived from the model, not asserted by the suite.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: lakekeeper-authz-contract/spec.md § Scenario "The catalog never advertises the management base, so a client cannot derive it safely"; plan.md findings table row 3; decision-log.md [8] "Base path"
- Issue: Upstream builds both bases from one `base_url`. `base_uri_catalog()` returns `{base_url}/catalog`, and `base_uri_management()` returns `{base_url}/management` (`crates/lakekeeper/src/request_metadata.rs:542-549`, tag `v0.13.1`). `determine_base_uri` adds `x-forwarded-prefix` to `base_url` (lines 702-770), so a forwarded prefix moves both bases together. Behind a proxy that strips `/lk`, a client that swaps the trailing `/catalog` for `/management` reaches the management API. The THEN step "`GET /lk/management/v1/info` SHALL answer 404" proves only that the test sends its request with no proxy present. The conclusion in the title, "so a client cannot derive it safely", does not follow from the scenario's steps. The case that is actually unsafe is a catalog URI that does not end in `/catalog`, for example behind a gateway that rewrites the path. The plan already states that case as its finding.
- Fix: Rename the scenario to "The catalog never advertises the management base". Delete the `/lk/management/v1/info` THEN step, or change its reason to "Lakekeeper serves no route under a forwarded prefix". In the README and in decision [8], cite `base_uri_management()` as Lakekeeper's own sibling convention. Name a path-rewriting gateway as the case that derivation cannot handle.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: lakekeeper-authz-contract/spec.md § Background bullets 5 and 6; § Scenario "The management API is mounted beside the catalog path, not under it"
- Issue:
  1. The shape rule defines objects only: "every JSON object has the same keys with the same JSON value types at every depth". It says nothing about arrays, such as the length of `results` or an error `stack` array. Two implementers can disagree on whether a two-element `results` matches a one-element fixture.
  2. Bullet 5's body `{"checks": [{"id", "identity": ...}], ...}` is not valid JSON. A reader cannot tell schema notation from a literal value.
  3. "relative to the catalog URI itself" has two readings. Under RFC 3986, resolving `management/v1/info` against `http://localhost:<port>/catalog` gives `http://localhost:<port>/management/v1/info`, which is the parent-relative URL.
- Fix: Add to bullet 6: "Arrays match when they have equal length and each element matches by position." Rewrite bullet 5 as a literal JSON example with placeholder values. In the scenario's WHEN step, replace both relative phrases with the literal URLs `http://localhost:<port>/management/v1/info` and `http://localhost:<port>/catalog/management/v1/info`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: lakekeeper-e2e-harness/spec.md § Scenario "The allowall suite runs on the allowall authorizer"; plan.md task 4.1, § Manual Testing rows 6 and 7
- Issue: The scenario's AND step states that the suite "SHALL fail, not skip, when it reports any other backend". Task 4.1's test calls `setup()`, and `setup()` calls `wait_for_exasol()` first. The OpenFGA stack starts no Exasol, so Manual Testing row 7 fails at the Exasol wait. The run never shows the expected "message naming the reported backend `openfga`". No automated test runs the allowall guard against an answer other than `allow-all`. Row 6 also lists the one-shot `lakekeeper-migrate` in an `up --wait` set, although the row's own note says "one-shots first, as in CI".
- Fix: In task 4.1, replace the `setup()` call with `wait_for_keycloak()`, `wait_for_lakekeeper()`, and `lakekeeper_bootstrap()`, followed by `assert_authz_backend`. The test then fails on the backend name whether or not Exasol runs. In row 6, start `lakekeeper-migrate` and `docker wait` it before the `up -d --wait` set, as the CI job does.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md task 3.2; lakekeeper-authz-contract/spec.md § Scenario "Committed batch-check fixtures match the live contract" (last AND step)
- Issue: Task 3.2 scans each fixture for "the live warehouse, table, and principal ids" of the current run. Keycloak and Lakekeeper mint new ids for each stack. A compare-mode run on a fresh stack therefore never sees the ids of the stack that captured the fixture. Task 3.2 also does not say whether the scan runs in capture mode. If normalization misses an id, the fixture leaks a capture-time UUID, and every later run still passes.
- Fix: In task 3.2, make the scan reject every 8-4-4-4-12 hex UUID that is not a declared placeholder. Run the scan in both capture mode and compare mode.

## Task Breakdown

No objection. Axis checked: each of the 11 scenarios in the two deltas maps to one named test in § Scenario Coverage. Task 3.1's codec tests correctly claim no scenario. All tasks stay in one group, because task 4.1 calls the `common/lakekeeper.rs` helpers that task 1.3 adds, and the shared file rules out a second group.

## Design Depth

No objection. Axis checked: `common/lakekeeper_authz.rs` owns one decision, which is how the contract fixture is provisioned and queried. `common/lakekeeper.rs` keeps the shared Keycloak and Lakekeeper basics. Decision [5] states the cross-crate fixture path as a trade-off. Decision [2] is the plan's only `Promotes to ADR: yes` entry, and it passes the gate. It sets an ownership boundary: the shared realm file belongs to the AWS deployment. A violation has a security consequence that a future plan could reintroduce. Decisions [1] and [3] to [8] are `no`, which is correct for test-harness choices.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md tasks 1.1, 1.5, 3.2, 3.6
- Issue: Several procedural sentences run to several times the 20-word cap. Task 1.1's second sentence nests three parenthetical service specifications in about 50 words. Task 3.6's sentence "It pulls ..., runs `docker wait` on ..., then brings up ..." chains three instructions. The artifacts contain no em dashes in prose, no semicolons, and no contractions.
- Fix: Split each listed task into sentences that each give one instruction. In task 1.1, move the details for each service into a short list with one item per service.
