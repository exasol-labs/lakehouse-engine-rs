# Plan Review Findings: add-lakekeeper-permission-check (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 11 (Blockers: 3, Advisory: 8)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed catastrophically. These are the likely causes:

1. The first implementation commit failed `cargo clippy --all-targets -- -D warnings` on rustc's `private_interfaces` lint. The implementer made `PermissionGate` `pub` to silence it, which widened the recorded pushdown façade without a spec delta. Routed to Feasibility, [HIDDEN_DEPENDENCY] BLOCKER.
2. A later charset edit let two Exasol users share one Lakekeeper principal. No scenario stated injectivity, so no scenario-traced test failed and the spec diff showed no requirement change. Routed to Requirement Quality, [COMPLETENESS_GAP] BLOCKER.
3. An operator gave the CONNECTION identity `manage_grants`, as plan.md § Impact suggests. The CONNECTION's client secret then became a credential that writes Lakekeeper grants, and nobody weighed that. Routed to Feasibility, [NFR_IGNORED] ADVISORY.

## Intent Fidelity

No objection, axis checked. Each #415 scope bullet maps to a plan element. The property, its default, and its rejection map to "Invalid permission properties are rejected before the CONNECTION is read". `ctx.current_user()` in dispatch maps to task 2.3 and the principal scenario. The total, validated mapping without user search maps to decision [5] and the principal scenario. One batch-check with an identity and `error-on-not-found: false` maps to the client request scenario. The shared session and the unchanged grant count map to the `pushdown-catalog-session` delta. The free function on `&CatalogSession` maps to decision [2]. Check-before-load and the `for_request` check list map to decision [1]. Fail-closed maps to the denied and the unsupported-kind scenarios. The four interview answers appear as decisions [4], [3], [1]/[6]/[7], and [5]. Decision [7] runs listing requests unchecked, which matches #416 ("Assert the deliberate metadata limitation rather than fixing it") and the interview's "as applicable".

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Key Interfaces (`pub(crate) struct PermissionGate`) and § Implementation Tasks 2.3
- Issue: Task 2.3 says "Thread `Option<&PermissionGate>` through `handle_pushdown_request`, `handle_pushdown`, and `plan_join`". `handle_pushdown` is `pub` and reachable as `lakehouse_engine::adapter::pushdown::handle_pushdown`, which `crates/lakehouse-engine/tests/pushdown_public_surface.rs` imports. A `pub(crate)` type in the signature of a reachable `pub fn` triggers the warn-by-default `private_interfaces` lint. A probe crate under rustc 1.96.1, edition 2024, reports "type `PermissionGate` is more private than the item `handle_pushdown`". The plan's own Checklist runs `cargo clippy --all-targets -- -D warnings`, so the planned interface fails the plan's lint gate. Each quick workaround breaks a recorded rule. A `pub` `PermissionGate` widens the façade that `vs-adapter/pushdown-module-structure` § "Public pushdown façade resolves at every pre-refactor path" forbids widening outside a planned scenario. A narrowed `handle_pushdown` breaks that external probe.
- Fix: In plan.md § Key Interfaces and task 2.3, carry the gate as a `pub(crate)` field on `ResolvedConnectionConfig`, which `handle_pushdown` and `plan_join` already receive as `conn`, and read it from `conn` when they call `TableScanResolver::for_request`. A probe crate confirms that a `pub(crate)` field of a `pub(crate)` type on that `pub` struct compiles without a warning. Remove the `handle_pushdown`, `handle_pushdown_request`, and `plan_join` signature changes and the "pass `None` from every existing test caller" step. State the carrier choice in decision [1].
- Escalation: MECHANICAL (settled by the compiler and the recorded façade spec, no judgment call)

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact, bullet 2
- Issue: Impact tells operators that the CONNECTION identity "needs a grant that includes `can_read_assignments` on the checked tables, for example `manage_grants`". The #414 README (`crates/lakehouse-catalog/tests/fixtures/lakekeeper/batch-check/README.md` § Privilege to check another identity) lists every privilege that passes the check: `manage_grants`, `ownership`, `pass_grants` with `select`, project `project_admin`, and project `security_admin`. Each of them also permits writing grants, and the README records no read-only option on v0.13.1. With the check on, the CONNECTION's client secret is therefore a grant-writing credential. The plan never states this consequence.
- Fix: Add one sentence to plan.md § Impact: "Every Lakekeeper v0.13.1 privilege that permits the check also permits writing grants, so the CONNECTION's client secret becomes a grant-administration credential." Name #416's trust-model documentation as the place that explains it to operators.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 3.2
- Issue: The task says "Create `LK_PERM_UNINSPECTING` the same way, then replace its CONNECTION with the client credentials of `lakehouse-reader-b`." "The same way" reads as "through the operator CONNECTION" of `LK_PERM_LAKEHOUSE`. If both schemas share one CONNECTION object, the replacement also switches `LK_PERM_LAKEHOUSE` to `lakehouse-reader-b`. `permission_check_grants_decide_each_mapped_users_query` then fails with `CannotInspectPermissions`. The task also names no Exasol-side cleanup, although the suite reuses a persisted Exasol. `e2e_credential_exposure_test.rs` drops its virtual schema, users, and CONNECTION before it creates them.
- Fix: In task 3.2, name one CONNECTION constant per schema, for example `LK_PERM_CATALOG_CREDS` and `LK_PERM_UNINSPECTING_CREDS`. Add `DROP VIRTUAL SCHEMA IF EXISTS ... CASCADE`, `DROP USER IF EXISTS ... CASCADE`, and `DROP CONNECTION IF EXISTS` before the creates, as `e2e_credential_exposure_test.rs` does.

## Requirement Quality

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Background bullets 3 and 6, `vs-adapter/lakekeeper-permission-client/spec.md` § Background bullet 4
- Issue: These Background facts back no GIVEN, WHEN, or THEN step of their own spec:
  - Check bullet 3: "Exasol lets only the schema owner, a holder of `ALTER` on the schema, or a holder of `ALTER ANY VIRTUAL SCHEMA` change them (Exasol `ALTER VIRTUAL SCHEMA` usage notes)." No scenario names who may change a property.
  - Check bullet 6: "or when `LAKEKEEPER__OPENID_SUBJECT_CLAIM` names a claim whose value the template reproduces. A direct login's id (`oidc~<opaque sub>`, #414) is not derivable." No scenario configures a subject claim or a direct login.
  - Client bullet 4: "A live check on `v0.13.1` (2026-10-02) showed warehouse `lakehouse_authz` answer the warehouse UUID that `/management/v1/warehouse` lists." This is evidence, and decision-log.md [3] already records it.
  - The Iceberg-spec sentence of client bullet 4 is not leakage. CLAUDE.md requires the delta to name that Lakekeeper-specific reading.
- Fix: In the check spec, delete the privilege sentence from Background bullet 3, or move it into "The principal is derived from the querying user through USER_MAPPING" as the THEN step "the adapter SHALL read `USER_MAPPING` only from the virtual schema's properties, never from the UDF context or the pushdown SQL". Delete the two quoted sentences from Background bullet 6, because plan.md § Impact already tells operators about the subject claim. In the client spec, delete the live-check sentence from Background bullet 4, and keep the UUID rule and the Iceberg-spec sentence.
- Escalation: MECHANICAL (settled by reading each Background line against the scenarios in the same file)

#### [COMPLETENESS_GAP] BLOCKER
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Background bullet 5 and § Scenario "A user name the mapping cannot accept is refused before any catalog request", plan.md § Scenario Coverage row for `each_placeholder_maps_its_accepted_names_injectively`
- Issue: #415 requires: "Validate the substitution so a crafted user name cannot produce another user's principal." The delta states that property only in Background bullet 5: "Each placeholder is injective over the names it accepts, and the literal text is fixed, so two Exasol users never map to one principal." No scenario step asserts it. The unmappable-user scenario asserts only the refusal of names outside the charset. The plan traces `each_placeholder_maps_its_accepted_names_injectively` to that scenario, so the test traces to a scenario that does not state what the test checks. A charset edit that broke injectivity would fail no scenario. The design itself holds: a read-only batch-check probe on this stack (Lakekeeper v0.13.1) answered `allowed: true` for `oidc~template-user@corp` and `allowed: false` for `oidc~TEMPLATE-USER@corp` and `oidc~Template-User@corp`. Lakekeeper principal ids are therefore case-sensitive, and the `$1` rule stays injective at the catalog. The defect is the missing normative statement.
- Fix: Add a THEN step to "A user name the mapping cannot accept is refused before any catalog request": "AND two distinct user names that the template's placeholder accepts SHALL map to two distinct principals, for example `ALICE` and `alice` under `$1`". Add the step to this existing scenario, not a new one, because the spec already holds 10 scenarios and an 11th crosses the library threshold of more than 10. Keep the test row and point it at the updated scenario. Delete the injectivity sentence from Background bullet 5, or keep it only as a restatement of the new step.
- Escalation: MECHANICAL (settled by reading #415, the delta, and the coverage table)

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `vs-adapter/lakekeeper-permission-client/spec.md` § Scenario "The batch-check request carries one identity-bearing read check per distinct table", plan.md tasks 1.2 and 1.3
- Issue: The THEN steps require "each check under a unique id" and require the request to "equal the `request` of each fixture once the fixture's placeholders are substituted". Every fixture sends the literal id `read-data` (`CHECK_ID` in `tests/common/lakekeeper_authz.rs`). The fixture README's placeholder table lists no id placeholder. The plan never states the id scheme. A natural scheme such as `0`, `t0`, or the identifier fails the equality test. The cheapest repair is then to normalize ids in the test, which unpins the wire contract that the fixtures exist to hold.
- Fix: State the check-id scheme in task 1.2 and in the scenario, for example "the first distinct identifier's check id is `read-data`, and the n-th after it is `read-data-<n>`". As an alternative, state that the fixture comparison substitutes the check id, and name the scheme anyway.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: decision-log.md [6], plan.md task 2.2, recorded `vs-adapter/catalog-kind-selection` § Scenario "The catalog kind is matched at one construction site and nowhere else"
- Issue: The recorded scenario states "each of the three matching sites SHALL be EXHAUSTIVE" and "no other production module SHALL match on the enum". Task 2.2 adds a fourth exhaustive match, `CatalogKind::supports_lakekeeper_permission_check`, in `adapter/catalog_kind.rs`. That file is in the recorded four-file permitted set, so the file-level rule holds. The recorded count of three matching sites becomes false, and no delta updates it.
- Fix: Add a `vs-adapter/catalog-kind-selection/spec.md` delta with a `DELTA:CHANGED` block for that scenario. The block names the predicate in `adapter/catalog_kind.rs` as the fourth exhaustive match and changes "three matching sites" to "four".

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Background bullet 7 and § Scenario "The management API base is derived from the catalog URI or set on the catalog's origin", decision-log.md [4]
- Issue: The rule "the last path segment `catalog`" leaves three inputs undecided. A catalog URI with a trailing slash (`http://lakekeeper:8181/catalog/`) has an empty last segment, although `resolve_load_table_prefix` trims trailing slashes from the same URI. One URI can carry an explicit default port and the other none (`http://h` against `http://h:80`). An override can carry userinfo. The implementer decides each pass or fail case alone.
- Fix: State in decision [4] and in the scenario that the adapter trims trailing slashes before derivation, compares ports after default-port normalization (`url::Url::port_or_known_default`), and rejects an override with userinfo. Add the three cases to `management_base_is_derived_or_set_on_the_catalog_origin`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/lakekeeper-permission-client/spec.md` § Scenario "A failed or malformed batch-check is an error that carries no credential", decision-log.md [3]
- Issue: `CatalogSession::resolve` turns a failed `/v1/config` lookup into an empty prefix. The doc comment of `resolve_load_table_prefix` states: "A missing prefix or unreachable config endpoint yields an EMPTY prefix". With the check on, a 401 or a 5xx on `/v1/config` therefore reaches the operator as "prefix is empty or not a UUID". That message hides the real cause.
- Fix: In that scenario, require the empty-prefix error to state that the `/v1/config` lookup failed or returned no prefix, and to name the CONNECTION's warehouse.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/lakekeeper-permission-client/spec.md` § Scenario "The Lakekeeper client extends the catalog crate's public surface through a reviewed probe edit", last AND step, plan.md task 1.4
- Issue: The step requires that "no `lakehouse-catalog` source SHALL name `lakehouse-engine`, an Exasol user, or a virtual-schema property". Task 1.4 turns that into a probe assertion that "`lakekeeper.rs` names no Exasol user or virtual-schema property". A source-text assertion needs a token list, and "an Exasol user" names none.
- Fix: In task 1.4, list the tokens that the probe rejects, for example `PERMISSION_CHECK`, `USER_MAPPING`, `LAKEKEEPER_MANAGEMENT_URL`, `current_user`, `scope_user`, and `UdfContext`.

## Task Breakdown

No objection, axis checked. The Scenario Coverage table maps every scenario of the four deltas to at least one named test. Every task from 1.1 to 3.4 names files that its group's Knowledge column lists. Group B depends on the three items that Group A exports, and the two groups share no source module. The call-site counts that task 2.3 touches are small: 10 test callers of `handle_pushdown` and 11 of `for_request`. The only mis-traced test row is the injectivity test, covered by the [COMPLETENESS_GAP] BLOCKER above.

## Design Depth

No objection, axis checked. The plan's Quick Diagnostic table answers every question for both new modules. Lakekeeper wire knowledge has one owner (`lakehouse_catalog::lakekeeper`), and Exasol mapping knowledge has one owner (`adapter/permission.rs`). The `resolve` guard on unchecked identifiers turns a future bypass into a refusal. ADR gate: decision [1] (single enforcement chokepoint) and decision [2] (module ownership across the crate boundary) each change architecture or design, so `Promotes to ADR: yes` passes. Decisions [3] to [9] are `no`. The fix of the [HIDDEN_DEPENDENCY] BLOCKER also removes the parameter threading through the 13-argument `handle_pushdown` and the 15-argument `plan_join`.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Implementation Tasks 1.2, 2.1, 2.3, 2.4, and 3.1
- Issue: Several procedural sentences exceed the 20-word cap and carry several instructions each. The second sentence of task 1.2 holds eight instructions after "and `batch_check`:" in about 60 words. Task 3.1 uses "Ensure", which the guardrails replace with "make sure that".
- Fix: Split each listed task line into imperative sentences of one instruction and at most 20 words. Replace "Ensure `select` on it for ..." in task 3.1 with "Grant `select` on it to ...".
