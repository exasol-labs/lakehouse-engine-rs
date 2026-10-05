# Plan Review Findings: add-lakekeeper-permission-check (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 2, Advisory: 4)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck
- Resolved: [HIDDEN_DEPENDENCY] A gate parameter on `handle_pushdown` fails the plan's own clippy gate. Evidence: task 2.4 adds `pub(crate) permission_gate: Option<PermissionGate>` to `ResolvedConnectionConfig`. It keeps the signatures of `handle_pushdown_request`, `handle_pushdown`, and `plan_join`. Task 2.5 adds the gate parameter only to `for_request`, which is `pub(super)`. `ResolvedConnectionConfig` derives no trait, so `PermissionGate` needs no `Clone` or `Debug`. Round 1 also asked to state the carrier in decision [1]. PR #457 point 5 moves the field out of that ADR, so task 2.4 and the `[plan-review]` entry now hold it.
- Resolved: [IMPLEMENTATION_LEAKAGE] Background facts of two deltas backed no scenario step. Evidence: the property-privilege sentence, the subject-claim and direct-login sentences, and the live-check sentence are gone. I checked each of the 13 Background bullets of the merged delta against its scenarios, and each one backs a step.
- Resolved: [COMPLETENESS_GAP] No scenario step stated the mapping's injectivity. Evidence: "USER_MAPPING never gives two Exasol users one principal" holds the step "two distinct user names SHALL never get one principal". The rule-list design keeps that property. `lower` and `replace(a,b)` are injective on the inputs they accept, and so is any composition of them. Two outputs `P1·x·S1` and `P2·y·S2` are equal only if one prefix starts the other and one suffix ends the other. The literal-text clash check therefore over-approximates and misses no clash.

## Premortem

Six months from now this plan failed catastrophically. These are the likely causes:

1. A test written from "The check is accepted only for an Iceberg REST catalog URI that ends in /catalog" used the SDK's default `TestContext`, which reports no current user. The adapter returned the unmappable-user error, and the test failed. The implementer moved the kind check ahead of the mapping. The URI check still needed the CONNECTION, so the user scenario then broke instead. Routed to Requirement Quality, [REQUIREMENT_CONFLICT] BLOCKER.
2. An operator set the check on a REST catalog that is not Lakekeeper but sits under a `/catalog` path. That server answers unknown routes the way the Iceberg REST reference server does: 400 JSON `BadRequestException`. Every query failed with "HTTP 400" and no statement that the catalog must be Lakekeeper, which PR #457 point 2 asked for. Routed to Requirement Quality, [COMPLETENESS_GAP] BLOCKER.
3. An operator wrote the default `*` rule before the `ETL_SVC` service-account rule. CREATE accepted the mapping. `ETL_SVC` got `oidc~etl.svc@corp.net`, its jobs were denied, and the operator granted that id instead of fixing the rule order. Routed to Requirement Quality, [COMPLETENESS_GAP] ADVISORY.

## Intent Fidelity

No objection, axis checked. Each point of the PR #457 review maps to a plan element.
- Point 1: one error string in the kind scenario, decision [6], and task 2.3 (`!=` against `CatalogKind::IcebergRest`). No rule names `GLUE`. `dispatch` calls `resolve_connection_config` for pushdown (`adapter/mod.rs:112`), and `handle_create_virtual_schema` calls it for the other three request types (`adapter/mod.rs:188`).
- Point 2: decision [3] drops the UUID rule and the detection call. Task 1.3 sends the prefix unchanged. The failure scenario names the URL and the status. Its status rule has a gap, raised below as a mechanical defect, not as a substituted ask.
- Point 3: `LAKEKEEPER_MANAGEMENT_URL` survives only in decision [4] Alternatives and the superseded interview answer. Background bullet 2, the kind scenario, and task 1.4 cover the trailing `/` and the missing `/catalog`.
- Point 4: the only code-like token in any delta scenario is the recorded phrase "file resolver". Task 1.5 holds the surface probe. The identity rule reads "every check of the request SHALL name the principal". Background carries no JSON document. Tasks 2.4 and 2.5 say "every" instead of counts. The client spec is merged, and the merged spec holds 10 scenarios.
- Point 5: decisions [1] and [2] follow the reviewer's wording and name no field, lint, path, or type. Decision [9] moved to the intro of plan.md § 3.
- Point 6: I traced each example user through the first-match rules of the mapping scenario. Each one gives the stated principal. The five rules to keep map to the mapping scenario (properties only, identity), the invalid-property and clash scenarios (CREATE/SET), Background bullet 10 (forbidden characters), and the clash scenario (no shared principal). Background bullet 12 states which clashes CREATE catches and which it cannot.
- Rebase: `git merge-base --is-ancestor origin/main HEAD` succeeds, and `origin/main` is `908af7e` (#455).

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 2.7, last sentence
- Issue: The task says "Iterate the kind test over every `CATALOG_KIND` constant of `catalog_kind.rs`, so a new kind joins the test." `catalog_kind.rs` declares three separate constants: `CATALOG_KIND_UNITY_CATALOG`, `CATALOG_KIND_DIRECT_STORAGE`, and `CATALOG_KIND_GLUE`. No list holds them. A test iterates only a list that it spells out itself, so a fourth constant does not join the test.
- Fix: Delete "so a new kind joins the test" from task 2.7. As an alternative, add a crate-private array of the three constants. Make the test and the error message of `resolve_catalog_kind` read that array.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Scenario "The check is accepted only for an Iceberg REST catalog URI that ends in /catalog" and § Scenario "A user that USER_MAPPING cannot map is refused before any request", plan.md § Implementation Tasks 2.4
- Issue: Two scenarios demand different errors for one pushdown input. Take `CATALOG_KIND = 'GLUE'`, a valid mapping, and an absent current user. The kind scenario requires "the error `PERMISSION_CHECK = 'LAKEKEEPER' requires the Iceberg REST catalog kind`". The user scenario requires "an error that names the user". The same conflict holds under Iceberg REST for a catalog URI without `/catalog`. There the user scenario also states "MUST NOT read the CONNECTION", and the URI comes from the CONNECTION. Task 2.4 maps the user first, so the code meets the user scenario and breaks the kind scenario. The user scenario's GIVEN ("a virtual schema with the check on") also overlaps the invalid-property and clash scenarios. `PermissionSettings::parse` decides those inputs first.
- Fix: In the kind scenario, change the WHEN to "the adapter handles a createVirtualSchema, refresh, or setProperties request, or a pushdown request whose current user USER_MAPPING maps". In the user scenario, change the GIVEN to "a virtual schema with the check on and a valid USER_MAPPING whose rules do not clash". Keep the order of task 2.4.
- Escalation: MECHANICAL (settled by reading the two scenarios and task 2.4, no judgment call)

#### [COMPLETENESS_GAP] BLOCKER
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Scenario "A failed batch-check refuses the query with an error that names the cause", second THEN step. decision-log.md [3] Rationale and Consequences. plan.md § Implementation Tasks 1.4.
- Issue: PR #457 point 2 asks that a failure against a server that is not Lakekeeper "says the catalog must be a Lakekeeper server". The scenario keys that statement to "a 404, a 405, or a body that is not a batch-check answer". Decision [3] rests on "A server without the batch-check route answers 404, 405, or a body that is not JSON". A read-only probe on 2026-10-05 falsifies that claim:
  - The repo's Iceberg REST reference server (`lakehouse-engine-rs-iceberg-rest-1`, port 18181) answered `POST /management/v1/action/batch-check` with 400 `application/json`: `{"error":{"message":"No route for request: POST management/v1/action/batch-check","type":"BadRequestException","code":400}}`. A non-Lakekeeper server under a `/catalog` path that answers this way gets no Lakekeeper statement.
  - Lakekeeper `v0.13.1` (port 28181, operator token) answered an empty `warehouse-id` and the non-UUID `lakehouse_authz` with 422 `text/plain`: "Failed to deserialize the JSON body into the target type: ... TabularIdentOrUuid". Decision [3] Consequences says "The check then fails or denies". It fails, with 422.
  - The phrase "a body that is not a batch-check answer" leaves open whether it covers non-2xx bodies. If it does, a 403 `CannotInspectPermissions` also gets the Lakekeeper statement.
- Fix: In that THEN step, key the Lakekeeper statement to "every non-2xx answer other than a 403 `CannotInspectPermissions`, and every 2xx body that is not a batch-check answer". Require that the error carry the redacted answer body, so that a 422 names its cause. In decision [3], replace the last Rationale sentence with the two probe results above. In decision [3] Consequences, replace "fails or denies" with "fails with 422". In task 1.4, add a 400 JSON `BadRequestException` case and a 422 `text/plain` case.
- Escalation: MECHANICAL (settled by a live probe on the running stack, and the fix is a wording change)

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: plan.md § Implementation Tasks 1.3 and 1.5, recorded `vs-adapter/catalog-crate-structure` § `pub`-set scenario, recorded `vs-adapter/catalog-crate-public-surface-extensions`
- Issue: The recorded `catalog-crate-structure` states "exactly these items SHALL be `pub` on `lakehouse-catalog`". `catalog-crate-public-surface-extensions` calls itself "the running history of what gets ADDED to that `pub` set and why". Each earlier addition recorded a scenario there, most recently the AWS identity resolver (#441). Task 1.3 adds three `pub` items, and no delta records them. PR #457 point 4 moved the surface scenario out of the behavior spec, and the plan follows the reviewer. After the merge, the recorded enumeration is false, and the addition history skips this change. The probe edit of task 1.5 holds the code to the new set, but the library does not show that set.
- Fix: Ask the PR #457 reviewer to choose between two options. One: record the three items in a one-scenario delta of a new `vs-adapter/catalog-crate-public-surface-extensions-lakekeeper`. Two: accept the drift. Until the reviewer answers, name the three unrecorded items in one line of plan.md § Impact.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/lakekeeper-permission-check/spec.md` § Background bullets 6, 11, and 12, decision-log.md [5] Consequences, plan.md § Impact bullet 4
- Issue: PR #457 point 6 calls "a mistake in the mapping" the real risk. The plan catches clashes, but it accepts a rule that an earlier rule always shadows. Under `* -> oidc~{*|lower|replace(_,.)}@corp.net; ETL_SVC -> oidc~6f1c...`, `ETL_SVC` gets `oidc~etl.svc@corp.net`, and the fixed entry never applies. The clash check also has a limit that Impact does not state. A fixed entry cannot coexist with a `*` rule of the same `<idp>~` that has empty text after `{*}`. For example, `* -> oidc~{*|lower}` and `ETL_SVC -> oidc~6f1c...` clash. That rejection is correct, because a delimited user `6F1C...` would get the fixed id.
- Fix: Decide whether createVirtualSchema, refresh, and setProperties reject a rule that an earlier rule always shadows. Record that choice in decision [5]. State the shadowing behavior and the fixed-entry limit in plan.md § Impact.

## Task Breakdown

No objection, axis checked. § Scenario Coverage maps every scenario of the three deltas to a named test, including all 10 merged scenarios. The production `for_request` call sites are the two that task 2.5 names (`pushdown/mod.rs:170`, `joins/mod.rs:136`). Five files construct `ResolvedConnectionConfig`, and all five sit in Group B's Knowledge. `make test-e2e-lakekeeper` runs with `--test-threads=1`, and the suite sets up once through `OnceLock`, so the drop-first step of task 3.3 cannot race. #414's `authz_direct_login_principal_id_is_idp_prefix_and_token_subject` proves that Lakekeeper grants and allows a never-registered id, which task 3.2 depends on. Groups A and B share no source file, and Group B consumes the three exports of Group A.

## Design Depth

No objection, axis checked. The plan's Quick Diagnostic table answers every question for both new modules. `lakekeeper_management_url` is the one owner of the URL derivation. The adapter's create-time check and `lakekeeper_batch_check` both call it. The kind rule is one `!=` in `resolve_connection_config`, which resolves the kind from properties before the CONNECTION read. `adapter/mod.rs` is one of the four files that recorded `vs-adapter/catalog-kind-selection` lets name a `CatalogKind` variant, and `CatalogKind` derives `PartialEq`. ADR gate: decisions [1] and [2] change architecture and design, so `Promotes to ADR: yes` passes. Decisions [3] to [8] and the three `[plan-review]` entries are `no`.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: decision-log.md [1] Alternatives, [3] Decision, [3] to [6] Alternatives, plan.md § Summary sentence 1
- Issue: Decisions [1] and [2] become ADRs, so their prose outlives the plan. The first Alternatives sentence of decision [1] runs to 34 words, past the 25-word cap. The last Decision sentence of decision [3] joins three facts with "and" in 30 words. The Alternatives sentence of decision [4] runs to 37 words. Decisions [3] to [6] label an alternative "the prior design", which narrates the revision. After the merge, no reader can tell which design that was. The first sentence of plan.md § Summary runs to 27 words.
- Fix: Split each listed sentence into sentences of one fact and at most 25 words. Replace each "the prior design" with a description of that alternative.
