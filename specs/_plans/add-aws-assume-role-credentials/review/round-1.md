# Plan Review Findings: add-aws-assume-role-credentials (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 13 (Blockers: 3, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three failure stories:

1. A later plan edits the direct-storage path and follows the recorded spec literally. `connection-credentials-direct-storage` still says a direct-storage scan spec carries a CONNECTION reference. The "fix" drops the sealed envelope for a role CONNECTION, the scan UDF reads the base key pair from the CONNECTION, and every direct-storage role query fails with access denied. Routes to [REQUIREMENT_CONFLICT].
2. A Glue role CONNECTION, the ticket's own shape, loses the `catalogs/{account-id}` prefix after a refactor. No scenario is violated, because this plan scopes the prefix scenario away from role CONNECTIONs. The same CONNECTION omits `region`, so its scans fail on an empty store region while its catalog calls succeed. Routes to [COMPLETENESS_GAP], once as a blocker and once as an advisory.
3. The local E2E stays green while real STS rejects the engine's signed GET. No scenario requires the stub to reject a bad signature, so a lenient stub passes. The only real-STS check is an opt-in suite behind a manual `tofu apply`. Under dashboard load, STS throttling surfaces as failed queries, because no retry exists. Routes to [IMPLEMENTATION_LEAKAGE], [UNSTATED_ASSUMPTION], and [NFR_IGNORED].

## Intent Fidelity

Checked: interview answers Q1 to Q8 each map to plan content. Q1 is decision [1] plus the signing scenario. Q2 is decision [1] plus the sealed-envelope scenario. Q3 is decision [2]. Q4 covers three catalog kinds in the storage scenario. Q5 is decision [6]. Q6 is decision [4]. Q7 is Group D. Q8 is decision [7]. The pairing rule carries the requested "stricter than AWS" note.

#### [INTENT_DRIFT] ADVISORY
- Location: decision-log.md § [10] Rationale + plan.md § Design, Non-Goals ("MinIO-native role semantics") + plan.md task 5.1
- Issue: The ticket asks "Check this if it works with MinIO". Decision [10] reads that note "as regression safety", and the interview never confirmed that reading. The plan's own evidence answers the spike negatively: MinIO's `AssumeRole` ignores `RoleArn`, so a CONNECTION whose `aws_sts_endpoint` names MinIO receives a session with the base user's policy, not the role's. Task 5.1 does not tell an operator this. A MinIO base user that already holds bucket access would read with no role applied, and nothing would say so.
- Fix: Add to task 5.1 a `docs/catalogs.md` statement that MinIO's STS ignores `aws_assume_role_arn` and that role assumption needs an AWS-compatible STS endpoint. State the spike answer in plan.md § Summary ("MinIO cannot model role assumption natively, so the local suite uses an STS stub") so the requester can confirm that it closes the spike.

## Feasibility

Checked: the CLAUDE.md Iceberg and Delta compliance-gate exemption in decision [9] holds. No scenario changes file selection, predicate semantics, schema or type mapping, or delete handling. The one protocol-facing change, omitting `X-Iceberg-Access-Delegation`, quotes the Iceberg REST OpenAPI `required: false` text in the new spec's Background. `quick-xml 0.39.4` with `serde` is already in `Cargo.lock` through `object_store 0.13.2` (verified). `sign_request` already takes a `service` argument and a `session_token` (`sigv4.rs:93-100`).

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md task 3.1 + decision-log.md § [10] Consequences + e2e-harness/assume-role-e2e/spec.md § Background, bullet 3
- Issue: The whole local E2E design rests on the pinned `pgsty/silo` image serving `AssumeRole` for a MinIO user. The plan states this but cites only a web search (pgsty/silo PR #146). CLAUDE.md § Verification discipline governs Exasol SQL capabilities, so its letter does not bind this MinIO fact. The gate in task 3.1 sits in the right place, because Groups A and B do not depend on it. A late failure still forces a rewrite of the assume-role-e2e spec mid-implementation, and the plan names no fallback. No cheap fallback exists: a static MinIO key pair carries no `SessionToken`, which the response scenario treats as an error.
- Fix: Before Group C starts, run one signed `AssumeRole` POST against the pinned `pgsty/silo` image as a non-root MinIO user, and record the observed response in decision [10]. In task 3.1, replace "stop and escalate" with the concrete escalation content: the observed MinIO response and the spec sections that change if it fails.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: decision-log.md § [3] Consequences + plan.md § Verification, Checklist
- Issue: Real AWS STS acceptance of the signed `GET`, with the always-signed `x-amz-content-sha256` header, is checked only by `cloud_assume_role_reaches_glue_and_s3_through_the_role`. That test skips without four SSM variables and needs a manual `tofu apply`. The Checklist has no row for it, so the plan can ship without its headline path ever running against AWS.
- Fix: Add a Checklist row "Cloud assume-role | `cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test cloud_assume_role -- --test-threads=1` with the assume-role variables exported | 2 passed, 0 skipped". State in task 4.2 that the PR is not marked ready until this row passes once.

#### [NFR_IGNORED] ADVISORY
- Location: decision-log.md § [7] + vs-adapter/connection-credentials-assume-role/spec.md § "A failed AssumeRole is a clear, credential-safe error"
- Issue: Decision [7] rejects retries because "a failure is a configuration defect in the common case". STS throttling (the `Throttling` error code) and 5xx responses are transient, not configuration defects. Every pushdown, CREATE, REFRESH, and SET PROPERTIES now makes one STS call. A concurrent dashboard workload therefore turns STS throttling into failed user queries.
- Fix: In decision [7], either add one bounded retry for the `Throttling` code and 5xx statuses inside the 30-second budget, with a matching scenario step and `sts_tests.rs` test, or record throttling as an accepted limitation and state it in the task 5.1 `docs/catalogs.md` text.

## Requirement Quality

Checked: the five production reads of `use_vended_credentials` that decide a storage source (`connection.rs:316`, `support.rs:1585`, `delta_format_reader.rs:93`, `iceberg.rs:102`, `session.rs:247`) are exactly the sites tasks 1.3, 2.2, 2.4, and 2.5 replace. The remaining read (`connection.rs:161`) rejects the field under direct storage, which is outside the precedence. Every planned match fails closed: no site falls back to the base identity or to vending. The rewritten recorded scenarios in storage-backend-enum, delta-table-planning, rest-catalog-oauth-auth, and connection-credentials differ from the recorded text only in the intended clauses (word-level diff checked).

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/vs-adapter/connection-credentials-direct-storage/spec.md` § "Credential vending is unreachable under the direct-storage kind" + plan.md § Features
- Issue: The recorded scenario applies to every direct-storage virtual schema and requires "the shard-invariant common spec SHALL carry a REFERENCE to the CONNECTION that supplied the backend". This plan seals every role CONNECTION (connection-credentials-assume-role § "The scan receives the session credentials only inside the sealed envelope"). Task 3.6 tests a direct-storage role CONNECTION, and the direct-storage path reaches the same `scan_storage_for` call (`adapter/pushdown/mod.rs:245`). After merge, the library states both "REFERENCE" and "sealed" for one input. No delta amends the recorded scenario, and plan.md § Features does not list the feature. Softer stale wording rides along. It is scoped by its GIVEN or sits in unchanged Background, so it is not a hard conflict: `connection-credentials` § "Optional credential fields default sensibly" ("storage_block's output is never read" under vending), the `connection-credentials` Background bullet "Under vending, a supplied storage credential is IRRELEVANT", and the `storage-backend-enum` Background bullet "`storage_block` ... runs only when vending is disabled".
- Fix: Add `specs/_plans/add-aws-assume-role-credentials/vs-adapter/connection-credentials-direct-storage/spec.md` with a `DELTA:CHANGED` copy of § "Credential vending is unreachable under the direct-storage kind". In the copy, make the REFERENCE step read "a REFERENCE to the CONNECTION when the storage credential source of `vs-adapter/connection-credentials-assume-role` is the CONNECTION, and the sealed envelope of `vs-adapter/scan-spec-credential-reference` when it is an assumed role". List the feature in plan.md § Features and in Parallelization group B's Knowledge. Map the scenario in § Scenario Coverage to a new `pushdown_tests.rs` test, `a_direct_storage_role_connection_seals_its_storage_block`, added to task 2.5. Leave the softer stale bullets alone unless those sections are rewritten for another reason, per decision [8].
- Escalation: MECHANICAL (settled by reading the recorded scenario against this plan's own sealing scenario)

#### [COMPLETENESS_GAP] BLOCKER
- Location: vs-adapter/pushdown-planning-cloud-credentials/spec.md § feature description + decision-log.md § [8] + vs-adapter/connection-credentials-assume-role/spec.md § "Session credentials sign every SigV4 catalog request"
- Issue: The new description ends "so every scenario below describes a CONNECTION that names no role". That clause also scopes out § "SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request", a rule that has nothing to do with credentials. After merge, no scenario requires the `catalogs/{warehouse}` prefix, or forbids the `/v1/config` call, for a SigV4 CONNECTION that names a role. That is the ticket's own CONNECTION shape. The assume-role signing scenario pins only the signature and the signing region. The scoping is also invisible where agents read specs: `speq feature get "<domain>/<feature>/<scenario>"` prints a scenario without its feature description (checked live on `vs-adapter/storage-backend-enum`). § "Vended S3 credentials are the sole storage source regardless of catalog auth mode" therefore reads unscoped there.
- Fix: Change the description's final clause to "so every scenario below, except § 'SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request', describes a CONNECTION that names no role". Add to connection-credentials-assume-role § "Session credentials sign every SigV4 catalog request" the step "*AND* each catalog request SHALL carry the prefix that `vs-adapter/pushdown-planning-cloud-credentials` § 'SigV4/Glue derives the catalogs/{account-id} REST prefix on every catalog request' specifies, unchanged by the role". Extend task 1.7's `assumed_session_signs_load_table_and_namespace_enumeration` to assert the `catalogs/` path segment. Add one sentence to decision [8] Consequences stating that scenario-level retrieval does not show the description scope.
- Escalation: MECHANICAL (resolved by editing the plan's own description sentence and one scenario step)

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: e2e-harness/assume-role-e2e/spec.md § Background, bullets 1 and 3
- Issue: Bullet 1 ("MinIO alone cannot model role assumption", with the MinIO documentation quote) is design rationale. No scenario step depends on it, and decision-log.md § [10] already holds it. Bullet 3 states that the stub "recomputes the SigV4 signature for the service `sts` with the base user's secret" and answers `SignatureDoesNotMatch` on a mismatch. No scenario sends a badly signed request or requires that error. plan.md § Scenario Coverage still cites the stub's independent check as evidence for connection-credentials-assume-role § "The AssumeRole request carries the role, the external id, and the base identity's signature". A stub that skips verification passes every scenario as written.
- Fix: Delete bullet 1 from the assume-role-e2e Background. Add the scenario "A wrong base secret fails CREATE VIRTUAL SCHEMA with the STS signature error": GIVEN a CONNECTION that carries the base `access_key`, a wrong `secret_key`, the role, and the accepted external id, WHEN the suite creates a virtual schema through it, THEN `CREATE VIRTUAL SCHEMA` SHALL fail with an error naming the STS `SignatureDoesNotMatch` code, AND the error MUST NOT contain either secret key. Add its test, `a_wrong_base_secret_fails_create_with_signature_does_not_match`, to task 3.5 and to plan.md § Scenario Coverage.
- Escalation: MECHANICAL (the Background rule and the scenario list settle it)

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Scenario Coverage, row "Session credentials are the storage credential under every catalog kind" + plan.md tasks 2.3 and 2.4
- Issue: Interview Q4 chose the generic pipeline for "Iceberg REST (incl. Glue) and Delta/Unity static-storage CONNECTIONs alike". The Delta path is proven only by `DeltaFormatReader` unit tests (`an_assumed_role_reads_through_the_session_backend`, `a_role_with_vending_requests_no_temporary_credentials`). No test drives a Unity-kind pushdown through a role CONNECTION from the adapter entry point. Nothing therefore proves that the substituted backend reaches `DeltaFormatReader`, or that the sealed envelope serves a Delta scan. The design makes success likely, because both readers take `conn.storage` through one `ConnectionStorage` (`adapter/pushdown/mod.rs:220`). The gap is evidence, not design. A local stack for it exists: `docker-compose.unity.yml` backs an OSS Unity Catalog with MinIO under `make test-e2e-unity`.
- Fix: Add to task 2.3 an `adapter_tests.rs` case for `CatalogKind::UnityCatalogNative` that asserts the pushdown's storage block unseals to the session backend. If the planner prefers E2E evidence, add one role scenario to `e2e_unity_test.rs` instead, and state the choice in decision [10].

#### [COMPLETENESS_GAP] ADVISORY
- Location: vs-adapter/connection-credentials-assume-role/spec.md § "Session credentials are the storage credential under every catalog kind" + plan.md task 5.1
- Issue: A role storage backend takes `endpoint`, `region`, and `path_style` from the CONNECTION alone. Under role plus vending, the vended `client.region` and `s3.endpoint` are dropped as well, not only the vended credentials. The ticket's own example CONNECTION states no `region`. Its store region is therefore empty, and `scan/object_store.rs:237` passes that value unchanged to `AmazonS3Builder::with_region`. The scan outcome for a bucket outside the default region is unverified. The catalog and STS calls succeed either way, so a failure appears only at scan time. Task 5.1 adds `region` to the documented example without a matching rule.
- Fix: Add a step to the scenario: "*AND* the store address of a CONNECTION that names a role SHALL NOT read any vended `client.region`, `s3.endpoint`, or `s3.path-style-access` value". In task 5.1, state that a role CONNECTION on AWS S3 without an `endpoint` MUST state `region`. Alternatively, add a `validate_assume_role_creds` rule that names `region` for a role CONNECTION with neither `endpoint` nor `region`, with a `connection_tests.rs` test.

## Task Breakdown

Checked: the group order holds. B consumes A's public items, C needs B's engine behavior, D reuses the `CatalogConnectionPassword` fields C adds, and E documents B. Every spec delta has an implementing task, and every cited existing test name exists in the tree (verified for 10 names).

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md task 3.1
- Issue: Task 3.1 bundles a live capability gate, a SigV4 verifier, a SigV4 signer for the outbound MinIO POST, three HTTP routes, and two XML response shapes into one untagged task. The stub has no tests of its own, yet it is the only independent check of the engine's canonicalization.
- Fix: Split task 3.1 into 3.1a (the live check and the decision [10] update) and 3.1b (the stub). Tag 3.1b `[expert]`. Add a stub self-test that accepts one request signed with a published AWS SigV4 test vector and rejects one tampered request with `SignatureDoesNotMatch`.

## Design Depth

Checked: `Promotes to ADR: yes` appears on decision [1] and decision [2] only. Both pass the promotion gate. Decision [1] fixes where identity resolution lives (the adapter entry points, never the scan UDF). Decision [2] fixes the single owner of the storage-source precedence. The eight other entries are `no`. The quick diagnostic in plan.md answers every question with evidence.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md task 2.3 + decision-log.md § [1]
- Issue: The substitution is two statements (call `resolve_aws_identity`, then rebuild `storage` with `storage_block`), repeated at two entry points. `StorageCredentialSource::AssumedRole` then asserts that `storage` holds a session, and nothing enforces it. A third entry point that skips the call makes both format readers read the base key pair, and `scan_storage_for` then seals the base key pair. The enterprise setup fails closed. A setup whose base identity has direct access silently breaks "no object-storage read SHALL use the base key pair". One `ConnectionCreds` type also holds both the stated and the effective credentials, so a second `resolve_aws_identity` call would chain roles with the session as its base.
- Fix: In task 2.3, add one async method on `ResolvedConnectionConfig`, for example `assume_role(self) -> Result<Self, UdfError>`, that performs the call and the rebuild. Make both entry points call it. Its doc comment states that it runs once per request and that every storage reader depends on it.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: decision-log.md § [2] Decision + vs-adapter/connection-credentials-assume-role/spec.md § "One function owns the storage credential source precedence", WHEN step
- Issue: Decision [2] says "The five sites are" and then names six: the Iceberg split, the Delta split, the access-delegation header, the Unity Catalog temporary-credentials request, `scan_storage_for`, and the `path_style` guard. The code has five checks. The temporary-credentials request sits inside the Delta reader's split (`delta_format_reader.rs:93-114`).
- Fix: In decision [2] and in the scenario's WHEN step, name the Unity Catalog temporary-credentials request as part of the Delta reader's split, so that each list has five entries.

#### [PROSE_BLOAT] ADVISORY
- Location: vs-adapter/connection-credentials-assume-role/spec.md § Background, bullets 4 and 8
- Issue: Bullet 4 already states "`DurationSeconds` defaults to `3600`". Bullet 8 repeats the 3600-second lifetime and restates the THEN step "MUST NOT carry `DurationSeconds`".
- Fix: Delete Background bullet 8.
