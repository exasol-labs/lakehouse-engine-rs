# Decision Log: change-vended-storage-resolution-scheme-driven

Tracking issue: **#276** (`feat(storage): vended SAS credentials`), slice D of six (A-F) for Azure Data Lake Storage Gen2 (`abfss://`) support.

## Interview

### Round 1

**Q1:** The selected credential source may carry several `adls.sas-token.<host>` keys (multi-account vended payload). How should the Azure arm pick one?

**A1:** "Match the anchor's host." Recover `<host>` from each key and select the one equal to the host of the anchor location (the table's own `abfss://…@<host>/…` URI). No match → no vended SAS.

**Q2:** When a vended SAS is selected, where does `Adls { account_name }` come from?

**A2:** "Derive from the recovered host." `account-name` = the label before `.dfs.` in the recovered host, overriding any static CONNECTION value, so the vended (account, SAS) pair is internally consistent.

**Q3:** What should the Azure arm do when the selected source carries no `adls.sas-token.*` key at all?

**A3:** Originally "Return base unchanged". REVERSED in round 2 — see A5/A8. The answer is now a clear error.

**Q4:** How far should this slice's blast radius reach beyond the Azure arm in `vended.rs`? (multi-select)

**A4:** Selected: (a) Fix the S3-only anchor docs — `resolve_vended_storage`'s doc comment and the call-site comment in `file_resolution.rs` both assert the anchor "must be an S3 URI" / "can never match an S3 prefix"; with Azure vending live these are wrong, correct the prose. (b) Discharge the #276 tracked exception — remove the inline `#276` citations from the specs. (c) Leave E2E to #278 — ship unit-test coverage only; vended-SAS E2E against a live Lakekeeper stays in issue #278 (slice F). NOT selected: recording the static-SAS-TTL ceiling.

### Round 2

The user then reconsidered, verbatim:

> "I think we have to reconsider the previous answer. When vended credentials is used, S3 or Azure fields in the CONNECTION are irrelevant, and shouldn't be read or used. The variant in resolve_vended_storage should rely only on the URI scheme. When vended credentials are requested and not provided, a clear error should be reported"

**Q5:** "Missing vended credentials → clear error" and "CONNECTION storage credentials are irrelevant under vending" apply to the S3 arm as much as Azure. How far does this slice go?

**A5:** "Both arms now." Apply the rule uniformly: under vending, neither arm reads static credentials and both error when the response vends none. This knowingly changes shipped S3 behaviour (the current spec guarantees an empty/absent vended access key preserves the static one) and puts the Glue and Lakekeeper vended E2E paths in the blast radius.

**Q6:** Under vending, which CONNECTION storage fields are "irrelevant" — only the secrets, or the transport config too?

**A6:** "Every storage field ignored." Under vending the whole storage block comes from the `loadTable` response; an absent `s3.endpoint` or `client.region` is NOT backfilled from the CONNECTION.

**Q7:** The variant now comes from the anchor URI scheme. What happens for a scheme that is neither S3 nor ADLS — or an empty table location?

**A7:** "Error naming the scheme." Only `s3://`/`s3a://` → S3 and `abfss://`/`abfs://` → Adls; anything else, including an empty location that falls back to the warehouse URI, is a `UdfError::User` naming the unsupported scheme. No silent default.

**Q8:** Record the static-SAS-TTL ceiling in the spec?

**A8:** "Skip it." Declined twice. No spec bullet about SAS TTL expiry.

## Design Decisions

### [1] Two selectors on disjoint inputs, not one selection site

- **Decision:** `storage_block` stays the STATIC selector (reads the CONNECTION credential shape); `resolve_vended_storage` becomes the VENDED selector (reads the `loadTable` response's table location scheme). Exactly one site — the `use_vended_credentials` branch in `resolve_file_list` — chooses between them. `vs-adapter/storage-backend-enum`'s "ONLY place a backend is SELECTED FROM INPUT" clause is superseded by a "EXACTLY TWO selectors on disjoint inputs, ONE decision point" clause.
- **Alternatives:**
  - *Keep one selector by passing the URI scheme into `storage_block`.* Rejected: the scheme is known only after `loadTable`, which runs later, once per table, and never at all on the `createVirtualSchema` path. A single selector would have to be deferred past `loadTable` for every path including the non-vended one, and would still have no answer for a request that resolves no table.
  - *Have the vended arm mutate the payload `storage_block` returned.* Rejected by the feature's own existing clause: "reaching into the payload to finish construction is exactly the knowledge this feature removes."
  - *Declare the scheme switch "not really a selection" and leave the one-site clause intact.* Rejected: it selects a variant from an input. Leaving the fence textually intact and factually false is worse on a credentials path than superseding it.
- **Rationale:** the count that matters is DECISION POINTS, not selector functions. Two selectors that read disjoint inputs, run on mutually exclusive branches, and are chosen at one site cannot disagree — there is no path on which both run and no value either can override. Naming them both, and bounding each, is honest; collapsing them would require deferring CONNECTION parsing past table load for every request.
- **Promotes to ADR:** yes

### [2] Delete the `base: &StorageBackend` parameter

- **Decision:** `resolve_vended_storage(result, anchor, allow_http) -> Result<StorageBackend, UdfError>`. It takes no `StorageBackend`, no `ConnectionCreds`, and no other CONNECTION-derived value. The `allow_http` parameter added by decision [3] is a virtual-schema property, not a CONNECTION field, so it does not reopen what this deletion closed.
- **Alternatives:** keep `base` and read only `allow_http` from it; add a fourth enum method `StorageBackend::allow_http()` so the read goes through the enum rather than a `match`.
- **Rationale:** with the parameter gone, "no CONNECTION storage field is read under vending" is enforced by the signature rather than by auditing the body, and no future edit can quietly reintroduce a per-field preservation rule. Both alternatives leave exactly one CONNECTION-derived read under vending — the coupling A6 exists to remove — and the enum-accessor variant additionally has to answer `allow_http` for an `Adls` base whose account credentials are irrelevant, which is a question with no meaningful answer. The interface also gets narrower while absorbing more work: a whole `StorageBackend` parameter is exchanged for one boolean, six per-field absence conventions drop to none, and variant selection previously made by the caller's caller moves inside.
- **Promotes to ADR:** yes

### [3] `ALLOW_HTTP` stays the operator's consent gate for plaintext transport

- **Decision:** thread the resolved `ALLOW_HTTP` virtual-schema property into `resolve_vended_storage` as its own `bool` parameter. A vended plain-`http://` `s3.endpoint`, and an `abfs://` anchor, are honoured only when it is true, and otherwise return a `UdfError::User` naming the plaintext scheme and the `ALLOW_HTTP` property.
- **Alternatives:** derive `allow_http` from the vended endpoint's scheme (this plan's ORIGINAL choice, reversed in round 1 of plan review); keep reading it off the `base` backend; add a `StorageBackend::allow_http()` accessor.
- **Rationale:** the derivation was a security regression in the DEFAULT configuration, and the default is the secure one. `crates/lakehouse-engine/src/adapter/mod.rs:190` defaults `allow_http` to false when the property is absent, so the shipped rule permits plaintext to NO endpoint; the derivation would have permitted it to any endpoint a catalog names as `http://`. A misconfigured or compromised catalog could then have vended `s3.endpoint = http://…` and put the vended STS access key, secret key, and session token in cleartext, with the operator holding no control and receiving no error. The plan's original "strictly narrower than the shipped behaviour" claim held only when `ALLOW_HTTP` was already true; it is withdrawn. Threading the value costs a 4-tuple on `resolve_connection_config` (2 call sites) and one `bool` parameter on four functions — and that follows the convention already established there, since `s3_max_connections` and the DataFusion tuning knobs are virtual-schema scalars travelling the same path.
- **Rationale caveat:** removing `ALLOW_HTTP` from the vended path was a PLANNER decision, not an interview outcome. A6's subject is the CONNECTION ("S3 or Azure fields **in the CONNECTION** are irrelevant"), and `allow_http` is not a CONNECTION field — it reaches `storage_block` from `PROP_ALLOW_HTTP`. The interview never discussed it, so citing A6 as authority for dropping it was an overreach; this decision restores it.
- **Boundary note, reconciling with [4]:** [4] rejects conditioning the address rule on `s3.path-style-access` because that would put the engine's builder logic inside the catalog crate. Deriving `allow_http` there would have been the same leak by the same test — "plaintext is permitted" is a policy the engine's store builder enforces (`scan/object_store.rs:163`), not a property of the vended payload. Threading the value in resolves both: it is resolved outside the catalog crate and only consumed there.
- **Promotes to ADR:** yes

### [4] A vended payload naming neither a region nor an endpoint is an error

- **Decision:** the S3 arm requires a non-empty `client.region` OR a non-empty `s3.endpoint`, else it returns a `UdfError::User` naming both keys.
- **Alternatives:** leave an absent region empty and let the object-store builder do whatever it does; require `client.region` unconditionally; make the requirement conditional on `s3.path-style-access` too, mirroring what `register_side_store` actually consumes; **narrow the error to the case that actually misroutes** — emit it only when the payload names neither value AND the anchor is an AWS-hosted `s3://` URI, which would neither misroute nor create a blocking unverifiable premise, since Lakekeeper places its store by endpoint and is unaffected either way. That last option was rejected because it makes the rule turn on recognising "an AWS-hosted URI", a host-pattern test the plan has no way to state without encoding AWS endpoint conventions in the catalog crate — and a wrong answer fails silently in exactly the direction the rule exists to prevent.
- **Rationale:** this extends A5's own principle ("requested but not satisfied is an error") from the keys to the address, which A6 makes necessary — once the CONNECTION cannot backfill, those two values are the only ones that can place the store. Leaving the region empty is the silent failure the whole change exists to remove: an AWS store would be addressed as a region-less URL. Requiring the region unconditionally was rejected because it would break the Lakekeeper vended path, which places its store by endpoint. Conditioning on `path_style` was rejected as information leakage: it would encode the engine's builder logic inside the catalog crate, whereas "a payload must name a region or an endpoint" is a property of the payload alone.
- **Rationale caveat:** this is a rule the interview did not ask for. It is recorded here as a planner decision rather than an interview outcome, and it is the reason task 4.2 exists.
- **Promotes to ADR:** yes

### [5] Glue's vended `client.region` is a named risk with a blocking verification obligation, not an assumption

- **Decision:** state plainly in `plan.md` § Impact and in the spec Background that whether AWS Glue vends `client.region` was NOT verified, and add an assertion to the Glue vended E2E (task 4.2) that makes the assumption falsifiable.
- **Alternatives:** assume Glue vends it (documented behaviour, plausible); special-case the SigV4 path to keep backfilling the static region; block the plan until an AWS account is available.
- **Rationale:** CLAUDE.md requires a claimed capability or limitation to be verified against a live system, not assumed from documentation or memory. The Glue path is reachable only through the env-gated `cloud-e2e` suite against a live AWS account, which skips in the planning environment, so verification is genuinely unavailable here — and saying so is the required answer, not a gap. Special-casing SigV4 would reintroduce a static backfill on exactly one auth mode, which is the two-rules-on-one-path defect A5 removes. Decision [4] already converts the unknown from a silent misroute into a clear error, which makes shipping-pending-verification safe rather than reckless.
- **Promotes to ADR:** no

### [6] Static storage credentials under vending are ignored, not rejected

- **Decision:** `validate_creds` gains no rule. A CONNECTION supplying storage credentials alongside `use_vended_credentials = true` is accepted, and those fields are simply never read into the effective scan storage. The existing Azure-and-S3 mixed-fields rejection still applies.
- **Alternatives:** reject the combination as a misconfiguration; warn.
- **Rationale:** the user's framing is that the fields are irrelevant, and a SigV4 CONNECTION legitimately carries a static `access_key`, `secret_key`, and `region` for catalog signing while vending its storage credentials — rejecting would break exactly that shape. The mixed-fields guard is kept because that input declares two incompatible intents whether or not either is read; relaxing it would trade a loud, cheap error for a class of misconfiguration nobody can observe. Saying "ignored" explicitly in the spec is the substance of this decision: an unstated irrelevance is the same silent ambiguity the surrounding rules exist to prevent.
- **Promotes to ADR:** no

### [7] `account_name` derived from the host's first dot-separated label

- **Decision:** derive `account_name` from the recovered host's first dot-separated label, and error when the host has no label to read.
- **Alternatives:** split literally on `.dfs.` per A2's wording; keep any static CONNECTION `account_name`; emit an empty `account_name`.
- **Rationale:** the first-label reading agrees with A2 exactly on the `<account>.dfs.core.windows.net` form the interview named, and additionally covers the `blob.core.windows.net` and `*.fabric.microsoft.com` host forms `MicrosoftAzureBuilder::with_url` also accepts — which a literal `.dfs.` split would silently fail on. It is also what `object_store` itself does: its `with_url` splits the host once on `.` and treats the first label as the account. Emitting an empty `account_name` was rejected because `vs-adapter/storage-backend-enum` records `adls.account-name` as a deliberate wrong-account guard, and an empty value disarms it.
- **Promotes to ADR:** no

### [8] The ADLS key spelling is an implementation convention, and the spec says so

- **Decision:** record that the Iceberg REST OpenAPI spec enumerates AWS configuration keys and NO ADLS ones, so `adls.sas-token.<host>` is the Iceberg Java `AzureProperties` convention (live-verified against a real Lakekeeper response) read under the spec's `additionalProperties: string` allowance — not normative REST-spec text.
- **Alternatives:** present the key as spec-mandated; omit the provenance.
- **Rationale:** verified by fetching `apache/iceberg` `open-api/rest-catalog-open-api.yaml` (main) and searching it for `adls.`, which returns nothing; the `LoadTableResult` description's only key enumeration is `## AWS Configurations`. Claiming spec authority for a key the spec never names would be exactly the from-memory assertion CLAUDE.md forbids. The two rules that ARE normative — read `storage-credentials` before `config`, and choose the longest matching `prefix` — are quoted verbatim in the spec delta and are now exercised for ADLS for the first time, from the same single `select_credential_source` the S3 arm uses.
- **Promotes to ADR:** no

### [9] Scheme-driven selection is grounded in the Iceberg table spec, not merely convenient

- **Decision:** justify reading the backend off the table location by quoting `apache/iceberg` `format/spec.md`: `location` is "The table's base location", required in v1, v2, and v3; optional in v4 but "Must be an absolute path when present", with "When the `location` field is not present (v4 and later), the table location must be provided"; and "**Relative path** -- A path string that does not start with a URI scheme", so an absolute location is scheme-bearing by definition.
- **Alternatives:** justify it only as "the scheme is the only place the backend is knowable".
- **Rationale:** CLAUDE.md requires any plan touching scanning or schema/credential handling to be checked against the Iceberg spec with the normative section quoted rather than recalled. The quotes turn A7's error case from arbitrary strictness into spec consistency: a scheme-less or absent location is a malformed (v1-v3) or incompletely-provided (v4) catalog response, so erroring is the spec-faithful response. This plan introduces no Iceberg deviation and fixes none; it changes no file pruning, snapshot or manifest reading, delete-file handling, or type mapping.
- **Promotes to ADR:** no

### [10] Test edits are bounded by a published disposition table

- **Decision:** `plan.md` § Test Disposition names all 27 existing tests that exercise `resolve_vended_storage` and classifies each KEEP / RESTATE / REPLACE / DELETE, under two invariants: no source-selection assertion (longest prefix, matched-entry-authoritative, no per-key fallback to the flat `config` map) may weaken, and no vending-DISABLED assertion may change.
- **Alternatives:** let the implementer decide test-by-test; delete the failing tests and write fresh ones.
- **Rationale:** these tests encode exactly the preservation rules A6 removes, so they are SUPPOSED to change — which is precisely the situation in which unbounded test editing becomes "editing the gate into agreement with the delta". Publishing the disposition up front makes each edit auditable against a stated reason, and pinning the two invariants keeps the Iceberg-compliance evidence and the untouched disabled path as a real characterization gate.
- **Promotes to ADR:** no

### [11] Issue #276's "additive, S3 unaffected" framing is rejected in the plan text

- **Decision:** `plan.md` § Impact states the real blast radius — a breaking change to a shipped credentials path — rather than inheriting the issue's claim.
- **Alternatives:** keep the issue's framing; split into an additive Azure-only slice plus a follow-up strict-rule slice.
- **Rationale:** the framing is false once the variant follows the scheme, because a vended-only Azure CONNECTION cannot reach an Azure arm that `storage_block` selects. Splitting was rejected because the additive half is unreachable dead code on its own: the Azure arm would only ever run for a CONNECTION carrying static Azure credentials, which is not the vended use case. Shipping the two halves separately would mean shipping a slice whose only reachable behaviour is the one being deleted.
- **Promotes to ADR:** no

### [12] Vended-SAS E2E stays in issue #278

- **Decision:** unit-test coverage only for the Azure arm; live vended-SAS E2E against a Lakekeeper ADLS warehouse remains issue **#278** (slice F). Declared a Non-Goal in `plan.md`.
- **Alternatives:** pull the vended-SAS E2E forward into this slice.
- **Rationale:** A4 selected this boundary explicitly. `e2e-harness/azure-e2e-harness` already records that its vended sibling case "joins this same target and this same stack in a later slice", so the deferral is named in a spec rather than left implicit. The consequence is stated rather than hidden: "no scan-side change is needed for a vended SAS" is a code-level reading of `register_side_store`'s Azure arm, and `plan.md` § Verification Obligations records that #278's E2E is what discharges it.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Per-side scheme selection is discarded for a join's dimension side

- **Finding:** `HIDDEN_DEPENDENCY` BLOCKER. `resolve_one_join_side` (`joins/planning.rs:340`) resolves each side's own `effective_storage`, but `join_fan_out_scan_spec` (`joins/sql_builders.rs:556`) keeps only `primary.effective_storage` as the spec's single `CommonScanSpec.storage`, and `StoreRegistration.backend` (`scan/object_store.rs:107-110`) is documented as a whole-spec value. That collapse was variant-safe only because both sides previously took their variant from one `storage_block` output. Verified against the code. An `s3://` fact joined to an `abfss://` dimension would run the S3 arm over the dim's Azure files, taking the Azure host as a bucket name; two `abfss://` sides on different accounts would read the dim through the fact's account and host-matched SAS, defeating the ADLS arm's whole design. `validate_sides_share_one_store` cannot catch either — it fires only when two sides SHARE a registry key, and different schemes produce different keys. The original plan's § Impact never named the join path.
- **Direction change:** added task 2.2 (plan-time guard in `plan_join`, immediately after the per-side resolution loop, comparing variant and — for ADLS — `account_name`, returning `UdfError::User` naming no credential value), a `DELTA:NEW` scenario "A join whose sides resolve to different storage backends is rejected at plan time", a Background bullet grounding it in the three cited code sites, a fence clause in `vs-adapter/storage-backend-enum`, a § Verification row, task 3.7's unit test, and a § Impact "Join blast radius" paragraph. Scoped the guard to variant and account ONLY: the same collapse ALSO discards a per-prefix vended *credential* difference, but that is pre-existing (`select_credential_source` already runs per side today), and guarding on full backend equality could break every vended join against a catalog minting per-table STS keys — unverified either way. Recorded as task 2.4, a recommendation to file its own issue, plus verification obligation 3.
- **Promotes to ADR:** yes

### [2] [plan-review] The unverified Glue premise is the whole vended credential set, and the green test masks it

- **Finding:** `UNSTATED_ASSUMPTION` BLOCKER. The plan named only `client.region`. Verified at `crates/lakehouse-engine/tests/cloud_e2e_test.rs:139-159`: `catalog_connection_password_vended` is `catalog_connection_password()` plus one flag, so the vended CONNECTION carries a static `access_key`, `secret_key`, AND `session_token` from the AWS env. The shipped preservation rule preserves all of them, so `cloud_scan_reads_with_vended_credentials` passing today is fully compatible with Glue vending ZERO storage credentials — the scan would read with the test's own static keys. Task 4.2 as written asserted only the address and would have passed while the key pair was absent and the strict rule killed the whole Glue vended path at plan time.
- **Direction change:** extended task 4.2 to assert a non-empty `s3.access-key-id` AND `s3.secret-access-key`, assert the address (`client.region` or `s3.endpoint`), and REPORT `s3.session-token` presence, failing with the absent key name and no credential value. Rewrote `e2e-harness/cloud-e2e-harness`' Background bullet 2 and scenario clauses, `plan.md` § Impact's Glue bullet, and § Verification Obligations item 1 to state that the premise is the whole credential set and that the shipped green test cannot evidence it. Added the third unnamed case: an absent vended `s3.session-token` beside a vended TEMPORARY key pair now yields `None` and fails at read time rather than plan time.
- **Promotes to ADR:** no

### [3] [plan-review] Deriving `allow_http` from the vended endpoint was a security regression

- **Finding:** `NFR_IGNORED` BLOCKER. The plan claimed the derivation was "strictly NARROWER" than the shipped behaviour. Verified false at `crates/lakehouse-engine/src/adapter/mod.rs:190-192`, which defaults `allow_http` to false when `ALLOW_HTTP` is absent — and that default is the secure one. With the property unset, the shipped rule permits plaintext to no endpoint while the derivation would permit it to any endpoint a catalog names as `http://`, so a misconfigured or compromised catalog could put vended STS credentials in cleartext with no operator control and no error. The `abfs://` half of the scheme mapping compounded it: the Azure backend carries no HTTP knob, so a plaintext `abfs://` anchor had nothing gating it at all.
- **Direction change:** reversed the derivation. `ALLOW_HTTP` is threaded in as its own `bool` parameter — a virtual-schema property, not a CONNECTION field, so decision [2]'s deletion of `base` still holds — and gates BOTH a vended plain-`http://` endpoint and an `abfs://` anchor, erroring otherwise. Deleted the "strictly narrower" claim from the spec Background and from decision [3]. Rewrote decision [3] wholesale, added its `Rationale caveat` (the `ALLOW_HTTP` removal was a planner decision, not A6, whose subject is the CONNECTION) and its boundary note reconciling it with [4]. Added task 2.3 for the plumbing, error-path cases to task 3.3, a § Verification manual-testing row, and the corresponding clauses in the two E2E deltas.
- **Promotes to ADR:** yes

### [4] [plan-review] Two `DELTA:CHANGED` blocks were renames the recorder cannot match

- **Finding:** `REQUIREMENT_CONFLICT` BLOCKER. `/speq:spec-merge` defines `DELTA:CHANGED` as "Replace scenario with same name" — confirmed in the skill's marker table — and lists rewriting scenario wording during merge as an anti-pattern. Both renamed blocks had no same-name target: the shipped headings are "Vended S3 credentials **override static credentials** regardless of catalog auth mode" (`:84`) and "…**adopts the vended region**" (`:110`). The merge would append rather than replace, leaving the superseded preservation clauses live at `:90` and `:116-118` — the exact rule this plan removes — sitting beside their replacement in the permanent library on a credentials path. No rename precedent exists in `_recorded/006-*` or `007-*`. The same defect hit `vs-adapter/connection-credentials`' feature-description line, which sits outside every DELTA marker, and `_recorded/007-*` shows the recorder keeping the shipped description rather than adopting a delta's.
- **Direction change:** restored both `DELTA:CHANGED` headings to their exact SHIPPED names, keeping the new clause bodies, so the merge name-matches and REPLACES each scenario — which is what deletes the preservation clauses at `:90` and `:116-118`. The reviewer's alternative, a `DELTA:REMOVED` + `DELTA:NEW` pair, was tried first and rejected on three counts: `speq plan validate` requires GIVEN/WHEN/THEN in every `### Scenario:` block, so a heading-only REMOVED block FAILS validation (observed); there is no `DELTA:REMOVED` precedent anywhere in this repository; and a recorder applying REMOVED but skipping NEW would delete a credentials scenario outright, whereas CHANGED cannot lose it. Each body now opens with a `RENAME PENDING` note, and task 5.3 carries the exact old→new heading text as a record-time edit. Added task 5.2 for the feature-description line with its exact replacement text, and added both to § Dead Code Removal's spec-text rows so `/speq:record` cannot silently drop them.
- **Promotes to ADR:** no

### [5] [plan-review] The "no catch-all `_` arm" clause was unimplementable and self-falsifying

- **Finding:** `AMBIGUOUS_REQUIREMENT` BLOCKER. The clause required no catch-all "so a third backend is a build failure at this site". The selector matches on a URI SCHEME string, not on a `StorageBackend`, so the catch-all is mandatory — A7 itself mandates an error branch for every other scheme — and because the site never matches on the enum, a third variant compiles cleanly here and is left unreachable from vending: the exact silent gap the clause claimed to prevent. The shipped clause it replaced (`:147`) was true only because it matched on the input backend's variants. No pass/fail test could be written for it, and the plan's own standard in decision [1] — "leaving the fence textually intact and factually false is worse on a credentials path" — condemned it.
- **Direction change:** replaced it with a testable pair plus an honest admission: the mapping SHALL be a total function over its input (four accepted schemes yield a backend, every other input including the empty string yields `UdfError::User`, so the catch-all is REQUIRED here); adding a third variant SHALL NOT break this site's build, stated plainly; and a source-level probe in `catalog_public_surface.rs` SHALL assert the selector's source names every `StorageBackend` variant, as the compensating gate. Restated `plan.md`'s "Closed match, no `_` arm" Patterns row as "Total function over the scheme string", added the probe to task 3.5, and added the matching fence clause to `vs-adapter/storage-backend-enum`.
- **Promotes to ADR:** no

### [6] [plan-review] Advisory findings folded in

- **Finding:** ten `ADVISORY` findings, report-only. Six were cheap and clearly correct on lines already being edited, so they were folded in rather than deferred.
- **Direction change:** decision [4] gained the narrowed "AWS-hosted URI only" alternative with its rejection reason; § Impact gained a Databricks Unity Catalog bullet (mission Core Capability 7 reaches this path and no in-repo suite covers it) and `cloud-e2e-harness` a matching Background sentence; the ADLS missing-SAS error clause was amended so the anchor host sits outside the `adls.sas-token` label that `redact_credentials` truncates (`redaction.rs:52`, `:69-76`), with an after-redaction assertion added to task 3.3; three "KEEP unedited" Test Disposition rows became "KEEP — call updated to the new arity and `Result`; every assertion unchanged", since deleting `base` changes every call expression, and decision [10] now says "27 existing tests that exercise" rather than "27 call sites"; task 3.2's unreachable "empty anchor" case became a warehouse-style scheme-less anchor per `file_resolution.rs`'s fallback; § Parallelization made 1.2 → 1.3 and 3.1 → 3.2/3.3/3.4 sequential, since each group edits one region of one file; the "Every consumer holds a backend" § Verification row became "Unit + Review" with its two fence clauses named review-enforced rather than mapped to a probe that cannot count selection sites in another crate; and task 3.8 was added to prove `register_side_store` builds a store for `abfs://` and `s3a://`, since `abfs://` appears nowhere in the repository yet the mapping now admits it. The `PROSE_BLOAT` finding was applied to `plan.md` § Summary and the `connection-credentials` description line.
- **Promotes to ADR:** no

### [7] [plan-review] Round 2 — the withdrawn `allow_http` derivation survived in three places

- **Finding:** `REQUIREMENT_CONFLICT` BLOCKER. Round 1's `[NFR_IGNORED]` security reversal was applied to the decision but not to every consequence, leaving the rejected derivation live in three places — so the finding it protects against was still open. Worst was `vs-adapter/storage-backend-enum/spec.md` Background bullet 13, which MERGES INTO THE PERMANENT LIBRARY verbatim and still said the value "is read off the vended `s3.endpoint`'s scheme", contradicting line 30 of the same file and `pushdown-planning-cloud-credentials` Background bullets 18-20. Second, `plan.md` § Test Disposition prescribed "REPLACE. `allow_http` derived from the vended endpoint's scheme", and task 3.1 applies that table "exactly as tabled" — so the plan ordered an implementer to write a unit test PINNING the behaviour decision [3] reversed as a security regression, in direct conflict with task 1.2. Third, § Impact said the Lakekeeper config "derives `allow_http`". Decision [1]'s own standard condemns all three: leaving a fence textually intact and factually false is worse on a credentials path than having none.
- **Direction change:** replaced `storage-backend-enum` bullet 13 wholesale, retitled "`allow_http` is threaded in as a resolved virtual-schema property, so the vended selector takes one new non-CONNECTION parameter", stating that slice C's "arrives from the `ALLOW_HTTP` VS property" reading is unchanged, that deriving it from the vended endpoint was rejected as a security regression, and that the parameter is not a CONNECTION value. Changed the § Test Disposition row to "REPLACE. `allow_http` comes from the threaded `ALLOW_HTTP` parameter; a vended plain-`http://` endpoint with `allow_http` false errors". Replaced § Impact's "and derives `allow_http`" with the harness's `ALLOW_HTTP = 'true'` satisfying the consent gate, citing `tests/common/e2e_harness.rs:270`.
- **Promotes to ADR:** no

### [8] [plan-review] Round 2 — three shipped Background bullets asserting the removed rule were superseded by nothing

- **Finding:** `REQUIREMENT_CONFLICT` BLOCKER. Round 1's fix closed the scenario-level recording defect — all seven `DELTA:CHANGED` headings were verified to name-match byte for byte — but the same defect survived one level up in the Background. Three bullets in the shipped `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md` assert the removed rule in the PRESENT TENSE, are superseded by no delta bullet, and were named by no task: `:50-51` ("so absence preserves the static values and the Glue vended path is unchanged"), `:58` (the field-for-field guarantee, naming `merge_vended_into_storage`, which § Dead Code Removal deletes), and `:61` ("not yet exercised for ADLS", false the moment the ADLS arm reads the vended SAS and directly contradicted by this plan's own Iceberg-REST-compliance bullet). Task 5.1 named only `:60` and `:62`. Verified all three verbatim in the shipped file. `/speq:record` would have left the permanent spec asserting both rules on a credentials path.
- **Direction change:** added three `SUPERSEDES` bullets to the delta's `DELTA:NEW` Background, each quoting the superseded sentence and stating its replacement — absence means absent; `merge_vended_into_storage` is deleted and the S3 arm constructs from the vended source, with only `select_credential_source` keeping its body verbatim; both REST rules are now exercised for ADLS. Extended task 5.1 to name `:50-51`, `:58`, and `:61` as record-time removals alongside `:60`, `:62`, and `:147`, and added a § Dead Code Removal spec-text row for them.
- **Promotes to ADR:** no

### [9] [plan-review] Round 2 — the join guard's only gate could not be built where the plan put it

- **Finding:** `TRACEABILITY_GAP` BLOCKER. Round 1's join guard (finding [1]) was placed inline in `plan_join`, and task 3.7 asked for a `plan_join` unit test. Verified unbuildable: `plan_join` (`joins/mod.rs:102`) takes `session: &CatalogSession` and reaches the mismatch only by awaiting `resolve_one_join_side` per side (`:126-139`), which calls `resolve_file_list` and performs live catalog I/O — so the two divergent backends are OUTPUTS of that I/O and no unit test can supply them. The guard would have shipped with no falsifiable gate, leaving round-1 finding [1] half-closed on a wrong-store credentials path. The repository already had the answer: `select_broadcast_sides` is documented as "the pure, catalog-free core of side selection so it is unit-testable without a live Iceberg catalog" (`planning.rs:289`), and its tests build `ResolvedJoinSide` values from the `resolved_side` and `sample_storage` fixtures — both verified to exist.
- **Direction change:** rewrote task 2.2 to extract the comparison into a pure `pub(super) fn validate_sides_share_one_backend(sides: &[ResolvedJoinSide]) -> Result<(), UdfError>` in `joins/planning.rs` beside `select_broadcast_sides`, called from `plan_join` immediately after the resolution loop and before the empty-side shortcut, and recorded why the inline placement was rejected. Repointed task 3.7 and the § Verification > Scenario Coverage row to that function, requiring the tests to build sides from the existing `resolved_side` / `sample_storage` fixtures with no catalog session, and updated the § Manual Testing command.
- **Promotes to ADR:** no

### [10] [plan-review] Round 2 — advisory residues folded in, and one factual citation corrected

- **Finding:** two `ADVISORY` residues the user approved, both on lines already being edited. (a) `AMBIGUOUS_REQUIREMENT`: task 1.1 still instructed "select the variant from the anchor's URI scheme with no `_` arm" — the exact clause round 1 refuted and the plan withdrew everywhere else, contradicting § Design > Patterns and delta clause 75, and not compilable, in the one place that is the implementer's primary contract. (b) `COMPLETENESS_GAP`: the compensating variant probe could be written so it cannot fail — a hardcoded `["S3", "Adls"]` list satisfied the clause as written and keeps passing after a third variant is added, which is the precise silent gap the probe exists to prevent.
- **Direction change:** task 1.1 now requires the catch-all arm returning `UdfError::User` naming the unsupported scheme, per the total-function row. Clause 77, `storage-backend-enum` clause 31, and task 3.5 now require the probe to EXTRACT the variant list from `storage.rs`'s `enum StorageBackend` source — reachable through `catalog_public_surface.rs`'s `CATALOG_SOURCES` `include_str!` table, verified at `:38` — and assert each extracted name appears in `vended.rs`, with a new clause stating that a hardcoded list does NOT satisfy the requirement.
- **Also corrected, outside the approved advisory set:** the symbol `join_scan_spec` does not exist anywhere in the repository. It was my own citation error from round 1, appearing six times across `plan.md`, `decision-log.md`, and the cloud-credentials delta. All six now read `join_fan_out_scan_spec` (`joins/sql_builders.rs:556`). Only the name and line number were corrected; the round-2 advisory's broader point — that the collapse is specific to `build_broadcast_join_sql` while `build_side_fan_out_sql` carries each side's own backend, so the guard also rejects a mixed-backend join the unaccelerated fallback would have served — was NOT applied, because it was not in the approved set and the fail-loud guard remains correct either way.
- **Promotes to ADR:** no
