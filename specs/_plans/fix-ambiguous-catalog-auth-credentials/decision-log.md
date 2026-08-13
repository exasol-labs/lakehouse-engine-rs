# Decision Log: fix-ambiguous-catalog-auth-credentials

## Interview

**Q:** Issue #331 already decided the fix: reject a CONNECTION supplying both `token` and `client_id`/`client_secret`. Once `validate_creds` rejects that combination up front, `resolve_catalog_auth` (Iceberg) and `resolve_unity_auth` (Unity) can never actually receive both fields together — their existing if/else precedence order becomes dead-in-practice code. Should the plan also touch those two functions, or leave them untouched?
**A:** (verbatim) "Why should we leave there dead code!!??? Boy-scout principle. Clean that up"

## Design Decisions

### [1] Reject the ambiguous CONNECTION rather than declare a precedence

- **Decision:** Extend `validate_creds` with a rule rejecting a CONNECTION that supplies a `token` together with a complete `client_id`/`client_secret` pair, under both catalog kinds, naming all three fields and leaking no value.
- **Alternatives:** Declare one shared precedence across both kinds — smaller diff, no CONNECTION becomes invalid.
- **Rationale:** Decided in issue #331 and not re-litigated here. Silent precedence is the defect; a declared precedence still resolves an ambiguous credentials input the operator did not intend, and it contradicts the principle already recorded for `validate_creds` rules 2 and 3 ("an undeclared precedence between two credential sets would resolve an ambiguous credentials input silently").
- **Promotes to ADR:** no

### [2] The duplicated mode decision gets ONE owner, not three corrected chains

- **Decision:** Add a crate-private `ConnectionCreds::supplied_catalog_auth()` returning a three-variant `SuppliedCatalogAuth`, declared in `crates/lakehouse-catalog/src/creds.rs`, and collapse `resolve_catalog_auth`, `inject_catalog_auth_props`, and `resolve_unity_auth` onto it.
- **Alternatives:** Restructure each of the three functions in place into its own `match` over the presence tuple — three small local diffs, no new type, no cross-module reach.
- **Rationale:** The user's boy-scout instruction covers the dead precedence; the duplication is why the precedence could be wrong in the first place. One decision with three homes and nothing enforcing agreement is the back-door leakage `/speq:design-philosophy` singles out, and issue #331 IS the moment two of those homes disagreed. Three independently-correct copies still agree only by coincidence. One owner makes the two catalog kinds identical by construction.
- **Promotes to ADR:** no

### [3] Rule 6 fires only when all three fields are present

- **Decision:** The new rule rejects only `token` AND `client_id` AND `client_secret`. A `token` beside half a pair keeps falling to the existing OAuth2-completeness rule with its existing message.
- **Alternatives:** Fire on a `token` plus ANY OAuth2 field, following the "partial OAuth still signals catalog-auth intent" convention `has_catalog_auth` documents. That reports the ambiguity — the larger defect — on the first round-trip for a doubly-malformed CONNECTION.
- **Rationale:** The narrow form changes ZERO recorded error texts. The wide form would replace the message `incomplete_oauth_rejected_no_leak` and the `connection-credentials-catalog-auth` scenario "Incomplete OAuth2 client credentials are rejected naming only the missing field" both pin, for an input that is rejected either way. Verified that the narrow form still closes the accepted-shape set: all five invalid combinations are covered between rule 6 (all three present) and rule 7 (exactly one of the pair), so the classifier's invariant holds. Cost accepted: an operator supplying a token plus half a pair fixes the pair first and sees the ambiguity error on a second attempt.
- **Promotes to ADR:** no

### [4] Rule 6 sits after the SigV4 rules; its order against rule 7 is immaterial

- **Decision:** Call the new helper between `validate_sigv4_creds` and `validate_oauth2_creds`.
- **Alternatives:** Ahead of the SigV4 rules, so the ambiguity is reported before a signing-mode defect.
- **Rationale:** Rule 4 already rejects every CONNECTION combining `use_sigv4` with any catalog-auth field, so an ambiguous set that also enables SigV4 never reaches rule 6 — placing rule 6 first would change the error text `sigv4_and_catalog_auth_mutually_exclusive` pins. Against rule 7 the placement is behaviourally inert (the two rules' inputs are disjoint), so position 6 was chosen for reading order alone: a conflict of intent before an incompleteness within one intent.
- **Promotes to ADR:** no

### [5] The classifier stays infallible; the user-facing error stays in the engine

- **Decision:** `supplied_catalog_auth` returns a total `SuppliedCatalogAuth`, never a `Result`. `validate_creds` produces the operator error independently, testing the three fields inline as its four sibling helpers do.
- **Alternatives:** Have the classifier return `Result` and let `validate_creds` consume it, giving the exclusivity rule literally one home.
- **Rationale:** The two answer different questions — "which mode do these fields describe" versus "which combinations are user errors, named field by field". A fallible classifier would push an error path into `resolve_unity_auth`, which is deliberately synchronous and infallible so that building a session issues no request, and would either move operator-facing error text into the catalog crate or add a translation layer. Precedent for the split: `has_catalog_auth` already lives on `ConnectionCreds` and is consumed by `validate_creds` across the same crate edge for rule 4.
- **Promotes to ADR:** no

### [6] Validation-rejected shapes classify as the no-auth mode

- **Decision:** Every combination rules 6 and 7 reject maps to `SuppliedCatalogAuth::Unauthenticated`, in an arm that lists those patterns explicitly and whose comment names the two rules as its enforcer.
- **Alternatives:** A fourth `Malformed` variant; a `debug_assert!` on the invariant; `unreachable!()`.
- **Rationale:** Every such shape is unreachable once the rules hold, so the arm exists only for a path that bypassed validation. `Malformed` forces three consumers to handle a case none can act on and would need to carry supplied field names to be useful. `unreachable!()` panics inside a UDF. A bare `debug_assert!` guards test builds only. Classifying as no-auth means a bypassed validation fails on the catalog's own 401 — loud, local, and never a silently chosen credential. This is the plan's one behavioural change on unreachable input, and it is stated in the spec rather than left implicit.
- **Promotes to ADR:** no

### [7] No bare `_` wildcard arm in the classifier

- **Decision:** Enumerate all eight presence patterns across three arms; no catch-all.
- **Alternatives:** `_ => Unauthenticated`, which is shorter and reads the same today.
- **Rationale:** An invisible residual is how the invalid combinations went unnoticed. An exhaustive pattern set makes a future change to the three fields a compile error at the one site that owns the decision. The test's five rejected-shape rows exist for the same reason: a `_ => StaticToken(..)` slip would pass every other assertion.
- **Promotes to ADR:** no

### [8] AWS SigV4 stays outside the shared classification

- **Decision:** `SuppliedCatalogAuth` carries three variants and no SigV4 case. `resolve_catalog_auth` keeps its `use_sigv4` early return ahead of the match, with a doc comment stating the two are mutually exclusive upstream rather than ranked.
- **Alternatives:** A four-variant enum covering all catalog-auth strategies, which is the most faithful model of "SigV4 and token/OAuth are mutually exclusive strategies".
- **Rationale:** Only one of the three consumers can ever see `use_sigv4`: `validate_kind_preconditions` rejects it under the Unity kind, and `inject_catalog_auth_props` is reached only on the unsigned path. A shared variant would force two consumers to handle a case that cannot reach them — the shallow-interface cost `/speq:design-philosophy` warns about, paid for a fidelity no caller uses.
- **Promotes to ADR:** no

### [9] `oauth2_server_uri` and `scope` stay outside the classifier

- **Decision:** The `ClientCredentials` variant carries `client_id` and `client_secret` only; each consumer reads and defaults the other two fields itself.
- **Alternatives:** Carry all five auth fields, so the OAuth2 mode arrives fully described.
- **Rationale:** The two consumers default them differently — Unity derives `{host}/oidc/v1/token` from the CONNECTION address and defaults the scope to `all-apis`, while the Iceberg REST path leaves both properties unset for the catalog to fill. A shared carrier would impose one consumer's default on the other, or carry values neither trusts. The exclusivity question does not involve either field.
- **Promotes to ADR:** no

### [10] `non_empty` collapses to one declaration rather than gaining a third

- **Decision:** Move `non_empty` into `creds.rs` as `pub(crate)`, delete both existing byte-identical declarations, and repoint 16 call sites.
- **Alternatives:** Add a third copy beside the classifier; or keep two and import one into `creds.rs`.
- **Rationale:** The classifier is a third consumer of a two-line predicate that already has two copies. `vs-adapter/catalog-crate-structure` already names `non_empty` in its crate-private set, so the relocation changes no visibility and needs no surface delta beyond recording the single-declaration rule. Clippy's `-D warnings` enforces it: a retained unused copy is `dead_code`.
- **Promotes to ADR:** no

### [11] `has_catalog_auth` is left untouched, and no new predicate joins it

- **Decision:** Rule 6's helper tests the three fields inline. `has_catalog_auth` keeps its current meaning and its single rule-4 consumer.
- **Alternatives:** Reuse `has_catalog_auth` for rule 6; add a sibling `has_ambiguous_catalog_auth()` on `ConnectionCreds`.
- **Rationale:** `has_catalog_auth` deliberately answers "does this CONNECTION intend catalog auth at all", counting a PARTIAL pair — the opposite reading from rule 6, which needs a COMPLETE pair beside a token. Merging them would break rule 4. A new one-line accessor with exactly one caller is the shallow-module red flag `vs-adapter/adapter-module-structure` already records, and all four existing `validate_*_creds` helpers test their fields inline.
- **Promotes to ADR:** no

### [12] `validate_exclusive_catalog_auth_creds`, not `validate_pat_oauth_creds`

- **Decision:** Name the fifth helper `validate_exclusive_catalog_auth_creds`.
- **Alternatives:** `validate_pat_oauth_creds`, as the planning brief suggested.
- **Rationale:** "PAT" is Databricks vocabulary; the Iceberg REST side calls the same field a static bearer token, and the rule is kind-independent. The chosen name keeps the `validate_*_creds` shape of its four siblings and states the rule rather than one kind's word for one field.
- **Promotes to ADR:** no

### [13] Five spec deltas, with one normative home and four citations

- **Decision:** Put both new scenarios in `vs-adapter/connection-credentials-catalog-auth`; give `connection-credentials`, `rest-catalog-oauth-auth`, `unity-catalog-auth`, and `catalog-crate-structure` citing deltas only.
- **Alternatives:** One delta on `connection-credentials` (which owns `validate_creds`' rule list) and nothing else.
- **Rationale:** `connection-credentials-catalog-auth` already declares the three modes mutually exclusive and is the feature `connection-credentials` already delegates the catalog-auth modes to, so the rule and the classifier belong there. The four citing deltas exist because each owns a recorded statement that would otherwise become stale: `connection-credentials`' rule enumeration would read as exhaustive while omitting the new rule and its Unity scenario's "at most one" precondition would stay an assumption; `rest-catalog-oauth-auth` and `unity-catalog-auth` own the two resolvers whose precedence disappears; `catalog-crate-structure` owns the crate-private item set and names `non_empty` in it. Per CLAUDE.md a known gap must never be silent.
- **Promotes to ADR:** no

### [14] One E2E test, Iceberg kind only

- **Decision:** Add `create_vs_ambiguous_catalog_auth_errors_no_secret` to `crates/lakehouse-engine/tests/e2e_scan_test.rs`. Cover both kinds in unit tests; cover one kind end to end.
- **Alternatives:** Unit tests only, as issue #331's scope names ("Tests: the ambiguous input, per kind"); or an E2E case per kind.
- **Rationale:** No unit test can show the error reaches an operator through Exasol and the deployed `.so` — `StubCtx` stops short of that. The pattern already exists (`create_vs_unreachable_catalog_errors_no_secret`), so the cost is one test. A second E2E for the Unity kind would re-prove the same kind-independent rule through a second stack, which is why per-kind coverage stays at the unit level.
- **Promotes to ADR:** no

### [15] No Iceberg-table-spec check, stated rather than skipped

- **Decision:** Record in `plan.md` § Apache Iceberg spec compliance that the check does not apply, and why.
- **Alternatives:** Omit the section, since the plan touches no scan path.
- **Rationale:** CLAUDE.md conditions the check on scanning, pushdown, or schema/type handling. This plan changes CONNECTION validation and catalog-auth strategy selection, reads no manifest, and moves no `ScanSpec` field. An absent section reads as an oversight; a stated non-applicability reads as a decision.
- **Promotes to ADR:** no

### [16] No adversarial plan review for this plan

- **Decision:** Ship the plan without a `plan-reviewer` round.
- **Alternatives:** The standard `/speq:plan` review loop.
- **Rationale:** Explicit orchestrator override on direct user instruction. Recorded here because it changes the plan's evidence trail: § Test Disposition, § Dead Code Removal, and the eight-shape closure table in § Design carry the checks a review round would otherwise have contributed, and no `review/round-N.md` file exists for this plan.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. No plan-review round ran for this plan; see Design Decision [16]. -->
