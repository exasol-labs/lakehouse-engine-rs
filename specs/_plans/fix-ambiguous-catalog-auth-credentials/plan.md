# Plan: fix-ambiguous-catalog-auth-credentials

## Summary

Reject a CONNECTION that supplies a `token` together with a complete `client_id`/`client_secret` pair, under both catalog kinds, naming all three fields and leaking no value. Then give the token-versus-OAuth mode decision ONE owner so the three functions that each re-derived it — and that answered the ambiguous input two different ways — stop encoding a precedence that no longer exists; fixes issue #331.

## Design

### Context

Two defects, and the second is the reason the first was possible.

**The ambiguity.** `validate_creds` (`crates/lakehouse-engine/src/adapter/connection.rs`) accepts a CONNECTION supplying a non-empty `token` alongside a non-empty `client_id` and `client_secret`. Nothing declares which mechanism applies, and the two catalog kinds answer OPPOSITELY:

| Function | Order | Winner when both are supplied |
|---|---|---|
| `resolve_catalog_auth` (`crates/lakehouse-catalog/src/auth.rs:207`) | `use_sigv4`, then the pair, then `token` | OAuth2 |
| `inject_catalog_auth_props` (`crates/lakehouse-catalog/src/auth.rs:21`) | the pair, then `token` | OAuth2 |
| `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs:168`) | `token`, then the pair | personal access token |

That contradicts the stated principle behind `validate_creds` rules 2 and 3, which exist because "an undeclared precedence between two credential sets would resolve an ambiguous credentials input silently". `inject_catalog_auth_props`' own doc comment already asserts "Token and client-credentials are mutually exclusive by construction" — a claim its own body contradicts.

**The duplication.** One decision — which of three mutually exclusive modes a credential set describes — has THREE homes with nothing enforcing agreement. That recurrence is the back-door leakage `/speq:design-philosophy` singles out, and it is not incidental to the bug: two independent copies of a decision are exactly how one copy comes to disagree with another. Rejecting the ambiguous input alone would leave three chains free to drift apart again, each still reading as "check A first, then B" about two cases that can no longer co-occur.

- **Goals** — the ambiguous CONNECTION is a named user error under both kinds; one owner for the mode decision; no branch anywhere implies an order between `token` and the pair; every existing error text and every accepted CONNECTION byte-identical.
- **Non-Goals** — no declared precedence (issue #331 rejected it: silent precedence is the problem, not the solution); no new CONNECTION field; no change to the SigV4 rules, the Azure rules, the `warehouse` rule, or `has_catalog_auth`; no `Result` added to `resolve_unity_auth`; no change to the crate's public surface; no change to the OAuth2 grant mechanics, the token endpoint defaults, or the cache-and-refresh behaviour.

### Decision

Close the accepted-shape set with one validation rule, then classify once and match everywhere.

#### Architecture

```
                    read_connection (engine)
                            │
                    validate_creds ── rule 6 (NEW): reject token + complete pair
                            │
                            ▼
              accepted shapes: {}  ·  {token}  ·  {client_id, client_secret}
                            │
                            ▼
   ConnectionCreds::supplied_catalog_auth()  ←── the ONE mode decision (catalog crate)
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
 resolve_catalog_auth  inject_catalog_    resolve_unity_auth
  (auth.rs)             auth_props         (unity/auth.rs)
                        (auth.rs)
```

**Rule 6, and why it requires all three fields.** The new helper rejects only when `token`, `client_id`, AND `client_secret` are all present. A `token` alongside HALF a pair is already rejected by rule 7 (OAuth2 completeness) with its existing message, so the two rules are disjoint and no recorded error text changes. Together they close the accepted set to exactly three shapes:

| `token` | `client_id` | `client_secret` | Outcome |
|---|---|---|---|
| – | – | – | accepted → no auth |
| ✓ | – | – | accepted → static bearer |
| – | ✓ | ✓ | accepted → OAuth2 client credentials |
| – | ✓ | – | rejected, rule 7 (unchanged message) |
| – | – | ✓ | rejected, rule 7 (unchanged message) |
| ✓ | ✓ | ✓ | rejected, rule 6 (NEW) |
| ✓ | ✓ | – | rejected, rule 7 (unchanged message) |
| ✓ | – | ✓ | rejected, rule 7 (unchanged message) |

That closure is what makes the classifier's invariant real. The rule sits after the SigV4 rules so every SigV4 error stays byte-identical, and before rule 7 for readability only — the two are disjoint, so their relative order has no behavioural consequence.

**The classifier.** `ConnectionCreds` gains one crate-private method in `crates/lakehouse-catalog/src/creds.rs`, beside the fields it reads and beside the existing `has_catalog_auth`:

```rust
pub(crate) enum SuppliedCatalogAuth<'a> {
    Unauthenticated,
    StaticToken(&'a str),
    ClientCredentials { client_id: &'a str, client_secret: &'a str },
}

impl ConnectionCreds {
    pub(crate) fn supplied_catalog_auth(&self) -> SuppliedCatalogAuth<'_> { /* see below */ }
}
```

Its body is a single `match` over the presence tuple `(non_empty(&self.token), non_empty(&self.client_id), non_empty(&self.client_secret))`, with **no bare `_` wildcard arm**. Two arms name the two authenticated shapes as patterns; one arm lists every remaining pattern explicitly and carries the comment naming rules 6 and 7 as its enforcer. Shape, not literal text:

```rust
match (non_empty(&self.token), non_empty(&self.client_id), non_empty(&self.client_secret)) {
    (None, Some(client_id), Some(client_secret)) =>
        SuppliedCatalogAuth::ClientCredentials { client_id, client_secret },
    (Some(token), None, None) => SuppliedCatalogAuth::StaticToken(token),
    // Every remaining shape is one `validate_creds` rejects before a session
    // exists: rule 6 for a token beside a complete pair, rule 7 for a partial
    // pair. Reaching one here means validation was bypassed, so the honest
    // answer is "this describes no auth mode" — the request then fails on the
    // catalog's own 401 rather than on a credential the operator never
    // unambiguously supplied.
    (None, None, None) | (None, Some(_), None) | (None, None, Some(_))
    | (Some(_), Some(_), None) | (Some(_), None, Some(_)) | (Some(_), Some(_), Some(_)) =>
        SuppliedCatalogAuth::Unauthenticated,
}
```

Enumerating the rejected shapes rather than writing `_` is the point: a bare wildcard is how the invalid combinations became invisible in the first place, and an exhaustive pattern set turns a future field change into a compile error at the one site that owns the decision.

**The three consumers each become a `match` with no order.** `resolve_catalog_auth` keeps its `use_sigv4` early return ahead of the match — SigV4 and catalog token/OAuth are strategies rule 4 already rejects in combination, so that branch chooses between two upstream-exclusive strategies rather than ranking them, and its doc comment must say so instead of listing a "precedence". `resolve_unity_auth` stays synchronous and infallible: the classifier returns a total value, so no error path is introduced.

**`oauth2_server_uri` and `scope` stay out of the classifier.** Both consumers of the OAuth2 mode default them differently — Unity derives `{host}/oidc/v1/token` from the CONNECTION address and defaults the scope to `all-apis`, while the Iceberg REST path leaves both properties unset for the catalog to fill. Carrying them into the shared type would force one default onto the other consumer or leave the type carrying values neither trusts.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single owner for one decision | `ConnectionCreds::supplied_catalog_auth` | Removes the back-door leakage of the mode decision across three functions in two module trees. The disagreement between two of those copies IS issue #331 |
| Closed accepted-shape set, then classify | rule 6 + rule 7 → classifier | The classifier can state a real invariant only because validation leaves exactly three shapes; either half alone is incomplete |
| Exhaustive patterns, no `_` arm | the classifier's `match` | A wildcard is what let the invalid combinations stay invisible; explicit patterns make a future field change a compile error at the owning site |
| Decision beside the data | `creds.rs`, next to `has_catalog_auth` | It reads `ConnectionCreds`' own three fields and serves two sibling module trees; declaring it in either would make one depend on the other's internals |
| Total value, no new error path | `SuppliedCatalogAuth` | Keeps `resolve_unity_auth` synchronous and infallible, and keeps the user-facing error text in the engine where every other credential error lives |

`/speq:design-philosophy` Quick Diagnostic, answered for the one new abstraction: a one-sentence summary names its responsibility ("which of the three mutually exclusive catalog-auth modes did this CONNECTION supply"); calling it is easier than re-deriving three `Option` presence checks in the right order — which is precisely the reimplementation that produced two different answers; changing how it decides forces no edit outside `creds.rs`; its doc comment states the invariant it depends on and names the rules that enforce it rather than restating its name; it is the sole owner of that decision; the module boundary is unchanged and both consumers are in-crate; no tactical shortcut is taken; and it depends on nothing — pure computation over three `Option<String>` fields. It is not a pass-through: it collapses three field reads, the empty-string-means-absent rule, and the pair-completeness rule into one named answer.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Reject the ambiguous CONNECTION | Declare one shared precedence across both kinds | Decided in issue #331 and not re-litigated. Silent precedence is the defect; a declared one still resolves a misconfiguration the operator did not intend |
| Also restructure all three resolvers | Add the rule and leave the chains as dead-in-practice code | Directed by the user in the clarifying interview ("Why should we leave there dead code!!??? Boy-scout principle"). The chains would still read as a precedence between two cases that cannot co-occur, and three independent copies of one decision are how the copies came to disagree |
| Rule 6 fires only on all three fields | Fire on a `token` plus ANY OAuth2 field | The narrow form changes ZERO recorded error texts: a token beside half a pair is already rejected by rule 7. The wide form would replace rule 7's message for an input rejected either way — one fewer operator round-trip on a doubly-malformed CONNECTION, paid for with a spec'd error text |
| One shared classifier owning the mode | Restructure each of the three functions in place into its own `match` | Three correct copies still agree by coincidence, which is the state that produced the bug. One owner makes the two kinds identical by construction |
| Classifier in the catalog crate, validation error in the engine | Have the classifier return `Result` and let `validate_creds` consume it | The two answer different questions: "which mode do these fields describe" versus "which combinations are user errors, named field by field". A `Result` classifier would push an error path into `resolve_unity_auth` (today infallible and synchronous by design) and either move operator-facing error text into the catalog crate or add a translation layer. Precedent: `has_catalog_auth` already lives on `ConnectionCreds` and is consumed by `validate_creds` across the same edge |
| Invalid shapes classify as `Unauthenticated` | A fourth `Malformed` variant; or `debug_assert!`; or `unreachable!()` | Every invalid shape is unreachable once rules 6 and 7 hold, so the arm exists for a bypassed-validation path only. A `Malformed` variant forces three consumers to handle a case none can act on; `unreachable!()` panics inside a UDF; a bare `debug_assert!` guards only test builds. Classifying as no-auth fails on the catalog's own 401 — loud, local, and never a silently chosen credential |
| AWS SigV4 stays outside the classifier | A fourth `Sigv4` variant covering all four strategies | Only the Iceberg strategy resolution can see `use_sigv4`: the Unity kind rejects it in `validate_kind_preconditions` and the prop-injection path never receives it. A shared variant would force two consumers to handle an unreachable case |
| `oauth2_server_uri` and `scope` stay outside the classifier | Carry all five auth fields in the `ClientCredentials` variant | The two consumers default both fields differently — Unity derives the endpoint from the CONNECTION address, Iceberg REST leaves the property unset. A shared carrier would impose one consumer's default on the other |
| `non_empty` collapses to one declaration in `creds.rs` | Add a third copy beside the classifier; or keep two and import one | Two byte-identical two-line copies already exist; the classifier would make three. `vs-adapter/catalog-crate-structure` already names `non_empty` in its crate-private set, so the relocation changes no visibility |
| `has_catalog_auth` left untouched | Reuse it for rule 6, or fold both into one predicate | It answers "does this CONNECTION intend catalog auth at all", deliberately including a PARTIAL pair, and serves rule 4 alone. Rule 6 needs the opposite reading — a COMPLETE pair beside a token. Merging them would break rule 4 |
| No new `ConnectionCreds` predicate for rule 6 | Add `has_ambiguous_catalog_auth()` beside `has_catalog_auth` | All four existing `validate_*_creds` helpers test their fields inline; a one-line accessor used by exactly one caller is the shallow-module red flag `vs-adapter/adapter-module-structure` already records |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/connection-credentials-catalog-auth | CHANGED | `vs-adapter/connection-credentials-catalog-auth/spec.md` |
| vs-adapter/connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/rest-catalog-oauth-auth | CHANGED | `vs-adapter/rest-catalog-oauth-auth/spec.md` |
| vs-adapter/unity-catalog-auth | CHANGED | `vs-adapter/unity-catalog-auth/spec.md` |
| vs-adapter/catalog-crate-structure | CHANGED | `vs-adapter/catalog-crate-structure/spec.md` |

`connection-credentials-catalog-auth` is the normative home for both new scenarios: it already declares the three modes mutually exclusive, and it is the feature `connection-credentials` already delegates the catalog-auth modes to. The other four deltas CITE it rather than restating the rule — `connection-credentials` because it owns the rule LIST, `rest-catalog-oauth-auth` and `unity-catalog-auth` because they own the two resolvers whose precedence disappears, and `catalog-crate-structure` because it owns which items are `pub` and which are crate-private.

`vs-adapter/unity-catalog-client` and `vs-adapter/pushdown-catalog-session` get NO delta, deliberately: they own the callers (`UnityCatalogSession::new`, `CatalogSession::resolve`), whose signatures, error behaviour, and request shapes are untouched.

## Impact

No accepted CONNECTION changes behaviour, and no existing error message changes text. A CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair now fails at `createVirtualSchema` or `pushdown` with an error naming all three fields, where it previously succeeded and silently authenticated one way on the Iceberg REST kind and the other way on the native Unity kind.

Breaking change, narrowly: an operator running such a CONNECTION today gets a working virtual schema and will get a hard error after this ships. That is the intended correction — the CONNECTION declares two credential intents and the engine picked one without saying which. The fix names both and asks the operator to remove one. No repository fixture, harness, or E2E CONNECTION supplies all three fields: a sweep of every `ConnectionCreds` and `CatalogConnectionPassword` construction site found exactly one, `serializes_catalog_auth_fields_when_present` in `crates/lakehouse-engine/tests/common/stack.rs`, which asserts only JSON serialization and never reaches validation. `cloud_e2e_test.rs` already builds its password token-XOR-OAuth.

No wire-format, capability, generated-SQL, or public-surface change. No credential value reaches any error message, returned SQL, or log line on any path this plan touches.

## Apache Iceberg spec compliance

CLAUDE.md requires an Iceberg-table-spec check for any plan touching scanning, pushdown, or schema/type handling. This plan touches none of the three: it changes CONNECTION credential validation and the selection of a REST-catalog authentication strategy. No file planning, manifest reading, predicate pushdown, projection, delete application, or type mapping is read or modified, and `ScanSpec` gains and loses no field. The check is therefore not applicable, and the omission is recorded here rather than left silent.

## Dependencies

None added; no dependency version changes.

## Implementation Tasks

1. **Reject the ambiguous CONNECTION.**
   1.1 Add the failing test `token_with_complete_oauth_pair_is_rejected_under_both_kinds` to `crates/lakehouse-engine/src/adapter/connection_tests.rs`, per § Test Disposition. Confirm it fails against the unmodified `validate_creds` before task 1.2.
   1.2 Add `fn validate_exclusive_catalog_auth_creds(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError>` to `crates/lakehouse-engine/src/adapter/connection.rs`, following the four existing `validate_*_creds` helpers' shape and error style: return `UdfError::User` naming `token`, `client_id`, and `client_secret` when all three are `is_some()`, and `Ok(())` otherwise. Use `is_some()` rather than a non-empty check, matching `validate_oauth2_creds` — `parse_creds` already resolves every field through `nonempty_str`, so `Some("")` is unreachable from a CONNECTION. Call it from `validate_creds` between `validate_sigv4_creds` and `validate_oauth2_creds`. Update `validate_creds`' doc comment: insert the new rule as 6, renumber OAuth2 completeness to 7, and state both why the new rule sits after the SigV4 rules (every SigV4 error stays byte-identical) and that the two are disjoint because rule 6 requires all three fields while rule 7 requires exactly one of the pair. Do not touch `has_catalog_auth`.

2. **One shared mode classifier.**
   2.1 Add the failing test `supplied_catalog_auth_names_one_mode_per_field_shape` to `crates/lakehouse-catalog/src/creds_tests.rs`, per § Test Disposition. It will not compile until 2.2 declares the API; that non-compiling state is the red phase.
   2.2 In `crates/lakehouse-catalog/src/creds.rs` declare `pub(crate) enum SuppliedCatalogAuth<'a>` and `pub(crate) fn ConnectionCreds::supplied_catalog_auth(&self) -> SuppliedCatalogAuth<'_>` with the exhaustive no-wildcard `match` and the doc comments from § Design — the enum's doc states that the three variants are the three shapes `validate_creds` accepts, and the method's doc names rules 6 and 7 as the enforcer that makes the invariant true. In the same task, MOVE `non_empty` into `creds.rs` as `pub(crate)`, DELETE both existing declarations (`crates/lakehouse-catalog/src/auth.rs:60-63` and `crates/lakehouse-catalog/src/unity/auth.rs:205-207`), and repoint every use — 16 in `auth.rs`, 5 in `unity/auth.rs` as counted before task 3 — per § Dead Code Removal. Task 3 later deletes the subset of those uses that sit inside the mode chains, so the import must be correct at both stages; the surviving consumers are `oauth2_client_credentials_grant`, `redact_catalog_auth_error`, and each OAuth arm's `oauth2_server_uri`/`scope` reads. Add no `pub` item and edit no re-export in `lib.rs`. [expert]

3. **Repoint the three consumers onto the classifier.**
   3.1 Add the three failing consumer-pin tests, per § Test Disposition: `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape` and `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` in `crates/lakehouse-catalog/src/auth_tests.rs`, and `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` in `crates/lakehouse-catalog/src/unity/auth_tests.rs`. Expect the `resolve_catalog_auth` case to fail by ATTEMPTING A NETWORK GRANT against the unmodified code rather than by a clean assertion mismatch — today that shape enters the OAuth2 branch. A network error is a legitimate red; do not conclude the test is broken.
   3.2 Rewrite `resolve_catalog_auth` and `inject_catalog_auth_props` (`crates/lakehouse-catalog/src/auth.rs`) to `match creds.supplied_catalog_auth()` with one arm per mode. Keep `resolve_catalog_auth`'s `use_sigv4` early return ahead of the match and replace its numbered "Precedence mirrors …" doc list with a statement that SigV4 and catalog token/OAuth are mutually exclusive upstream, so the branch chooses between two exclusive strategies rather than ranking them. `oauth2_client_credentials_grant` keeps its `&ConnectionCreds` signature; the `ClientCredentials { .. }` arm binds nothing. Rewrite `inject_catalog_auth_props`' doc comment so its "Token and client-credentials are mutually exclusive by construction" claim CITES the enforcing rule and the classifier instead of asserting an unenforced invariant. Read `oauth2_server_uri` and `scope` inside the OAuth2 arm exactly as before. [expert]
   3.3 Rewrite `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs`) to `match creds.supplied_catalog_auth()` with one arm per mode, keeping the function synchronous and infallible and keeping the `{host}/oidc/v1/token` endpoint default and the `all-apis` scope default inside the OAuth arm. Replace its doc comment's mode-selection sentence with one naming the shared classifier as the selector.

4. **Correct the resolver test that documents a forbidden combination.**
   4.1 In `crates/lakehouse-catalog/src/auth_tests.rs`, rename `resolve_catalog_auth_precedence_non_network_branches` to `resolve_catalog_auth_selects_one_strategy_per_non_network_shape`, delete the `sigv4_creds.token = Some(BEARER_TOK.into());` line and the "regardless of any token also being set" comment — that fixture is a SigV4-plus-token CONNECTION rule 4 rejects — and restate the numbered "Precedence #N" comments as the shapes they actually drive. Keep every assertion's substance: SigV4 → `Sigv4`, token alone → `Bearer`, nothing → `None`.

5. **Stop the harness from modelling a rejected CONNECTION.**
   5.1 In `crates/lakehouse-engine/tests/common/stack.rs`, split `serializes_catalog_auth_fields_when_present` into two cases — one supplying `token` alone, one supplying `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` — keeping every existing assertion but distributing it. The test asserts only JSON serialization and passes either way; it is split because a fixture supplying all three now models a CONNECTION the adapter rejects, and a future reader would copy it as a valid shape.

6. **End-to-end proof that the rejection reaches an operator.**
   6.1 Add `create_vs_ambiguous_catalog_auth_errors_no_secret` to `crates/lakehouse-engine/tests/e2e_scan_test.rs`, beside `create_vs_unreachable_catalog_errors_no_secret` and following its shape: build a `CatalogConnectionPassword` supplying sentinel `token`, `client_id`, and `client_secret`, create the CONNECTION, attempt `CREATE VIRTUAL SCHEMA`, and assert the response is an error whose text names `token`, `client_id`, and `client_secret` and contains none of the three sentinel VALUES. Iceberg kind only — the Unity kind is covered per-kind by task 1.1's unit test, and one deployed-adapter proof is what this task adds. Requires a manually started Docker stack (`docker compose up -d --wait exasol`); without it every DB-backed test FAILS rather than skipping.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 → 1.2 |
| Group B | 2.1 → 2.2 |
| Group C | 5.1 |
| Group D | 3.1 → (3.2 · 3.3) |
| Group E | 4.1 · 6.1 |

Sequential dependencies:
- Groups A, B, and C are mutually independent and may run concurrently: A touches only the engine's validation, B only the catalog crate's `creds.rs` plus the two `non_empty` declarations, C only a test-harness assertion.
- Group B → Group D. Tasks 3.2 and 3.3 call the classifier B declares; within D they touch disjoint files and may run concurrently after 3.1.
- Group D → Group E. Task 4.1 edits tests of the code 3.2 rewrites. Task 6.1 needs task 1.2's rule to be in the built `.so`, so it also runs after A.
- **All groups ship in ONE commit.** The classifier's doc comment names rules 6 and 7 as its enforcer, so landing task 2 without task 1 would put a false invariant in the tree — the same defect this plan removes from `inject_catalog_auth_props`.

Tasks 2.2 and 3.2 are tagged `[expert]`. 2.2 introduces a type whose correctness rests on an exhaustive pattern set over three `Option`s plus a cross-module helper relocation touching 21 call sites in two module trees; 3.2 is a cross-file rewrite whose "same behaviour for every accepted shape" claim has to be reasoned about, and whose `use_sigv4` branch must not be restored as a precedence. The remaining eight tasks copy an established pattern in one file each.

## Test Disposition

| Test | File | Disposition |
|---|---|---|
| `token_with_complete_oauth_pair_is_rejected_under_both_kinds` | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | NEW. Drives `read_connection` through `StubCtx` twice over one password supplying `warehouse`, sentinel `token`, sentinel `client_id`, and sentinel `client_secret` — once under `CatalogKind::IcebergRest`, once under `CatalogKind::UnityCatalogNative` — asserting each returns `Err`, that each message names `token`, `client_id`, and `client_secret`, and that neither contains any of the three sentinel VALUES. Adds a third case supplying `token` and `client_id` only, asserting the message still names the MISSING `client_secret` — that is the disjointness assertion, and it fails if rule 6 is widened to fire on any OAuth2 field |
| `supplied_catalog_auth_names_one_mode_per_field_shape` | `crates/lakehouse-catalog/src/creds_tests.rs` | NEW. Drives `supplied_catalog_auth` over all EIGHT presence combinations of `token`, `client_id`, and `client_secret`, asserting `(–,✓,✓)` → `ClientCredentials` with both values carried, `(✓,–,–)` → `StaticToken` with the value carried, and each of the other six → `Unauthenticated`. Adds three empty-string cases — `Some("")` in each field position — asserting each classifies as absent. The five rejected-shape rows are what pin the no-wildcard arm: a `_ => StaticToken(..)` slip passes every other row |
| `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape` | `crates/lakehouse-catalog/src/auth_tests.rs` | NEW. Builds `creds_no_auth()` with `token`, `client_id`, and `client_secret` all set and asserts `resolve_catalog_auth` returns `CatalogAuth::None` WITHOUT contacting the network. Red against today's code by attempting the OAuth2 grant. This is the test that pins the Iceberg consumer onto the classifier: an in-place `if pair … if token …` chain returns `Bearer` here |
| `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` | `crates/lakehouse-catalog/src/auth_tests.rs` | NEW. Same credential shape; asserts the props map receives no `token`, `credential`, `oauth2-server-uri`, or `scope` entry. Red against today's code, which injects `credential` |
| `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | NEW. Same credential shape; asserts `UnityAuth::None`. Red against today's code, which returns `Pat`. Together with the two rows above, this is the assertion that would have caught issue #331: three consumers, one shape, one answer |
| `resolve_catalog_auth_precedence_non_network_branches` | `crates/lakehouse-catalog/src/auth_tests.rs` | RENAMED to `resolve_catalog_auth_selects_one_strategy_per_non_network_shape` and CORRECTED by task 4.1. Its SigV4 case loses `sigv4_creds.token = Some(BEARER_TOK.into())` and the "regardless of any token also being set" comment: that fixture is a CONNECTION rule 4 rejects, and the comment asserts a precedence over an input validation forbids. Every assertion's substance survives — SigV4 → `Sigv4`, token alone → `Bearer(BEARER_TOK)`, nothing → `None` |
| `serializes_catalog_auth_fields_when_present` | `crates/lakehouse-engine/tests/common/stack.rs` | SPLIT by task 5.1 into a token-mode case and an OAuth2-mode case. Every existing assertion is kept, distributed across the two. Passes before and after — the split removes a fixture that models a CONNECTION the adapter now rejects |
| `create_vs_ambiguous_catalog_auth_errors_no_secret` | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | NEW E2E. The only test that proves the rejection reaches an operator through Exasol and the deployed `.so` rather than through `StubCtx`. Asserts an error response naming all three fields and containing none of the three sentinel values |
| `incomplete_oauth_rejected_no_leak` | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | UNCHANGED. Both of its passwords supply exactly one of `client_id` and `client_secret` and NO `token`, so rule 6 never fires and both messages stay byte-identical. Verified, not assumed — this is the test rule 6's placement was chosen to protect |
| `sigv4_and_catalog_auth_mutually_exclusive` | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | UNCHANGED. Its two passwords are SigV4-plus-token and SigV4-plus-pair; neither supplies all three, and rule 4 fires ahead of rule 6 in both |
| `mixed_azure_and_s3_credential_fields_are_rejected`, `unity_kind_validation_skips_warehouse_and_rejects_sigv4`, `validation_is_parameterized_by_catalog_kind`, `unity_connection_reuses_existing_auth_fields`, and every `has_catalog_auth_*` test | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | UNCHANGED. None supplies all three auth fields, and `has_catalog_auth` is untouched |
| `build_rest_catalog_sets_token_prop`, `build_rest_catalog_sets_credential_and_oauth_props`, `build_rest_catalog_no_auth_props_when_no_auth` | `crates/lakehouse-catalog/src/auth_tests.rs` | UNCHANGED. Each supplies exactly one mode's fields, and each mode's injected props are byte-identical through the classifier |
| `build_rest_catalog_empty_token_injects_nothing` | `crates/lakehouse-catalog/src/auth_tests.rs` | UNCHANGED, and load-bearing: `token = Some("")` must keep classifying as absent. It is the pre-existing pin on the empty-string rule the classifier now owns |
| `pat_is_applied_as_bearer_verbatim`, `oauth_m2m_mints_bearer_via_client_credentials`, `oauth_token_is_cached_and_refreshed_before_expiry`, `oauth_grant_missing_expires_in_is_a_clear_error`, `oauth_grant_zero_expires_in_is_a_clear_error`, `unauthenticated_mode_sends_no_authorization_header`, `failed_oauth_grant_is_credential_safe_error` | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | UNCHANGED. Each supplies exactly one mode's fields; the grant mechanics, defaults, cache, and error redaction are untouched |
| `catalog_public_surface.rs`, whole file | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | UNCHANGED. No item's `pub` visibility changes and no re-export moves; both added items are crate-private |
| `debug_redacts_every_secret_bearing_field` | `crates/lakehouse-catalog/src/creds_tests.rs` | UNCHANGED. Constructs all three auth fields but only formats `Debug`; it never classifies |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `non_empty` in `crates/lakehouse-catalog/src/auth.rs` and in `crates/lakehouse-catalog/src/unity/auth.rs` | Both byte-identical declarations replaced by one in `creds.rs`. A retained unused copy fails `cargo clippy --workspace --all-targets -- -D warnings` as `dead_code` |
| Branch chain | `resolve_catalog_auth`'s `if pair … if let Some(token) …` sequence (`auth.rs`) | Replaced by a match over the shared mode. The order it encodes ranks two shapes rule 6 makes non-co-occurrent |
| Branch chain | `inject_catalog_auth_props`' `if let (Some, Some) … else if let Some(token) …` (`auth.rs`) | Same |
| Branch chain | `resolve_unity_auth`'s `if let Some(token) … if let (Some, Some) …` sequence (`unity/auth.rs`) | Same, in the opposite order — which is the defect |
| Doc-comment claim | `resolve_catalog_auth`'s numbered "Precedence mirrors `inject_catalog_auth_props`" list (`auth.rs`) | Ranks the pair against the token, and ranks `use_sigv4` against both. Neither pair of cases can co-occur once rules 4 and 6 hold |
| Doc-comment claim | `resolve_unity_auth`'s mode-selection sentence (`unity/auth.rs`) | Describes a per-field selection the classifier now owns |
| Test fixture line | `sigv4_creds.token = Some(BEARER_TOK.into());` and the "regardless of any token also being set" comment in `resolve_catalog_auth_precedence_non_network_branches` (`auth_tests.rs`) | Models a SigV4-plus-token CONNECTION rule 4 rejects, and documents a precedence over an input validation forbids |
| Test fixture shape | the combined `token` + `client_id` + `client_secret` password in `serializes_catalog_auth_fields_when_present` (`tests/common/stack.rs`) | Models a CONNECTION the adapter now rejects |

`inject_catalog_auth_props`' "Token and client-credentials are mutually exclusive by construction" sentence is NOT removed. Issue #331 offered "either make the claim true or delete it"; task 1.2 makes it true, so task 3.2 keeps it and adds the citation naming what enforces it.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| vs-adapter/connection-credentials-catalog-auth — A CONNECTION supplying both a static token and OAuth2 client credentials is rejected | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `token_with_complete_oauth_pair_is_rejected_under_both_kinds` |
| vs-adapter/connection-credentials-catalog-auth — A CONNECTION supplying both a static token and OAuth2 client credentials is rejected | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_ambiguous_catalog_auth_errors_no_secret` |
| vs-adapter/connection-credentials-catalog-auth — One classifier decides the catalog-auth mode and every consumer reads it | Unit | `crates/lakehouse-catalog/src/creds_tests.rs` | `supplied_catalog_auth_names_one_mode_per_field_shape` |
| vs-adapter/connection-credentials-catalog-auth — One classifier decides the catalog-auth mode and every consumer reads it | Unit | `crates/lakehouse-catalog/src/auth_tests.rs` and `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape`, `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape`, `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` |
| vs-adapter/connection-credentials — Credential validation is parameterized by the resolved catalog kind | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `validation_is_parameterized_by_catalog_kind` (unchanged, covers the behaviour-unchanged clause) and `token_with_complete_oauth_pair_is_rejected_under_both_kinds` (covers the added both-kinds clause) |
| vs-adapter/connection-credentials — A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `unity_connection_reuses_existing_auth_fields` (unchanged) and `token_with_complete_oauth_pair_is_rejected_under_both_kinds` (covers the added enforcement clause) |
| vs-adapter/rest-catalog-oauth-auth — Static bearer token is attached to unsigned catalog requests | Unit | `crates/lakehouse-catalog/src/auth_tests.rs` | `build_rest_catalog_sets_token_prop` (unchanged) and `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` (covers the no-ordering clause) |
| vs-adapter/rest-catalog-oauth-auth — OAuth2 client credentials drive the catalog client-credentials grant | Unit | `crates/lakehouse-catalog/src/auth_tests.rs` | `build_rest_catalog_sets_credential_and_oauth_props` (unchanged) and `resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape` (covers the shared-classifier clause) |
| vs-adapter/unity-catalog-auth — A personal access token is applied as the bearer verbatim | Unit | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `pat_is_applied_as_bearer_verbatim` (unchanged) and `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` (covers the no-ordering clause) |
| vs-adapter/unity-catalog-auth — OAuth machine-to-machine mints a bearer token via the client-credentials grant | Unit | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `oauth_m2m_mints_bearer_via_client_credentials` (unchanged; its `oauth2_server_uri`/`scope` defaults prove those fields stayed out of the classifier) |
| vs-adapter/catalog-crate-structure — The crate exposes the concept-level API and hides every mechanism step | Integration | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | the whole external-vantage probe, unchanged — it compiles only if every enumerated `pub` item is still `pub`, and it names neither added item because both are crate-private. The single-`non_empty`-declaration clause is enforced by `cargo clippy --workspace --all-targets -- -D warnings`, which rejects an unused duplicate as `dead_code` |

Unit rather than integration for every scenario except the E2E row, which the speq default permits only for pure computation with no I/O. `validate_creds` and its five helpers are total functions over a parsed `serde_json::Value`; `supplied_catalog_auth` is a total function over three `Option<String>` fields; `inject_catalog_auth_props` writes an in-memory `HashMap`; `resolve_unity_auth` is synchronous and issues no request. The two `resolve_catalog_auth` rows assert the NON-network branches only, exactly as the pre-existing test in that file does — the OAuth2 branch's grant is covered by the mock-server tests already in `auth_tests.rs`. The one E2E row exists because no unit test can prove the error reaches an operator through Exasol and the deployed `.so`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/connection-credentials-catalog-auth | `cargo test -p lakehouse-engine --lib adapter::connection` | All `adapter::connection` tests pass, including `token_with_complete_oauth_pair_is_rejected_under_both_kinds`; every pre-existing test in the module passes with no edit |
| vs-adapter/connection-credentials-catalog-auth | `cargo test -p lakehouse-catalog --lib creds` | `supplied_catalog_auth_names_one_mode_per_field_shape` passes across all eight shapes and the three empty-string cases |
| vs-adapter/rest-catalog-oauth-auth | `cargo test -p lakehouse-catalog --lib auth::` | All `auth` tests pass, including the two new consumer pins and the renamed strategy-selection test |
| vs-adapter/unity-catalog-auth | `cargo test -p lakehouse-catalog --lib unity::auth` | All Unity auth tests pass, including `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` |
| vs-adapter/catalog-crate-structure | `grep -rn 'fn non_empty' crates/lakehouse-catalog/src/` | Exactly one line, in `crates/lakehouse-catalog/src/creds.rs` |
| vs-adapter/catalog-crate-structure | `grep -rn 'pub enum SuppliedCatalogAuth\|pub fn supplied_catalog_auth' crates/lakehouse-catalog/src/` | No output — neither item reaches the crate's public surface |
| vs-adapter/connection-credentials-catalog-auth | `docker compose up -d --wait exasol` then `make test-e2e` | `create_vs_ambiguous_catalog_auth_errors_no_secret` passes: `CREATE VIRTUAL SCHEMA` returns an error naming `token`, `client_id`, and `client_secret`, with none of the three sentinel values in the message. Check the exit code, not just the tail — `make test-e2e | tail` masks it |
| vs-adapter/connection-credentials | `python3 -c "import re,pathlib; [print(p) for p in pathlib.Path('crates').rglob('*.rs') for m in re.finditer(r'(ConnectionCreds\|CatalogConnectionPassword)\s*\{', p.read_text()) if re.search(r'\btoken:\s*Some', p.read_text()[m.start():m.start()+900]) and re.search(r'\bclient_id:\s*Some', p.read_text()[m.start():m.start()+900])]"` | No output — no fixture anywhere supplies both a `token` and a `client_id`. Before this plan the sweep printed exactly one hit, `tests/common/stack.rs`, which task 5.1 splits |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `docker compose up -d --wait exasol` then `make test-e2e` | 0 failures; verify the exit code |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| Spec validation | `speq plan validate fix-ambiguous-catalog-auth-credentials` | pass |
| Issue link | the implementing commit body ends with `Closes #331` | per CLAUDE.md § Feature tracking |
