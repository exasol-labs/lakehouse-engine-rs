# Code Review Findings: fix-ambiguous-catalog-auth-credentials

## Summary
- Files reviewed: 11
- Total findings: 5 (standard: 4, expert: 1)

### Verification notes (no fix required)

- **The task-3.2 deviation from § Test Disposition is justified and lost no coverage.** The deleted
  case (c) of `build_rest_catalog_sets_credential_and_oauth_props`
  (`crates/lakehouse-catalog/src/auth_tests.rs`) asserted `credential` IS injected and `token` is
  NOT, for the exact `token` + complete-pair shape rule 6 now rejects — after the rewrite that shape
  injects nothing, so the case could not be kept as written. Its two assertions are both re-covered:
  the new `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` asserts the
  corrected expectation for that same shape, and "OAuth mode must NEVER set token" survives verbatim
  in cases (a) and (b). The old case (d) was correctly renumbered to (c). The plan's Test Disposition
  row still reads UNCHANGED for that test and is now stale; `plan.md` is outside this review's
  changed-files scope, so no fix task is raised for it.
- **No other coverage dropped as a side effect of the parallel agents.** Across all 11 files the diff
  deletes exactly three assertion lines: the two from the case above, and one
  `assert_eq!(parsed["token"], "bearer-token")` in `tests/common/stack.rs` that reappears verbatim in
  the split-out `serializes_token_auth_field_when_present`. No `#[test]`/`#[tokio::test]` function was
  removed; one was renamed as the plan directed.
- **The diffs are structurally clean.** No orphaned braces, duplicated fragments, half-applied
  renames, or stale imports — the two `non_empty` deletions closed cleanly, both consumers import the
  relocated helper, all 12 surviving `non_empty` call sites resolve, and the `[`ConnectionCreds::supplied_catalog_auth`]`
  intra-doc link in `unity/auth.rs` resolves against the `use crate::ConnectionCreds` already in that
  file. Nothing reads like a symbolic-vs-manual edit mismatch.

## Standard fixes

### crates/lakehouse-engine/src/adapter/connection_tests.rs

#### [MISSING_BOUNDARY_TEST] The rule-6/rule-7 disjointness case asserts nothing that distinguishes the two rules
- Location: lines 1043-1055, the `pw_partial` case of `token_with_complete_oauth_pair_is_rejected_under_both_kinds`
- Issue: the case exists to prove rule 6 does NOT fire on `token` + `client_id` alone — the test's own
  doc comment says "it fails if rule 6 were widened to fire on a token beside any single OAuth2 field".
  It does not. Its only assertion is `msg.contains("client_secret")`, and rule 6's message
  (`"...these are mutually exclusive, remove one: token, client_id, client_secret"`,
  `connection.rs:249-256`) contains `client_secret` just as rule 7's message
  (`"...OAuth2 client credentials require both client_id and client_secret; missing field: client_secret"`,
  `connection.rs:265-268`) does. A widened rule 6 passes this case unchanged, so the boundary between
  the two rules is untested.
- Fix: In `crates/lakehouse-engine/src/adapter/connection_tests.rs`, in the `pw_partial` case of
  `token_with_complete_oauth_pair_is_rejected_under_both_kinds`, add two assertions after the existing
  `client_secret` one: `assert!(msg.contains("missing field: client_secret"), "rule 7 must fire, not rule 6: {msg}")`
  and `assert!(!msg.contains("mutually exclusive"), "rule 6 must not fire on a token beside half a pair: {msg}")`.

### crates/lakehouse-catalog/src/creds.rs

#### [MISSING_DESIGN_INTENT] The classifier's invariant does not name what makes `is_some()` and `non_empty` equivalent
- Location: lines 123-132 (`supplied_catalog_auth` doc comment)
- Issue: the doc asserts "Exactly one mode is ever describable because `validate_creds` rejects every
  other shape". That holds only because the two sides use the same notion of "supplied" — but rule 6
  (`validate_exclusive_catalog_auth_creds`, `crates/lakehouse-engine/src/adapter/connection.rs:246`)
  tests `is_some()` while this classifier tests `non_empty`. For a directly-constructed
  `ConnectionCreds` with `token: Some("")` plus a complete pair the two disagree: rule 6 rejects it,
  the classifier names `ClientCredentials`. `creds_tests.rs` deliberately pins that disagreement
  (`empty_token` case). The invariant survives only because `parse_creds`
  (`crates/lakehouse-engine/src/adapter/connection.rs:330-332`) resolves `token`, `client_id`, and
  `client_secret` through `nonempty_str`, so `Some("")` is unreachable from a real CONNECTION — and
  nothing in either doc comment says so. That is the same defect shape this plan removed from
  `inject_catalog_auth_props`: an invariant asserted without naming its enforcer.
- Fix: In `crates/lakehouse-catalog/src/creds.rs`, extend the `supplied_catalog_auth` doc comment with
  a sentence stating that rule 6 tests field presence while this method tests non-emptiness, and that
  the two coincide because the engine's `parse_creds` normalizes every empty credential field to
  `None` before validation, so `Some("")` never reaches either.

#### [OUTDATED_COMMENT] `non_empty`'s doc claims a universality its own file contradicts
- Location: lines 162-166
- Issue: the doc says "every catalog-auth reader agrees on that by calling this rather than testing
  `is_some()`". `ConnectionCreds::has_catalog_auth`, 40 lines above in the same file, is a catalog-auth
  reader and tests `is_some()` on all three of `token`, `client_id`, and `client_secret` — deliberately,
  per its own doc. The claim is false as written.
- Fix: In `crates/lakehouse-catalog/src/creds.rs`, reword the second paragraph of `non_empty`'s doc
  comment to scope the claim to the mode decision — state that the mode classifier treats an empty
  field as absent by calling this, and that `has_catalog_auth` deliberately does not, because it asks
  whether catalog auth was INTENDED rather than which mode was supplied.

### crates/lakehouse-engine/tests/common/stack.rs

#### [INLINE_COMMENT] Floating `//` comment explains two tests but is attached to neither
- Location: lines 464-465
- Issue: a bare `//` block sits between `omits_catalog_auth_fields_when_absent` and
  `serializes_token_auth_field_when_present`, explaining why BOTH split-out tests exist. No other test
  in `mod catalog_connection_password_tests` carries a comment in that form, and rustdoc/`cargo doc`
  associate it with nothing, so the rationale is invisible from either test it justifies.
- Fix: In `crates/lakehouse-engine/tests/common/stack.rs`, delete the two-line `//` comment at lines
  464-465 and give both `serializes_token_auth_field_when_present` and
  `serializes_oauth2_auth_fields_when_present` a `///` doc comment stating that token and OAuth2 are
  separate CONNECTION shapes because `validate_creds` rule 6 rejects a password supplying both, so
  each is modelled on its own.

## Expert fixes

### crates/lakehouse-catalog/src/auth.rs

#### [INFORMATION_LEAKAGE] The pair-completeness decision still has a second home in the OAuth2 grant
- Location: lines 133-138 (`oauth2_client_credentials_grant`) and lines 228-232 (`resolve_catalog_auth`)
- Issue: `resolve_catalog_auth`'s `SuppliedCatalogAuth::ClientCredentials { .. }` arm discards the two
  values the classifier just extracted and hands `creds` down, and `oauth2_client_credentials_grant`
  then re-derives them itself with `non_empty(&creds.client_id).ok_or_else(...)` and
  `non_empty(&creds.client_secret).ok_or_else(...)`. So "a complete pair means a non-empty `client_id`
  AND a non-empty `client_secret`" is still owned in two modules — `creds.rs` and `auth.rs` — which is
  the back-door leakage this plan set out to close, and the reason two copies of the mode decision
  drifted apart in issue #331 in the first place. The two `ok_or_else` branches are also unreachable
  from the only in-crate call site (the `ClientCredentials` arm has already proven both fields
  non-empty) and no test asserts either error text (`grep -rn "OAuth2 grant requires" crates/` returns
  only the two declaration sites). `resolve_unity_auth`'s OAuth arm, by contrast, uses the values the
  classifier bound — the asymmetry between the two consumers is exactly what this plan removed
  elsewhere.
- Fix: In `crates/lakehouse-catalog/src/auth.rs`, replace the two `non_empty(...).ok_or_else(...)`
  bindings at the top of `oauth2_client_credentials_grant` with one
  `let SuppliedCatalogAuth::ClientCredentials { client_id, client_secret } = creds.supplied_catalog_auth() else { return Err(UdfError::User("OAuth2 grant requires a complete client_id/client_secret pair but none was resolved".into())); };`
  so the grant reads the single mode owner instead of re-deriving pair completeness. Keep the
  function's existing three-argument `&ConnectionCreds` signature (it still reads `oauth2_server_uri`
  and `scope`) and keep `resolve_catalog_auth`'s `ClientCredentials { .. }` arm binding nothing. Verify
  the pre-existing direct caller `oauth_client_credentials_grant`-based mock-server test at
  `crates/lakehouse-catalog/src/auth_tests.rs:239` still passes — it supplies a complete pair with no
  `token`, so it must continue to reach the grant.
