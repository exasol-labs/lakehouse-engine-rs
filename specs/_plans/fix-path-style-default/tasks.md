# Tasks: fix-path-style-default

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Credential tri-state and non-vended resolution)
- [x] 1.1 Widen `ConnectionCreds.path_style` and `StorageCreds.path_style` to `Option<bool>` in `crates/lakehouse-catalog/src/creds.rs`, keeping both `Debug` impls rendering the field's value.
- [x] 1.2 Change `StorageCreds::from_json` to preserve the tri-state: an absent or non-boolean `path_style` yields `None`.
- [x] 1.3 Resolve `None` to `false` inside `StorageCreds::backend`, the one selector both readers call, and state in its doc comment why the resolution lives there rather than in the parse step.
- [x] 1.4 Update `From<&ConnectionCreds> for StorageCreds` and `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs:246-283`) for the widened field.
- [x] 1.5 Add the `validate_creds` guard: reject a CONNECTION supplying a non-empty `endpoint`, stating no `path_style`, with `use_vended_credentials` false. The message names `path_style`, names both values and what each reaches, and carries no credential value.
- [x] 1.6 Update the sixteen `ConnectionCreds` and `StorageCreds` literal sites listed in plan.md § Test Disposition so the workspace compiles.
- [x] 1.7 Write the `from_json` tri-state tests: absent yields `None`, `true` yields `Some(true)`, `false` yields `Some(false)`, and a non-boolean value yields `None`.
- [x] 1.8 Write the `backend` resolution tests: `None` resolves to `false`, `Some(v)` resolves to `v`, and the adapter-side and scan-side readers derive a field-for-field equal backend from a password omitting `path_style`.
- [x] 1.9 Write the guard tests: the rejection fires for endpoint-without-`path_style`; it does not fire for an explicit `false` beside an endpoint, for no endpoint, or with vending enabled; and the message names no credential value.
- [x] 1.10 Update the five `connection_tests.rs` assertions named in plan.md § Test Disposition and delete the stale `:56` comment.

## Phase 2: Implementation (Group B: Vended CONNECTION-wins merge)
- [x] 2.1 Widen `StaticStoreAddress` (`crates/lakehouse-catalog/src/storage.rs:271-307`) with a non-`pub` `path_style: Option<bool>`, add its accessor, and extend the single `From<&ConnectionCreds>` conversion.
- [x] 2.2 Change `s3_backend`'s `path_style` resolution to the three-step chain: the address's stated value, else the vended value, else the endpoint-presence derivation. Update its doc comment so the demotion of the derivation to last resort is stated, not inferred. [expert]
- [x] 2.3 Confirm the Unity arm needs no edit: `uc_vended_s3` (`crates/lakehouse-catalog/src/unity/vended.rs:160`) already yields `path_style: None` into the same shared `s3_backend`, so the override reaches both catalog kinds from one implementation.
- [x] 2.4 Write the precedence tests: a stated `Some(false)` beats a vended `true`, a stated `Some(true)` beats a vended `false`, `None` falls through to the vended value, and `None` with no vended value falls through to the endpoint-presence derivation.
- [x] 2.5 Write a Unity-arm precedence test proving a stated CONNECTION value wins there through the same shared function.
- [x] 2.6 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: read the added accessor from the external vantage, and confirm the two source-level probes cover the third field without a hardcoded field list.

## Phase 2: Implementation (Group C: Operator documentation)
- [x] 3.1 Update `docs/catalogs.md:45` to state the new default and the endpoint rule, and check every recipe in that file states `path_style` where it configures an `endpoint`.
- [x] 3.2 Check `docs/install.md:185` and the installer template at `deploy/scripts/install.sh:1478-1484` against plan.md § Migration, and edit only what § Migration shows is wrong.

## Phase 3: Verification
- [x] 4.1 Run automated checklist (build, test, e2e, clippy, fmt)
- [x] 4.2 Scenario coverage audit against plan.md § Verification > Scenario Coverage
- [ ] 4.3 Manual verification steps against the local Docker Exasol container
- [x] 4.4 Generate verification-report.md
