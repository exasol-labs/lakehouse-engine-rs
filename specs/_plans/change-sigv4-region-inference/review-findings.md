# Code Review Findings: change-sigv4-region-inference

## Summary
- Files reviewed: 16
- Total findings: 9 (standard: 9, expert: 0)
- Evidence: `cargo clippy --all-targets -p lakehouse-catalog -p lakehouse-engine` and `cargo clippy -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test` finish with no warnings. `cargo test -p lakehouse-catalog` passes (195 + 1 + 18). `cargo test -p lakehouse-engine --lib adapter::connection` passes (59). None of the findings below is a failing build or test. Each is a guardrail, test-quality, or duplication defect.

## Standard fixes

### crates/lakehouse-catalog/src/namespace.rs

#### [TOO_MANY_ARGUMENTS] `list_in_namespace_signed` now takes five parameters
- Location: line 227 (`list_in_namespace_signed`), recursive call at line ~280, caller `list_namespace_tables` at line 69, `signed_get_json` at line 169
- Issue: this change adds `region` as a fifth parameter, next to `catalog_uri`, `ns`, `warehouse`, and `creds`. Only `ns` varies across the recursion. The other four are passed through unchanged at every level and into every `signed_get_json(url, creds, region)` call. That is a context object spread across a parameter list.
- Fix: In crates/lakehouse-catalog/src/namespace.rs, add a private struct `SignedEnumeration<'a> { catalog_uri: &'a str, prefix: &'a str, creds: &'a ConnectionCreds, region: &'a str }`. Turn `list_in_namespace_signed` into a method `fn list_in_namespace_signed<'s>(&'s self, ns: &'s NamespaceIdent) -> Pin<Box<dyn Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 's>>` whose recursion calls `self.list_in_namespace_signed(&child)`. Turn `signed_get_json(url, creds, region)` into the method `self.signed_get_json(url)`. In `list_namespace_tables`, build the struct in the SigV4 branch from `catalog_uri`, `&prefix`, `creds`, and `&region`, then call the method. In crates/lakehouse-catalog/src/namespace_tests.rs, change `signed_enumeration_is_signed_for_the_resolved_region` to build `SignedEnumeration { catalog_uri: &catalog_uri, prefix: "catalogs/123456789012", creds: &creds, region: "eu-west-1" }` and call `.list_in_namespace_signed(&ns)`. Keep every URL, signing, error-text, and best-effort child-listing behavior unchanged, and keep every existing assertion.

### crates/lakehouse-catalog/src/sigv4.rs

#### [INFORMATION_LEAKAGE] The refusal text is copied into two more test files
- Location: sigv4.rs line 27 (`MISSING_SIGNING_REGION`); auth_tests.rs line 463 (`sigv4_auth_refuses_without_signing_region`); namespace_tests.rs line 204 (`signed_enumeration_refuses_without_signing_region`); sigv4_tests.rs line 316
- Issue: sigv4.rs owns the refusal message, but three test files each spell out the full literal. `auth.rs` and `namespace.rs` pass the error on with `?` unchanged. The auth and namespace tests only need to prove that the sigv4 refusal reaches the caller, not what it says. Rewording the message now takes four edits, one of which is a production file. This is the third copy of the literal, so the Rule of Three applies.
- Fix: In crates/lakehouse-catalog/src/sigv4.rs, change `const MISSING_SIGNING_REGION` to `pub(crate) const MISSING_SIGNING_REGION`. In crates/lakehouse-catalog/src/auth_tests.rs `sigv4_auth_refuses_without_signing_region` and crates/lakehouse-catalog/src/namespace_tests.rs `signed_enumeration_refuses_without_signing_region`, replace the expected string literal with `crate::sigv4::MISSING_SIGNING_REGION`. Leave the literal in crates/lakehouse-catalog/src/sigv4_tests.rs `required_signing_region_refuses_without_a_signing_region` as the only text pin, because that pin proves the message carries no credential and no URI.

### crates/lakehouse-catalog/src/session_tests.rs

#### [OUTDATED_COMMENT] The assertion message says "endpoint's region", but the test cannot tell it from the stated region
- Location: `catalog_session_resolve_sigv4_no_config_roundtrip`, lines 334-344
- Issue: `base_creds()` states `region = "us-east-1"` and the catalog URI is `https://glue.us-east-1.amazonaws.com/iceberg`, so both sources give `us-east-1`. The message "carrying the endpoint's region" claims more than the assertion proves. This is the only test that drives the public `CatalogSession::resolve` entry point, and it would still pass if the stated region won.
- Fix: In crates/lakehouse-catalog/src/session_tests.rs `catalog_session_resolve_sigv4_no_config_roundtrip`, add `creds.region = String::new();` after `creds.use_sigv4 = true;`, so the asserted `"us-east-1"` can only come from the Glue endpoint. Keep the existing assertion and its message.

### crates/lakehouse-engine/src/adapter/connection_tests.rs

#### [MISSING_BOUNDARY_TEST] The branch that must NOT append the Glue-endpoint clause is never checked
- Location: `sigv4_requires_access_secret_region`, the "Missing access_key" case (line 905) and the "Missing secret_key" case (line 920). Production branch: `validate_sigv4_creds` `glue_hint` `else { "" }` in crates/lakehouse-engine/src/adapter/connection.rs
- Issue: the new assertions check only that the clause appears when `region` is named. No test checks that it is absent when only `access_key` or `secret_key` is missing. If `validate_sigv4_creds` always appended the clause, every test would still pass.
- Fix: In crates/lakehouse-engine/src/adapter/connection_tests.rs `sigv4_requires_access_secret_region`, add `assert!(!msg.contains("https://glue.<region>.amazonaws.com"), "must not state the Glue-endpoint alternative when region is not named: {msg}");` to the "Missing access_key" case and to the "Missing secret_key" case.

#### [INLINE_COMMENT] Three new `// region intentionally absent` comments
- Location: lines 1017 (`sigv4_region_required_for_non_standard_glue_hosts`), 1054 (`sigv4_region_derived_from_standard_glue_endpoint_is_accepted`), 1080 (`sigv4_derived_region_places_no_store_under_vending`)
- Issue: each new test's doc comment already says that `region` is omitted, and the JSON literal shows that no `region` key is present. The inline comments repeat that.
- Fix: In crates/lakehouse-engine/src/adapter/connection_tests.rs, delete the `// region intentionally absent` line inside the `serde_json::json!` literal in `sigv4_region_required_for_non_standard_glue_hosts`, `sigv4_region_derived_from_standard_glue_endpoint_is_accepted`, and `sigv4_derived_region_places_no_store_under_vending`. Leave the pre-existing one at line 961 alone.

### crates/lakehouse-engine/tests/cloud_e2e_test.rs

#### [TOO_MANY_ARGUMENTS] `setup_cloud_vs` now takes five parameters
- Location: line 361 (`setup_cloud_vs`), callers in `cloud_smoke_projection_filter_query`, `cloud_sigv4_region_derived_from_glue_endpoint_lists_table`, `cloud_perf_grouped_aggregate_smoke`
- Issue: this change adds `password` as a fifth parameter, next to `conn`, `env`, `conn_name`, and `vs_name`. Those last three always travel together and describe one CONNECTION-plus-virtual-schema target.
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, add a private struct `CloudVsTarget<'a> { conn_name: &'a str, vs_name: &'a str, password: CatalogConnectionPassword }`. Change the signature to `fn setup_cloud_vs(conn: &mut ExaConn, env: &CloudEnv, target: &CloudVsTarget)`, reading `target.conn_name`, `target.vs_name`, and `&target.password` in the body. Update all three callers to build a `CloudVsTarget` with their current values.

#### [SHRINKABLE] `setup_cloud_vs_vended` is now a copy of `setup_cloud_vs`
- Location: line 383 (`setup_cloud_vs_vended`), caller in `cloud_scan_reads_with_vended_credentials` (line ~708)
- Issue: once `setup_cloud_vs` accepts the password, `setup_cloud_vs_vended` builds exactly the SQL that `setup_cloud_vs` would build from `CLOUD_CATALOG_CONN_VENDED`, `format!("{CLOUD_VS_NAME}_VENDED")`, and `env.catalog_connection_password_vended()`. It repeats the whole body.
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, delete `setup_cloud_vs_vended`. In `cloud_scan_reads_with_vended_credentials`, call `setup_cloud_vs` instead, passing conn name `CLOUD_CATALOG_CONN_VENDED`, virtual schema name `format!("{CLOUD_VS_NAME}_VENDED")`, and password `env.catalog_connection_password_vended()`, in the parameter shape the `[TOO_MANY_ARGUMENTS]` fix above gives `setup_cloud_vs`.

#### [SHRINKABLE] Third copy of the virtual-schema table-name derivation
- Location: `vs_table` (line 345), `cloud_scan_reads_with_vended_credentials` `vended_table` block (line 710), new `cloud_sigv4_region_derived_from_glue_endpoint_lists_table` `expected_table` (line 580)
- Issue: the new test copies the chain `glue_table.split('.').next_back().unwrap_or(glue_table).to_uppercase()` a third time. Both `vs_table` and the vended test already contain it. If the adapter's table-naming rule changes, all three copies must change.
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, add `fn vs_table_name(glue_table: &str) -> String` that returns the uppercased last dot-separated component. Rewrite `vs_table` as `format!("{CLOUD_VS_NAME}.{}", vs_table_name(glue_table))`. Replace the `vended_table` block's derivation with `vs_table_name(&env.glue_table)`, keeping its `{CLOUD_VS_NAME}_VENDED.` prefix. Replace `expected_table`'s derivation with `vs_table_name(&env.glue_table)`. Move the existing "The adapter uppercases the table's last component." note from inside `vs_table` into a doc comment on `vs_table_name`.

#### [INLINE_COMMENT] Inline comment repeats the test's doc comment
- Location: line 567, `cloud_sigv4_region_derived_from_glue_endpoint_lists_table`
- Issue: `// List tables only — no SELECT against the table, so no data file is read.` repeats the doc comment's "no data file is read".
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs `cloud_sigv4_region_derived_from_glue_endpoint_lists_table`, delete the inline comment `// List tables only — no SELECT against the table, so no data file is read.`

## Expert fixes
[none]
