# Tasks: add-rest-catalog-oauth-auth

## Phase 2: Implementation (Group A) — struct + signature threading
- [x] 2.1 connection.rs: add `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope` (all `Option<String>`) to `ConnectionCreds` and parse in `parse_creds`; add `has_catalog_auth` helper
- [x] 2.2 pushdown.rs: thread `&ConnectionCreds` into `build_rest_catalog`; update three call sites + dummy-catalog list-namespaces call

## Phase 2: Implementation (Group B) — validation + prop injection
- [x] 2.3 connection.rs: `warehouse`-only base validation + conditional SigV4 guard (require access_key/secret_key/region when use_sigv4); reject incomplete OAuth2; reject SigV4 + catalog-auth; keep use_vended_credentials independent [expert]
- [x] 2.4 pushdown.rs: three-mode catalog auth prop injection in `build_rest_catalog` (none / token / credential+oauth2-server-uri+scope); ensure redaction covers token/secret [expert]

## Phase 2: Implementation (Group C) — unit tests
- [x] 2.5 connection.rs unit tests (4.1–4.7: token, oauth, incomplete oauth, sigv4 mutual-excl, warehouse-only, optional defaults, sigv4 requires access/secret/region)
- [x] 2.6 pushdown.rs unit tests (5.1–5.4: token prop, credential+oauth props, no-auth props, scan-spec carries no auth) [expert]

## Phase 2: Implementation (Group D) — E2E + tracking
- [x] 2.7 cloud_e2e_test.rs: add token/OAuth catalog-auth E2E entry (gated; fails not skips when DB unavailable)
- [x] 2.8 Open GitHub issue (#21) for the feature; reference in implementing commit (`Closes #21`)

## Phase 4: Code Review
- [x] 4.1 Review all changed files (0 blocker/major; 4 minor + 2 nit — worthwhile ones fixed)

## Phase 5: Verification
- [x] 5.1 Build (`make cross-musl-udf-build`) → exit 0 (release, 15m)
- [x] 5.2 Test (`cargo test -p lakehouse-engine --lib`) → 282 passed, 0 failed
- [x] 5.3 Lint (`cargo clippy --all-targets --all-features`) → 0 errors/warnings
- [x] 5.4 Format (`cargo fmt --check`) → clean
- [x] 5.5 Scenario coverage audit (13/13 present + passing) + E2E manual testing green
