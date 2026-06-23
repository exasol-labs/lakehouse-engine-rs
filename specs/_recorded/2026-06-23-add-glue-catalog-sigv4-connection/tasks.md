# Tasks: add-glue-catalog-sigv4-connection

## Phase 2: Implementation (Group A — SDK bump + memory wiring)
- [x] 1.1 Bump exasol-udf-sdk/exasol-udf-macros 0.14.0 → 0.16.0 (workspace + engine Cargo.toml), keep Cargo.lock in sync, fix any 0.15 dead-API fallout
- [x] 1.2 Thread ctx.memory_limit() into build_session_context (replace scan/mod.rs:445 0-sentinel; remove ponytail markers scan/mod.rs:438, scan/runtime.rs:17)
- [x] 1.3 Unit test: positive limit → 0.6×limit pool; 0 limit → default budget

## Phase 2: Implementation (Group B — CONNECTION credential source)
- [x] 2.1 Add adapter/connection.rs: read_connection(ctx,name)->Resolved{uri,creds}, storage_block, catalog_block, REQUIRED_CRED_KEYS, credential-safe errors
- [x] 2.2 Replace extract_connection_props with CONNECTION-based path; thread &dyn UdfContext into handle_pushdown_request; add CATALOG_CONNECTION property
- [x] 2.3 Unit tests: missing connection name, malformed password, missing required fields, optional-field defaults — no credential leak

## Phase 2: Implementation (Group C — SigV4 client) [expert]
- [x] 3.1 Add aws-sigv4 + aws-credential-types + runtime reqwest; confirm rustc-1.92 co-resolution (no cargo update surprises) [expert]
- [x] 3.2 adapter/sigv4.rs: sign a reqwest request given creds + region + glue service; keys never logged [expert]

## Phase 2: Implementation (Group E — signing-wire + vending) [expert]
- [x] 3.3 Wire signing into resolve_table_schema/resolve_file_list when use_sigv4; keep unsigned RestCatalogBuilder path otherwise [expert]
- [x] 3.4 Unit tests: signed request carries SigV4 header for region+glue; keys absent from error/debug; disabled path unsigned
- [x] 4.1 When use_vended_credentials: signed load_table GET, deserialize LoadTableResult, extract vended s3.* (storage_credentials longest-prefix, fallback config) [expert]
- [x] 4.2 merge_vended_into_storage(static,vended) into each ScanSpec.storage (endpoint/region/path_style preserved); resolve-once in planning [expert]
- [x] 4.3 Unit tests: vended overrides static; storage_credentials preferred over config; vending-disabled keeps static; no credential in error
- [x] 4.4 Extend redact_* coverage to bearer token + vended STS keys

## Phase 2: Implementation (Group D — cloud E2E harness)
- [x] 5.1 Add cloud-e2e cargo feature (distinct from exasol-e2e); tests/cloud_e2e_test.rs gated on it
- [x] 5.2 env-var discovery + skip-when-absent helper in tests/common (early return; never fail on absence)
- [x] 5.3 Smoke test: CONNECTION from env, Glue-backed VS, projection+filter query, row sanity; DSN validateservercertificate=0
- [x] 5.4 Perf/aggregate smoke: grouped COUNT/SUM, sane values, record wall-clock (no hard threshold)
- [x] 5.5 Vended-credentials end-to-end assertion (scan reads via vended creds; no credential in output)

## Phase 3: Verification
- [x] 6.1 cargo test (host) green; clippy --all-targets + fmt clean
- [x] 6.2 make test-e2e (local Docker) green — CONNECTION path against MinIO/REST, use_sigv4/use_vended false
- [x] 6.3 Code review (code-reviewer agent)

## Phase 4: Review fixes
- [x] R1 Redact sign-error (pushdown.rs:156) + serde deser error (pushdown.rs:182) through redact_catalog_error [expert]
- [x] R2 Fix vended-cred location anchor: use metadata.location() not catalog_props.uri (pushdown.rs:1242) [expert]
- [x] R3 emit.rs redact_credentials: redact ALL occurrences, not just first
- [x] R4 build_load_table_url: doc the ARN/config-endpoint ceiling + add ARN-shaped warehouse test
- [x] R5 Move inline ponytail risk comments (pushdown.rs:1153,1188) to handle_pushdown doc block
- [x] R6 cloud_test_skips_when_creds_absent: remove unsafe env mutation race
- [x] R7 Fix typo 'unsidned' (sigv4.rs:233)
