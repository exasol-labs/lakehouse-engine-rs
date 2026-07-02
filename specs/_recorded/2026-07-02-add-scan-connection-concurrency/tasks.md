# Tasks: add-scan-connection-concurrency

## Phase 2: Implementation (Group A - prereq)
- [x] 2.1 Dependency bump exasol-udf-sdk/exasol-udf-macros 0.20.0 -> 0.20.1, update CLAUDE.md, revisit create_vs_records_cluster_nodes_property. Closes #43.

## Phase 2: Implementation (Group B)
- [x] 2.2 Add s3_max_connections field to CommonSpec/ScanSpec (scan/spec.rs), split/merge impls, fixtures
- [x] 2.3 Adapter resolution + AUTO derivation: PROP_S3_MAX_CONNECTIONS/NOTE_S3_MAX_CONNECTIONS/DEFAULT, resolve_s3_max_connections [expert]

## Phase 2: Implementation (Group C)
- [x] 2.4 Wire round-trip: build_adapter_notes + pushdown planning reads NOTE_S3_MAX_CONNECTIONS into CommonSpec
- [x] 2.5 Apply budget to object store via AmazonS3Builder::with_client_options in build_s3_store [expert]

## Phase 2: Implementation (Group D)
- [x] 2.6 Unit tests: resolution (FIXED/AUTO/0-cores), serde default, common-spec-once
- [x] 2.7 Integration test: scan_applies_s3_max_connections_to_object_store (scan_two_arg.rs)
- [x] 2.8 Docs: docs/tuning.md new knob, docs/performance.md native-IMPORT parity goal + pre-0.20.1 node-count caveat

## Phase 3: Verification (Group E)
- [ ] 3.1 Benchmark sweep.sh: few-big-shards + high S3_MAX_CONNECTIONS row vs native IMPORT (validation, not spec)
- [ ] 3.2 Re-gate 180M-row full-emit gap after Task 2.1 lands (validation, not spec)
- [x] 3.3 Gate: cargo test, cargo clippy --all-targets, cargo fmt
