//! Common helpers for lakehouse-engine E2E integration tests.
//!
//! Local-stack helpers (exasol_ws, seed, stack) panic (never skip) when the
//! stack is unavailable — per project rules. Cloud helpers (cloud) skip
//! cleanly when the required environment variables are absent. Lakekeeper
//! helpers (lakekeeper) share the local-stack fail-loud contract.
#![cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
// Each integration-test binary compiles this module independently, so a helper
// used by only one binary (e.g. query_row_count / SEED_ROWS_SCORE_GT_15 in
// e2e_scan_test) reads as dead code when the other binary is compiled.
#![allow(dead_code)]

#[cfg(feature = "azure-e2e")]
pub mod azure;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
pub mod e2e_harness;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
pub mod exasol_ws;
#[cfg(feature = "exasol-e2e")]
pub mod int96_fixtures;
#[cfg(any(feature = "lakekeeper-e2e", feature = "azure-e2e"))]
pub mod lakekeeper;
#[cfg(feature = "exasol-e2e")]
pub mod pos_delete_fixtures;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
pub mod seed;
pub mod stack;
#[cfg(feature = "exasol-e2e")]
pub mod type_promotion_fixtures;
