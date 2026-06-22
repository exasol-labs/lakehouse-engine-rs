//! Common helpers for lakehouse-engine E2E integration tests.
//!
//! All helpers panic (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]
// Each integration-test binary compiles this module independently, so a helper
// used by only one binary (e.g. query_row_count / SEED_ROWS_SCORE_GT_15 in
// e2e_scan_test) reads as dead code when the other binary is compiled.
#![allow(dead_code)]

pub mod exasol_ws;
pub mod seed;
pub mod stack;
