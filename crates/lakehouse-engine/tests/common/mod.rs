//! Common helpers for lakehouse-engine E2E integration tests.
//!
//! All helpers panic (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]

pub mod exasol_ws;
pub mod seed;
pub mod stack;
