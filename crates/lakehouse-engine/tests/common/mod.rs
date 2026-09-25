//! Local-stack and Lakekeeper helpers panic (never skip) when their stack is
//! unavailable; cloud helpers skip when their environment variables are absent.
#![cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
// Each test binary compiles this module separately, so helpers used by only one binary look dead.
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
#[cfg(any(feature = "exasol-e2e", feature = "azure-e2e"))]
pub mod raw_parquet;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e"
))]
pub mod seed;
pub mod stack;
#[cfg(any(feature = "exasol-e2e", feature = "unity-e2e"))]
pub mod timestamp_precision;
#[cfg(feature = "exasol-e2e")]
pub mod type_promotion_fixtures;
