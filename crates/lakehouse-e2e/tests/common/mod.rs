//! Shared E2E harness: stack provisioning, the Exasol client, fixture writers, and cloud setup.
//! Helpers panic, never skip, on a missing stack or variable; `cloud-e2e` helpers skip instead.
#![cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
// Each test binary compiles this module separately, so helpers used by only one binary look dead.
#![allow(dead_code)]

#[cfg(feature = "azure-e2e")]
pub mod azure;
#[cfg(any(feature = "azure-e2e", feature = "glue-e2e"))]
pub mod cloud_fixture;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub mod e2e_harness;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "cloud-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub mod exasol_ws;
#[cfg(feature = "glue-e2e")]
pub mod glue;
#[cfg(feature = "exasol-e2e")]
pub mod identity_partition_fixtures;
#[cfg(feature = "exasol-e2e")]
pub mod int96_fixtures;
#[cfg(any(feature = "lakekeeper-e2e", feature = "azure-e2e"))]
pub mod lakekeeper;
#[cfg(feature = "lakekeeper-e2e")]
pub mod lakekeeper_authz;
#[cfg(feature = "exasol-e2e")]
pub mod name_mapping_fixtures;
#[cfg(feature = "exasol-e2e")]
pub mod pos_delete_fixtures;
#[cfg(any(feature = "exasol-e2e", feature = "unity-e2e"))]
pub mod pruning_cases;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub mod pruning_fixture;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub mod raw_parquet;
#[cfg(any(
    feature = "exasol-e2e",
    feature = "lakekeeper-e2e",
    feature = "azure-e2e",
    feature = "unity-e2e",
    feature = "glue-e2e"
))]
pub mod seed;
pub mod stack;
#[cfg(any(feature = "exasol-e2e", feature = "unity-e2e", feature = "glue-e2e"))]
pub mod timestamp_precision;
#[cfg(feature = "exasol-e2e")]
pub mod type_promotion_fixtures;
