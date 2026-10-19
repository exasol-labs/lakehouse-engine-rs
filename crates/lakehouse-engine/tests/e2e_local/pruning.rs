//! Runs the pruning case table (`common/pruning_cases.rs`) on Iceberg and Hive direct storage.
//! FAILS (never skips) when the stack is unreachable.

use crate::common::e2e_harness::runtime;
use crate::common::e2e_harness::{exa_conn, resolve_fixture_files};
use crate::common::pruning_cases::{
    Format, assert_cases_hold, assert_one_shaped_file_per_label,
    assert_single_group_aggregates_match_oracle,
};
use crate::common::pruning_fixture::{
    PRUNING_FILES, PRUNING_NAMESPACE, PRUNING_TABLE, hive_object_uri,
};
use crate::common::stack::{CatalogConnectionPassword, local_stack_connection_password};
use crate::fixtures::pruning_vss;

pub(crate) const CONN_HIVE: &str = "PRUNING_HIVE_CREDS";

/// No `warehouse`: the direct-storage kind rejects that field.
pub(crate) fn hive_storage_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: String::new(),
        ..local_stack_connection_password()
    }
}

/// Scenario: Pruning keeps every file with a matching row for every filter shape
#[test]
fn pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need() {
    pruning_vss();

    let iceberg: Vec<String> = runtime()
        .block_on(resolve_fixture_files(PRUNING_NAMESPACE, PRUNING_TABLE))
        .into_iter()
        .map(|file| file.path)
        .collect();
    let hive: Vec<String> = PRUNING_FILES.iter().map(hive_object_uri).collect();

    assert_one_shaped_file_per_label("Iceberg", &iceberg);
    assert_one_shaped_file_per_label("Hive", &hive);
}

/// Scenario: Pruning keeps every file with a matching row for every filter shape
/// Scenario: A NOT over a partly translated predicate keeps every file with a matching row
/// Scenario: A NOT over a fully translated predicate still prunes
#[test]
fn iceberg_pruning_keeps_every_file_with_a_matching_row() {
    pruning_vss();
    assert_cases_hold(&mut exa_conn(), Format::Iceberg);
}

/// Scenario: A predicate on partition columns prunes files before their footers are read
#[test]
fn hive_partition_pruning_keeps_every_file_with_a_matching_row() {
    pruning_vss();
    assert_cases_hold(&mut exa_conn(), Format::Hive);
}

/// Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result
/// Scenario: A single-group request that does not decompose routes to the qualified wrapper
/// Scenario: Single-group HAVING is applied on every path
/// Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list
#[test]
fn iceberg_single_group_aggregates_match_the_oracle() {
    pruning_vss();
    assert_single_group_aggregates_match_oracle(&mut exa_conn(), Format::Iceberg);
}

/// Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result
/// Scenario: A single-group request that does not decompose routes to the qualified wrapper
/// Scenario: Single-group HAVING is applied on every path
/// Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list
#[test]
fn hive_single_group_aggregates_match_the_oracle() {
    pruning_vss();
    assert_single_group_aggregates_match_oracle(&mut exa_conn(), Format::Hive);
}
