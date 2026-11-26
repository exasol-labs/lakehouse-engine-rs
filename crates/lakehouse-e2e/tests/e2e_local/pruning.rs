//! Runs the pruning case table (`common/pruning_cases.rs`) on Iceberg and Hive direct storage.
//! FAILS (never skips) when the stack is unreachable.

use crate::common::e2e_harness::runtime;
use crate::common::e2e_harness::{
    SCAN_SCRIPT_NAME, VsProps, create_virtual_schema, create_virtual_schema_with_password,
    exa_conn, exa_session, isolated_pushdown_statement, resolve_fixture_files,
};
use crate::common::identity_partition_fixtures::TYPES_TABLE;
use crate::common::pruning_cases::{
    Format, VS_HIVE, VS_ICEBERG, assert_cases_hold, assert_clock_cases_hold,
    assert_grouped_aggregates_match_oracle, assert_one_shaped_file_per_label,
    assert_single_group_aggregates_match_oracle,
};
use crate::common::pruning_fixture::{
    PRUNING_FILES, PRUNING_HIVE_BASE, PRUNING_NAMESPACE, PRUNING_TABLE, hive_object_uri,
    oracle_statements, write_pruning_hive_copy,
};
use crate::common::seed::{E2E_HIGH_CARD_TABLE, HIGH_CARD_ROWS, seed_pruning_cases};
use crate::common::stack::{
    CatalogConnectionPassword, iceberg_catalog_url, local_stack_connection_password,
};
use crate::fixtures::{LAKEHOUSE_VS, lakehouse_vs, stack, vs_table};

use std::sync::OnceLock;

const CONN_HIVE: &str = "PRUNING_HIVE_CREDS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        stack();
        runtime().block_on(async {
            seed_pruning_cases(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed the Iceberg pruning fixture")
        });
        write_pruning_hive_copy(PRUNING_HIVE_BASE);

        let mut conn = exa_conn();
        for statement in oracle_statements() {
            conn.execute(&statement);
        }
        create_virtual_schema(&mut conn, &VsProps::new(VS_ICEBERG, PRUNING_NAMESPACE));
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_HIVE, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_HIVE),
            PRUNING_HIVE_BASE,
            &hive_storage_password(),
        );
    });
}

/// No `warehouse`: the direct-storage kind rejects that field.
fn hive_storage_password() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: String::new(),
        ..local_stack_connection_password()
    }
}

/// Scenario: Pruning keeps every file with a matching row for every filter shape
#[test]
fn pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need() {
    setup();

    let iceberg: Vec<String> = runtime()
        .block_on(resolve_fixture_files(PRUNING_NAMESPACE, PRUNING_TABLE))
        .into_iter()
        .map(|file| file.path)
        .collect();
    let hive: Vec<String> = PRUNING_FILES
        .iter()
        .map(|file| hive_object_uri(PRUNING_HIVE_BASE, file))
        .collect();

    assert_one_shaped_file_per_label("Iceberg", &iceberg);
    assert_one_shaped_file_per_label("Hive", &hive);
}

/// Scenario: Pruning keeps every file with a matching row for every filter shape
/// Scenario: A NOT over a partly translated predicate keeps every file with a matching row
/// Scenario: A NOT over a fully translated predicate still prunes
/// Scenario: A LIKE pattern prunes by its literal prefix
/// Scenario: A not-equal comparison prunes
/// Scenario: LIKE predicate translates with optional escape character
/// Scenario: Every pruning path keeps every file with a matching row for every column type
/// Scenario: A digits-only text literal converts for an integer column
#[test]
fn iceberg_pruning_keeps_every_file_with_a_matching_row() {
    setup();
    assert_cases_hold(exa_conn, Format::Iceberg);
}

/// Scenario: A predicate on partition columns prunes files before their footers are read
/// Scenario: A LIKE pattern prunes by its literal prefix
/// Scenario: A not-equal comparison prunes
/// Scenario: LIKE predicate translates with optional escape character
/// Scenario: Every pruning path keeps every file with a matching row for every column type
#[test]
fn hive_partition_pruning_keeps_every_file_with_a_matching_row() {
    setup();
    assert_cases_hold(exa_conn, Format::Hive);
}

/// Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result
/// Scenario: A single-group request that does not decompose routes to the qualified wrapper
/// Scenario: Single-group HAVING is applied on every path
/// Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list
#[test]
fn iceberg_single_group_aggregates_match_the_oracle() {
    setup();
    assert_single_group_aggregates_match_oracle(exa_conn, Format::Iceberg);
}

/// Scenario: A nested aggregate over an expression declines the split
/// Scenario: A group key travels with its exact type
/// Scenario: An unprojected bare-column group key takes its column type
/// Scenario: An all-pruned grouped request types a text scalar over an aggregate
#[test]
fn iceberg_grouped_aggregates_match_the_oracle() {
    setup();
    assert_grouped_aggregates_match_oracle(exa_conn, Format::Iceberg);
}

/// Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result
/// Scenario: A single-group request that does not decompose routes to the qualified wrapper
/// Scenario: Single-group HAVING is applied on every path
/// Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list
#[test]
fn hive_single_group_aggregates_match_the_oracle() {
    setup();
    assert_single_group_aggregates_match_oracle(exa_conn, Format::Hive);
}

/// Scenario: A nested aggregate over an expression declines the split
/// Scenario: A group key travels with its exact type
/// Scenario: An unprojected bare-column group key takes its column type
/// Scenario: An all-pruned grouped request types a text scalar over an aggregate
#[test]
fn hive_grouped_aggregates_match_the_oracle() {
    setup();
    assert_grouped_aggregates_match_oracle(exa_conn, Format::Hive);
}

/// Scenario: A filter on a clock function or a time zone literal prunes files at plan time
#[test]
fn clock_and_tstz_filters_prune_files() {
    setup();
    let mut conn = exa_conn();
    // In a session whose zone is the folding zone, the TSTZ literal folds to its own wall clock.
    let db_timezone = conn.query_columns("SELECT DBTIMEZONE")[0][0].clone();
    let db_timezone = db_timezone.as_str().expect("DBTIMEZONE is text");
    let zone = format!("TIME_ZONE = '{db_timezone}'");
    conn.execute(&format!("ALTER SESSION SET {zone}"));
    assert_clock_cases_hold(|| exa_session(&zone), Format::Iceberg);

    // `dt` is an identity partition holding 2024-02-29 and NULL.
    lakehouse_vs();
    let partitioned = format!(
        "SELECT ID FROM {} WHERE DT = CURRENT_DATE",
        vs_table(LAKEHOUSE_VS, TYPES_TABLE)
    );
    let plan = isolated_pushdown_statement(&mut conn, &partitioned);
    assert!(
        !plan.contains(SCAN_SCRIPT_NAME),
        "{partitioned} must prune every file: {plan}"
    );
    assert_eq!(conn.query_row_count(&partitioned), 0, "{partitioned}");
}

/// Scenario: A filter of thousands of OR terms returns its rows
#[test]
fn a_filter_of_thousands_of_or_terms_returns_its_rows() {
    // The adapter and the scan crashed at about 5,000 terms when each nested one level per term.
    const TERMS: i64 = 6_000;
    lakehouse_vs();
    let mut conn = exa_conn();
    let even_ids = (0..TERMS).map(|k| format!("ID = {}", 2 * k));
    let query = format!(
        "SELECT COUNT(*) FROM {} WHERE {}",
        vs_table(LAKEHOUSE_VS, E2E_HIGH_CARD_TABLE),
        even_ids.collect::<Vec<_>>().join(" OR ")
    );
    // The seed holds the ids 1 to HIGH_CARD_ROWS.
    let expected = (1..=HIGH_CARD_ROWS as i64)
        .filter(|id| id % 2 == 0 && *id < 2 * TERMS)
        .count() as i64;

    let plan = isolated_pushdown_statement(&mut conn, &query);
    assert!(
        plan.contains(SCAN_SCRIPT_NAME),
        "the OR filter must reach the scan: {plan}"
    );
    assert_eq!(conn.query_scalar_i64(&query), expected);
}
