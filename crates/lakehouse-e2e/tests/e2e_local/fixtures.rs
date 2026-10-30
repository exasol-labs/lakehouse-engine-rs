use crate::common::e2e_harness::{
    SCAN_SCRIPT_NAME, VsProps, create_schema_and_scripts, create_virtual_schema, exa_conn,
    explain_virtual_sql, install_slc, runtime, upload_so,
};
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_LINEITEM_TABLE, E2E_NAMESPACE, E2E_TABLE, E2E_TYPED_TABLE,
    seed_all_types, seed_complex_types_probe, seed_distinct_probe, seed_events,
    seed_high_card_probe, seed_ident_part_bytes, seed_typed_distinct_probe,
};
use crate::common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_seaweedfs,
};
use std::sync::OnceLock;

pub const LAKEHOUSE_VS: &str = "MY_LAKEHOUSE";

pub fn stack() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        wait_for_iceberg_catalog();
        install_slc();
        upload_so();
        create_schema_and_scripts(&mut exa_conn());
    });
}

const WAREHOUSE: &str = "s3://warehouse/";

fn seed<T>(what: &str, seeding: impl Future<Output = anyhow::Result<T>>) {
    runtime()
        .block_on(seeding)
        .unwrap_or_else(|e| panic!("seed {what}: {e:?}"));
}

pub fn events() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| seed("events", seed_events(&iceberg_catalog_url(), WAREHOUSE)));
}

pub fn typed_probe() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        seed(
            "typed_distinct_probe",
            seed_typed_distinct_probe(&iceberg_catalog_url(), WAREHOUSE),
        )
    });
}

pub fn complex_probe() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        seed(
            "complex_probe",
            seed_complex_types_probe(&iceberg_catalog_url(), WAREHOUSE),
        )
    });
}

/// Seeds every table its modules query first, because CREATE enumerates the namespace once.
pub fn lakehouse_vs() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        events();
        typed_probe();
        let url = iceberg_catalog_url();
        seed("all_types", seed_all_types(&url, WAREHOUSE));
        seed("distinct_probe", seed_distinct_probe(&url, WAREHOUSE));
        seed("high_card_probe", seed_high_card_probe(&url, WAREHOUSE));
        seed("ident_part_bytes", seed_ident_part_bytes(&url, WAREHOUSE));
        create_virtual_schema(&mut exa_conn(), &VsProps::new(LAKEHOUSE_VS, E2E_NAMESPACE));
    });
}

pub fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

pub fn events_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_TABLE)
}

pub fn typed_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_TYPED_TABLE)
}

pub fn vs_dim_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_DIM_TABLE)
}

pub fn vs_fact_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_FACT_TABLE)
}

pub fn vs_lineitem_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_LINEITEM_TABLE)
}

/// `PUSHDOWN_SQL` only: the echoed request JSON repeats the user's select list
/// and would satisfy a naive substring probe.
pub fn explain_virtual_pushdown_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    cols[1]
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn assert_pushed_to_scan_udf(conn: &mut ExaConn, sql: &str, shape: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "{shape} must drive the scan UDF: {pushed}"
    );
}

pub fn emits_clause(pushed_sql: &str) -> String {
    let marker = "EMITS (";
    let start = pushed_sql
        .find(marker)
        .unwrap_or_else(|| panic!("no EMITS clause in pushed SQL:\n{pushed_sql}"))
        + marker.len();
    let mut depth = 1i32;
    let mut end = pushed_sql.len();
    for (i, c) in pushed_sql[start..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    pushed_sql[start..end].to_string()
}
