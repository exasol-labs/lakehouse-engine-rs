use crate::common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, DISTRIBUTOR_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, VsProps,
    create_schema_and_scripts, create_virtual_schema, create_virtual_schema_with_password,
    exa_conn, explain_virtual_sql, install_slc, runtime, upload_so,
};
use crate::common::exasol_ws::ExaConn;
use crate::common::pos_delete_fixtures::NAMESPACE as POS_DELETE_NAMESPACE;
use crate::common::pruning_cases::{VS_HIVE, VS_ICEBERG};
use crate::common::pruning_fixture::{
    PRUNING_HIVE_BASE, PRUNING_NAMESPACE, oracle_statements, write_pruning_hive_copy,
};
use crate::common::raw_parquet::write_parquet_fixture;
use crate::common::seed::{
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_LINEITEM_TABLE, E2E_NAMESPACE, E2E_TABLE,
    E2E_TSPRECISION_NAMESPACE, E2E_TYPED_TABLE, seed_all_types, seed_complex_types_join_probe,
    seed_complex_types_probe, seed_distinct_probe, seed_events, seed_high_card_probe,
    seed_pruning_cases, seed_timestamp_precision_probe, seed_typed_distinct_probe,
};
use crate::common::stack::{
    ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID, iceberg_catalog_url,
    iceberg_catalog_url_internal, local_stack_connection_password, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_seaweedfs,
};
use crate::{
    assume_role, complex_type, credential_exposure, direct_storage, emit_declaration, join,
    positional_deletes, pruning, timestamp_precision,
};
use std::sync::OnceLock;

pub const LAKEHOUSE_VS: &str = "MY_LAKEHOUSE";
pub const GROUND_TRUTH_LINEITEM_TABLE: &str = "GROUND_TRUTH_LINEITEM";

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
        create_virtual_schema(&mut exa_conn(), &VsProps::new(LAKEHOUSE_VS, E2E_NAMESPACE));
    });
}

pub fn join_vss() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        events();
        let mut conn = exa_conn();
        create_virtual_schema(&mut conn, &VsProps::new(join::VS_NAME, E2E_NAMESPACE));
        create_virtual_schema(
            &mut conn,
            &VsProps::new(join::VS_NAME_LOW, E2E_NAMESPACE).with_join_broadcast_max_bytes("1"),
        );
    });
}

pub fn pos_delete_vss() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        let mut conn = exa_conn();
        create_virtual_schema(
            &mut conn,
            &VsProps::new(positional_deletes::SAMESHARD_VS_NAME, POS_DELETE_NAMESPACE)
                .with_parallelism_factor(1),
        );
        create_virtual_schema(
            &mut conn,
            &VsProps::new(positional_deletes::SPLITSHARD_VS_NAME, POS_DELETE_NAMESPACE)
                .with_parallelism_factor(positional_deletes::SPLIT_PARALLELISM_FACTOR),
        );
    });
}

pub fn complex_type_vs() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        complex_probe();
        seed(
            "complex_join_probe",
            seed_complex_types_join_probe(&iceberg_catalog_url(), WAREHOUSE),
        );
        create_virtual_schema(
            &mut exa_conn(),
            &VsProps::new(complex_type::VS_NAME, E2E_NAMESPACE),
        );
    });
}

pub fn timestamp_precision_vs() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        seed(
            "timestamp_precision_probe",
            seed_timestamp_precision_probe(&iceberg_catalog_url(), WAREHOUSE),
        );
        create_virtual_schema(
            &mut exa_conn(),
            &VsProps::new(timestamp_precision::VS_NAME, E2E_TSPRECISION_NAMESPACE),
        );
    });
}

pub fn emit_declaration_vs() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        typed_probe();
        complex_probe();
        create_virtual_schema(
            &mut exa_conn(),
            &VsProps::new(emit_declaration::VS_NAME, E2E_NAMESPACE),
        );
    });
}

pub fn credential_exposure_vs() {
    use credential_exposure::{
        CONN_NAME, OWNER_PASSWORD, OWNER_USER, READER_PASSWORD, READER_USER, VS_NAME,
    };
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        events();

        let mut sys = exa_conn();
        let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {READER_USER} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {OWNER_USER} CASCADE"));
        sys.execute(&format!("DROP CONNECTION IF EXISTS {CONN_NAME}"));

        sys.execute(&format!(
            "CREATE USER {OWNER_USER} IDENTIFIED BY \"{OWNER_PASSWORD}\""
        ));
        for priv_name in [
            "CREATE SESSION",
            "CREATE CONNECTION",
            "CREATE VIRTUAL SCHEMA",
        ] {
            sys.execute(&format!("GRANT {priv_name} TO {OWNER_USER}"));
        }
        for script in [
            ADAPTER_SCRIPT_NAME,
            SCAN_SCRIPT_NAME,
            DISTRIBUTOR_SCRIPT_NAME,
        ] {
            sys.execute(&format!(
                "GRANT EXECUTE ON SCRIPT {SCHEMA_NAME}.{script} TO {OWNER_USER}"
            ));
        }

        sys.execute(&format!(
            "CREATE USER {READER_USER} IDENTIFIED BY \"{READER_PASSWORD}\""
        ));
        sys.execute(&format!("GRANT CREATE SESSION TO {READER_USER}"));

        let mut owner = credential_exposure::owner_conn();
        create_virtual_schema_with_password(
            &mut owner,
            &VsProps::new(VS_NAME, E2E_NAMESPACE).with_catalog_conn_name(CONN_NAME),
            &iceberg_catalog_url_internal(),
            &local_stack_connection_password(),
        );
        owner.execute(&format!(
            "GRANT SELECT ON SCHEMA {VS_NAME} TO {READER_USER}"
        ));
    });
}

pub fn direct_storage_vss() {
    use direct_storage::{
        BASE_DIRECT, BASE_DISCOVERY, CONN_DIRECT, CONN_DISCOVERY, VS_DIRECT, VS_DIRECT_NARROW,
        VS_DISCOVERY, VS_DISCOVERY_EMPTY_NS, VS_DISCOVERY_NS, VS_HIVE_OFF, create_direct_vs,
        direct_storage_password, direct_vs,
    };
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        direct_storage::write_all_fixtures();

        let mut conn = exa_conn();
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DIRECT, "").with_catalog_kind("DIRECT_STORAGE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(VS_DIRECT_NARROW, CONN_DIRECT).with_merge_schema("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &direct_vs(VS_HIVE_OFF, CONN_DIRECT).with_hive_partitioning("FALSE"),
            BASE_DIRECT,
            &direct_storage_password(),
        );

        create_direct_vs(&mut conn, VS_DISCOVERY, CONN_DISCOVERY, BASE_DISCOVERY);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DISCOVERY_NS, "sub")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DISCOVERY),
            BASE_DISCOVERY,
            &direct_storage_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DISCOVERY_EMPTY_NS, "nonexistent_ns")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DISCOVERY),
            BASE_DISCOVERY,
            &direct_storage_password(),
        );
    });
}

pub fn assume_role_vss() {
    use assume_role::{
        BASE_DIRECT, CONN_BASE, CONN_DIRECT, CONN_ROLE, VS_BASE, VS_DIRECT, VS_ROLE,
        assume_role_password, base_only_password, direct_batch, direct_storage_role_password,
    };
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        events();
        write_parquet_fixture(
            &format!("{BASE_DIRECT}rows/file1.parquet"),
            direct_batch(&[1, 2, 3]),
        );

        let mut conn = exa_conn();
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_BASE, E2E_NAMESPACE).with_catalog_conn_name(CONN_BASE),
            &iceberg_catalog_url_internal(),
            &base_only_password(),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_ROLE, E2E_NAMESPACE).with_catalog_conn_name(CONN_ROLE),
            &iceberg_catalog_url_internal(),
            &assume_role_password(ASSUME_ROLE_BASE_SECRET_KEY, ASSUME_ROLE_EXTERNAL_ID),
        );
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DIRECT, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(CONN_DIRECT),
            BASE_DIRECT,
            &direct_storage_role_password(),
        );
    });
}

pub fn pruning_vss() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        seed(
            "pruning_cases",
            seed_pruning_cases(&iceberg_catalog_url(), WAREHOUSE),
        );
        write_pruning_hive_copy();

        let mut conn = exa_conn();
        for statement in oracle_statements() {
            conn.execute(&statement);
        }
        create_virtual_schema(&mut conn, &VsProps::new(VS_ICEBERG, PRUNING_NAMESPACE));
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_HIVE, "")
                .with_catalog_kind("DIRECT_STORAGE")
                .with_catalog_conn_name(pruning::CONN_HIVE),
            PRUNING_HIVE_BASE,
            &pruning::hive_storage_password(),
        );
    });
}

pub fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

/// Materialized natively so Exasol computes the ground truth independently of the
/// pushdown path, formatted identically to the wrapper's Exasol-side aggregation.
pub fn ground_truth_lineitem() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        exa_conn().execute(&format!(
            "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} AS \
             SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
            vs_lineitem_table()
        ));
    });
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
