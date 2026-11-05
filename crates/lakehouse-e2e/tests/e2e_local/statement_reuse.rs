//! Counts the requests that one statement sends through the forward proxy, because only the
//! proxy log shows how often the adapter loads a table (`specs/testing.md` § Correctness oracles).

use crate::common::e2e_harness::{
    VsProps, create_virtual_schema, create_virtual_schema_with_password, exa_conn, int_column,
    isolated_pushdown_statement, pushdown_statements, runtime, text_column,
};
use crate::common::pos_delete_fixtures::NAMESPACE;
use crate::common::seed::{E2E_TABLE, SEED_TOTAL_ROWS};
use crate::common::stack::{
    ICEBERG_REST_VIA_PROXY, PROXY_ADAPTER_SCHEMA, SEAWEEDFS_VIA_PROXY, count_proxy_requests,
    iceberg_catalog_url_internal,
};
use crate::fixtures::{
    PROXY_DIRECT_VS, PROXY_ICEBERG_VS, PROXY_ROLE_VS, append_file, count, drop_table_if_exists,
    ensure_id_val_table, id_val_batch, lakehouse_vs, password_via_proxy, proxy_props,
    proxy_virtual_schemas, seed_catalog, vs_table,
};

use iceberg::NamespaceIdent;

const LATER_NAMESPACE: &str = "e2e_statement_reuse_later";
const LATER_TABLE: &str = "later";
const LATER_VS: &str = "REUSE_LATER_VS";

fn config_get() -> String {
    format!("GET {ICEBERG_REST_VIA_PROXY}/v1/config")
}

fn load_table_get(catalog: &str, table: &str) -> String {
    format!("GET {catalog}/v1/namespaces/{NAMESPACE}/tables/{table}?")
}

fn assume_role_post() -> String {
    format!("POST {SEAWEEDFS_VIA_PROXY}/ ")
}

fn directory_list(directory: &str) -> String {
    format!("GET {SEAWEEDFS_VIA_PROXY}/warehouse?list-type=2&prefix={directory}%2Frows%2F ")
}

fn events_of(vs_name: &str) -> String {
    vs_table(vs_name, E2E_TABLE)
}

/// Scenario: Every reference to one table in a statement loads the table once and prunes it with its own filter
#[test]
fn a_script_reloads_the_table_for_each_of_its_statements() {
    proxy_virtual_schemas();
    let events = events_of(PROXY_ICEBERG_VS);
    let script = format!("{PROXY_ADAPTER_SCHEMA}.RELOAD_PER_STATEMENT");
    let mut conn = exa_conn();
    conn.execute(&format!(
        "CREATE OR REPLACE LUA SCRIPT {script}() AS\n\
         query(\"SELECT COUNT(*) FROM {events}\")\n\
         query(\"SELECT COUNT(*) FROM {events}\")"
    ));

    let loads_events = load_table_get(ICEBERG_REST_VIA_PROXY, E2E_TABLE);
    let [loads] = count_proxy_requests([loads_events.as_str()], || {
        conn.execute(&format!("EXECUTE SCRIPT {script}()"));
    });

    assert_eq!(
        loads, 2,
        "each statement of a script loads the table itself"
    );
}

/// Scenario: The pushdowns of one statement share one catalog session per virtual schema
/// Scenario: Single-table pushdown builds one catalog session and reuses it
#[test]
fn each_union_branch_pushes_the_sql_it_pushes_alone() {
    proxy_virtual_schemas();
    let events = events_of(PROXY_ICEBERG_VS);
    let branches = [
        format!("SELECT ID FROM {events} WHERE ID <= 5"),
        format!("SELECT ID FROM {events} WHERE ID > 15"),
    ];
    let mut conn = exa_conn();
    let alone: Vec<String> = branches
        .iter()
        .map(|branch| isolated_pushdown_statement(&mut conn, branch))
        .collect();

    let together = pushdown_statements(&mut conn, &branches.join(" UNION ALL "));

    assert_eq!(
        together, alone,
        "each branch pushes the SQL it pushes as a statement of its own"
    );
}

struct Reference {
    shape: &'static str,
    sql: String,
    rows: i64,
    request: String,
    configs: usize,
}

/// Scenario: Every reference to one table in a statement loads the table once and prunes it with its own filter
/// Scenario: The pushdowns of one statement share one catalog session per virtual schema
/// Scenario: Single-table pushdown builds one catalog session and reuses it
/// Scenario: N-table join reuses one session across all legs
/// Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind
/// Scenario: A direct-storage pushdown loads its table from the directory its identifier note names
#[test]
fn every_reference_to_a_table_loads_it_once() {
    proxy_virtual_schemas();
    let events = events_of(PROXY_ICEBERG_VS);
    let direct_rows = vs_table(PROXY_DIRECT_VS, "rows");
    let loads_events = load_table_get(ICEBERG_REST_VIA_PROXY, E2E_TABLE);
    let cases = [
        Reference {
            shape: "UNION ALL with a filter per branch",
            sql: format!(
                "SELECT COUNT(*) FROM (SELECT ID FROM {events} WHERE ID <= 5 \
                 UNION ALL SELECT ID FROM {events} WHERE ID > 15)"
            ),
            rows: 10,
            request: loads_events.clone(),
            configs: 1,
        },
        Reference {
            shape: "CTE referenced twice with a filter per reference",
            sql: format!(
                "WITH e AS (SELECT ID FROM {events}) SELECT COUNT(*) FROM \
                 (SELECT ID FROM e WHERE ID <= 5 UNION ALL SELECT ID FROM e WHERE ID > 15)"
            ),
            rows: 10,
            request: loads_events.clone(),
            configs: 1,
        },
        Reference {
            shape: "self-join with a filter per side",
            sql: format!(
                "SELECT COUNT(*) FROM {events} a JOIN {events} b ON a.ID = b.ID \
                 WHERE a.ID <= 10 AND b.ID >= 6"
            ),
            rows: 5,
            request: loads_events,
            configs: 1,
        },
        Reference {
            shape: "direct-storage UNION ALL",
            sql: format!(
                "SELECT COUNT(*) FROM (SELECT * FROM {direct_rows} UNION ALL SELECT * FROM {direct_rows})"
            ),
            rows: 6,
            request: directory_list("proxy_direct"),
            configs: 0,
        },
    ];

    for case in cases {
        let mut rows = 0;
        let [requests, configs] =
            count_proxy_requests([case.request.as_str(), config_get().as_str()], || {
                rows = count(&case.sql)
            });
        assert_eq!(rows, case.rows, "{}: rows", case.shape);
        assert_eq!(requests, 1, "{}: loads", case.shape);
        assert_eq!(configs, case.configs, "{}: /v1/config lookups", case.shape);
    }
}

/// Scenario: Every reference to one table in a statement loads the table once and prunes it with its own filter
#[test]
fn a_later_statement_reads_a_file_added_after_the_first() {
    lakehouse_vs();
    let rt = runtime();
    let catalog = seed_catalog(rt, "lakehouse-e2e-statement-reuse-later");
    let namespace = NamespaceIdent::new(LATER_NAMESPACE.to_string());
    rt.block_on(async {
        drop_table_if_exists(&catalog, &namespace, LATER_TABLE).await;
        ensure_id_val_table(&catalog, LATER_NAMESPACE, LATER_TABLE, 1, 1.0).await;
    });
    create_virtual_schema(&mut exa_conn(), &VsProps::new(LATER_VS, LATER_NAMESPACE));
    let later = vs_table(LATER_VS, LATER_TABLE);
    let ids = format!("SELECT ID FROM {later} ORDER BY ID");
    assert_eq!(
        int_column(&exa_conn().query_columns(&ids)[0]),
        vec![1],
        "the first statement reads the initial file"
    );

    rt.block_on(append_file(
        &catalog,
        LATER_NAMESPACE,
        LATER_TABLE,
        id_val_batch(2, 2.0),
    ));

    assert_eq!(
        int_column(&exa_conn().query_columns(&ids)[0]),
        vec![1, 2],
        "a later statement loads the table again, so it reads the added file"
    );
}

/// Scenario: Two virtual schemas in one statement use their own CONNECTION
#[test]
fn two_virtual_schemas_in_one_statement_load_the_table_once_each() {
    proxy_virtual_schemas();
    let mut conn = exa_conn();
    // Each CONNECTION names the catalog by its own address, so the proxy log shows which
    // CONNECTION each load used: a virtual schema reading the other's state loads at its address.
    let catalog_b = iceberg_catalog_url_internal();
    for (vs, creds, catalog) in [
        ("REUSE_VS_A", "REUSE_CREDS_A", ICEBERG_REST_VIA_PROXY),
        ("REUSE_VS_B", "REUSE_CREDS_B", catalog_b.as_str()),
    ] {
        create_virtual_schema_with_password(
            &mut conn,
            &proxy_props(vs, NAMESPACE, creds),
            catalog,
            &password_via_proxy(),
        );
    }
    // Exasol pushes a UNION ALL's trailing ORDER BY into its first virtual-schema leg only and
    // leaves the union unsorted, so the order is applied outside a derived table.
    let sql = format!(
        "SELECT SRC, ID, NAME FROM (SELECT 'A' AS SRC, ID, NAME FROM {a} \
         UNION ALL SELECT 'B', ID, NAME FROM {b}) ORDER BY SRC, ID",
        a = events_of("REUSE_VS_A"),
        b = events_of("REUSE_VS_B"),
    );
    let mut columns = Vec::new();

    let [loads_a, loads_b] = count_proxy_requests(
        [
            load_table_get(ICEBERG_REST_VIA_PROXY, E2E_TABLE).as_str(),
            load_table_get(&catalog_b, E2E_TABLE).as_str(),
        ],
        || columns = conn.query_columns(&sql),
    );

    let expected_ids: Vec<i64> = (1..=SEED_TOTAL_ROWS as i64)
        .chain(1..=SEED_TOTAL_ROWS as i64)
        .collect();
    let expected_names: Vec<Option<String>> = (1..=SEED_TOTAL_ROWS)
        .chain(1..=SEED_TOTAL_ROWS)
        .map(|id| Some(format!("event-{id:02}")))
        .collect();
    assert_eq!(int_column(&columns[1]), expected_ids);
    assert_eq!(text_column(&columns[2]), expected_names);
    assert_eq!(
        [loads_a, loads_b],
        [1, 1],
        "each virtual schema loads the table once, at its own CONNECTION's address"
    );
}

/// Scenario: The adapter assumes the role once per statement and virtual schema and substitutes the session
#[test]
fn a_role_session_serves_every_pushdown_of_a_statement() {
    proxy_virtual_schemas();
    let events = events_of(PROXY_ROLE_VS);
    let mut rows = 0;

    let [assumptions] = count_proxy_requests([assume_role_post().as_str()], || {
        rows = count(&format!(
            "SELECT COUNT(*) FROM (SELECT ID FROM {events} UNION ALL SELECT ID FROM {events})"
        ));
    });

    assert_eq!(rows, 2 * SEED_TOTAL_ROWS as i64);
    assert_eq!(
        assumptions, 1,
        "one AssumeRole call for the whole statement"
    );
}
