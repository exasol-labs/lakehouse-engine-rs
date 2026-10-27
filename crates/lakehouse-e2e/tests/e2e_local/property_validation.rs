//! Property validation of CREATE, REFRESH, and ALTER ... SET (#534). Every test names its own
//! virtual schemas, and the stub swap works in an adapter schema of its own, so no shared
//! schema or script changes.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_TOTAL_ROWS};
use crate::fixtures::{lakehouse_vs, vs_table};

use std::collections::BTreeMap;

const PREFIX: &str = "invalid virtual schema properties:";

/// Name, minimum, and maximum of each whole-number tuning property.
const WHOLE_NUMBER_RANGES: [(&str, &str, &str); 7] = [
    ("PARALLELISM_FACTOR", "1", "255"),
    ("DATAFUSION_THREADS_PER_UDF", "1", "65535"),
    ("DATAFUSION_TARGET_PARTITIONS", "1", "65535"),
    ("S3_MAX_CONNECTIONS", "1", "65535"),
    ("INSTANCE_OVERHEAD_MB", "0", "65535"),
    ("DATAFUSION_BATCH_SIZE", "1", "4294967295"),
    ("JOIN_BROADCAST_MAX_BYTES", "1", "18446744073709551615"),
];

const TWENTY_ONE_DIGITS: &str = "123456789012345678901";

fn quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn property_clauses(props: &[(&str, &str)]) -> String {
    props
        .iter()
        .map(|(name, value)| format!(" {name} = '{}'", quote(value)))
        .collect()
}

fn base_props() -> Vec<(&'static str, &'static str)> {
    vec![
        ("CATALOG_CONNECTION", DEFAULT_CATALOG_CONN_NAME),
        ("ALLOW_HTTP", "true"),
        ("NAMESPACE", E2E_NAMESPACE),
    ]
}

fn create_sql(adapter_schema: &str, vs: &str, props: &[(&str, &str)]) -> String {
    format!(
        "CREATE VIRTUAL SCHEMA {vs} USING {adapter_schema}.{ADAPTER_SCRIPT_NAME} WITH{}",
        property_clauses(props)
    )
}

fn drop_vs(conn: &mut ExaConn, vs: &str) {
    conn.execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
}

/// The CONNECTION and the namespace's tables exist once `lakehouse_vs` has run.
fn create_ok(conn: &mut ExaConn, vs: &str, extra: &[(&str, &str)]) {
    drop_vs(conn, vs);
    let props: Vec<_> = base_props()
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
    conn.execute(&create_sql(SCHEMA_NAME, vs, &props));
}

/// The error text of a CREATE that sets exactly `props`; the schema must not exist afterwards.
#[track_caller]
fn create_error(conn: &mut ExaConn, vs: &str, props: &[(&str, &str)]) -> String {
    drop_vs(conn, vs);
    let sql = create_sql(SCHEMA_NAME, vs, props);
    let msg = query_error(conn, &sql);
    assert_eq!(
        schema_count(conn, vs),
        0,
        "a rejected CREATE must leave no virtual schema: {sql}"
    );
    msg
}

fn schema_count(conn: &mut ExaConn, vs: &str) -> i64 {
    conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{vs}'"
    ))
}

fn notes(conn: &mut ExaConn, vs: &str) -> BTreeMap<String, String> {
    schema_notes(conn, vs)
        .into_iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k, s.to_string())))
        .collect()
}

fn note_usize(notes: &BTreeMap<String, String>, key: &str) -> usize {
    notes
        .get(key)
        .unwrap_or_else(|| panic!("ADAPTER_NOTES must hold {key}: {notes:?}"))
        .parse()
        .unwrap_or_else(|e| panic!("{key} must be a whole number ({e}): {notes:?}"))
}

fn persisted_properties(conn: &mut ExaConn, vs: &str) -> BTreeMap<String, String> {
    let cols = conn.query_columns(&format!(
        "SELECT PROPERTY_NAME, PROPERTY_VALUE FROM SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES \
         WHERE SCHEMA_NAME = '{vs}'"
    ));
    cols[0]
        .iter()
        .zip(&cols[1])
        .map(|(n, v)| (value_to_string(n), value_to_string(v)))
        .collect()
}

fn table_count(conn: &mut ExaConn, vs: &str) -> i64 {
    conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = '{vs}'"
    ))
}

fn invalid_value(name: &str, echoed: &str, expected: &str) -> String {
    format!("invalid value '{echoed}' for '{name}': expected {expected}")
}

fn whole_number(min: &str, max: &str) -> String {
    format!("a whole number from {min} to {max}")
}

/// Scenario: An unknown property name fails the statement
#[test]
fn create_rejects_unknown_property_names_before_reading_the_connection() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let msg = create_error(
        &mut conn,
        "PV_UNKNOWN",
        &[
            ("CATALOG_CONNECTION", "PV_NO_SUCH_CONNECTION"),
            ("NAMESPACE", E2E_NAMESPACE),
            ("MADE_UP_PROPERTY", "value-of-an-unknown-property"),
        ],
    );

    assert!(
        msg.contains(&format!("{PREFIX} unknown property 'MADE_UP_PROPERTY'")),
        "{msg}"
    );
    assert!(
        !msg.contains("value-of-an-unknown-property"),
        "the value of an unknown property must not appear: {msg}"
    );
    assert!(
        !msg.contains("PV_NO_SUCH_CONNECTION"),
        "the name check must run before the CONNECTION is read: {msg}"
    );
}

/// Scenario: The error suggests the nearest known name for a close misspelling
#[test]
fn create_suggests_the_nearest_known_property_name() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let mut props = base_props();
    props.extend([
        ("NAMESPCE", "x"),
        ("PARALLELISM_FACTR", "x"),
        ("PERMISSION_CHEK", "x"),
        ("MADE_UP_PROPERTY", "x"),
    ]);
    let msg = create_error(&mut conn, "PV_SUGGEST", &props);

    for entry in [
        "unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?)",
        "unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?)",
        "unknown property 'PERMISSION_CHEK' (did you mean 'PERMISSION_CHECK'?)",
    ] {
        assert!(msg.contains(entry), "missing `{entry}`: {msg}");
    }
    assert!(
        msg.contains("unknown property 'MADE_UP_PROPERTY'")
            && !msg.contains("unknown property 'MADE_UP_PROPERTY' (did you mean"),
        "MADE_UP_PROPERTY is too far from every name for a suggestion: {msg}"
    );
}

/// Scenario: Every property the adapter reads is known under every catalog kind
/// Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes
/// Scenario: Adapter records the DataFusion threading mode in the virtual-schema adapterNotes
/// Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes
/// Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes
/// Scenario: Adapter records the memory-pool fraction in the virtual-schema adapterNotes
/// Scenario: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes
/// Scenario: FIXED mode uses the operator-supplied thread and partition values verbatim
/// Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema
#[test]
fn create_accepts_every_known_property_name() {
    const VS: &str = "PV_ALL_NAMES";
    lakehouse_vs();
    let mut conn = exa_conn();

    create_ok(
        &mut conn,
        VS,
        &[
            ("PARALLELISM_FACTOR", "3"),
            ("DATAFUSION_THREADING_MODE", "FIXED"),
            ("DATAFUSION_TARGET_PARTITIONS", "6"),
            ("DATAFUSION_THREADS_PER_UDF", "3"),
            ("DATAFUSION_BATCH_SIZE", "4096"),
            ("MEMORY_POOL_FRACTION", "0.5"),
            ("INSTANCE_OVERHEAD_MB", "100"),
            ("JOIN_BROADCAST_MAX_BYTES", "1048576"),
            ("S3_MAX_CONNECTIONS", "7"),
            ("MERGE_SCHEMA", "false"),
            ("HIVE_PARTITIONING", "false"),
            ("USER_MAPPING", "{{ user }}"),
        ],
    );

    let notes = notes(&mut conn, VS);
    for (key, expected) in [
        ("PARALLELISM_FACTOR", "3"),
        ("DF_THREADING_MODE", "FIXED"),
        ("DF_TARGET_PARTITIONS", "6"),
        ("DF_THREADS_PER_UDF", "3"),
        ("DF_BATCH_SIZE", "4096"),
        ("MEMORY_POOL_FRACTION", "0.5"),
        ("INSTANCE_OVERHEAD_MB", "100"),
        ("JOIN_BROADCAST_MAX_BYTES", "1048576"),
        ("S3_MAX_CONNECTIONS", "7"),
    ] {
        assert_eq!(
            notes.get(key).map(String::as_str),
            Some(expected),
            "{key} must record the supplied value: {notes:?}"
        );
    }
    drop_vs(&mut conn, VS);
}

/// Scenario: Threading mode defaults to AUTO when the property is absent
/// Scenario: AUTO mode gives the node's single scan instance every core
/// Scenario: AUTO derivation sizes the per-instance budget from the resolved thread count
/// Scenario: Adapter derives the per-node core count from available_parallelism() on every request
#[test]
fn create_without_tuning_properties_records_the_defaults() {
    const VS: &str = "PV_DEFAULTS";
    lakehouse_vs();
    let mut conn = exa_conn();

    create_ok(&mut conn, VS, &[]);

    let notes = notes(&mut conn, VS);
    let threads = note_usize(&notes, "DF_THREADS_PER_UDF");
    assert!(
        threads >= 1,
        "threads derive from the node's cores: {notes:?}"
    );
    assert_eq!(
        note_usize(&notes, "DF_TARGET_PARTITIONS"),
        2 * threads,
        "{notes:?}"
    );
    assert_eq!(
        note_usize(&notes, "S3_MAX_CONNECTIONS"),
        4 * threads,
        "{notes:?}"
    );
    for (key, expected) in [
        ("PARALLELISM_FACTOR", "1"),
        ("DF_THREADING_MODE", "AUTO"),
        ("DF_BATCH_SIZE", "8192"),
        ("MEMORY_POOL_FRACTION", "0.6"),
        ("INSTANCE_OVERHEAD_MB", "200"),
        ("JOIN_BROADCAST_MAX_BYTES", "134217728"),
    ] {
        assert_eq!(
            notes.get(key).map(String::as_str),
            Some(expected),
            "{key} must hold its default: {notes:?}"
        );
    }
    drop_vs(&mut conn, VS);
}

/// Scenario: An invalid tuning value fails the statement and names the accepted values
/// Scenario: A whole-number tuning value is checked against the range of its type
/// Scenario: A tuning value is checked on its own and never replaced by its default
#[test]
fn create_rejects_each_invalid_tuning_value() {
    const VS: &str = "PV_INVALID";
    lakehouse_vs();
    let mut conn = exa_conn();

    for (name, min, max) in WHOLE_NUMBER_RANGES {
        let above_max = (max.parse::<u128>().expect("range maximum") + 1).to_string();
        let expected = whole_number(min, max);
        for value in [above_max.as_str(), TWENTY_ONE_DIGITS] {
            let msg = create_error(
                &mut conn,
                VS,
                &[base_props().as_slice(), &[(name, value)]].concat(),
            );
            assert!(
                msg.contains(&invalid_value(name, value, &expected)),
                "{name} = '{value}' must fail with the range of its type: {msg}"
            );
        }
    }

    let s3_range = whole_number("1", "65535");
    for value in ["AUTO", "0"] {
        let props = [base_props().as_slice(), &[("S3_MAX_CONNECTIONS", value)]].concat();
        let msg = create_error(&mut conn, VS, &props);
        assert!(
            msg.contains(&invalid_value("S3_MAX_CONNECTIONS", value, &s3_range)),
            "S3_MAX_CONNECTIONS = '{value}' must fail, never select the derived budget: {msg}"
        );
    }

    let batch_size_range = whole_number("1", "4294967295");
    let msg = create_error(
        &mut conn,
        VS,
        &[base_props().as_slice(), &[("DATAFUSION_BATCH_SIZE", "0")]].concat(),
    );
    assert!(
        msg.contains(&invalid_value(
            "DATAFUSION_BATCH_SIZE",
            "0",
            &batch_size_range
        )),
        "a batch size of 0 must fail instead of clamping to 1: {msg}"
    );

    let long_value = format!("'{}", "b".repeat(149));
    let echoed = format!("''{}…", "b".repeat(99));
    let msg = create_error(
        &mut conn,
        VS,
        &[
            base_props().as_slice(),
            &[("DATAFUSION_BATCH_SIZE", long_value.as_str())],
        ]
        .concat(),
    );
    assert!(
        msg.contains(&invalid_value(
            "DATAFUSION_BATCH_SIZE",
            &echoed,
            &batch_size_range
        )),
        "a long value is cut at 100 characters with an ellipsis and its quote doubled: {msg}"
    );

    for (name, value, expected) in [
        (
            "MEMORY_POOL_FRACTION",
            "60",
            "a number greater than 0 and at most 1",
        ),
        (
            "MEMORY_POOL_FRACTION",
            "0",
            "a number greater than 0 and at most 1",
        ),
        ("DATAFUSION_THREADING_MODE", "garbage", "AUTO or FIXED"),
        ("ALLOW_HTTP", "yes", "TRUE or FALSE"),
    ] {
        let mut props = base_props();
        props.retain(|(n, _)| *n != name);
        props.push((name, value));
        let msg = create_error(&mut conn, VS, &props);
        assert!(
            msg.contains(&invalid_value(name, value, expected)),
            "{name} = '{value}' must fail: {msg}"
        );
    }

    // AUTO mode never reads DATAFUSION_TARGET_PARTITIONS, yet the value is still checked.
    let msg = create_error(
        &mut conn,
        VS,
        &[
            base_props().as_slice(),
            &[("DATAFUSION_TARGET_PARTITIONS", "0")],
        ]
        .concat(),
    );
    assert!(
        msg.contains(&invalid_value(
            "DATAFUSION_TARGET_PARTITIONS",
            "0",
            &whole_number("1", "65535")
        )),
        "an ignored value must still be checked: {msg}"
    );
}

/// Scenario: One error reports every unknown name and every invalid value
#[test]
fn create_reports_every_property_problem_in_one_error() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let msg = create_error(
        &mut conn,
        "PV_ALL_PROBLEMS",
        &[
            ("CATALOG_CONNECTION", DEFAULT_CATALOG_CONN_NAME),
            ("NAMESPCE", "sales"),
            ("PARALLELISM_FACTR", "64"),
            ("DATAFUSION_BATCH_SIZE", "abc"),
            ("MADE_UP_PROPERTY", "x"),
        ],
    );

    let expected = format!(
        "{PREFIX} unknown property 'MADE_UP_PROPERTY'; \
         unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?); \
         unknown property 'PARALLELISM_FACTR' (did you mean 'PARALLELISM_FACTOR'?); \
         invalid value 'abc' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295"
    );
    assert!(msg.contains(&expected), "{msg}");
}

/// Scenario: A misspelled required property reports the misspelling
#[test]
fn misspelled_required_property_reports_the_misspelling() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let msg = create_error(
        &mut conn,
        "PV_MISSPELLED",
        &[
            ("CATALOG_CONECTION", DEFAULT_CATALOG_CONN_NAME),
            ("NAMESPACE", E2E_NAMESPACE),
        ],
    );
    assert!(
        msg.contains("unknown property 'CATALOG_CONECTION' (did you mean 'CATALOG_CONNECTION'?)"),
        "{msg}"
    );
    assert!(
        !msg.contains("CATALOG_CONNECTION is required"),
        "the name check must precede the required-property check: {msg}"
    );

    let mut props = base_props();
    props.push(("CATALOG_KIND", "NOPE"));
    let msg = create_error(&mut conn, "PV_BAD_KIND", &props);
    assert!(
        msg.contains("unrecognized 'CATALOG_KIND' value 'NOPE'"),
        "{msg}"
    );
    assert!(
        !msg.contains(PREFIX),
        "CATALOG_KIND is checked by its own feature, so the catalog does not report it: {msg}"
    );
}

/// Scenario: An empty or null property value counts as absent
#[test]
fn empty_and_null_property_values_count_as_absent() {
    const VS: &str = "PV_EMPTY_NULL";
    lakehouse_vs();
    let mut conn = exa_conn();

    // Exasol rejects an empty or null value in CREATE before it calls the adapter.
    for name in ["NAMESPACE", "MEMORY_POOL_FRACTION"] {
        for literal in ["''", "null"] {
            drop_vs(&mut conn, VS);
            let sql = format!(
                "CREATE VIRTUAL SCHEMA {VS} USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH \
                 CATALOG_CONNECTION = '{DEFAULT_CATALOG_CONN_NAME}' ALLOW_HTTP = 'true' \
                 NAMESPACE = '{E2E_NAMESPACE}' {name} = {literal}"
            );
            let msg = query_error(&mut conn, &sql);
            assert!(
                msg.contains(&format!(
                    "Value of property {name} must not be null or empty."
                )),
                "Exasol rejects CREATE with {name} = {literal}: {msg}"
            );
        }
    }

    // ALTER ... SET sends '' and null to the adapter as a JSON null, which unsets the property.
    create_ok(&mut conn, VS, &[("MEMORY_POOL_FRACTION", "0.5")]);
    for literal in ["''", "null"] {
        conn.execute(&format!(
            "ALTER VIRTUAL SCHEMA {VS} SET MEMORY_POOL_FRACTION = '0.5'"
        ));
        assert_eq!(notes(&mut conn, VS)["MEMORY_POOL_FRACTION"], "0.5");

        conn.execute(&format!(
            "ALTER VIRTUAL SCHEMA {VS} SET MEMORY_POOL_FRACTION = {literal}"
        ));
        assert_eq!(
            notes(&mut conn, VS)["MEMORY_POOL_FRACTION"],
            "0.6",
            "SET MEMORY_POOL_FRACTION = {literal} must fall back to the default"
        );
        assert!(
            !persisted_properties(&mut conn, VS).contains_key("MEMORY_POOL_FRACTION"),
            "SET MEMORY_POOL_FRACTION = {literal} must remove the property"
        );

        conn.execute(&format!("ALTER VIRTUAL SCHEMA {VS} REFRESH"));
        assert_eq!(notes(&mut conn, VS)["MEMORY_POOL_FRACTION"], "0.6");
    }

    // A required property that is unset reports the missing property, not an invalid value.
    for literal in ["''", "null"] {
        let sql = format!("ALTER VIRTUAL SCHEMA {VS} SET NAMESPACE = {literal}");
        let msg = query_error(&mut conn, &sql);
        assert!(
            msg.contains("property 'NAMESPACE' is required"),
            "{sql}: {msg}"
        );
        assert!(!msg.contains(PREFIX), "{sql}: {msg}");
        conn.execute(&format!("ALTER VIRTUAL SCHEMA {VS} REFRESH"));
    }
    drop_vs(&mut conn, VS);
}

/// Scenario: Set properties validates the merged properties after null unsets
#[test]
fn set_properties_rejects_problems_and_keeps_the_schema() {
    const VS: &str = "PV_SET";
    lakehouse_vs();
    let mut conn = exa_conn();

    create_ok(&mut conn, VS, &[("MEMORY_POOL_FRACTION", "0.5")]);
    let properties = persisted_properties(&mut conn, VS);
    let notes_before = notes(&mut conn, VS);
    let tables = table_count(&mut conn, VS);
    assert!(tables > 0, "{VS} must hold the namespace's tables");

    for (statement, expected) in [
        (
            "SET PV_MADE_UP = 'x'",
            "unknown property 'PV_MADE_UP'".to_string(),
        ),
        (
            "SET MEMORY_POOL_FRACTION = '60'",
            invalid_value(
                "MEMORY_POOL_FRACTION",
                "60",
                "a number greater than 0 and at most 1",
            ),
        ),
    ] {
        let sql = format!("ALTER VIRTUAL SCHEMA {VS} {statement}");
        let msg = query_error(&mut conn, &sql);
        assert!(
            msg.contains(PREFIX) && msg.contains(&expected),
            "{sql}: {msg}"
        );
        assert_eq!(persisted_properties(&mut conn, VS), properties, "{sql}");
        assert_eq!(notes(&mut conn, VS), notes_before, "{sql}");
        assert_eq!(table_count(&mut conn, VS), tables, "{sql}");
    }
    drop_vs(&mut conn, VS);
}

const STUB_ADAPTER_SCHEMA: &str = "PV_STUB_ADAPTER";
const STRAY_VS: &str = "PV_STRAY";
const STRAY_NAME: &str = "PV_STRAY_NAME";

/// Persists whatever `ALTER ... SET` names, as an adapter that predates the check did.
const PERSISTING_ADAPTER_BODY: &str = r#"import json


def adapter_call(request_json):
    kind = json.loads(request_json)["type"]
    if kind == "getCapabilities":
        return json.dumps({"type": kind, "capabilities": []})
    if kind == "setProperties":
        return json.dumps({"type": kind})
    raise Exception("the stub adapter answers only setProperties, got " + kind)
"#;

/// Swaps the stub in, sets the stray properties, and puts the engine adapter back.
fn persist_stray_properties(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT \
         {STUB_ADAPTER_SCHEMA}.{ADAPTER_SCRIPT_NAME} AS\n{PERSISTING_ADAPTER_BODY}\n/"
    ));
    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {STRAY_VS} SET {STRAY_NAME} = 'x' MEMORY_POOL_FRACTION = '60'"
    ));
    create_schema_and_scripts_in(conn, STUB_ADAPTER_SCHEMA);
}

/// Scenario: A schema persisted with an invalid property refuses refresh until the property is unset
/// Scenario: Pushdown and drop never validate properties
#[test]
fn stray_persisted_properties_block_refresh_but_not_queries() {
    lakehouse_vs();
    let mut conn = exa_conn();

    create_schema_and_scripts_in(&mut conn, STUB_ADAPTER_SCHEMA);
    drop_vs(&mut conn, STRAY_VS);
    conn.execute(&create_sql(STUB_ADAPTER_SCHEMA, STRAY_VS, &base_props()));
    persist_stray_properties(&mut conn);
    let persisted = persisted_properties(&mut conn, STRAY_VS);
    assert_eq!(
        persisted.get(STRAY_NAME).map(String::as_str),
        Some("x"),
        "{persisted:?}"
    );

    let count = format!("SELECT COUNT(*) FROM {}", vs_table(STRAY_VS, E2E_TABLE));
    assert_eq!(
        conn.query_scalar_i64(&count),
        SEED_TOTAL_ROWS as i64,
        "a pushdown must plan from the adapterNotes and ignore the stray properties"
    );

    let refresh = format!("ALTER VIRTUAL SCHEMA {STRAY_VS} REFRESH");
    let msg = query_error(&mut conn, &refresh);
    assert!(
        msg.contains(PREFIX)
            && msg.contains(&format!("unknown property '{STRAY_NAME}'"))
            && msg.contains(&invalid_value(
                "MEMORY_POOL_FRACTION",
                "60",
                "a number greater than 0 and at most 1"
            )),
        "REFRESH must list every stray property: {msg}"
    );
    assert_eq!(conn.query_scalar_i64(&count), SEED_TOTAL_ROWS as i64);

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {STRAY_VS} SET {STRAY_NAME} = null MEMORY_POOL_FRACTION = null"
    ));
    let persisted = persisted_properties(&mut conn, STRAY_VS);
    assert!(
        !persisted.contains_key(STRAY_NAME) && !persisted.contains_key("MEMORY_POOL_FRACTION"),
        "SET ... = null must remove the stray properties: {persisted:?}"
    );
    conn.execute(&refresh);
    assert_eq!(conn.query_scalar_i64(&count), SEED_TOTAL_ROWS as i64);

    persist_stray_properties(&mut conn);
    conn.execute(&format!("DROP VIRTUAL SCHEMA {STRAY_VS} CASCADE"));
    assert_eq!(schema_count(&mut conn, STRAY_VS), 0);
}
