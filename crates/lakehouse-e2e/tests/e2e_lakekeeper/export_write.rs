//! `EXPORT ... INTO SCRIPT` into new tables of the Lakekeeper warehouses. Each test exports into
//! namespaces of its own and reads the new tables back through a virtual schema over them.

use std::collections::BTreeSet;
use std::sync::{LazyLock, OnceLock};

use serde_json::Value;

use super::{CONN_STATIC, CONN_VENDED, LAKEKEEPER_CATALOG_URI_INTERNAL, setup};
use crate::common::e2e_harness::{
    ExportRoute, NO_SECRETS, SCHEMA_NAME, VsProps, assert_holds_rows,
    assert_refused_before_reading, create_proxied_schema_and_scripts,
    create_virtual_schema_with_password, error_text, exa_conn, export_error, exported_rows,
    failing_source, run_namespace,
};
use crate::common::exasol_ws::ExaConn;
use crate::common::lakekeeper::{
    WAREHOUSE_STATIC, WAREHOUSE_VENDED, lakekeeper_connection_password, table_metadata,
};
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE};
use crate::common::stack::{
    self, CatalogConnectionPassword, PROXY_ADAPTER_SCHEMA, build_create_connection_sql,
};

const SOURCE_SCHEMA: &str = "LK_EXPORT_E2E";
const SOURCE_TABLE: &str = "LK_EXPORT_E2E.SRC_ROWS";
const SOURCE_ROWS: i64 = 1_000;
const TYPES_TABLE: &str = "LK_EXPORT_E2E.ALL_TYPES";
const ROWS_16K: &str = "LK_EXPORT_E2E.ROWS_16K";
const CROSS_JOIN_SIDE: &str = "LK_EXPORT_E2E.SIDE_20K";
static FAILING_SOURCE: LazyLock<String> = LazyLock::new(|| failing_source(SOURCE_TABLE));
const ALREADY_EXISTS: &str = "already exists; the export only creates new tables";
const PROXIED_CONN: &str = "LK_EXPORT_PROXY_CREDS";

/// The Exasol source tables of the exports.
fn sources() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        setup();
        let mut sys = exa_conn();
        sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA}"));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {SOURCE_TABLE} AS SELECT \
               CAST(LEVEL AS DECIMAL(9,0)) AS ID, CAST('v' || LEVEL AS VARCHAR(100)) AS LABEL \
             FROM DUAL CONNECT BY LEVEL <= {SOURCE_ROWS}"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {ROWS_16K} AS \
             SELECT CAST(LEVEL AS DECIMAL(9,0)) AS ID FROM DUAL CONNECT BY LEVEL <= 16000"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {CROSS_JOIN_SIDE} AS \
             SELECT CAST(LEVEL AS DECIMAL(9,0)) AS ID FROM DUAL CONNECT BY LEVEL <= 20000"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {TYPES_TABLE} (ID DECIMAL(9,0), D9_0 DECIMAL(9,0), \
             D12_0 DECIMAL(12,0), D18_0 DECIMAL(18,0), D9_2 DECIMAL(9,2), D18_2 DECIMAL(18,2), \
             D36_6 DECIMAL(36,6), DBL DOUBLE, FLAG BOOLEAN, TXT VARCHAR(50), CHR CHAR(5), \
             DT DATE, TS TIMESTAMP, TS6 TIMESTAMP(6))"
        ));
        // Exasol stores no DOUBLE beyond ±1.7976E308, short of the IEEE 754 maximum.
        sys.execute(&format!(
            "INSERT INTO {TYPES_TABLE} VALUES \
             (1, 123456789, 123456789012, 123456789012345678, 1234567.89, 1.50, 100.000000, \
              2.2250738585072014E-308, TRUE, 'plain', 'ab', DATE '2024-02-29', \
              TIMESTAMP '2024-01-01 12:34:56.123', TIMESTAMP '2024-01-01 12:34:56.123456'), \
             (2, -999999999, -999999999999, -999999999999999999, -9999999.99, 100.00, \
              999999999999999999999999999999.999999, 1.7976E308, FALSE, \
              'Grüße, 日本語', 'xyz', DATE '0001-01-01', TIMESTAMP '0001-01-01 00:00:00.000', \
              TIMESTAMP '0001-01-01 00:00:00.000001'), \
             (3, 0, 0, 0, 0.00, 0.00, -999999999999999999999999999999.999999, \
              -1.7976E308, TRUE, 'x', 'abcde', DATE '9999-12-31', \
              TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '9999-12-31 23:59:59.999999'), \
             (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        ));
    });
}

/// A Lakekeeper warehouse and the export script and CONNECTION a statement reaches it through.
#[derive(Clone, Copy)]
struct Target {
    warehouse: &'static str,
    vended: bool,
    route: ExportRoute,
}

const STATIC: Target = Target {
    warehouse: WAREHOUSE_STATIC,
    vended: false,
    route: ExportRoute {
        script_schema: SCHEMA_NAME,
        connection: CONN_STATIC,
    },
};

const VENDED: Target = Target {
    warehouse: WAREHOUSE_VENDED,
    vended: true,
    route: ExportRoute {
        script_schema: SCHEMA_NAME,
        connection: CONN_VENDED,
    },
};

/// The static warehouse through the proxy-only scripts, so the proxy log shows each request.
const PROXIED_STATIC: Target = Target {
    route: ExportRoute {
        script_schema: PROXY_ADAPTER_SCHEMA,
        connection: PROXIED_CONN,
    },
    ..STATIC
};

impl Target {
    fn export(self, source: &str, table: &str, options: &str) -> String {
        self.route.export(source, table, options)
    }

    /// The same target through a CONNECTION named `connection` with `password`.
    fn through(self, connection: &'static str, password: &CatalogConnectionPassword) -> Self {
        exa_conn().execute(&build_create_connection_sql(
            connection,
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            password,
        ));
        Self {
            route: ExportRoute {
                connection,
                ..self.route
            },
            ..self
        }
    }

    fn password(self) -> CatalogConnectionPassword {
        lakekeeper_connection_password(self.warehouse, self.vended)
    }

    /// Creates the virtual schema `vs` over `namespace`.
    fn read_back(self, vs: &str, namespace: &str) {
        create_virtual_schema_with_password(
            &mut exa_conn(),
            &VsProps::new(vs, namespace).with_catalog_conn_name(self.route.connection),
            LAKEKEEPER_CATALOG_URI_INTERNAL,
            &self.password(),
        );
    }

    fn metadata(self, table: &str) -> Option<Value> {
        table_metadata(self.warehouse, table)
    }

    #[track_caller]
    fn assert_no_table(self, case: &str, table: &str) {
        assert!(
            self.metadata(table).is_none(),
            "{case}: the export must not create {table}"
        );
    }
}

/// Installs the proxy-only scripts and a CONNECTION to the static warehouse whose addresses
/// only the forward proxy resolves.
fn proxied_static() -> Target {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        setup();
        let proxy_url = stack::proxy_url_internal();
        let proxy = [("HTTP_PROXY", proxy_url.as_str())];
        let password = STATIC.password().via_proxy();
        let catalog = stack::via_proxy(LAKEKEEPER_CATALOG_URI_INTERNAL);
        let token_uri = password
            .oauth2_server_uri
            .clone()
            .expect("the OAuth2 client-credentials CONNECTION names a token endpoint");
        let mut sys = exa_conn();
        create_proxied_schema_and_scripts(
            &mut sys,
            PROXY_ADAPTER_SCHEMA,
            &proxy,
            &proxy,
            &[&catalog, &token_uri, &password.endpoint],
        );
        sys.execute(&build_create_connection_sql(
            PROXIED_CONN,
            &catalog,
            &password,
        ));
    });
    PROXIED_STATIC
}

#[track_caller]
fn assert_names(case: &str, error: &str, fragments: &[&str]) {
    assert!(
        fragments.iter().all(|fragment| error.contains(fragment)),
        "{case}: the error must name {fragments:?}: {error}"
    );
}

/// The name and Iceberg type of each column of `metadata`'s current schema.
fn iceberg_columns(metadata: &Value) -> Vec<(&str, &str)> {
    let schema_id = &metadata["current-schema-id"];
    let schema = metadata["schemas"]
        .as_array()
        .and_then(|schemas| schemas.iter().find(|s| &s["schema-id"] == schema_id))
        .unwrap_or_else(|| panic!("no current schema: {metadata}"));
    schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            (
                field["name"].as_str().unwrap(),
                field["type"].as_str().unwrap(),
            )
        })
        .collect()
}

fn snapshots(metadata: &Value) -> &[Value] {
    metadata["snapshots"].as_array().map_or(&[], Vec::as_slice)
}

/// The values of the snapshot summary property `key`, one per snapshot.
fn summary_values(metadata: &Value, key: &str) -> Vec<String> {
    snapshots(metadata)
        .iter()
        .map(|snapshot| {
            snapshot["summary"][key]
                .as_str()
                .unwrap_or_else(|| panic!("a snapshot without {key}: {snapshot}"))
                .to_string()
        })
        .collect()
}

/// Scenario: The export creates a missing namespace and table
#[test]
fn export_creates_missing_namespace_and_table() {
    sources();
    let namespace = format!("{}.inner", run_namespace("two_levels"));
    let table = format!("{namespace}.t");

    assert_eq!(
        exported_rows(&mut exa_conn(), &STATIC.export(SOURCE_TABLE, &table, "")),
        SOURCE_ROWS
    );

    let metadata = STATIC
        .metadata(&table)
        .unwrap_or_else(|| panic!("the catalog must hold {table}"));
    assert_eq!(
        iceberg_columns(&metadata),
        [("ID", "decimal(9,0)"), ("LABEL", "string")]
    );
    let vs = "LK_EXPORT_TWO_LEVELS";
    STATIC.read_back(vs, &namespace);
    assert_holds_rows(
        &mut exa_conn(),
        &format!("{vs}.T"),
        &format!("SELECT ID, LABEL FROM {SOURCE_TABLE}"),
        SOURCE_ROWS,
    );
}

/// Scenario: Every supported type reads back unchanged, NULLs included
/// Scenario: Static and vended storage credentials both write the table
#[test]
fn export_round_trips_every_supported_type() {
    sources();
    let vended = VENDED.password();
    assert!(
        vended.endpoint.is_empty() && vended.access_key.is_empty() && vended.secret_key.is_empty(),
        "the vended CONNECTION must hold no static storage field, which would override the vended one"
    );

    for (target, vs) in [
        (VENDED, "LK_EXPORT_TYPES_VENDED"),
        (STATIC, "LK_EXPORT_TYPES_STATIC"),
    ] {
        let namespace = run_namespace(&vs.to_lowercase());
        let table = format!("{namespace}.all_types");
        let sql = target.export(TYPES_TABLE, &table, "");
        assert_eq!(exported_rows(&mut exa_conn(), &sql), 4, "{vs}");
        // A round trip alone reads a wider decimal or a `timestamptz` back as equal values.
        assert_eq!(
            iceberg_columns(&target.metadata(&table).unwrap()),
            TYPES_ICEBERG_COLUMNS,
            "{vs}"
        );
        target.read_back(vs, &namespace);
        assert_types_read_back(&mut exa_conn(), &format!("{vs}.ALL_TYPES"));
    }
}

/// The Iceberg column of each `TYPES_TABLE` column, as the export maps its Exasol type.
const TYPES_ICEBERG_COLUMNS: [(&str, &str); 14] = [
    ("ID", "decimal(9,0)"),
    ("D9_0", "decimal(9,0)"),
    ("D12_0", "decimal(12,0)"),
    ("D18_0", "decimal(18,0)"),
    ("D9_2", "decimal(9,2)"),
    ("D18_2", "decimal(18,2)"),
    ("D36_6", "decimal(36,6)"),
    ("DBL", "double"),
    ("FLAG", "boolean"),
    ("TXT", "string"),
    ("CHR", "string"),
    ("DT", "date"),
    ("TS", "timestamp"),
    ("TS6", "timestamp"),
];

/// Asserts that `table` holds the rows of `TYPES_TABLE`, each matched by its ID with equal
/// values. A MINUS would push a GROUP BY over `table`, whose DOUBLE keys travel as text that
/// Exasol reads back as another double at ±1.7976E308.
#[track_caller]
fn assert_types_read_back(conn: &mut ExaConn, table: &str) {
    let columns = [
        "D9_0", "D12_0", "D18_0", "D9_2", "D18_2", "D36_6", "DBL", "FLAG", "TXT", "CHR", "DT",
        "TS", "TS6",
    ];
    let equal = columns
        .map(|c| format!("(v.{c} = s.{c} OR (v.{c} IS NULL AND s.{c} IS NULL))"))
        .join(" AND ");
    let matched =
        format!("SELECT COUNT(*) FROM {table} v JOIN {TYPES_TABLE} s ON v.ID = s.ID WHERE {equal}");

    assert_eq!(
        conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {table}")),
        4
    );
    assert_eq!(
        conn.query_scalar_i64(&matched),
        4,
        "every value reads back unchanged"
    );
    assert_eq!(
        conn.query_columns(&format!("SELECT CHR FROM {table} WHERE ID = 1")),
        [[Value::from("ab   ")]],
        "a CHAR value keeps its trailing blanks"
    );
}

/// Scenario: A column of an unsupported type fails the export before any data is written
#[test]
fn export_refuses_unsupported_column_types() {
    sources();
    let namespace = run_namespace("unsupported_types");
    // Measured: which of Exasol's two refusals a column gets varies between statements, and a
    // column Exasol lets through meets the export's own refusal.
    let refusals = |column: &str, spelling: &str| {
        [
            format!("Column type not supported: {spelling}"),
            "VM error: Unknown data type found".to_string(),
            format!("column {column} has type {spelling}, which the export cannot write"),
        ]
    };
    let cases = [
        (
            "CAST('550e8400e29b11d4a716446655440000' AS HASHTYPE)",
            "HASHTYPE(16 BYTE)",
        ),
        (
            "CAST('1 12:00:00.000' AS INTERVAL DAY TO SECOND)",
            "INTERVAL DAY(2) TO SECOND(3)",
        ),
        (
            "CAST(TIMESTAMP '2024-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)",
            "TIMESTAMP(3) WITH LOCAL TIME ZONE",
        ),
        ("CAST('POINT (1 2)' AS GEOMETRY)", "GEOMETRY"),
        (
            "CAST('1-2' AS INTERVAL YEAR TO MONTH)",
            "INTERVAL YEAR(2) TO MONTH",
        ),
    ];

    for (index, (expression, spelling)) in cases.into_iter().enumerate() {
        let table = format!("{namespace}.t{index}");
        let source = format!("(SELECT {expression} AS C FROM DUAL)");
        let error = export_error(
            &mut exa_conn(),
            &STATIC.export(&source, &table, ""),
            NO_SECRETS,
        );
        assert!(
            refusals("C", spelling)
                .iter()
                .any(|refusal| error.contains(refusal)),
            "{spelling}: the export must fail with a refusal of the type: {error}"
        );
        STATIC.assert_no_table(spelling, &table);
    }
    let table = format!("{namespace}.timestamp_9");
    let source = "(SELECT CAST(TIMESTAMP '2024-01-01 00:00:00.123456789' AS TIMESTAMP(9)) AS TS9 \
                  FROM DUAL)";
    let error = export_error(
        &mut exa_conn(),
        &STATIC.export(source, &table, ""),
        NO_SECRETS,
    );
    assert_names(
        "TIMESTAMP(9)",
        &error,
        &[
            "column TS9 has type TIMESTAMP(9), which the export cannot write",
            "cast a TIMESTAMP(7) to TIMESTAMP(9) column to TIMESTAMP(6)",
        ],
    );
    STATIC.assert_no_table("TIMESTAMP(9)", &table);
}

/// Scenario: An export into an existing table is refused
#[test]
fn export_refuses_an_existing_table() {
    sources();
    let target = proxied_static();
    let existing = format!("{E2E_NAMESPACE}.{E2E_TABLE}");
    let snapshot_ids = || {
        let metadata = STATIC.metadata(&existing).expect("the seeded table exists");
        summary_ids(&metadata)
    };
    let before = snapshot_ids();
    let mut error = String::new();

    let log = stack::proxy_log_during(|| {
        error = assert_refused_before_reading(
            "existing table",
            &mut exa_conn(),
            &target.export(&FAILING_SOURCE, &existing, ""),
            &[&format!("table {existing} {ALREADY_EXISTS}")],
            NO_SECRETS,
        );
    });

    assert_eq!(snapshot_ids(), before, "the table keeps its snapshots");
    let table_head = format!("/namespaces/{E2E_NAMESPACE}/tables/{E2E_TABLE} HTTP");
    let requests: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("Request"))
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|line| line.contains("HEAD ") && line.contains(&table_head))
            .count(),
        1,
        "one table-existence check: {requests:#?}"
    );
    assert!(
        !requests
            .iter()
            .any(|line| line.contains("/namespaces HTTP")),
        "an existing table is refused before any namespace call: {requests:#?}"
    );
}

fn summary_ids(metadata: &Value) -> BTreeSet<String> {
    snapshots(metadata)
        .iter()
        .map(|snapshot| snapshot["snapshot-id"].to_string())
        .collect()
}

/// Scenario: Two exports that create the same table leave one winner
#[test]
fn export_racing_exports_leave_one_winner() {
    sources();
    let namespace = run_namespace("race");
    let table = format!("{namespace}.raced");
    // The cross join takes seconds before its one row, so both statements pass the pre-read check.
    let source = |marker: &str| {
        format!(
            "(SELECT '{marker}' AS MARKER, COUNT(*) AS N FROM {CROSS_JOIN_SIDE} a, \
             {CROSS_JOIN_SIDE} b WHERE a.ID + b.ID > 0)"
        )
    };

    let outcomes: Vec<(&str, Value)> = std::thread::scope(|scope| {
        let racers: Vec<_> = ["A", "B"]
            .map(|marker| {
                let sql = STATIC.export(&source(marker), &table, "");
                scope.spawn(move || (marker, exa_conn().try_execute(&sql)))
            })
            .into_iter()
            .collect();
        racers
            .into_iter()
            .map(|racer| racer.join().expect("an export thread"))
            .collect()
    });

    let (winners, losers): (Vec<_>, Vec<_>) = outcomes
        .iter()
        .partition(|(_, response)| response["status"] == "ok");
    assert_eq!(
        winners.len(),
        1,
        "exactly one export succeeds: {outcomes:?}"
    );
    let error = error_text(&losers[0].1, "the losing export");
    assert_names("losing export", &error, &[&table]);
    let vs = "LK_EXPORT_RACE";
    STATIC.read_back(vs, &namespace);
    let markers = exa_conn().query_columns(&format!("SELECT MARKER FROM {vs}.RACED"));
    assert_eq!(
        markers,
        [[Value::from(winners[0].0)]],
        "only the winner's rows"
    );
}

/// Scenario: Concurrent writers create one table, and every commit carries the export ID
#[test]
fn export_concurrent_writers_commit_once_with_the_export_id() {
    sources();
    let namespace = run_namespace("writers");
    let vs = "LK_EXPORT_WRITERS";
    let tables = ["first", "second"];

    for table in tables {
        let sql = VENDED.export(
            ROWS_16K,
            &format!("{namespace}.{table}"),
            "WRITERS_PER_NODE = '16'",
        );
        assert_eq!(exported_rows(&mut exa_conn(), &sql), 16_000, "{table}");
    }
    VENDED.read_back(vs, &namespace);

    let mut export_ids = BTreeSet::new();
    for table in tables {
        let metadata = VENDED.metadata(&format!("{namespace}.{table}")).unwrap();
        let ids: BTreeSet<String> = summary_values(&metadata, "lakehouse.export-id")
            .into_iter()
            .collect();
        let writers: BTreeSet<String> = summary_values(&metadata, "lakehouse.export-writer")
            .into_iter()
            .collect();
        assert_eq!(
            snapshots(&metadata).len(),
            16,
            "{table}: one snapshot per writer"
        );
        assert_eq!(ids.len(), 1, "{table}: one export ID: {ids:?}");
        assert_eq!(writers.len(), 16, "{table}: 16 writers: {writers:?}");
        export_ids.extend(ids);
        assert_holds_rows(
            &mut exa_conn(),
            &format!("{vs}.{}", table.to_uppercase()),
            &format!("SELECT ID FROM {ROWS_16K}"),
            16_000,
        );
    }
    assert_eq!(export_ids.len(), 2, "each export has its own ID");
}

/// Scenario: A bad CONNECTION fails before any data is read
#[test]
fn export_bad_catalog_connection_fails_before_reading() {
    sources();
    let namespace = run_namespace("bad_catalog");
    let wrong_secret = "LkExportWrongClientSecret2026";
    let unreachable = "http://lakekeeper:1/catalog";
    let password = STATIC.password();
    let mut sys = exa_conn();
    sys.execute(&build_create_connection_sql(
        "LK_EXPORT_UNREACHABLE",
        unreachable,
        &password,
    ));
    let cases = [
        (
            "unreachable catalog",
            Target {
                route: ExportRoute {
                    connection: "LK_EXPORT_UNREACHABLE",
                    ..STATIC.route
                },
                ..STATIC
            },
            unreachable.to_string(),
        ),
        (
            "wrong client secret",
            STATIC.through(
                "LK_EXPORT_WRONG_CLIENT_SECRET",
                &CatalogConnectionPassword {
                    client_secret: Some(wrong_secret.to_string()),
                    ..STATIC.password()
                },
            ),
            LAKEKEEPER_CATALOG_URI_INTERNAL.to_string(),
        ),
    ];

    for (case, target, address) in cases {
        let table = format!("{namespace}.t");
        assert_refused_before_reading(
            case,
            &mut exa_conn(),
            &target.export(&FAILING_SOURCE, &table, ""),
            &[&format!("the catalog {address}")],
            &[wrong_secret, &password.secret_key, &password.access_key],
        );
        STATIC.assert_no_table(case, &table);
    }
}

/// Scenario: A wrong storage key fails the writer, naming the storage
#[test]
fn export_wrong_storage_secret_fails_at_the_writer() {
    sources();
    let namespace = run_namespace("wrong_key");
    let wrong_key = "LkExportWrongStorageKey2026";
    let target = STATIC.through(
        "LK_EXPORT_WRONG_STORAGE_KEY",
        &CatalogConnectionPassword {
            secret_key: wrong_key.to_string(),
            ..STATIC.password()
        },
    );

    // Without SINGLE_COMMIT the export creates the table before any writer runs.
    for (case, options, snapshot_count) in [
        ("parallel", "", Some(0)),
        ("single_commit", "SINGLE_COMMIT = 'true'", None),
    ] {
        let table = format!("{namespace}.{case}");
        let error = export_error(
            &mut exa_conn(),
            &target.export(SOURCE_TABLE, &table, options),
            &[wrong_key, &STATIC.password().access_key],
        );
        assert_names(
            case,
            &error,
            &[&format!(
                "writing the data files of table {table} to storage failed"
            )],
        );
        assert_eq!(
            target.metadata(&table).map(|m| snapshots(&m).len()),
            snapshot_count,
            "{case}: the snapshot count of {table}, None when it does not exist"
        );
    }
}

/// Scenario: SINGLE_COMMIT creates the table only together with its rows
#[test]
fn export_single_commit_creates_the_table_with_its_rows() {
    sources();
    let table = format!("{}.t", run_namespace("single_commit"));

    let sql = STATIC.export(SOURCE_TABLE, &table, "SINGLE_COMMIT = 'true'");
    assert_eq!(exported_rows(&mut exa_conn(), &sql), SOURCE_ROWS);

    let metadata = STATIC
        .metadata(&table)
        .expect("the export creates the table");
    assert_eq!(
        summary_values(&metadata, "total-records"),
        [SOURCE_ROWS.to_string()],
        "one snapshot of all rows"
    );
    assert!(
        metadata["metadata-log"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "no earlier metadata version: {}",
        metadata["metadata-log"]
    );
}

/// Scenario: An empty source exports zero rows
#[test]
fn export_empty_source_creates_no_table() {
    sources();
    let table = format!("{}.t", run_namespace("empty"));
    // A column type the writer refuses, which no writer checks when no row arrives.
    let source = format!(
        "(SELECT ID, CAST(TIMESTAMP '2024-01-01 00:00:00.123456789' AS TIMESTAMP(9)) AS TS9 \
         FROM {SOURCE_TABLE} WHERE ID < 0)"
    );

    assert_eq!(
        exported_rows(&mut exa_conn(), &STATIC.export(&source, &table, "")),
        0
    );
    STATIC.assert_no_table("empty source", &table);
}
