//! `EXPORT ... INTO SCRIPT` into existing tables of the local REST catalog. The Spark tables come
//! from `create_export_append_fixture.sql`, mirrored in `common/export_append_fixtures.rs`.

use std::sync::OnceLock;

use parquet::basic::{Compression, ZstdLevel};
use serde_json::Value;

use crate::common::e2e_harness::{
    NO_SECRETS, VsProps, create_virtual_schema, exa_conn, export_error, exported_rows,
    run_namespace, text_column,
};
use crate::common::export_append_fixtures::{
    APPENDABLE_COLUMNS, APPENDABLE_ROWS, APPENDABLE_TEXT, APPENDABLE_V1, APPENDABLE_V1_SPEC,
    APPENDABLE_V1_SPEC_ID, APPENDABLE_V2, APPENDABLE_V2_SPEC, APPENDABLE_V2_SPEC_ID, NAMESPACE,
    ODD_TYPES, ODD_TYPES_COLUMNS, V1_VOID_COLUMN, V1_VOID_COLUMN_COLUMNS, V1_VOID_COLUMN_SPEC, V3,
    V3_COLUMNS,
};
use crate::common::table_state::{
    DataFileFooter, assert_data_files, assert_one_new_export, data_file_footers, data_prefix,
    iceberg_columns, local_table_metadata, objects_under, remove_table_properties,
    reset_to_unexported_snapshot, set_table_properties, snapshot_ids,
};
use crate::fixtures::{
    EXPORT_DIRECT, EXPORT_PROXIED, lakehouse_vs, proxy_virtual_schemas, stack, vs_table,
};

const SOURCE_SCHEMA: &str = "EXPORT_APPEND_E2E";
/// The rows the Exasol source appends, under unquoted names in another column order.
const SOURCE_ROWS: &str = "EXPORT_APPEND_E2E.SOURCE_ROWS";
const LEVELS_20K: &str = "EXPORT_APPEND_E2E.LEVELS_20K";
const ROWS_1K: &str = "EXPORT_APPEND_E2E.ROWS_1K";
/// 200,000 rows of 128 hex characters, about 25 MB that Parquet barely compresses.
const WIDE_ROWS: &str = "EXPORT_APPEND_E2E.WIDE_ROWS";
const MIB: i64 = 1024 * 1024;
const READ_VS: &str = "EXPORT_APPEND_READ";
const READ_CONN: &str = "EXPORT_APPEND_READ_CREDS";

/// `SOURCE_ROWS` as `APPENDABLE_TEXT` renders them, in `ID` order: each decimal type's smallest
/// value, NULL in every optional column, and each decimal type's largest value.
const SOURCE_TEXT_ROWS: [[Option<&str>; 9]; 3] = [
    [
        Some("-999999999999999999"),
        Some("-999999999"),
        Some("-9999999999.99"),
        Some("min"),
        Some("1900-01-01"),
        Some("1969-12-31 23:59:59.999"),
        Some("1970-01-01 00:00:00.000"),
        Some("FALSE"),
        Some("-2.5"),
    ],
    [
        Some("0"),
        None,
        None,
        Some("nulls"),
        None,
        None,
        None,
        None,
        None,
    ],
    [
        Some("999999999999999999"),
        Some("999999999"),
        Some("9999999999.99"),
        Some("max"),
        Some("2024-03-01"),
        Some("2024-03-01 08:00:00.789"),
        Some("2024-03-01 12:00:00.456"),
        Some("TRUE"),
        Some("2.5"),
    ],
];

fn setup() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        let mut sys = exa_conn();
        sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA}"));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {SOURCE_ROWS} (NAME VARCHAR(20), SCORE DOUBLE, \
             CREATED TIMESTAMP, ID DECIMAL(18,0), QTY DECIMAL(9,0), AMOUNT DECIMAL(12,2), \
             FLAG BOOLEAN, TS TIMESTAMP, \"DAY\" DATE)"
        ));
        sys.execute(&format!(
            "INSERT INTO {SOURCE_ROWS} VALUES \
             ('max', 2.5, TIMESTAMP '2024-03-01 12:00:00.456', 999999999999999999, 999999999, \
              9999999999.99, TRUE, TIMESTAMP '2024-03-01 08:00:00.789', DATE '2024-03-01'), \
             ('min', -2.5, TIMESTAMP '1970-01-01 00:00:00.000', -999999999999999999, \
              -999999999, -9999999999.99, FALSE, TIMESTAMP '1969-12-31 23:59:59.999', \
              DATE '1900-01-01'), \
             ('nulls', NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL)"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {LEVELS_20K} AS \
             SELECT CAST(LEVEL AS DECIMAL(18,0)) AS N FROM DUAL CONNECT BY LEVEL <= 20000"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {ROWS_1K} AS SELECT CAST(LEVEL AS DECIMAL(9,0)) AS ID, \
             CAST('v' || LEVEL AS VARCHAR(100)) AS LABEL FROM DUAL CONNECT BY LEVEL <= 1000"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {WIDE_ROWS} AS SELECT CAST(LEVEL AS DECIMAL(18,0)) AS ID, \
             HASH_SHA256(LEVEL) || HASH_SHA256(-LEVEL) AS PAYLOAD \
             FROM DUAL CONNECT BY LEVEL <= 200000"
        ));
        create_virtual_schema(
            &mut sys,
            &VsProps::new(READ_VS, NAMESPACE).with_catalog_conn_name(READ_CONN),
        );
    });
}

fn qualified(table: &str) -> String {
    format!("{NAMESPACE}.{table}")
}

/// The text rows of `rows`, NULL as `None`.
fn text_rows(rows: &[[Option<&str>; 9]]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|row| row.iter().map(|value| value.map(String::from)).collect())
        .collect()
}

/// The rows of appendable table `table` read through the virtual schema, as `APPENDABLE_TEXT`
/// renders them, in `ID` order.
fn read_rows(table: &str) -> Vec<Vec<Option<String>>> {
    let columns: Vec<Vec<Option<String>>> = exa_conn()
        .query_columns(&format!(
            "SELECT {APPENDABLE_TEXT} FROM {} ORDER BY ID",
            vs_table(READ_VS, table)
        ))
        .iter()
        .map(|column| text_column(column))
        .collect();
    let row_count = columns.first().map_or(0, Vec::len);
    (0..row_count)
        .map(|row| columns.iter().map(|column| column[row].clone()).collect())
        .collect()
}

/// The format version, the transforms of the current partition spec, and the columns of a table.
type Shape<'a> = (u64, Vec<&'a str>, Vec<(&'a str, &'a str, bool)>);

/// The shape of `metadata`'s table, which a fixture guard compares with the ground truth.
fn shape(metadata: &Value) -> Shape<'_> {
    let spec_id = &metadata["default-spec-id"];
    let spec = metadata["partition-specs"]
        .as_array()
        .and_then(|specs| specs.iter().find(|spec| &spec["spec-id"] == spec_id))
        .unwrap_or_else(|| panic!("no default partition spec: {metadata}"));
    let transforms = spec["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["transform"].as_str().unwrap())
        .collect();
    (
        metadata["format-version"].as_u64().unwrap(),
        transforms,
        iceberg_columns(metadata),
    )
}

/// Scenario: Unquoted Exasol names append to a table that another engine created
/// Scenario: A narrower export type widens into the table column
/// Scenario: Rows read through a virtual schema append back into their table
#[test]
fn export_appends_into_spark_tables() {
    stack();
    setup();
    let spark = text_rows(&APPENDABLE_ROWS);
    let source = text_rows(&SOURCE_TEXT_ROWS);
    let spark_and_source: Vec<_> = [&source[..1], &source[1..2], &spark[..], &source[2..]].concat();
    let spark_twice: Vec<_> = spark
        .iter()
        .flat_map(|row| [row.clone(), row.clone()])
        .collect();
    let read_back = format!("(SELECT * FROM {})", vs_table(READ_VS, APPENDABLE_V2));
    let cases = [
        (
            "v2 from an Exasol table",
            APPENDABLE_V2,
            (2, APPENDABLE_V2_SPEC, APPENDABLE_V2_SPEC_ID),
            SOURCE_ROWS.to_string(),
            "",
            1,
            spark_and_source.clone(),
        ),
        (
            "v2 from its own virtual schema",
            APPENDABLE_V2,
            (2, APPENDABLE_V2_SPEC, APPENDABLE_V2_SPEC_ID),
            read_back,
            "",
            1,
            spark_twice,
        ),
        (
            "v1 from an Exasol table, 2 writers",
            APPENDABLE_V1,
            (1, APPENDABLE_V1_SPEC, APPENDABLE_V1_SPEC_ID),
            SOURCE_ROWS.to_string(),
            "WRITERS_PER_NODE = '2'",
            2,
            spark_and_source,
        ),
    ];

    for (case, table, (version, transforms, spec_id), source, options, writers, expected) in cases {
        reset_to_unexported_snapshot(NAMESPACE, table);
        let before = local_table_metadata(NAMESPACE, table);
        assert_eq!(
            (shape(&before), &before["default-spec-id"]),
            (
                (version, transforms.to_vec(), APPENDABLE_COLUMNS.to_vec()),
                &Value::from(spec_id)
            ),
            "fixture guard: {case}"
        );

        let sql = EXPORT_DIRECT.export(&source, &qualified(table), options);
        assert_eq!(exported_rows(&mut exa_conn(), &sql), 3, "{case}");

        let after = local_table_metadata(NAMESPACE, table);
        assert_one_new_export(case, &before, &after, writers);
        assert_eq!(iceberg_columns(&after), APPENDABLE_COLUMNS, "{case}");
        assert_eq!(read_rows(table), expected, "{case}");
    }
}

/// Scenario: A void partition field named like its column takes appends
#[test]
fn export_appends_into_a_void_partition_field_named_like_its_column() {
    stack();
    setup();
    reset_to_unexported_snapshot(NAMESPACE, V1_VOID_COLUMN);
    let before = local_table_metadata(NAMESPACE, V1_VOID_COLUMN);
    assert_eq!(
        shape(&before),
        (
            1,
            V1_VOID_COLUMN_SPEC.to_vec(),
            V1_VOID_COLUMN_COLUMNS.to_vec()
        ),
        "fixture guard"
    );

    let sql = EXPORT_DIRECT.export(
        "(SELECT CAST(2 AS DECIMAL(18,0)) AS ID, DATE '2024-01-16' AS \"DAY\" FROM DUAL)",
        &qualified(V1_VOID_COLUMN),
        "",
    );
    assert_eq!(exported_rows(&mut exa_conn(), &sql), 1);

    let after = local_table_metadata(NAMESPACE, V1_VOID_COLUMN);
    assert_one_new_export(V1_VOID_COLUMN, &before, &after, 1);
    let rows = exa_conn().query_columns(&format!(
        "SELECT TO_CHAR(ID), TO_CHAR(\"DAY\", 'YYYY-MM-DD') FROM {} ORDER BY ID",
        vs_table(READ_VS, V1_VOID_COLUMN)
    ));
    let rows: Vec<Vec<Option<String>>> = rows.iter().map(|column| text_column(column)).collect();
    let text = |values: [&str; 2]| values.map(|value| Some(value.to_string())).to_vec();
    assert_eq!(
        rows,
        vec![text(["1", "2"]), text(["2024-01-15", "2024-01-16"])],
        "the Spark row and the exported row, by column"
    );
}

/// Scenario: Every column mismatch fails the export in one error before any data is written
/// Scenario: A format-version 3 table is refused before any data is written
#[test]
fn export_refuses_appends_the_table_cannot_take() {
    stack();
    setup();
    let namespace = run_namespace("append");
    let mixed_case = format!("{namespace}.mixed_case");
    let create = EXPORT_DIRECT.export(
        "(SELECT CAST(1 AS DECIMAL(18,0)) AS ID, CAST('a' AS VARCHAR(10)) AS \"Code\", \
         CAST('b' AS VARCHAR(10)) AS \"code\", CAST(1.5 AS DECIMAL(10,2)) AS AMOUNT FROM DUAL)",
        &mixed_case,
        "",
    );
    assert_eq!(exported_rows(&mut exa_conn(), &create), 1);
    let mismatched = |table: &str, mismatches: &str| {
        format!("the export columns do not match the columns of table {table}: {mismatches}")
    };
    let cases = [
        (
            "four column mismatches",
            (namespace.as_str(), "mixed_case"),
            "(SELECT CAST('c' AS VARCHAR(10)) AS CODE, CAST(2.5 AS DOUBLE) AS AMOUNT, \
             CAST('e' AS VARCHAR(10)) AS EXTRA FROM DUAL)",
            mismatched(
                &mixed_case,
                "table column ID (decimal(18, 0)) has no export column; export column EXTRA \
                 (VARCHAR(10) UTF8) has no table column; export column AMOUNT (DOUBLE) cannot \
                 fill table column AMOUNT (decimal(10, 2)); export column CODE (VARCHAR(10) UTF8) \
                 matches table columns Code (string) and code (string) only by letter case",
            ),
            None,
        ),
        (
            "a float column",
            (NAMESPACE, ODD_TYPES),
            "(SELECT CAST(2 AS DECIMAL(18,0)) AS ID, CAST(0.5 AS DOUBLE) AS RATIO, \
             CAST('two' AS VARCHAR(10)) AS NOTE FROM DUAL)",
            mismatched(
                &qualified(ODD_TYPES),
                "export column RATIO (DOUBLE) cannot fill table column ratio (float)",
            ),
            Some((2, &[][..], &ODD_TYPES_COLUMNS[..])),
        ),
        (
            "a format version 3 table",
            (NAMESPACE, V3),
            "(SELECT CAST(2 AS DECIMAL(18,0)) AS ID FROM DUAL)",
            format!(
                "table {} has format version 3; the export appends only to tables of format \
                 version 1 or 2",
                qualified(V3)
            ),
            Some((3, &[][..], &V3_COLUMNS[..])),
        ),
    ];

    for (case, (namespace, table), source, refusal, fixture) in cases {
        let before = local_table_metadata(namespace, table);
        if let Some((version, transforms, columns)) = fixture {
            assert_eq!(
                shape(&before),
                (version, transforms.to_vec(), columns.to_vec()),
                "fixture guard: {case}"
            );
        }
        let data = data_prefix(&before);
        let files = objects_under(&data);

        let sql = EXPORT_DIRECT.export(source, &format!("{namespace}.{table}"), "");
        let error = export_error(&mut exa_conn(), &sql, NO_SECRETS);

        assert!(error.contains(&refusal), "{case}: {error}");
        let after = local_table_metadata(namespace, table);
        assert_eq!(
            (snapshot_ids(&after), objects_under(&data)),
            (snapshot_ids(&before), files),
            "{case}: the table keeps its snapshots, and the export leaves no data file"
        );
    }
}

/// Scenario: A value that the table column cannot hold fails the export
#[test]
fn export_fails_on_a_value_the_column_cannot_hold() {
    stack();
    setup();
    // Every column of the table, with row 10,000 of 20,000 holding `id` and `qty` as given.
    let source = |id: &str, qty: &str| {
        format!(
            "(SELECT {id} AS ID, CAST({qty} AS DECIMAL(10,0)) AS QTY, \
             CAST(NULL AS DECIMAL(20,2)) AS AMOUNT, CAST('n' AS VARCHAR(10)) AS NAME, \
             CAST(NULL AS DATE) AS \"DAY\", CAST(NULL AS TIMESTAMP) AS TS, \
             CAST(NULL AS TIMESTAMP) AS CREATED, CAST(NULL AS BOOLEAN) AS FLAG, \
             CAST(NULL AS DOUBLE) AS SCORE FROM {LEVELS_20K} ORDER BY N)"
        )
    };
    let table = qualified(APPENDABLE_V2);
    let cases = [
        (
            "NULL for a required column",
            source("CASE WHEN N = 10000 THEN NULL ELSE N END", "1"),
            format!("an export row holds NULL for the required column id (long) of table {table}"),
        ),
        (
            "a value outside the int range",
            source("N", "CASE WHEN N = 10000 THEN 2147483648 ELSE N END"),
            format!(
                "an export row holds a value outside the range of column qty (int) of table \
                 {table}"
            ),
        ),
    ];

    for (case, source, refusal) in cases {
        let before = local_table_metadata(NAMESPACE, APPENDABLE_V2);
        let data = data_prefix(&before);
        let files = objects_under(&data);

        let sql = EXPORT_DIRECT.export(&source, &table, "SINGLE_COMMIT = 'true'");
        let error = export_error(&mut exa_conn(), &sql, NO_SECRETS);

        assert!(error.contains(&refusal), "{case}: {error}");
        let after = local_table_metadata(NAMESPACE, APPENDABLE_V2);
        assert_eq!(
            (snapshot_ids(&after), objects_under(&data)),
            (snapshot_ids(&before), files),
            "{case}: the table keeps its snapshots, and the export leaves no data file"
        );
    }
}

/// Scenario: SINGLE_COMMIT appends one snapshot, also on a catalog without staged creation
#[test]
fn export_single_commit_appends_one_snapshot_without_staged_creation() {
    stack();
    setup();
    proxy_virtual_schemas();
    // The `scripts/proxy/filter` line answers HTTP 403 to every table creation in this namespace.
    let (namespace, table) = ("e2e_export_no_stage", "appended");
    let name = format!("{namespace}.{table}");
    // Creates the table on a fresh volume, and appends to it on a persisted one.
    exported_rows(&mut exa_conn(), &EXPORT_DIRECT.export(ROWS_1K, &name, ""));
    let before = local_table_metadata(namespace, table);

    let sql = EXPORT_PROXIED.export(ROWS_1K, &name, "SINGLE_COMMIT = 'true'");
    assert_eq!(exported_rows(&mut exa_conn(), &sql), 1_000);

    let after = local_table_metadata(namespace, table);
    let added = assert_one_new_export("SINGLE_COMMIT", &before, &after, 1);
    assert_eq!(added[0]["summary"]["added-records"], "1000");
}

/// The first `rows` rows of `WIDE_ROWS`.
fn wide_rows(rows: u32) -> String {
    format!("(SELECT * FROM {WIDE_ROWS} WHERE ID <= {rows})")
}

/// A new table of `WIDE_ROWS`' columns, created by an export of its first 10 rows with `options`.
fn wide_table(name: &str, options: &str) -> (String, String) {
    let namespace = run_namespace("write_properties");
    let table = format!("{namespace}.{name}");
    exported_rows(
        &mut exa_conn(),
        &EXPORT_DIRECT.export(&wide_rows(10), &table, options),
    );
    (namespace, table)
}

/// Asserts that the table `metadata` describes carries the write properties of a created table.
#[track_caller]
fn assert_created_properties(case: &str, metadata: &Value) {
    let properties = [
        "write.parquet.compression-codec",
        "write.parquet.row-group-size-bytes",
        "write.target-file-size-bytes",
    ]
    .map(|key| metadata["properties"][key].as_str());
    assert_eq!(
        properties,
        [Some("zstd"), Some("134217728"), Some("268435456")],
        "{case}"
    );
}

/// The data files of the table `metadata` describes that are not among `old`, the objects
/// `objects_under` listed before.
fn new_data_files(old: &[String], metadata: &Value) -> Vec<DataFileFooter> {
    data_file_footers(metadata)
        .into_iter()
        .filter(|file| !old.contains(&file.path))
        .collect()
}

/// Scenario: A table the export creates carries its write properties
/// Scenario: An append writes data files by the table's write properties
/// Scenario: A write property the table lacks takes the writer's default
#[test]
fn export_append_follows_the_table_write_properties() {
    stack();
    setup();
    let (namespace, table) = wide_table("snappy", "WRITERS_PER_NODE = '4'");
    let created = local_table_metadata(&namespace, "snappy");
    assert_created_properties("several writers", &created);
    assert!(
        snapshot_ids(&created).len() > 1,
        "several writers each commit a snapshot"
    );
    assert_data_files(
        "created table",
        &data_file_footers(&created),
        Compression::ZSTD(ZstdLevel::default()),
    );
    set_table_properties(
        &namespace,
        "snappy",
        &[
            ("write.format.default", "orc"),
            ("write.parquet.compression-codec", "snappy"),
            ("write.parquet.row-group-size-bytes", "2097152"),
            ("write.target-file-size-bytes", "4194304"),
        ],
    );
    let before = local_table_metadata(&namespace, "snappy");
    let old = objects_under(&data_prefix(&before));

    let sql = EXPORT_DIRECT.export(WIDE_ROWS, &table, "SINGLE_COMMIT = 'true'");
    assert_eq!(exported_rows(&mut exa_conn(), &sql), 200_000);

    let after = local_table_metadata(&namespace, "snappy");
    let added = new_data_files(&old, &after);
    assert_data_files("snappy append", &added, Compression::SNAPPY);
    // A 2 MiB row group holds several 8,192-row input batches, and the default 128 MiB one would
    // hold a whole 4 MiB file.
    let groups: Vec<_> = added
        .iter()
        .map(|file| {
            file.footer
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        groups.iter().any(|rows| rows.len() > 1)
            && groups.iter().flatten().any(|rows| *rows > 8_192),
        "a data file of several row groups, one of more than 8,192 rows: {groups:?}"
    );
    let sizes: Vec<u64> = added.iter().map(|file| file.size).collect();
    assert!(
        sizes.len() > 1 && sizes.iter().all(|size| *size <= 6 * MIB as u64),
        "several data files of at most 4 MiB plus one row group: {sizes:?}"
    );
    assert_eq!(after["properties"], before["properties"]);

    // Without the properties, an append takes the writer's defaults.
    remove_table_properties(
        &namespace,
        "snappy",
        &[
            "write.parquet.compression-codec",
            "write.parquet.row-group-size-bytes",
            "write.target-file-size-bytes",
        ],
    );
    let old = objects_under(&data_prefix(&after));
    let sql = EXPORT_DIRECT.export(&wide_rows(5), &table, "");
    assert_eq!(exported_rows(&mut exa_conn(), &sql), 5);
    let defaulted = new_data_files(&old, &local_table_metadata(&namespace, "snappy"));
    assert_data_files(
        "append without write properties",
        &defaulted,
        Compression::ZSTD(ZstdLevel::default()),
    );
}

/// Scenario: A table the export creates carries its write properties
/// Scenario: An unusable write property fails the export before any data is written
#[test]
fn export_refuses_unusable_write_properties() {
    stack();
    setup();
    // Unit tests name every other value.
    let (namespace, table) = wide_table("lzo", "SINGLE_COMMIT = 'true'");
    let created = local_table_metadata(&namespace, "lzo");
    assert_created_properties("SINGLE_COMMIT", &created);
    assert_data_files(
        "created with SINGLE_COMMIT",
        &data_file_footers(&created),
        Compression::ZSTD(ZstdLevel::default()),
    );
    set_table_properties(
        &namespace,
        "lzo",
        &[("write.parquet.compression-codec", "lzo")],
    );
    let before = local_table_metadata(&namespace, "lzo");
    let data = data_prefix(&before);
    let files = objects_under(&data);

    let sql = EXPORT_DIRECT.export(&wide_rows(10), &table, "");
    let error = export_error(&mut exa_conn(), &sql, NO_SECRETS);

    let named =
        format!("of table {table}: invalid write property write.parquet.compression-codec = 'lzo'");
    assert!(error.contains(&named), "{error}");
    let after = local_table_metadata(&namespace, "lzo");
    assert_eq!(
        (snapshot_ids(&after), objects_under(&data)),
        (snapshot_ids(&before), files),
        "the table keeps its snapshots, and the export leaves no data file"
    );
}
