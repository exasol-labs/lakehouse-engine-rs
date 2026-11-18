//! `EXPORT ... INTO SCRIPT` into partitioned tables of the local REST catalog. The Spark tables come
//! from `create_export_partition_fixture.sql`, mirrored in `common/export_partition_fixtures.rs`.
//! Spark is the oracle that computes every transform: each data file the export writes must carry
//! the partition values Spark gave its rows of the same `rid`.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use serde_json::Value;

use crate::common::e2e_harness::{
    AddedBy, NO_SECRETS, PartitionedFile, VsProps, assert_holds_rows,
    assert_refused_before_reading, create_virtual_schema, data_file_partitions, exa_conn,
    export_error, exported_rows, failing_source, run_namespace, value_to_string,
};
use crate::common::export_partition_fixtures::{
    BY_TIME, BY_TIME_ALL_NULL_RIDS, BY_TIME_NULL_RIDS, BY_TIME_ROWS, BY_TIME_SPEC, BY_VALUE,
    BY_VALUE_ALL_NULL_RIDS, BY_VALUE_FILTERS, BY_VALUE_NULL_RIDS, BY_VALUE_ROWS, BY_VALUE_SPEC,
    EVOLVED, EVOLVED_CURRENT_RIDS, EVOLVED_SPEC, NAMESPACE, STRADDLING_PAIRS,
};
use crate::common::table_state::{
    local_table_metadata, reset_to_unexported_snapshot, snapshot_ids, table_exists,
};
use crate::fixtures::{EXPORT_DIRECT, explain_virtual_pushdown_sql, lakehouse_vs, stack, vs_table};

const SOURCE_SCHEMA: &str = "EXPORT_PARTITION_E2E";
/// A native copy of `by_value` as Spark wrote it, the oracle for column bounds and filter rows.
const BY_VALUE_COPY: &str = "EXPORT_PARTITION_E2E.BY_VALUE_COPY";
const IDS: &str = "EXPORT_PARTITION_E2E.IDS";
const MANY: &str = "EXPORT_PARTITION_E2E.MANY";
const READ_VS: &str = "EXPORT_PARTITION_READ";
const READ_CONN: &str = "EXPORT_PARTITION_READ_CREDS";

fn setup() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        lakehouse_vs();
        let mut sys = exa_conn();
        sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA}"));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {IDS} AS \
             SELECT CAST(LEVEL AS DECIMAL(18,0)) AS ID FROM DUAL CONNECT BY LEVEL <= 3"
        ));
        create_virtual_schema(
            &mut sys,
            &VsProps::new(READ_VS, NAMESPACE).with_catalog_conn_name(READ_CONN),
        );
        reset_to_unexported_snapshot(NAMESPACE, BY_VALUE);
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {BY_VALUE_COPY} AS SELECT * FROM {}",
            read(BY_VALUE)
        ));
    });
}

fn read(table: &str) -> String {
    vs_table(READ_VS, table)
}

fn qualified(table: &str) -> String {
    format!("{NAMESPACE}.{table}")
}

/// A file's partition: the ID of its spec and each field's value as text.
type Tuple = (i32, Vec<Option<String>>);

fn tuple(file: &PartitionedFile) -> Tuple {
    let values = file
        .partition
        .iter()
        .map(|(_, value)| value.clone())
        .collect();
    (file.spec_id, values)
}

/// The partition Spark gave each `rid` of `table`, after its `main` is reset to Spark's snapshot.
fn spark_partitions(table: &str) -> HashMap<i64, Tuple> {
    reset_to_unexported_snapshot(NAMESPACE, table);
    data_file_partitions(NAMESPACE, table, AddedBy::OtherEngine)
        .iter()
        .flat_map(|file| file.rids.iter().map(move |rid| (*rid, tuple(file))))
        .collect()
}

/// Each `rid` of a file the export wrote whose partition differs from `expected(rid)`, with both.
fn misplaced(
    files: &[PartitionedFile],
    expected: impl Fn(i64) -> Option<Tuple>,
) -> Vec<(i64, Tuple, Option<Tuple>)> {
    files
        .iter()
        .flat_map(|file| file.rids.iter().map(move |rid| (*rid, tuple(file))))
        .filter_map(|(rid, actual)| {
            let expected = expected(rid);
            (expected.as_ref() != Some(&actual)).then_some((rid, actual, expected))
        })
        .collect()
}

/// The rows of `query` as text, NULL as `None`, sorted.
fn sorted_rows(query: &str) -> Vec<Vec<Option<String>>> {
    let columns = exa_conn().query_columns(query);
    let count = columns.first().map_or(0, Vec::len);
    let mut rows: Vec<Vec<Option<String>>> = (0..count)
        .map(|row| {
            columns
                .iter()
                .map(|column| (!column[row].is_null()).then(|| value_to_string(&column[row])))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// `rows` followed by `added`, sorted, as a table that held `rows` holds after appending `added`.
fn appended(
    rows: &[Vec<Option<String>>],
    added: &[Vec<Option<String>>],
) -> Vec<Vec<Option<String>>> {
    let mut all = [rows, added].concat();
    all.sort();
    all
}

/// The name and transform of each field of `metadata`'s default partition spec.
fn default_spec(metadata: &Value) -> Vec<(String, String)> {
    let spec_id = &metadata["default-spec-id"];
    metadata["partition-specs"]
        .as_array()
        .and_then(|specs| specs.iter().find(|spec| &spec["spec-id"] == spec_id))
        .unwrap_or_else(|| panic!("no default partition spec: {metadata}"))["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            (
                field["name"].as_str().unwrap().to_string(),
                field["transform"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn pairs(spec: &[(&str, &str)]) -> Vec<(String, String)> {
    spec.iter()
        .map(|(name, transform)| (name.to_string(), transform.to_string()))
        .collect()
}

fn rid_list(rids: &[i64]) -> String {
    rids.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether the rows `rids` of the native copy hold `literal` between the least and the greatest
/// value of `column`, as a data file's column bounds of those rows would.
fn bounds_hold(column: &str, literal: &str, rids: &[i64]) -> bool {
    !exa_conn().query_columns(&format!(
        "SELECT COUNT(*) FROM {BY_VALUE_COPY} WHERE RID IN ({}) \
         HAVING MIN({column}) <= {literal} AND MAX({column}) >= {literal}",
        rid_list(rids)
    ))[0]
        .is_empty()
}

/// The fixture shape the placement and pruning tests need, read from the catalog and the
/// manifests: the specs, two rows in every partition of every field, and bucket pairs of the
/// literal's other bucket whose values straddle the literal.
#[test]
fn test_export_partition_fixture_shape() {
    setup();
    for (table, spec) in [
        (BY_VALUE, pairs(&BY_VALUE_SPEC)),
        (BY_TIME, pairs(&BY_TIME_SPEC)),
        (EVOLVED, pairs(&EVOLVED_SPEC)),
    ] {
        assert_eq!(
            default_spec(&local_table_metadata(NAMESPACE, table)),
            spec,
            "fixture guard: the spec of {table}"
        );
    }
    for (table, rows) in [(BY_VALUE, BY_VALUE_ROWS), (BY_TIME, BY_TIME_ROWS)] {
        let spark = spark_partitions(table);
        assert_eq!(
            spark.len() as i64,
            rows,
            "fixture guard: the rows of {table}"
        );
        let mut rows_per_value: BTreeMap<(usize, Option<String>), usize> = BTreeMap::new();
        for (_, values) in spark.values() {
            for (field, value) in values.iter().enumerate() {
                *rows_per_value.entry((field, value.clone())).or_default() += 1;
            }
        }
        let thin: Vec<_> = rows_per_value
            .iter()
            .filter(|(_, rows)| **rows < 2)
            .collect();
        assert!(
            thin.is_empty(),
            "fixture guard: partitions of {table} with one row: {thin:?}"
        );
    }
    let spark = spark_partitions(BY_VALUE);
    for (field, (column, literal, literal_rid)) in BY_VALUE_FILTERS.iter().enumerate() {
        if !BY_VALUE_SPEC[field].1.starts_with("bucket") {
            continue;
        }
        let literal_bucket = &spark[literal_rid].1[field];
        for (a, b) in STRADDLING_PAIRS {
            assert!(
                spark[&a] == spark[&b]
                    && &spark[&a].1[field] != literal_bucket
                    && bounds_hold(column, literal, &[a, b])
                    && !bounds_hold(column, literal, &[a])
                    && !bounds_hold(column, literal, &[b]),
                "fixture guard: rids {a} and {b} must share a file of another bucket than \
                 {column} = {literal} and straddle the literal"
            );
        }
    }
}

/// Scenario: An append places every row in the partition that the table's spec assigns
#[test]
fn test_append_places_rows_like_spark() {
    setup();
    let cases = [
        (BY_VALUE, "SINGLE_COMMIT = 'true'", BY_VALUE_ROWS),
        (BY_VALUE, "WRITERS_PER_NODE = '2'", BY_VALUE_ROWS),
        (BY_TIME, "SINGLE_COMMIT = 'true'", BY_TIME_ROWS),
        (BY_TIME, "WRITERS_PER_NODE = '2'", BY_TIME_ROWS),
    ];

    for (table, options, rows) in cases {
        let case = format!("{table} with {options}");
        let spark = spark_partitions(table);
        let source = format!("(SELECT * FROM {})", read(table));
        let before = sorted_rows(&format!("SELECT * FROM {}", read(table)));

        let sql = EXPORT_DIRECT.export(&source, &qualified(table), options);
        assert_eq!(exported_rows(&mut exa_conn(), &sql), rows, "{case}");

        let files = data_file_partitions(NAMESPACE, table, AddedBy::Export);
        assert!(!files.is_empty(), "{case}: the export wrote no data file");
        assert_eq!(
            misplaced(&files, |rid| spark.get(&rid).cloned()),
            [],
            "{case}"
        );
        assert_eq!(
            sorted_rows(&format!("SELECT * FROM {}", read(table))),
            appended(&before, &before),
            "{case}"
        );
    }
}

/// Scenario: A row with a NULL partition column lands in the partition whose value is null
#[test]
fn test_null_partition_columns_land_in_null_partitions() {
    setup();
    let cases = [
        (BY_VALUE, BY_VALUE_NULL_RIDS, BY_VALUE_ALL_NULL_RIDS),
        (BY_TIME, BY_TIME_NULL_RIDS, BY_TIME_ALL_NULL_RIDS),
    ];

    for (table, null_rids, all_null_rids) in cases {
        let spark = spark_partitions(table);
        let source = format!(
            "(SELECT * FROM {} WHERE RID BETWEEN {} AND {})",
            read(table),
            null_rids.start(),
            null_rids.end()
        );
        let source_rows = sorted_rows(&format!("SELECT * FROM {source}"));
        let before = sorted_rows(&format!("SELECT * FROM {}", read(table)));

        let sql = EXPORT_DIRECT.export(&source, &qualified(table), "SINGLE_COMMIT = 'true'");
        exported_rows(&mut exa_conn(), &sql);

        let files = data_file_partitions(NAMESPACE, table, AddedBy::Export);
        assert_eq!(
            misplaced(&files, |rid| spark.get(&rid).cloned()),
            [],
            "{table}"
        );
        let all_null: Vec<&PartitionedFile> = files
            .iter()
            .filter(|file| file.rids.iter().any(|rid| all_null_rids.contains(rid)))
            .collect();
        assert!(
            !all_null.is_empty()
                && all_null
                    .iter()
                    .all(|file| file.partition.iter().all(|(_, value)| value.is_none())),
            "{table}: the rows NULL in every partition column carry a null value per field: \
             {all_null:?}"
        );
        assert_eq!(
            sorted_rows(&format!("SELECT * FROM {}", read(table))),
            appended(&before, &source_rows),
            "{table}: every NULL reads back as NULL"
        );
    }
}

/// Scenario: A filter on a partition column reads only the exported files of matching partitions
#[test]
fn test_partition_filter_prunes_exported_files() {
    setup();
    let spark = spark_partitions(BY_VALUE);
    let sql = EXPORT_DIRECT.export(
        &format!("(SELECT * FROM {})", read(BY_VALUE)),
        &qualified(BY_VALUE),
        "SINGLE_COMMIT = 'true'",
    );
    exported_rows(&mut exa_conn(), &sql);
    let files = data_file_partitions(NAMESPACE, BY_VALUE, AddedBy::Export);

    for (field, (column, literal, literal_rid)) in BY_VALUE_FILTERS.iter().enumerate() {
        let case = format!("{} ({column} = {literal})", BY_VALUE_SPEC[field].1);
        let partition = &spark[literal_rid].1[field];
        let mut expected: Vec<&str> = files
            .iter()
            .filter(|file| {
                &file.partition[field].1 == partition && bounds_hold(column, literal, &file.rids)
            })
            .map(|file| file.path.as_str())
            .collect();
        let query = format!(
            "SELECT RID FROM {} WHERE {column} = {literal}",
            read(BY_VALUE)
        );
        let pushed = explain_virtual_pushdown_sql(&mut exa_conn(), &query);
        let mut scanned: Vec<&str> = files
            .iter()
            .filter(|file| pushed.contains(file.path.rsplit('/').next().unwrap()))
            .map(|file| file.path.as_str())
            .collect();
        expected.sort();
        scanned.sort();

        assert!(
            !expected.is_empty(),
            "{case}: no export file holds the literal"
        );
        assert_eq!(scanned, expected, "{case}: {pushed}");
        let source_rows = sorted_rows(&format!(
            "SELECT RID FROM {BY_VALUE_COPY} WHERE {column} = {literal}"
        ));
        assert_eq!(
            sorted_rows(&query),
            appended(&source_rows, &source_rows),
            "{case}: the Spark rows and the exported rows of the native query"
        );
    }
}

/// Scenario: An append writes under the table's current partition spec
#[test]
fn test_append_writes_under_current_spec() {
    setup();
    let spark = spark_partitions(EVOLVED);
    let metadata = local_table_metadata(NAMESPACE, EVOLVED);
    let current_spec = metadata["default-spec-id"].as_i64().unwrap() as i32;
    let source = format!(
        "(SELECT * FROM {} WHERE RID BETWEEN {} AND {})",
        read(EVOLVED),
        EVOLVED_CURRENT_RIDS.start(),
        EVOLVED_CURRENT_RIDS.end()
    );
    let source_rows = sorted_rows(&format!("SELECT * FROM {source}"));
    let before = sorted_rows(&format!("SELECT * FROM {}", read(EVOLVED)));

    let sql = EXPORT_DIRECT.export(&source, &qualified(EVOLVED), "SINGLE_COMMIT = 'true'");
    assert_eq!(
        exported_rows(&mut exa_conn(), &sql),
        EVOLVED_CURRENT_RIDS.clone().count() as i64
    );

    let files = data_file_partitions(NAMESPACE, EVOLVED, AddedBy::Export);
    assert!(
        !files.is_empty() && files.iter().all(|file| file.spec_id == current_spec),
        "every export file carries spec {current_spec}: {files:?}"
    );
    assert_eq!(misplaced(&files, |rid| spark.get(&rid).cloned()), []);
    assert_eq!(
        sorted_rows(&format!("SELECT * FROM {}", read(EVOLVED))),
        appended(&before, &source_rows)
    );
}

/// Scenario: PARTITION_BY creates a partitioned table
#[test]
fn test_partition_by_creates_partitioned_table() {
    setup();
    let by_value = spark_partitions(BY_VALUE);
    let by_time = spark_partitions(BY_TIME);
    // Spark's values of the `by_value` fields s_id, s_b_bucket, and s_t_trunc and of every
    // `by_time` field, which the new table's fields apply to the same columns.
    let expected = |rid: i64| -> Option<Tuple> {
        let (_, value) = by_value.get(&rid)?;
        let (_, time) = by_time.get(&rid)?;
        let position = |name: &str| BY_VALUE_SPEC.iter().position(|(field, _)| *field == name);
        let values = ["s_id", "s_b_bucket", "s_t_trunc"]
            .iter()
            .map(|name| value[position(name).unwrap()].clone())
            .chain(time[..4].to_vec());
        Some((0, values.collect()))
    };
    let source = format!(
        "(SELECT v.RID, v.S_ID, v.S_B, v.S_T, t.D, t.TS_M, t.TZ_D, t.TS_H FROM {} v \
         JOIN {} t ON v.RID = t.RID)",
        read(BY_VALUE),
        read(BY_TIME)
    );
    let partition_by = "PARTITION_BY = 'S_ID, bucket(2, S_B), truncate(2, S_T), year(D), \
                        month(TS_M), day(TZ_D), hour(TS_H)'";
    let namespace = run_namespace("partition");
    let vs = "EXPORT_PARTITION_NEW";
    let cases = [
        ("single", "SINGLE_COMMIT = 'true'"),
        ("writers", "WRITERS_PER_NODE = '2'"),
    ];

    for (table, options) in cases {
        let sql = EXPORT_DIRECT.export(
            &source,
            &format!("{namespace}.{table}"),
            &format!("{options} {partition_by}"),
        );
        assert_eq!(
            exported_rows(&mut exa_conn(), &sql),
            BY_TIME_ROWS,
            "{table}"
        );

        assert_eq!(
            default_spec(&local_table_metadata(&namespace, table)),
            pairs(&[
                ("S_ID", "identity"),
                ("S_B_bucket", "bucket[2]"),
                ("S_T_trunc", "truncate[2]"),
                ("D_year", "year"),
                ("TS_M_month", "month"),
                ("TZ_D_day", "day"),
                ("TS_H_hour", "hour"),
            ]),
            "{table}"
        );
        let files = data_file_partitions(&namespace, table, AddedBy::Export);
        assert_eq!(misplaced(&files, expected), [], "{table}");
        let mut sys = exa_conn();
        create_virtual_schema(&mut sys, &VsProps::new(vs, &namespace));
        assert_holds_rows(
            &mut sys,
            &vs_table(vs, table),
            &format!("SELECT * FROM {source}"),
            BY_TIME_ROWS,
        );
    }
}

/// Scenario: An invalid PARTITION_BY fails the export before any data is read
#[test]
fn test_invalid_partition_by_is_refused_before_reading() {
    setup();
    let namespace = run_namespace("partition_by");
    let one_column = failing_source(IDS);
    let two_columns =
        format!("(SELECT 1 / (ID - ID) AS \"Ab\", 1 / (ID - ID) AS \"aB\" FROM {IDS})");
    let named_like_field =
        format!("(SELECT 1 / (ID - ID) AS X, 1 / (ID - ID) AS \"X_bucket\" FROM {IDS})");
    let cases = [
        ("empty", &one_column, "", "lists no partition field"),
        (
            "not a field list",
            &one_column,
            "bucket(16 X)",
            "'bucket(16 X)'",
        ),
        (
            "an unknown transform",
            &one_column,
            "zorder(X)",
            "'zorder(X)'",
        ),
        (
            "a width below 1",
            &one_column,
            "truncate(0, X)",
            "'truncate(0, X)'",
        ),
        ("an unknown column", &one_column, "NOPE", "'NOPE'"),
        (
            "an ambiguous column",
            &two_columns,
            "\"Ab\", bucket(4, ab)",
            "'bucket(4, ab)'",
        ),
        ("a duplicate field name", &one_column, "X, x", "'x'"),
        (
            "the name of another column",
            &named_like_field,
            "bucket(4, X)",
            "'bucket(4, X)'",
        ),
        (
            "two time transforms",
            &one_column,
            "year(X), day(X)",
            "'day(X)'",
        ),
    ];

    for (case, source, partition_by, field) in cases {
        let table = format!("{namespace}.t");
        let sql = EXPORT_DIRECT.export(source, &table, &format!("PARTITION_BY = '{partition_by}'"));

        assert_refused_before_reading(
            case,
            &mut exa_conn(),
            &sql,
            &["PARTITION_BY", field, "bucket(<n>, <column>)"],
            NO_SECRETS,
        );
        assert!(!table_exists(&table), "{case}: {table} must not exist");
    }
}

/// Scenario: A transform that the column's type does not take fails the export before any data is written
#[test]
fn test_partition_by_transform_type_mismatch_is_refused() {
    setup();
    let namespace = run_namespace("partition_type");
    let source = "(SELECT CAST(1.5 AS DOUBLE) AS R, DATE '2024-01-15' AS D FROM DUAL)";
    let cases = [
        (
            "bucket of a DOUBLE",
            "bucket(4, R)",
            "field R_bucket applies bucket to column R of type DOUBLE",
        ),
        (
            "hour of a DATE",
            "hour(D)",
            "field D_hour applies hour to column D of type DATE",
        ),
    ];

    for (case, partition_by, refusal) in cases {
        let table = format!("{namespace}.t");
        let sql = EXPORT_DIRECT.export(source, &table, &format!("PARTITION_BY = '{partition_by}'"));

        let error = export_error(&mut exa_conn(), &sql, NO_SECRETS);

        assert!(error.contains(refusal), "{case}: {error}");
        assert!(!table_exists(&table), "{case}: {table} must not exist");
    }
}

/// Scenario: PARTITION_BY into an existing table is refused
#[test]
fn test_partition_by_into_existing_table_is_refused() {
    setup();
    let before = local_table_metadata(NAMESPACE, BY_VALUE);
    let sql = EXPORT_DIRECT.export(
        &failing_source(IDS),
        &qualified(BY_VALUE),
        "PARTITION_BY = 'X'",
    );

    assert_refused_before_reading(
        "an existing table",
        &mut exa_conn(),
        &sql,
        &[
            &format!("table {} exists", qualified(BY_VALUE)),
            "PARTITION_BY applies only to a table the export creates",
        ],
        NO_SECRETS,
    );
    assert_eq!(
        snapshot_ids(&local_table_metadata(NAMESPACE, BY_VALUE)),
        snapshot_ids(&before)
    );
}

/// Scenario: An export into many partitions completes within a bounded amount of memory
#[test]
fn test_many_partitions_export_completes() {
    setup();
    let mut sys = exa_conn();
    // Narrow numeric rows: Arrow holds about 1,100 bytes per row of 40 DECIMAL columns, over
    // 600 MB in all, above the 512 MiB budget, and Exasol stores them at 4 to 8 bytes a value.
    let fillers: String = (1..=38)
        .map(|i| format!(", CAST(MOD(N * {i}, 999999937) AS DECIMAL(9,0)) AS C{i}"))
        .collect();
    sys.execute(&format!(
        "CREATE OR REPLACE TABLE {MANY} AS SELECT N, K{fillers} FROM ( \
         SELECT CAST((a.L - 1) * 1000 + b.L AS DECIMAL(18,0)) AS N, \
                CAST(MOD(b.L, 1000) AS DECIMAL(9,0)) AS K \
         FROM (SELECT LEVEL AS L FROM DUAL CONNECT BY LEVEL <= 560) a \
         CROSS JOIN (SELECT LEVEL AS L FROM DUAL CONNECT BY LEVEL <= 1000) b) \
         ORDER BY N"
    ));
    let namespace = run_namespace("partition_many");
    let table = "many";
    let sql = EXPORT_DIRECT.export(
        MANY,
        &format!("{namespace}.{table}"),
        "SINGLE_COMMIT = 'true' PARTITION_BY = 'K'",
    );

    assert_eq!(exported_rows(&mut sys, &sql), 560_000);

    let metadata = local_table_metadata(&namespace, table);
    let snapshots = metadata["snapshots"].as_array().unwrap();
    let data_files: i64 = snapshots[0]["summary"]["total-data-files"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        data_files > 1_000,
        "{data_files} data files for 1,000 partitions"
    );
    let vs = "EXPORT_PARTITION_MANY";
    create_virtual_schema(&mut sys, &VsProps::new(vs, &namespace));
    assert_eq!(
        sys.query_columns(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT N) FROM {}",
            vs_table(vs, table)
        ))
        .iter()
        .map(|column| value_to_string(&column[0]))
        .collect::<Vec<_>>(),
        ["560000", "560000"]
    );
    sys.execute(&format!("DROP TABLE {MANY}"));
}
