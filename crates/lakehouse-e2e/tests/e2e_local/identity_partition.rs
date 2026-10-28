//! An Iceberg data file reads the manifest's `identity` partition value of its own spec for that
//! column (#99), per the Iceberg spec's "Column Projection" rule (1), even where the file holds
//! the column.
//!
//! The fixtures are authored once at stack bring-up by
//! `scripts/spark-fixtures/create_identity_partition_fixture.sql`;
//! `tests/common/identity_partition_fixtures.rs` MUST stay in lockstep with that SQL.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::identity_partition_fixtures::{
    ABSENT_ROWS, ABSENT_TABLE, HELD_DIR, HELD_STORED_REGIONS, IMPORT_DIR_SUFFIX, NAMESPACE,
    REGION_COLUMN, RegionRow, TYPES_COLUMNS, TYPES_ID_COLUMN, TYPES_ID_DECLARED,
    TYPES_ROW_IDS_TEXT, TYPES_TABLE,
};
use crate::common::raw_parquet::get_fixture_object;
use crate::common::seed::build_seed_catalog;
use crate::common::stack::iceberg_catalog_url;
use crate::fixtures::{LAKEHOUSE_VS, assert_pushed_to_scan_udf, lakehouse_vs, stack, vs_table};

use arrow::array::AsArray;
use arrow::datatypes::Int32Type;
use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// `(path, footer column names)` of every data file the table's current snapshot plans.
fn planned_footers(table: &str) -> Vec<(String, Vec<String>)> {
    let tasks: Vec<_> = runtime().block_on(async {
        let catalog = build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "identity")
            .await
            .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
        let ident = TableIdent::new(NamespaceIdent::new(NAMESPACE.into()), table.into());
        let loaded = catalog
            .load_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("load {NAMESPACE}.{table}: {e}"));
        let scan = loaded
            .scan()
            .select_all()
            .build()
            .unwrap_or_else(|e| panic!("build a scan of {table}: {e}"));
        scan.plan_files()
            .await
            .unwrap_or_else(|e| panic!("plan the files of {table}: {e}"))
            .try_collect()
            .await
            .unwrap_or_else(|e| panic!("collect the file tasks of {table}: {e}"))
    });
    tasks
        .iter()
        .map(|task| {
            let path = task.data_file_path().to_string();
            let reader = SerializedFileReader::new(get_fixture_object(&path))
                .unwrap_or_else(|e| panic!("open committed Parquet file {path}: {e}"));
            let columns = reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect();
            (path, columns)
        })
        .collect()
}

/// `(id, region)` as the Parquet file itself stores them.
fn stored_regions(path: &str) -> Vec<(i32, Option<String>)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(get_fixture_object(path))
        .and_then(|builder| builder.build())
        .unwrap_or_else(|e| panic!("open committed Parquet file {path}: {e}"));
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|e| panic!("read {path}: {e}"));
        let ids = batch
            .column_by_name("id")
            .expect("id")
            .as_primitive::<Int32Type>();
        let regions = batch
            .column_by_name(REGION_COLUMN)
            .expect("region")
            .as_string::<i32>();
        rows.extend(
            ids.iter()
                .zip(regions.iter())
                .map(|(id, region)| (id.expect("id is never null"), region.map(str::to_string))),
        );
    }
    rows.sort();
    rows
}

/// The row tests are vacuous unless imported files lack their identity source columns, native
/// files hold them, and the held file stores other values than its partition value.
#[test]
fn identity_partition_fixtures_hold_the_shapes_the_row_tests_need() {
    stack();

    let absent = planned_footers(ABSENT_TABLE);
    let identity_columns: [(Vec<_>, Vec<&str>); 2] = [
        (absent.clone(), vec![REGION_COLUMN]),
        (
            planned_footers(TYPES_TABLE),
            TYPES_COLUMNS.iter().map(|c| c.name).collect(),
        ),
    ];
    for (files, columns) in identity_columns {
        for (path, footer) in files {
            let imported = path.contains(IMPORT_DIR_SUFFIX);
            for column in &columns {
                assert_eq!(
                    footer.iter().any(|c| c.eq_ignore_ascii_case(column)),
                    !imported,
                    "fixture guard: {path} must {}hold '{column}', got {footer:?}",
                    if imported { "not " } else { "" }
                );
            }
        }
    }

    let held: Vec<_> = absent
        .iter()
        .filter(|(path, _)| path.contains(HELD_DIR))
        .collect();
    assert_eq!(
        held.len(),
        1,
        "fixture guard: {ABSENT_TABLE} holds one held file"
    );
    let expected: Vec<(i32, Option<String>)> = HELD_STORED_REGIONS
        .iter()
        .map(|(id, region)| (*id, region.map(str::to_string)))
        .collect();
    assert_eq!(
        stored_regions(&held[0].0),
        expected,
        "fixture guard: the held file must store region values other than its partition value"
    );
}

fn id_region_rows(conn: &mut ExaConn, sql: &str) -> Vec<(i64, Option<String>)> {
    let cols = conn.query_columns(sql);
    assert_eq!(cols.len(), 2, "{sql} must return two columns: {cols:?}");
    int_column(&cols[0])
        .into_iter()
        .zip(text_column(&cols[1]))
        .collect()
}

fn expected_rows<'a>(rows: impl IntoIterator<Item = &'a RegionRow>) -> Vec<(i64, Option<String>)> {
    rows.into_iter()
        .map(|(id, region)| (*id, region.map(str::to_string)))
        .collect()
}

/// Scenario: An identity partition value fills a column the data file lacks, per the file's own spec
/// Scenario: An identity partition value wins over a column the data file holds
#[test]
fn identity_value_fills_absent_column_and_wins_over_held_column() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = vs_table(LAKEHOUSE_VS, ABSENT_TABLE);

    let sql = format!("SELECT id, {REGION_COLUMN} FROM {table} ORDER BY id");
    assert_pushed_to_scan_udf(&mut conn, &sql, "an id and region scan");
    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(&ABSENT_ROWS),
        "imported files read their partition value (id 7's null value and id 8's dropped spec \
         read NULL, the held file's ids 9 and 10 read eu), and native files read their own value"
    );

    // The held file's `region` bounds are `us`, so plan-time pruning keeps it for `us`, and the
    // filter must then see its partition value `eu`, never the stored `us` of id 9.
    let sql =
        format!("SELECT id, {REGION_COLUMN} FROM {table} WHERE {REGION_COLUMN} = 'us' ORDER BY id");
    assert_pushed_to_scan_udf(&mut conn, &sql, "a region filter");
    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(
            ABSENT_ROWS
                .iter()
                .filter(|(_, region)| *region == Some("us"))
        ),
        "a pushed-down filter returns exactly the rows whose partition value matches"
    );
}

/// Scenario: A reconstructed identity value reads the same as a stored one for every admitted type
#[test]
fn reconstructed_identity_values_equal_stored_values_for_every_type() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT * FROM {} ORDER BY {TYPES_ID_COLUMN}",
        vs_table(LAKEHOUSE_VS, TYPES_TABLE)
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &sql,
        "a scan of every identity-partitioned column",
    );

    let upper: Vec<String> = [TYPES_ID_COLUMN]
        .into_iter()
        .chain(TYPES_COLUMNS.iter().map(|c| c.name))
        .map(str::to_uppercase)
        .collect();
    let mut cases = vec![reads(
        &upper[0],
        TYPES_ID_DECLARED,
        TYPES_ROW_IDS_TEXT.map(Some),
    )];
    cases.extend(TYPES_COLUMNS.iter().zip(&upper[1..]).map(|(column, name)| {
        // Row 1 is imported, row 2 native with the same value, row 3 imported all-null.
        reads(
            name,
            column.declared,
            [Some(column.read_value), Some(column.read_value), None],
        )
    }));
    assert_type_matrix(&mut conn, LAKEHOUSE_VS, TYPES_TABLE, &cases);
}
