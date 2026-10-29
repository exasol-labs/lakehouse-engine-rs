//! An Iceberg data file reads the manifest's `identity` partition value of its own spec for that
//! column (#99), per the Iceberg spec's "Column Projection" rule (1), even where the file holds
//! the column. The same `add_files` fixture script imports a file without field ids whose nested
//! members only `schema.name-mapping.default` binds (#83), and a file whose same-named columns
//! neither a field id nor the mapping binds.
//!
//! The fixtures are authored once at stack bring-up by
//! `scripts/spark-fixtures/create_identity_partition_fixture.sql`;
//! `tests/common/identity_partition_fixtures.rs` MUST stay in lockstep with that SQL.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::identity_partition_fixtures::{
    ABSENT_ROWS, ABSENT_TABLE, HELD_DIR, HELD_STORED_REGIONS, IMPORT_DIR_SUFFIX, NAMESPACE,
    NESTED_IMPORT_DIR, NESTED_ROWS, NESTED_STREET_NAMES, NESTED_TABLE, NESTED_UNMAPPED_NAME,
    PROJECTION_IMPORT_DIR, PROJECTION_ROWS, PROJECTION_TABLE, PROJECTION_UNMAPPED, REGION_COLUMN,
    RegionRow, TYPES_COLUMNS, TYPES_ID_COLUMN, TYPES_ID_DECLARED, TYPES_ROW_IDS_TEXT, TYPES_TABLE,
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

fn load_table(table: &str) -> iceberg::table::Table {
    runtime().block_on(async {
        let catalog = build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "identity")
            .await
            .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
        let ident = TableIdent::new(NamespaceIdent::new(NAMESPACE.into()), table.into());
        catalog
            .load_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("load {NAMESPACE}.{table}: {e}"))
    })
}

/// `(path, footer column names)` of every data file the table's current snapshot plans.
fn planned_footers(table: &str) -> Vec<(String, Vec<String>)> {
    let loaded = load_table(table);
    let tasks: Vec<_> = runtime().block_on(async {
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

/// `(name, field id)` of a file's top-level fields, `None` for a field without one.
type RootFieldIds = Vec<(String, Option<i32>)>;

/// `(path, top-level field ids)` of every planned data file.
fn planned_root_field_ids(table: &str) -> Vec<(String, RootFieldIds)> {
    planned_footers(table)
        .into_iter()
        .map(|(path, _)| {
            let reader = SerializedFileReader::new(get_fixture_object(&path))
                .unwrap_or_else(|e| panic!("open committed Parquet file {path}: {e}"));
            let ids = reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .root_schema()
                .get_fields()
                .iter()
                .map(|field| {
                    let info = field.get_basic_info();
                    (info.name().to_string(), info.has_id().then(|| info.id()))
                })
                .collect();
            (path, ids)
        })
        .collect()
}

fn assert_only_imports_lack_field_ids(table: &str, import_dir: &str) {
    for (path, ids) in planned_root_field_ids(table) {
        assert_eq!(
            ids.iter().all(|(_, id)| id.is_some()),
            !path.contains(import_dir),
            "fixture guard: only the imported file {path} may lack field ids"
        );
    }
}

fn name_mapping(table: &iceberg::table::Table) -> iceberg::spec::NameMapping {
    let raw = table
        .metadata()
        .properties()
        .get("schema.name-mapping.default")
        .expect("fixture guard: the table sets a name mapping");
    serde_json::from_str(raw).expect("fixture guard: the name mapping parses")
}

fn mapped_field<'a>(
    fields: impl IntoIterator<Item = &'a iceberg::spec::MappedField>,
    name: &str,
) -> Option<&'a iceberg::spec::MappedField> {
    fields
        .into_iter()
        .find(|field| field.names().iter().any(|n| n == name))
}

fn schema_id(table: &iceberg::table::Table, path: &str) -> i32 {
    table
        .metadata()
        .current_schema()
        .field_by_name(path)
        .unwrap_or_else(|| panic!("fixture guard: {path} is in the schema"))
        .id
}

/// The nested row test is vacuous unless only the imported file lacks field ids and the mapping
/// holds every street name, lacks `zip_v1`, and points `city` at another member than `town`.
#[test]
fn nested_name_map_fixture_holds_the_shapes_the_row_test_needs() {
    stack();
    assert_only_imports_lack_field_ids(NESTED_TABLE, NESTED_IMPORT_DIR);

    let table = load_table(NESTED_TABLE);
    let mapping = name_mapping(&table);
    let addr =
        mapped_field(mapping.fields(), "addr").expect("fixture guard: the mapping maps addr");
    let id_of = |name: &str| {
        mapped_field(addr.fields().iter().map(AsRef::as_ref), name).and_then(|f| f.field_id())
    };
    let schema_id = |path: &str| schema_id(&table, path);
    for name in NESTED_STREET_NAMES {
        assert_eq!(
            id_of(name),
            Some(schema_id("addr.street")),
            "fixture guard: {name}"
        );
    }
    assert_eq!(
        id_of(NESTED_UNMAPPED_NAME),
        None,
        "fixture guard: zip_v1 is unmapped"
    );
    assert_eq!(
        id_of("city"),
        Some(schema_id("addr.city")),
        "fixture guard: `city` maps to the added member, not to `town`"
    );
}

/// Scenario: A nested member of a file without field ids binds through the name mapping
/// Scenario: An embedded nested field id wins over the name mapping
#[test]
fn nested_members_without_field_ids_bind_through_the_name_mapping() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, label, addr, items, attrs FROM {} ORDER BY id",
        vs_table(LAKEHOUSE_VS, NESTED_TABLE)
    );
    assert_pushed_to_scan_udf(&mut conn, &sql, "a nested-column scan");
    let cols = conn.query_columns(&sql);
    let json = |text: Option<String>| -> serde_json::Value {
        serde_json::from_str(&text.expect("a nested cell is not NULL"))
            .expect("a nested cell is valid JSON")
    };
    let actual: Vec<_> = int_column(&cols[0])
        .into_iter()
        .zip(text_column(&cols[1]))
        .zip(text_column(&cols[2]))
        .zip(text_column(&cols[3]))
        .zip(text_column(&cols[4]))
        .map(|((((id, label), addr), items), attrs)| {
            (id, label, json(addr), json(items), json(attrs))
        })
        .collect();
    let expected: Vec<_> = NESTED_ROWS
        .iter()
        .map(|(id, label, addr, items, attrs)| {
            let parse = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
            (
                *id,
                Some(label.to_string()),
                parse(addr),
                parse(items),
                parse(attrs),
            )
        })
        .collect();
    assert_eq!(
        actual, expected,
        "the imported row binds renamed members through the mapping at every depth, and the \
         native row's embedded ids win over the mapping's `city`"
    );
}

/// The projection row test is vacuous unless only the imported file lacks field ids, the mapping
/// omits `extra` and `info.memo`, and a native file holds `note` under a dropped field id.
#[test]
fn projection_by_id_fixture_holds_the_shapes_the_row_test_needs() {
    stack();
    assert_only_imports_lack_field_ids(PROJECTION_TABLE, PROJECTION_IMPORT_DIR);

    let table = load_table(PROJECTION_TABLE);
    let mapping = name_mapping(&table);
    let info =
        mapped_field(mapping.fields(), "info").expect("fixture guard: the mapping maps info");
    for path in PROJECTION_UNMAPPED {
        let mapped = match path.split_once('.') {
            Some((_, member)) => mapped_field(info.fields().iter().map(AsRef::as_ref), member),
            None => mapped_field(mapping.fields(), path),
        };
        assert!(mapped.is_none(), "fixture guard: {path} is unmapped");
    }

    let note_id = schema_id(&table, "note");
    assert!(
        planned_root_field_ids(PROJECTION_TABLE)
            .iter()
            .flat_map(|(_, ids)| ids)
            .any(|(name, id)| name == "note" && id.is_some_and(|id| id != note_id)),
        "fixture guard: a native file holds `note` under the dropped field's id"
    );
}

/// Scenario: A column is never bound by name to a field-id-bound logical field
#[test]
fn columns_bind_by_field_id_or_the_name_mapping_and_never_by_name() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, note, info, extra FROM {} ORDER BY id",
        vs_table(LAKEHOUSE_VS, PROJECTION_TABLE)
    );
    assert_pushed_to_scan_udf(&mut conn, &sql, "a scan of re-added and unmapped columns");
    let cols = conn.query_columns(&sql);
    let json = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
    let actual: Vec<_> = int_column(&cols[0])
        .into_iter()
        .zip(text_column(&cols[1]))
        .zip(text_column(&cols[2]))
        .zip(text_column(&cols[3]))
        .map(|(((id, note), info), extra)| {
            (id, note, json(&info.expect("info is not NULL")), extra)
        })
        .collect();
    let expected: Vec<_> = PROJECTION_ROWS
        .iter()
        .map(|(id, note, info, extra)| {
            (
                *id,
                note.map(str::to_string),
                json(info),
                extra.map(str::to_string),
            )
        })
        .collect();
    assert_eq!(
        actual, expected,
        "id 1's dropped `note` and `info.tag` and id 2's unmapped `extra` and `info.memo` read \
         NULL, never the file's values under the same name"
    );
}
