//! An Iceberg data file without embedded field ids binds its columns and nested members only
//! through `schema.name-mapping.default` (#83), and a column neither a field id nor the mapping
//! binds reads NULL even where the file holds a column of the same name.
//!
//! The fixtures are authored once at stack bring-up by
//! `scripts/spark-fixtures/create_name_mapping_fixture.sql`;
//! `tests/common/name_mapping_fixtures.rs` MUST stay in lockstep with that SQL.

use crate::common::e2e_harness::*;
use crate::common::name_mapping_fixtures::{
    NESTED_IMPORT_DIR, NESTED_ROWS, NESTED_STREET_NAMES, NESTED_TABLE, NESTED_UNMAPPED_NAME,
    PROJECTION_IMPORT_DIR, PROJECTION_ROWS, PROJECTION_TABLE, PROJECTION_UNMAPPED,
};
use crate::common::raw_parquet::get_fixture_object;
use crate::fixtures::{LAKEHOUSE_VS, assert_pushed_to_scan_udf, lakehouse_vs, stack, vs_table};
use crate::identity_partition::{load_table, planned_footers};

use parquet::file::reader::{FileReader, SerializedFileReader};

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
