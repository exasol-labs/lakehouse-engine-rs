//! An Iceberg data file that lacks the source column of its own `identity` partition field reads
//! the manifest's partition value for that column (#99), per the Iceberg spec's "Column
//! Projection" rule (1).
//!
//! The fixtures are authored once at stack bring-up by
//! `scripts/spark-fixtures/create_identity_partition_fixture.sql`;
//! `tests/common/identity_partition_fixtures.rs` MUST stay in lockstep with that SQL.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::identity_partition_fixtures::{
    ABSENT_IMPORT_DIR, ABSENT_ROWS, ABSENT_SPEC_ID_COUNT, ABSENT_TABLE,
    ABSENT_UNPARTITIONED_IMPORT_DIR, HIVE_DEFAULT_PARTITION, NAMESPACE, REGION_COLUMN,
    REQUIRED_IMPORT_DIR, REQUIRED_ROWS, REQUIRED_TABLE, RegionRow, TYPES_COLUMNS, TYPES_ID_COLUMN,
    TYPES_ID_DECLARED, TYPES_IMPORT_DIR, TYPES_ROW_IDS_TEXT, TYPES_TABLE,
};
use crate::common::raw_parquet::get_fixture_object;
use crate::common::seed::{DIM_CUSTOMER_ROWS, E2E_DIM_TABLE, build_seed_catalog};
use crate::common::stack::iceberg_catalog_url;
use crate::fixtures::{LAKEHOUSE_VS, assert_pushed_to_scan_udf, lakehouse_vs, stack, vs_table};

use futures::TryStreamExt;
use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::{BTreeMap, BTreeSet};

/// One data file as iceberg-rust's scan planner resolves it, with what the guard compares.
struct PlannedFile {
    path: String,
    spec_id: i32,
    /// `(source column name, rendered manifest value)` per `identity` field of the file's own spec.
    identity_values: Vec<(String, Option<String>)>,
    footer_columns: Vec<String>,
    region_required: bool,
}

impl PlannedFile {
    fn is_under(&self, dir: &str) -> bool {
        self.path.contains(&format!("/{dir}/"))
    }

    fn holds_column(&self, column: &str) -> bool {
        self.footer_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(column))
    }

    fn identity_value(&self, column: &str) -> &Option<String> {
        &self
            .identity_values
            .iter()
            .find(|(name, _)| name == column)
            .unwrap_or_else(|| {
                panic!(
                    "{}: its partition spec {} has no identity field on '{column}': {:?}",
                    self.path, self.spec_id, self.identity_values
                )
            })
            .1
    }
}

fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::Primitive(PrimitiveLiteral::Boolean(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::Int(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::Long(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::Float(v)) => v.0.to_string(),
        Literal::Primitive(PrimitiveLiteral::Double(v)) => v.0.to_string(),
        Literal::Primitive(PrimitiveLiteral::Int128(v)) => v.to_string(),
        Literal::Primitive(PrimitiveLiteral::String(v)) => v.clone(),
        other => panic!("unexpected identity partition literal {other:?}"),
    }
}

/// Plans through iceberg-rust, not the adapter, so the guard holds whatever the engine does.
fn plan_files(table: &str) -> Vec<PlannedFile> {
    let tasks: Vec<_> = runtime().block_on(async {
        let catalog = build_seed_catalog(
            &iceberg_catalog_url(),
            "s3://warehouse/",
            "identity-partition",
        )
        .await
        .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
        let ident = TableIdent::new(
            NamespaceIdent::new(NAMESPACE.to_string()),
            table.to_string(),
        );
        let loaded = catalog
            .load_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("load {NAMESPACE}.{table}: {e}"));
        loaded
            .scan()
            .select_all()
            .build()
            .unwrap_or_else(|e| panic!("build a scan of {table}: {e}"))
            .plan_files()
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
            let spec = task
                .partition_spec()
                .unwrap_or_else(|| panic!("{path}: the file task carries no partition spec"));
            let partition = task
                .partition()
                .unwrap_or_else(|| panic!("{path}: the file task carries no partition struct"));
            let identity_values = spec
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| field.transform == Transform::Identity)
                .map(|(index, field)| {
                    let source = task
                        .schema()
                        .name_by_field_id(field.source_id)
                        .unwrap_or_else(|| panic!("{path}: no column has id {}", field.source_id))
                        .to_string();
                    (
                        source,
                        partition.fields()[index].as_ref().map(render_literal),
                    )
                })
                .collect();
            let reader = SerializedFileReader::new(get_fixture_object(&path))
                .unwrap_or_else(|e| panic!("open committed Parquet file {path}: {e}"));
            let footer_columns = reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect();
            let region_required = task
                .schema()
                .field_by_name(REGION_COLUMN)
                .is_some_and(|field| field.required);
            PlannedFile {
                path,
                spec_id: spec.spec_id(),
                identity_values,
                footer_columns,
                region_required,
            }
        })
        .collect()
}

/// The value the file's Hive directory encodes for `region`, which `add_files` copies into the
/// manifest.
fn region_in_path(file: &PlannedFile) -> Option<String> {
    if file
        .path
        .contains(&format!("{REGION_COLUMN}={HIVE_DEFAULT_PARTITION}/"))
    {
        return None;
    }
    let (_, tail) = file
        .path
        .split_once(&format!("{REGION_COLUMN}="))
        .unwrap_or_else(|| panic!("{}: its path names no {REGION_COLUMN} directory", file.path));
    Some(tail.split('/').next().unwrap_or_default().to_string())
}

fn assert_imported_without_source_column(file: &PlannedFile, column: &str) {
    assert!(
        !file.holds_column(column),
        "fixture guard: imported file {} must hold no '{column}' column, so only the manifest \
         carries its value, got columns {:?}",
        file.path,
        file.footer_columns
    );
}

#[test]
fn identity_partition_fixture_imports_hold_no_source_column_and_carry_manifest_values() {
    stack();

    let absent = plan_files(ABSENT_TABLE);
    let spec_ids: BTreeSet<i32> = absent.iter().map(|f| f.spec_id).collect();
    assert_eq!(
        spec_ids.len(),
        ABSENT_SPEC_ID_COUNT,
        "fixture guard: {ABSENT_TABLE} must carry {ABSENT_SPEC_ID_COUNT} partition spec ids, \
         got {spec_ids:?}"
    );

    let imported: Vec<&PlannedFile> = absent
        .iter()
        .filter(|f| f.is_under(ABSENT_IMPORT_DIR))
        .collect();
    let mut imported_values = BTreeSet::new();
    for file in &imported {
        assert_imported_without_source_column(file, REGION_COLUMN);
        let expected = region_in_path(file);
        assert_eq!(
            file.identity_value(REGION_COLUMN),
            &expected,
            "fixture guard: the manifest partition value of {} must be the one its Hive \
             directory names",
            file.path
        );
        imported_values.insert(expected);
    }
    let expected_values: BTreeSet<Option<String>> = [None, Some("eu".into()), Some("us".into())]
        .into_iter()
        .collect();
    assert_eq!(
        imported_values, expected_values,
        "fixture guard: the files imported under the identity spec must carry eu, us, and a null \
         partition value"
    );

    let unpartitioned: Vec<&PlannedFile> = absent
        .iter()
        .filter(|f| f.is_under(ABSENT_UNPARTITIONED_IMPORT_DIR))
        .collect();
    assert_eq!(
        unpartitioned.len(),
        1,
        "fixture guard: {ABSENT_TABLE} must hold one file imported under the spec without a \
         region field, got {:?}",
        unpartitioned.iter().map(|f| &f.path).collect::<Vec<_>>()
    );
    assert_imported_without_source_column(unpartitioned[0], REGION_COLUMN);
    assert!(
        unpartitioned[0].identity_values.is_empty(),
        "fixture guard: the spec of {} must carry no identity field, got {:?}",
        unpartitioned[0].path,
        unpartitioned[0].identity_values
    );

    let native: Vec<&PlannedFile> = absent
        .iter()
        .filter(|f| !f.is_under(ABSENT_IMPORT_DIR) && !f.is_under(ABSENT_UNPARTITIONED_IMPORT_DIR))
        .collect();
    assert!(
        !native.is_empty(),
        "fixture guard: {ABSENT_TABLE} must hold natively written files"
    );
    for file in native {
        assert!(
            file.holds_column(REGION_COLUMN),
            "fixture guard: natively written file {} must hold '{REGION_COLUMN}', got {:?}",
            file.path,
            file.footer_columns
        );
    }

    let required = plan_files(REQUIRED_TABLE);
    let mut required_values = BTreeSet::new();
    for file in &required {
        assert!(
            file.is_under(REQUIRED_IMPORT_DIR),
            "fixture guard: every file of {REQUIRED_TABLE} must be imported, got {}",
            file.path
        );
        assert!(
            file.region_required,
            "fixture guard: '{REGION_COLUMN}' of {REQUIRED_TABLE} must be REQUIRED"
        );
        assert_imported_without_source_column(file, REGION_COLUMN);
        let expected = region_in_path(file);
        assert_eq!(
            file.identity_value(REGION_COLUMN),
            &expected,
            "fixture guard: the manifest partition value of {} must be the one its Hive \
             directory names",
            file.path
        );
        required_values.insert(expected);
    }
    let expected_required: BTreeSet<Option<String>> = REQUIRED_ROWS
        .iter()
        .map(|row| row.region.map(str::to_string))
        .collect();
    assert_eq!(
        required_values, expected_required,
        "fixture guard: the {REQUIRED_TABLE} files must carry exactly the partition values of \
         its rows"
    );

    let types = plan_files(TYPES_TABLE);
    let imported_types: Vec<&PlannedFile> = types
        .iter()
        .filter(|f| f.is_under(TYPES_IMPORT_DIR))
        .collect();
    assert_eq!(
        imported_types.len(),
        2,
        "fixture guard: {TYPES_TABLE} must hold two imported files (rows 1 and 3), got {:?}",
        imported_types.iter().map(|f| &f.path).collect::<Vec<_>>()
    );
    let mut null_files = 0;
    for file in imported_types {
        assert_eq!(
            file.footer_columns,
            [TYPES_ID_COLUMN],
            "fixture guard: imported file {} must hold only '{TYPES_ID_COLUMN}'",
            file.path
        );
        let all_null = file.path.contains(HIVE_DEFAULT_PARTITION);
        null_files += usize::from(all_null);
        for column in &TYPES_COLUMNS {
            let expected = (!all_null).then(|| column.manifest_value.to_string());
            assert_eq!(
                file.identity_value(column.name),
                &expected,
                "fixture guard: the manifest partition value of '{}' in {}",
                column.name,
                file.path
            );
        }
    }
    assert_eq!(
        null_files, 1,
        "fixture guard: exactly one imported {TYPES_TABLE} file must carry all-null partition \
         values"
    );
    let native_types: Vec<&PlannedFile> = types
        .iter()
        .filter(|f| !f.is_under(TYPES_IMPORT_DIR))
        .collect();
    assert_eq!(
        native_types.len(),
        1,
        "fixture guard: {TYPES_TABLE} must hold one natively written file (row 2), got {:?}",
        native_types.iter().map(|f| &f.path).collect::<Vec<_>>()
    );
    for column in TYPES_COLUMNS
        .iter()
        .map(|c| c.name)
        .chain([TYPES_ID_COLUMN])
    {
        assert!(
            native_types[0].holds_column(column),
            "fixture guard: natively written file {} must hold '{column}', got {:?}",
            native_types[0].path,
            native_types[0].footer_columns
        );
    }
}

fn absent_table() -> String {
    vs_table(LAKEHOUSE_VS, ABSENT_TABLE)
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
        .map(|row| (row.id, row.region.map(str::to_string)))
        .collect()
}

/// Scenario: An identity partition value fills a column the data file lacks, per the file's own spec
#[test]
fn identity_value_fills_absent_column_per_file_spec() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, {REGION_COLUMN} FROM {} ORDER BY id",
        absent_table()
    );
    assert_pushed_to_scan_udf(&mut conn, &sql, "an id and region scan");

    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(&ABSENT_ROWS),
        "imported files read their partition value (id 7's null value and id 8's dropped spec \
         read NULL), and native files read their own value"
    );
}

/// Scenario: A required identity column without a physical counterpart reads its partition value
#[test]
fn required_identity_column_reads_partition_value() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, {REGION_COLUMN} FROM {} ORDER BY id",
        vs_table(LAKEHOUSE_VS, REQUIRED_TABLE)
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &sql,
        "an id and region scan of a required column",
    );

    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(&REQUIRED_ROWS),
        "each row must carry its file's partition value, with no required-absent error"
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

fn group_counts(conn: &mut ExaConn, sql: &str) -> Vec<(Option<String>, i64)> {
    let cols = conn.query_columns(sql);
    let mut counts: Vec<(Option<String>, i64)> = text_column(&cols[0])
        .into_iter()
        .zip(int_column(&cols[1]))
        .collect();
    counts.sort();
    counts
}

fn ids(conn: &mut ExaConn, sql: &str) -> Vec<i64> {
    int_column(&conn.query_columns(sql)[0])
}

/// Scenario: Pushed-down filters, aggregates, TopN, and joins see the reconstructed value
#[test]
fn pushdown_shapes_see_reconstructed_identity_value() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = absent_table();
    let rows_where = |region: &str| -> Vec<i64> {
        ABSENT_ROWS
            .iter()
            .filter(|row| row.region == Some(region))
            .map(|row| row.id)
            .collect()
    };

    for region in ["eu", "us"] {
        let sql = format!("SELECT id FROM {table} WHERE {REGION_COLUMN} = '{region}' ORDER BY id");
        assert_pushed_to_scan_udf(&mut conn, &sql, "a region filter");
        assert_eq!(
            ids(&mut conn, &sql),
            rows_where(region),
            "WHERE {REGION_COLUMN} = '{region}' must return every row whose reconstructed value \
             matches, imported or native"
        );
    }

    let sql = format!("SELECT {REGION_COLUMN}, COUNT(*) FROM {table} GROUP BY {REGION_COLUMN}");
    assert_pushed_to_scan_udf(&mut conn, &sql, "a grouped aggregate");
    let mut counted: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for row in &ABSENT_ROWS {
        *counted.entry(row.region.map(str::to_string)).or_insert(0) += 1;
    }
    let expected_counts: Vec<(Option<String>, i64)> = counted.into_iter().collect();
    assert_eq!(
        group_counts(&mut conn, &sql),
        expected_counts,
        "each imported file's rows count under its partition value, not under NULL"
    );

    let sql = format!("SELECT COUNT(DISTINCT {REGION_COLUMN}) FROM {table}");
    assert_pushed_to_scan_udf(&mut conn, &sql, "COUNT(DISTINCT)");
    let distinct: BTreeSet<&str> = ABSENT_ROWS.iter().filter_map(|row| row.region).collect();
    assert_eq!(
        ids(&mut conn, &sql),
        [distinct.len() as i64],
        "COUNT(DISTINCT {REGION_COLUMN}) counts the reconstructed values and not NULL"
    );

    let top_n = 3;
    let sql = format!(
        "SELECT id, {REGION_COLUMN} FROM {table} ORDER BY {REGION_COLUMN}, id LIMIT {top_n}"
    );
    assert_pushed_to_scan_udf(&mut conn, &sql, "a TopN");
    let mut ranked: Vec<&RegionRow> = ABSENT_ROWS.iter().collect();
    // Exasol sorts NULL last in ascending order.
    ranked.sort_by_key(|row| (row.region.is_none(), row.region, row.id));
    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(ranked.into_iter().take(top_n)),
        "ORDER BY {REGION_COLUMN} LIMIT {top_n} ranks the reconstructed values"
    );

    // The broadcast contract needs disjoint column names, which `dim_customer` gives.
    let sql = format!(
        "SELECT a.id, a.{REGION_COLUMN} FROM {table} a JOIN {} c ON a.id = c.C_CUSTKEY \
         ORDER BY a.id",
        vs_table(LAKEHOUSE_VS, E2E_DIM_TABLE)
    );
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        has_broadcast_join_block(&pushed),
        "the join must be a broadcast join that drives one scan UDF: {pushed}"
    );
    assert_pushed_to_scan_udf(&mut conn, &sql, "a broadcast join");
    let joined = ABSENT_ROWS
        .iter()
        .filter(|row| row.id <= DIM_CUSTOMER_ROWS as i64);
    assert_eq!(
        id_region_rows(&mut conn, &sql),
        expected_rows(joined),
        "a broadcast join projecting {REGION_COLUMN} returns the reconstructed value"
    );
}
