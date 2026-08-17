use super::*;
use arrow::datatypes::{DataType, Field, TimeUnit};
use std::collections::BTreeMap;

fn declared_schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// `a` (Int32), `p` (Utf8), `b` (Int64) with `p` declared as the partition column:
/// a partition column sitting BETWEEN two file columns, which is what makes the
/// declared order differ from `file ++ partition` order.
fn split_with_middle_partition() -> PartitionedScanSchema {
    PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("p", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]),
        &["p".to_string()],
    )
    .expect("p is declared")
}

fn entry_with(values: &[(&str, Option<&str>)]) -> FileEntry {
    FileEntry::with_partition_values(
        "part-0.parquet",
        1,
        values
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.map(str::to_string)))
            .collect::<BTreeMap<_, _>>(),
    )
}

#[test]
fn declared_schema_is_split_into_file_fields_and_partition_fields() {
    let split = split_with_middle_partition();

    assert_eq!(
        split.declared_schema().fields().len(),
        3,
        "the declared schema keeps every column"
    );
    let file_names: Vec<&str> = split
        .file_source_schema()
        .file_schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        file_names,
        ["a", "b"],
        "partition columns leave the file schema"
    );
    let partition_names: Vec<&str> = split
        .file_source_schema()
        .table_partition_cols()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(partition_names, ["p"]);
}

#[test]
fn the_declared_schema_keeps_its_declared_column_order() {
    let declared = declared_schema(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &["p".to_string()])
        .expect("p is declared");

    assert_eq!(split.declared_schema().as_ref(), declared.as_ref());
}

#[test]
fn projection_indices_are_remapped_from_declared_order_to_file_then_partition_order() {
    let split = split_with_middle_partition();

    // Declared a=0, p=1, b=2; scan order is a=0, b=1, p=2.
    assert_eq!(
        split.remap_projection(Some(&vec![2, 1, 0])),
        Some(vec![1, 2, 0])
    );
    assert_eq!(split.remap_projection(Some(&vec![1])), Some(vec![2]));
}

#[test]
fn a_remapped_projection_selects_the_declared_fields_in_the_requested_order() {
    let split = split_with_middle_partition();
    let declared_projection = vec![2, 1];

    let remapped = split
        .remap_projection(Some(&declared_projection))
        .expect("a partitioned scan always projects explicitly");

    let scan_schema = split.file_source_schema();
    let selected: Vec<&Field> = remapped
        .iter()
        .map(|i| scan_schema.table_schema().field(*i))
        .collect();
    let expected: Vec<&Field> = declared_projection
        .iter()
        .map(|i| split.declared_schema().field(*i))
        .collect();
    assert_eq!(selected, expected);
}

#[test]
fn an_absent_projection_is_expanded_so_declared_order_survives_the_split() {
    let split = split_with_middle_partition();

    assert_eq!(split.remap_projection(None), Some(vec![0, 2, 1]));
}

#[test]
fn a_scan_without_partition_columns_keeps_its_schema_and_projection_unchanged() {
    let declared = declared_schema(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &[]).expect("no partitions");

    assert_eq!(
        split.file_source_schema().file_schema().as_ref(),
        declared.as_ref()
    );
    assert!(split.file_source_schema().table_partition_cols().is_empty());
    assert_eq!(split.remap_projection(None), None);
    assert_eq!(split.remap_projection(Some(&vec![1, 0])), Some(vec![1, 0]));
    assert!(
        split
            .partition_values_for(&FileEntry::new("part-0.parquet", 1))
            .expect("no partition columns to materialize")
            .is_empty()
    );
}

#[test]
fn a_partition_column_absent_from_the_declared_schema_is_refused() {
    let error = PartitionedScanSchema::split(
        declared_schema(vec![Field::new("a", DataType::Int32, true)]),
        &["missing".to_string()],
    )
    .expect_err("a partition column with no declared field cannot be materialized");

    assert!(error.contains("missing"), "{error}");
}

#[test]
fn a_repeated_partition_column_is_refused() {
    let error = PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("p", DataType::Utf8, true),
        ]),
        &["p".to_string(), "p".to_string()],
    )
    .expect_err("one declared field cannot occupy two partition positions");

    assert!(error.contains('p'), "{error}");
}

#[test]
fn a_partition_column_matching_two_declared_fields_is_refused() {
    let error = PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("p", DataType::Utf8, true),
            Field::new("p", DataType::Int32, true),
            Field::new("a", DataType::Int64, true),
        ]),
        &["p".to_string()],
    )
    .expect_err("one partition column cannot stand for two declared fields");

    assert!(error.contains('p'), "{error}");
}

#[test]
fn two_declared_fields_sharing_a_non_partition_name_keep_the_split_and_remap_consistent() {
    let declared = declared_schema(vec![
        Field::new("p", DataType::Utf8, true),
        Field::new("b", DataType::Int32, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &["p".to_string()])
        .expect("a repeated name outside the partition columns splits cleanly");

    let file_names: Vec<&str> = split
        .file_source_schema()
        .file_schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(file_names, ["b", "b"]);

    let remapped = split
        .remap_projection(None)
        .expect("a partitioned scan always projects explicitly");
    assert_eq!(remapped, vec![2, 0, 1]);

    let scan_schema = split.file_source_schema();
    let selected: Vec<&Field> = remapped
        .iter()
        .map(|i| scan_schema.table_schema().field(*i))
        .collect();
    let expected: Vec<&Field> = (0..declared.fields().len())
        .map(|i| declared.field(i))
        .collect();
    assert_eq!(selected, expected);
}

#[test]
fn partition_values_are_converted_to_their_declared_type() {
    let declared = declared_schema(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("l", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("d", DataType::Date32, true),
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("n", DataType::Decimal128(9, 2), true),
    ]);
    let partition_columns: Vec<String> = ["i", "l", "f", "s", "b", "d", "t", "n"]
        .iter()
        .map(|n| (*n).to_string())
        .collect();
    let split =
        PartitionedScanSchema::split(Arc::clone(&declared), &partition_columns).expect("declared");

    let values = split
        .partition_values_for(&entry_with(&[
            ("i", Some("-7")),
            ("l", Some("9007199254740993")),
            ("f", Some("1.5")),
            ("s", Some("us-east-1")),
            ("b", Some("true")),
            ("d", Some("2024-03-01")),
            ("t", Some("2024-03-01 12:34:56.000001")),
            ("n", Some("12.34")),
        ]))
        .expect("every value is representable");

    assert_eq!(
        values,
        vec![
            ScalarValue::Int32(Some(-7)),
            ScalarValue::Int64(Some(9007199254740993)),
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::Utf8(Some("us-east-1".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Date32(Some(19783)),
            ScalarValue::TimestampMicrosecond(Some(1709296496000001), None),
            ScalarValue::Decimal128(Some(1234), 9, 2),
        ]
    );
    for (value, field) in values.iter().zip(declared.fields()) {
        assert_eq!(
            &value.data_type(),
            field.data_type(),
            "'{}' must arrive as its declared type",
            field.name()
        );
    }
}

#[test]
fn an_absent_or_empty_partition_value_becomes_a_typed_null() {
    let split = PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("absent", DataType::Int32, true),
            Field::new("empty", DataType::Date32, true),
        ]),
        &["absent".to_string(), "empty".to_string()],
    )
    .expect("declared");

    let values = split
        .partition_values_for(&entry_with(&[("absent", None), ("empty", Some(""))]))
        .expect("both encode a null partition value");

    assert_eq!(
        values,
        vec![ScalarValue::Int32(None), ScalarValue::Date32(None)]
    );
}

#[test]
fn a_partition_value_the_declared_type_cannot_represent_is_refused() {
    let split = PartitionedScanSchema::split(
        declared_schema(vec![Field::new("part", DataType::Int32, true)]),
        &["part".to_string()],
    )
    .expect("declared");

    let error = split
        .partition_values_for(&entry_with(&[("part", Some("not-a-number"))]))
        .expect_err("an unrepresentable value must never be coerced or nulled");

    assert!(error.contains("part"), "{error}");
    assert!(error.contains("Int32"), "{error}");
    assert!(error.contains("not-a-number"), "{error}");
}

#[test]
fn an_out_of_range_partition_value_is_refused_rather_than_truncated() {
    let split = PartitionedScanSchema::split(
        declared_schema(vec![Field::new("part", DataType::Int32, true)]),
        &["part".to_string()],
    )
    .expect("declared");

    let error = split
        .partition_values_for(&entry_with(&[("part", Some("2147483648"))]))
        .expect_err("a value past the declared type's range must not wrap or truncate");

    assert!(error.contains("2147483648"), "{error}");
}

#[test]
fn a_partition_column_missing_from_a_file_entry_is_refused() {
    let split = split_with_middle_partition();

    let error = split
        .partition_values_for(&FileEntry::new("part-0.parquet", 1))
        .expect_err("an entry that logs no value for a partition column is a planning defect");

    assert!(error.contains('p'), "{error}");
    assert!(error.contains("part-0.parquet"), "{error}");
}

#[test]
fn partition_values_follow_partition_order_not_declared_order() {
    let split = PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("second", DataType::Utf8, true),
            Field::new("first", DataType::Utf8, true),
        ]),
        &["first".to_string(), "second".to_string()],
    )
    .expect("declared");

    let values = split
        .partition_values_for(&entry_with(&[("first", Some("1")), ("second", Some("2"))]))
        .expect("both logged");

    assert_eq!(
        values,
        vec![
            ScalarValue::Utf8(Some("1".to_string())),
            ScalarValue::Utf8(Some("2".to_string())),
        ],
        "values line up with table_partition_cols, which the opener zips them against"
    );
    let partition_names: Vec<&str> = split
        .file_source_schema()
        .table_partition_cols()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(partition_names, ["first", "second"]);
}
