use super::*;
use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_adapter::replace_columns_with_literals;
use std::collections::BTreeMap;

fn declared_schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// `p` sits between two file columns, so declared order differs from `file ++ partition` order.
fn split_with_middle_partition() -> PartitionedScanSchema {
    PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("p", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]),
        &["p".to_string()],
        &[],
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

fn projected_column_indices(projection: &ProjectionExprs) -> Vec<usize> {
    projection
        .iter()
        .map(|projected| {
            projected
                .expr
                .downcast_ref::<Column>()
                .expect("a column reference")
                .index()
        })
        .collect()
}

fn scan_indices(split: &PartitionedScanSchema, projection: Option<&Vec<usize>>) -> Vec<usize> {
    projected_column_indices(
        &split
            .projection_exprs(projection)
            .expect("a partitioned scan always projects explicitly"),
    )
}

fn partition_names(split: &PartitionedScanSchema) -> Vec<&str> {
    split
        .file_source_schema()
        .table_partition_cols()
        .iter()
        .map(|f| f.name().as_str())
        .collect()
}

fn file_names(split: &PartitionedScanSchema) -> Vec<&str> {
    split
        .file_source_schema()
        .file_schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect()
}

/// `region` is declared between two other columns and is no listed partition column.
fn region_declared() -> SchemaRef {
    declared_schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int32, true),
    ])
}

fn identity_split(files: &[FileEntry]) -> PartitionedScanSchema {
    PartitionedScanSchema::split(region_declared(), &[], files)
        .expect("region is declared exactly once")
}

/// The projection the Parquet opener hands the adapter for `file`: DataFusion has already
/// substituted each partition column, the hidden one included, by its per-file literal.
fn opened_projection(
    split: &PartitionedScanSchema,
    file: &FileEntry,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let literals: HashMap<String, ScalarValue> = split
        .file_source_schema()
        .table_partition_cols()
        .iter()
        .map(|field| field.name().clone())
        .zip(split.partition_values_for(file).expect("a valid entry"))
        .collect();
    split
        .projection_exprs(None)
        .expect("an identity column always projects explicitly")
        .iter()
        .map(|projected| {
            replace_columns_with_literals(Arc::clone(&projected.expr), &literals)
                .expect("substitution never fails")
        })
        .collect()
}

#[test]
fn declared_schema_is_split_into_file_fields_and_partition_fields() {
    let split = split_with_middle_partition();

    assert_eq!(
        split.declared_schema().fields().len(),
        3,
        "the declared schema keeps every column"
    );
    assert_eq!(
        file_names(&split),
        ["a", "b"],
        "partition columns leave the file schema"
    );
    assert_eq!(partition_names(&split), ["p"]);
}

#[test]
fn the_declared_schema_keeps_its_declared_column_order() {
    let declared = declared_schema(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &["p".to_string()], &[])
        .expect("p is declared");

    assert_eq!(split.declared_schema().as_ref(), declared.as_ref());
}

#[test]
fn projection_indices_are_remapped_from_declared_order_to_file_then_partition_order() {
    let split = split_with_middle_partition();

    // Declared a=0, p=1, b=2; scan order is a=0, b=1, p=2.
    assert_eq!(scan_indices(&split, Some(&vec![2, 1, 0])), vec![1, 2, 0]);
    assert_eq!(scan_indices(&split, Some(&vec![1])), vec![2]);
}

#[test]
fn a_remapped_projection_selects_the_declared_fields_in_the_requested_order() {
    let split = split_with_middle_partition();
    let declared_projection = vec![2, 1];

    let projected = split
        .projection_exprs(Some(&declared_projection))
        .expect("a partitioned scan always projects explicitly")
        .project_schema(split.file_source_schema().table_schema())
        .expect("every expression resolves against the scan schema");

    let expected: Vec<&Field> = declared_projection
        .iter()
        .map(|i| split.declared_schema().field(*i))
        .collect();
    assert_eq!(
        projected
            .fields()
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn an_absent_projection_is_expanded_so_declared_order_survives_the_split() {
    let split = split_with_middle_partition();

    assert_eq!(scan_indices(&split, None), vec![0, 2, 1]);
}

#[test]
fn a_scan_without_partition_columns_keeps_its_schema_and_projection_unchanged() {
    let declared = declared_schema(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int64, true),
    ]);
    let split = PartitionedScanSchema::split(
        Arc::clone(&declared),
        &[],
        &[FileEntry::new("part-0.parquet", 1)],
    )
    .expect("no partitions");

    assert_eq!(
        split.file_source_schema().file_schema().as_ref(),
        declared.as_ref()
    );
    assert!(split.file_source_schema().table_partition_cols().is_empty());
    assert!(split.projection_exprs(None).is_none());
    assert_eq!(scan_indices(&split, Some(&vec![1, 0])), vec![1, 0]);
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
        &[],
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
        &[],
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
        &[],
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
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &["p".to_string()], &[])
        .expect("a repeated name outside the partition columns splits cleanly");

    assert_eq!(file_names(&split), ["b", "b"]);
    assert_eq!(scan_indices(&split, None), vec![2, 0, 1]);

    let projected = split
        .projection_exprs(None)
        .expect("a partitioned scan always projects explicitly")
        .project_schema(split.file_source_schema().table_schema())
        .expect("every expression resolves against the scan schema");
    assert_eq!(projected.fields(), declared.fields());
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
    let split = PartitionedScanSchema::split(Arc::clone(&declared), &partition_columns, &[])
        .expect("declared");

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
        &[],
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
        &[],
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
        &[],
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
        &[],
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
    assert_eq!(partition_names(&split), ["first", "second"]);
}

#[test]
fn an_unlisted_partition_value_key_stays_a_file_column_beside_a_hidden_identity_column() {
    let split = identity_split(&[
        entry_with(&[("region", Some("eu"))]),
        FileEntry::new("part-1.parquet", 1),
    ]);

    assert_eq!(
        file_names(&split),
        ["id", "region", "amount"],
        "the column is still read from every file that holds it"
    );
    assert_eq!(partition_names(&split), ["__identity_1"]);
}

#[test]
fn the_hidden_identity_column_name_differs_from_every_declared_name() {
    let declared = declared_schema(vec![
        Field::new("__identity_1_", DataType::Int32, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("__identity_1", DataType::Int32, true),
    ]);

    let split =
        PartitionedScanSchema::split(declared, &[], &[entry_with(&[("region", Some("eu"))])])
            .expect("region is declared");

    assert_eq!(partition_names(&split), ["__identity_1__"]);
}

/// The scan index of each declared column's file-side input, a fallback's file column included.
fn file_side_scan_indices(split: &PartitionedScanSchema) -> Vec<usize> {
    split
        .projection_exprs(None)
        .expect("a split scan always projects explicitly")
        .iter()
        .map(|projected| {
            let column = match projected.expr.downcast_ref::<IdentityFallbackExpr>() {
                Some(fallback) => fallback.children()[0],
                None => &projected.expr,
            };
            column
                .downcast_ref::<Column>()
                .expect("a column reference")
                .index()
        })
        .collect()
}

#[test]
fn a_listed_partition_column_keeps_its_split_beside_an_identity_column() {
    let declared = declared_schema(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("p", DataType::Int32, true),
        Field::new("a", DataType::Int64, true),
    ]);

    let split = PartitionedScanSchema::split(
        declared,
        &["p".to_string()],
        &[entry_with(&[("p", Some("3")), ("region", Some("eu"))])],
    )
    .expect("both keys are declared");

    assert_eq!(file_names(&split), ["region", "a"]);
    assert_eq!(partition_names(&split), ["p", "__identity_0"]);
    assert_eq!(file_side_scan_indices(&split), vec![0, 2, 1]);
}

#[test]
fn an_unlisted_key_naming_no_declared_column_adds_no_column() {
    let split = identity_split(&[entry_with(&[("dropped", Some("x"))])]);

    assert!(split.file_source_schema().table_partition_cols().is_empty());
    assert!(split.projection_exprs(None).is_none());
}

#[test]
fn an_unlisted_key_matching_two_declared_fields_is_refused() {
    let error = PartitionedScanSchema::split(
        declared_schema(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("region", DataType::Int32, true),
        ]),
        &[],
        &[entry_with(&[("region", Some("eu"))])],
    )
    .expect_err("one identity value cannot stand for two declared fields");

    assert!(error.contains("region"), "{error}");
}

fn reduced_for_file(split: &PartitionedScanSchema, file: &FileEntry) -> Arc<dyn PhysicalExpr> {
    let region = Arc::clone(&opened_projection(split, file)[1]);
    region
        .downcast_ref::<IdentityFallbackExpr>()
        .expect("the column reads through its identity fallback")
        .reduce_for_file(|_| false)
        .expect("a substituted fallback reduces")
}

#[test]
fn each_file_reduces_to_its_value_a_null_or_its_file_column() {
    let files = [
        entry_with(&[("region", Some("eu"))]),
        entry_with(&[("region", None)]),
        entry_with(&[("region", Some(""))]),
        FileEntry::new("part-1.parquet", 1),
    ];
    let split = identity_split(&files);

    let literal_of = |file: &FileEntry| {
        reduced_for_file(&split, file)
            .downcast_ref::<Literal>()
            .expect("the file's value reduces to a literal")
            .value()
            .clone()
    };
    assert_eq!(
        literal_of(&files[0]),
        ScalarValue::Utf8(Some("eu".to_string()))
    );
    assert_eq!(
        literal_of(&files[1]),
        ScalarValue::Utf8(None),
        "an absent value is null"
    );
    assert_eq!(
        literal_of(&files[2]),
        ScalarValue::Utf8(None),
        "an empty value is null"
    );
    let reduced = reduced_for_file(&split, &files[3]);
    let column = reduced
        .downcast_ref::<Column>()
        .expect("a file carrying no key leaves the file column");
    assert_eq!(column.name(), "region");
}

#[test]
fn a_fallback_whose_file_side_is_not_a_column_fails() {
    let file = entry_with(&[("region", Some("eu"))]);
    let identity = identity_split(std::slice::from_ref(&file))
        .partition_values_for(&file)
        .expect("a valid entry")
        .remove(0);
    let fallback = IdentityFallbackExpr::new(
        Arc::new(Literal::new(ScalarValue::Utf8(None))),
        Arc::new(Literal::new(identity)),
    );

    let error = fallback
        .reduce_for_file(|_| false)
        .expect_err("a file side that is not a column cannot be reduced");

    assert!(error.to_string().contains("identity_fallback"), "{error}");
}

#[test]
fn an_identity_value_the_declared_type_cannot_represent_is_refused() {
    let declared = declared_schema(vec![Field::new("year", DataType::Int32, true)]);
    let file = entry_with(&[("year", Some("not-a-number"))]);
    let split = PartitionedScanSchema::split(declared, &[], std::slice::from_ref(&file))
        .expect("year is declared");

    let error = split
        .partition_values_for(&file)
        .expect_err("an unrepresentable value must never be coerced or nulled");

    assert!(error.contains("year"), "{error}");
    assert!(error.contains("not-a-number"), "{error}");
}

#[test]
fn a_fallback_column_projects_its_declared_field_from_its_file_and_hidden_columns() {
    let split = identity_split(&[entry_with(&[("region", Some("eu"))])]);

    let projection = split
        .projection_exprs(None)
        .expect("an identity column always projects explicitly");

    let region = &projection.as_ref()[1];
    assert_eq!(region.alias, "region");
    let fallback = region
        .expr
        .downcast_ref::<IdentityFallbackExpr>()
        .expect("the column reads through its identity fallback");
    let inputs: Vec<(&str, usize)> = fallback
        .children()
        .into_iter()
        .map(|child| {
            let column = child.downcast_ref::<Column>().expect("a column reference");
            (column.name(), column.index())
        })
        .collect();
    assert_eq!(inputs, [("region", 1), ("__identity_1", 3)]);
    let projected = projection
        .project_schema(split.file_source_schema().table_schema())
        .expect("every expression resolves against the scan schema");
    assert_eq!(projected.fields(), region_declared().fields());
}

/// Scenario: A column the data file holds wins over the file's identity partition value
#[test]
fn unreduced_fallback_evaluates_its_file_column() {
    let file = entry_with(&[("region", Some("eu"))]);
    let split = identity_split(std::slice::from_ref(&file));
    let region = &opened_projection(&split, &file)[1];
    let held: ArrayRef = Arc::new(StringArray::from(vec![Some("us"), None]));
    let batch = RecordBatch::try_new(
        Arc::clone(split.file_source_schema().file_schema()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::clone(&held),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .expect("a batch of the file schema");

    let read = region
        .evaluate(&batch)
        .and_then(|value| value.into_array(batch.num_rows()))
        .expect("an unreduced fallback evaluates");

    assert_eq!(
        read.as_ref(),
        held.as_ref(),
        "a file the adapter skips binds the column, so its values, NULL included, win"
    );
}

#[test]
fn a_fallback_reduced_without_its_per_file_literal_fails_naming_the_column() {
    let split = identity_split(&[entry_with(&[("region", Some("eu"))])]);
    let projection = split
        .projection_exprs(None)
        .expect("an identity column always projects explicitly");
    let fallback = projection.as_ref()[1]
        .expr
        .downcast_ref::<IdentityFallbackExpr>()
        .expect("the column reads through its identity fallback");

    let error = fallback
        .reduce_for_file(|_| false)
        .expect_err("an unsubstituted hidden column carries no per-file value");

    assert!(error.to_string().contains("region"), "{error}");
}
