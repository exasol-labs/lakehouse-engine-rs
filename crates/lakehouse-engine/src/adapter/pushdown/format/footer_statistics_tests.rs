use super::*;
use crate::adapter::pushdown::test_support::filter_json::{
    between, column, compare, equal, in_list, is_not_null, is_null, not, number, or,
};
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, ListBuilder, StringBuilder,
    TimestampMicrosecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use parquet::arrow::ArrowWriter;
use parquet::basic::{ColumnOrder, SortOrder};
use parquet::data_type::{ByteArray, Int96};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaDataBuilder, ParquetMetaDataReader,
    RowGroupMetaData,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{ColumnPath, SchemaDescriptor};
use serde_json::{Value as Json, json};
use std::sync::Arc;

const EPOCH_SECONDS_2024_01_01: i64 = 1_704_067_200;

fn file_holding(footer: ParquetMetaData, partition_values: &[(&str, &str)]) -> ParquetFile {
    ParquetFile {
        path: StorePath::from("table/f.parquet"),
        size: 1,
        partition_values: partition_values
            .iter()
            .map(|(key, value)| (key.to_string(), Some(value.to_string())))
            .collect(),
        footer: Some(Arc::new(footer)),
    }
}

/// The footer an `ArrowWriter` writes for `batch`, in row groups of `rows_per_group`.
fn written_footer(
    batch: &RecordBatch,
    rows_per_group: usize,
    props: WriterProperties,
) -> ParquetMetaData {
    let props = props
        .into_builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .build();
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).expect("open writer");
    if batch.num_rows() > 0 {
        writer.write(batch).expect("write batch");
    }
    writer.close().expect("close writer");
    ParquetMetaDataReader::new()
        .parse_and_finish(&Bytes::from(bytes))
        .expect("read back the footer")
}

fn written(batch: &RecordBatch, rows_per_group: usize) -> ParquetFile {
    file_holding(
        written_footer(batch, rows_per_group, WriterProperties::default()),
        &[],
    )
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("fixture batch")
}

fn int64s(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn typed(columns: &[(&str, DataType)]) -> Vec<(String, DataType)> {
    columns
        .iter()
        .map(|(name, data_type)| (name.to_string(), data_type.clone()))
        .collect()
}

fn keeps(filter: &Json, columns: &[(&str, DataType)], file: &ParquetFile) -> bool {
    let columns = typed(columns);
    FooterStatisticsFilter {
        predicate: PartitionPredicate::for_footer_statistics(Some(filter), &columns),
        columns,
    }
    .keeps(file)
}

fn greater(name: &str, value: &str) -> Json {
    compare("predicate_greater", column(name), number(value))
}

fn less(name: &str, value: &str) -> Json {
    compare("predicate_less", column(name), number(value))
}

fn timestamp(value: &str) -> Json {
    json!({"type": "literal_timestamp", "value": value})
}

/// A footer built by hand, for statistics `parquet` 58.3.0 never writes. Each row group is its
/// row count and one optional statistic per leaf of `message`.
fn hand_built(
    message: &str,
    column_orders: Option<Vec<ColumnOrder>>,
    row_groups: Vec<(i64, Vec<Option<Statistics>>)>,
) -> ParquetFile {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type(message).expect("fixture message type"),
    )));
    let total_rows = row_groups.iter().map(|(rows, _)| rows).sum();
    let file_metadata = FileMetaData::new(
        1,
        total_rows,
        None,
        None,
        Arc::clone(&schema),
        column_orders,
    );
    let builder = row_groups.into_iter().fold(
        ParquetMetaDataBuilder::new(file_metadata),
        |builder, (rows, statistics)| {
            let columns = statistics
                .into_iter()
                .enumerate()
                .map(|(leaf, statistics)| {
                    let chunk = ColumnChunkMetaData::builder(schema.column(leaf));
                    match statistics {
                        Some(statistics) => chunk.set_statistics(statistics),
                        None => chunk,
                    }
                    .build()
                    .expect("fixture column chunk")
                })
                .collect();
            builder.add_row_group(
                RowGroupMetaData::builder(Arc::clone(&schema))
                    .set_num_rows(rows)
                    .set_column_metadata(columns)
                    .build()
                    .expect("fixture row group"),
            )
        },
    );
    file_holding(builder.build(), &[])
}

const ID_MESSAGE: &str = "message t { REQUIRED INT64 id; }";

fn signed_order() -> Option<Vec<ColumnOrder>> {
    Some(vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)])
}

fn id_bounds(min: Option<i64>, max: Option<i64>, null_count: Option<u64>) -> Statistics {
    Statistics::int64(min, max, None, null_count, false)
}

/// Scenario: Row-group statistics evaluate the filter under three-valued logic
/// Scenario: Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds
#[test]
fn a_file_is_kept_iff_one_row_group_can_be_true() {
    let id = [("id", DataType::Int64)];
    let two_groups = written(&batch(vec![("id", int64s(&[1, 2, 3, 4, 5, 6, 7, 8]))]), 4);
    assert_eq!(
        two_groups.footer.as_ref().map(|f| f.num_row_groups()),
        Some(2)
    );
    for (label, filter, expected) in [
        ("only the first row group matches", less("ID", "3"), true),
        (
            "only the second row group matches",
            greater("ID", "6"),
            true,
        ),
        ("no row group matches", greater("ID", "100"), false),
        (
            "IN outside every row group",
            in_list("ID", vec![number("0"), number("9")]),
            false,
        ),
        (
            "BETWEEN the row groups",
            between("ID", number("5"), number("5")),
            true,
        ),
        ("IS NULL over zero null counts", is_null("ID"), false),
        ("IS NOT NULL", is_not_null("ID"), true),
    ] {
        assert_eq!(keeps(&filter, &id, &two_groups), expected, "{label}");
    }

    let nullable_ids =
        |values: &[Option<i64>]| -> ArrayRef { Arc::new(Int64Array::from(values.to_vec())) };
    let all_null = written(&batch(vec![("nul", nullable_ids(&[None; 4]))]), 4);
    let half_null = written(
        &batch(vec![(
            "nul",
            nullable_ids(&[Some(1), Some(2), Some(3), Some(4), None, None, None, None]),
        )]),
        4,
    );
    let all_null_statistics = all_null
        .footer
        .as_ref()
        .and_then(|footer| footer.row_group(0).column(0).statistics().cloned())
        .expect("the writer records an all-NULL chunk's statistics");
    assert!(
        all_null_statistics.is_min_max_deprecated()
            && all_null_statistics.min_bytes_opt().is_none(),
        "parquet 58.3.0 reads a chunk without min_value and max_value as deprecated"
    );
    let nul = [("nul", DataType::Int64)];
    for (label, filter, file, expected) in [
        (
            "IS NOT NULL over all NULLs",
            is_not_null("NUL"),
            &all_null,
            false,
        ),
        (
            "a comparison over all NULLs",
            greater("NUL", "2"),
            &all_null,
            false,
        ),
        ("IS NULL over all NULLs", is_null("NUL"), &all_null, true),
        (
            "IS NOT NULL over half NULLs",
            is_not_null("NUL"),
            &half_null,
            true,
        ),
        (
            "past the non-NULL bounds",
            greater("NUL", "4"),
            &half_null,
            false,
        ),
    ] {
        assert_eq!(keeps(&filter, &nul, file), expected, "{label}");
    }

    let no_footer = ParquetFile {
        footer: None,
        ..written(&batch(vec![("id", int64s(&[1]))]), 4)
    };
    assert!(
        keeps(&greater("ID", "100"), &id, &no_footer),
        "a file without a read footer is kept"
    );

    let no_row_group = written(&batch(vec![("id", int64s(&[]))]), 4);
    assert_eq!(
        no_row_group.footer.as_ref().map(|f| f.num_row_groups()),
        Some(0)
    );
    assert!(
        !keeps(&is_null("ID"), &id, &no_row_group),
        "a file holding zero row groups holds no row"
    );

    let empty_group = hand_built(
        ID_MESSAGE,
        signed_order(),
        vec![(0, vec![Some(id_bounds(Some(1), Some(4), None))])],
    );
    assert!(
        !keeps(&less("ID", "3"), &id, &empty_group),
        "a row group holding zero rows cannot be TRUE"
    );

    let seconds = |offsets: &[i64]| -> ArrayRef {
        Arc::new(TimestampSecondArray::from(
            offsets
                .iter()
                .map(|offset| EPOCH_SECONDS_2024_01_01 + offset)
                .collect::<Vec<_>>(),
        ))
    };
    let ts_s = written(&batch(vec![("ts_s", seconds(&[1, 2]))]), 4);
    let footer = ts_s.footer.as_ref().expect("footer");
    assert_eq!(
        footer
            .file_metadata()
            .schema_descr()
            .column(0)
            .logical_type_ref(),
        None,
        "parquet 58.3.0 stores a seconds timestamp as INT64 without a timestamp annotation"
    );
    let ts_s_type = [("ts_s", DataType::Timestamp(TimeUnit::Second, None))];
    let ts_s_compare = |kind: &str, value: &str| compare(kind, column("TS_S"), timestamp(value));
    assert!(!keeps(
        &ts_s_compare("predicate_less", "2024-01-01 00:00:00"),
        &ts_s_type,
        &ts_s
    ));
    assert!(keeps(
        &ts_s_compare("predicate_lessequal", "2024-01-01 00:00:01"),
        &ts_s_type,
        &ts_s
    ));
}

/// Scenario: A statistic whose ordering the Parquet specification leaves undefined keeps the file
#[test]
fn an_undefined_ordering_or_statistic_keeps_the_file() {
    let id = [("id", DataType::Int64)];
    let excluded = greater("ID", "100");
    let with = |orders: Option<Vec<ColumnOrder>>, statistics: Option<Statistics>| {
        hand_built(ID_MESSAGE, orders, vec![(4, vec![statistics])])
    };
    let usable = || Some(id_bounds(Some(1), Some(4), Some(0)));
    assert!(
        !keeps(&excluded, &id, &with(signed_order(), usable())),
        "the hand-built footer prunes when every gate passes"
    );

    for (label, file) in [
        ("no column_orders", with(None, usable())),
        (
            "an UNDEFINED column order",
            with(Some(vec![ColumnOrder::UNDEFINED]), usable()),
        ),
        (
            "an unknown union member",
            with(Some(vec![ColumnOrder::UNKNOWN]), usable()),
        ),
        (
            "a type-defined order whose sort order is undefined",
            with(
                Some(vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED)]),
                usable(),
            ),
        ),
        (
            "deprecated min and max only",
            with(
                signed_order(),
                Some(Statistics::int64(Some(1), Some(4), None, Some(0), true)),
            ),
        ),
        (
            "a missing minimum",
            with(signed_order(), Some(id_bounds(None, Some(4), Some(0)))),
        ),
        (
            "a missing maximum",
            with(signed_order(), Some(id_bounds(Some(1), None, Some(0)))),
        ),
        (
            "a minimum greater than its maximum",
            with(signed_order(), Some(id_bounds(Some(400), Some(4), Some(0)))),
        ),
        ("absent statistics", with(signed_order(), None)),
    ] {
        assert!(keeps(&excluded, &id, &file), "{label}");
    }

    let missing_null_count = with(signed_order(), Some(id_bounds(Some(1), Some(4), None)));
    for filter in [is_null("ID"), is_not_null("ID")] {
        assert!(
            keeps(&filter, &id, &missing_null_count),
            "a missing null count is unknown, never zero: {filter}"
        );
    }
    assert!(
        !keeps(
            &is_null("ID"),
            &id,
            &with(signed_order(), Some(id_bounds(Some(1), Some(4), Some(0))))
        ),
        "a present zero null count decides IS NULL"
    );

    let int96 = hand_built(
        "message t { OPTIONAL INT96 c; }",
        Some(vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED)]),
        vec![(
            2,
            vec![Some(Statistics::int96(
                Some(Int96::from(vec![0, 0, 2_440_588])),
                Some(Int96::from(vec![0, 0, 2_440_589])),
                None,
                Some(0),
                false,
            ))],
        )],
    );
    assert!(keeps(
        &compare(
            "predicate_greater",
            column("C"),
            timestamp("2030-01-01 00:00:00")
        ),
        &[("c", DataType::Timestamp(TimeUnit::Nanosecond, None))],
        &int96
    ));

    let undecodable = hand_built(
        "message t { OPTIONAL BYTE_ARRAY s (STRING); }",
        signed_order(),
        vec![(
            1,
            vec![Some(Statistics::byte_array(
                Some(ByteArray::from(vec![0xff, 0xfe])),
                Some(ByteArray::from(vec![0xff, 0xff])),
                None,
                Some(0),
                false,
            ))],
        )],
    );
    assert!(
        keeps(&equal("S", "a"), &[("s", DataType::Utf8)], &undecodable),
        "a bound that does not decode to the column's type is unknown"
    );

    let quiet = file_holding(
        written_footer(
            &batch(vec![("id", int64s(&[1, 2, 3, 4]))]),
            4,
            WriterProperties::builder()
                .set_column_statistics_enabled(ColumnPath::from("id"), EnabledStatistics::None)
                .build(),
        ),
        &[],
    );
    assert!(
        keeps(&excluded, &id, &quiet),
        "a column written without statistics"
    );
}

/// Scenario: A column the footer statistics cannot describe keeps the file
#[test]
fn an_undescribable_column_keeps_the_file() {
    let ids = written(&batch(vec![("id", int64s(&[1, 2, 3, 4]))]), 4);
    let excluded = greater("ID", "100");
    assert!(
        keeps(&excluded, &[], &ids),
        "a column absent from the declared columns is untranslatable"
    );
    assert!(
        keeps(
            &greater("OTHER", "100"),
            &[("other", DataType::Int64)],
            &ids
        ),
        "a column absent from the file"
    );

    let int64_file = written(&batch(vec![("w", int64s(&[1, 2, 3, 4]))]), 4);
    assert!(
        keeps(&greater("W", "100"), &[("w", DataType::Int32)], &int64_file),
        "a file type that neither equals nor widens to the folded type"
    );
    let int32_file = written(
        &batch(vec![(
            "w",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        )]),
        4,
    );
    assert!(
        !keeps(
            &greater("W", "4000000000"),
            &[("w", DataType::Int64)],
            &int32_file
        ),
        "an INT32 file's bounds widen to the folded INT64"
    );

    let utc_micros: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![EPOCH_SECONDS_2024_01_01 * 1_000_000])
            .with_timezone("UTC"),
    );
    let tstz = written(&batch(vec![("tstz", utc_micros)]), 4);
    assert!(keeps(
        &compare(
            "predicate_greater",
            column("TSTZ"),
            timestamp("2030-01-01 00:00:00")
        ),
        &[(
            "tstz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        )],
        &tstz
    ));

    let float16 = written(
        &batch(vec![(
            "f16",
            cast(&Float32Array::from(vec![1.0f32, 2.0]), &DataType::Float16).expect("Float16"),
        )]),
        4,
    );
    assert!(keeps(
        &greater("F16", "100"),
        &[("f16", DataType::Float16)],
        &float16
    ));

    let colliding = file_holding(
        written_footer(
            &batch(vec![(
                "grp",
                Arc::new(arrow::array::StringArray::from(vec!["zzz"])) as ArrayRef,
            )]),
            4,
            WriterProperties::default(),
        ),
        &[("grp", "a")],
    );
    let grp = [("grp", DataType::Utf8)];
    assert!(
        keeps(&equal("GRP", "a"), &grp, &colliding),
        "the partition value decides a partition column"
    );
    assert!(
        !keeps(&equal("GRP", "zzz"), &grp, &colliding),
        "a colliding Parquet column's statistics are never read"
    );
    assert!(
        keeps(
            &or(vec![equal("GRP", "a"), greater("ID", "100")]),
            &grp,
            &colliding
        ),
        "a partition value under OR"
    );
}

/// Scenario: A column the footer statistics cannot describe keeps the file
#[test]
fn a_nested_column_keeps_a_file_its_leaf_bounds_would_exclude() {
    let mut tags = ListBuilder::new(StringBuilder::new());
    for row in [vec!["hello", "world"], vec!["zzz"]] {
        for value in row {
            tags.values().append_value(value);
        }
        tags.append(true);
    }
    let nested = written(
        &batch(vec![
            ("id", int64s(&[1, 2])),
            ("tags", Arc::new(tags.finish()) as ArrayRef),
        ]),
        1,
    );
    let footer = nested.footer.as_ref().expect("footer");
    let leaf = footer.row_group(0).column(1);
    assert_eq!(leaf.column_descr().path().string(), "tags.list.item");
    let statistics = leaf.statistics().expect("leaf statistics");
    assert_eq!(
        (statistics.min_bytes_opt(), statistics.max_bytes_opt()),
        (Some(b"hello".as_slice()), Some(b"world".as_slice())),
        "the premise: the first row group's leaf bounds exclude the rendered document"
    );

    let rendered = equal("TAGS", r#"["hello","world"]"#);
    let list_type = footer_list_type(footer);
    for (label, column_type) in [("as text", DataType::Utf8), ("as a list", list_type)] {
        assert!(
            keeps(&rendered, &[("tags", column_type.clone())], &nested),
            "{label}: leaf statistics never decide a nested column"
        );
        assert!(
            keeps(&is_null("TAGS"), &[("tags", column_type)], &nested),
            "{label}: a leaf null count is not the column's null count"
        );
    }
}

fn footer_list_type(footer: &ParquetMetaData) -> DataType {
    let file_metadata = footer.file_metadata();
    parquet::arrow::parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )
    .expect("arrow schema")
    .field_with_name("tags")
    .expect("tags field")
    .data_type()
    .clone()
}

fn doubles(values: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

/// Scenario: A float bound or literal is widened or rejected so it never drops a matching row
#[test]
fn float_bounds_widen_zero_and_reject_nan() {
    let d = [("d", DataType::Float64)];
    let with_nan = written(&batch(vec![("d", doubles(&[1.0, f64::NAN, 4.0]))]), 4);
    assert!(
        !keeps(&greater("D", "5"), &d, &with_nan),
        "the writer's bounds exclude NaN, and a parity comparison prunes on them"
    );
    assert!(
        keeps(&not(less("D", "5")), &d, &with_nan),
        "the NaN row keeps a negated comparison"
    );

    let all_nan = written(&batch(vec![("d", doubles(&[f64::NAN, f64::NAN]))]), 4);
    assert!(
        keeps(&greater("D", "5"), &d, &all_nan),
        "an all-NaN row group carries no bound"
    );

    let zeros = written(&batch(vec![("d", doubles(&[0.0, 0.0]))]), 4);
    assert!(
        keeps(&less("D", "0"), &d, &zeros),
        "the written -0.0 minimum"
    );
    assert!(
        !keeps(&greater("D", "0"), &d, &zeros),
        "no stored zero is greater than +0.0 in total order"
    );
    assert!(
        keeps(
            &less("D", "-0"),
            &d,
            &written(&batch(vec![("d", doubles(&[-0.0, 1.0]))]), 4)
        ),
        "the scan reads -0 as the integer 0, so a stored -0.0 is less than it"
    );
    let negative_zero_float = written(
        &batch(vec![(
            "f",
            Arc::new(Float32Array::from(vec![-0.0f32, 1.0])) as ArrayRef,
        )]),
        4,
    );
    assert!(
        keeps(
            &less("F", "-0"),
            &[("f", DataType::Float32)],
            &negative_zero_float
        ),
        "the scan reads -0 as the integer 0, so a stored FLOAT -0.0 is less than it"
    );

    let double_message = "message t { OPTIONAL DOUBLE d; }";
    let double_bounds = |min: f64, max: f64| {
        hand_built(
            double_message,
            signed_order(),
            vec![(
                2,
                vec![Some(Statistics::double(
                    Some(min),
                    Some(max),
                    None,
                    Some(0),
                    false,
                ))],
            )],
        )
    };
    assert!(
        keeps(&less("D", "0"), &d, &double_bounds(0.0, 0.0)),
        "a +0 minimum widens to -0"
    );
    assert!(
        keeps(
            &compare("predicate_greaterequal", column("D"), number("0")),
            &d,
            &double_bounds(-0.0, -0.0)
        ),
        "a -0 maximum widens to +0"
    );
    assert!(
        keeps(&greater("D", "100"), &d, &double_bounds(f64::NAN, 4.0)),
        "a NaN minimum is unknown"
    );
    assert!(
        keeps(&less("D", "-100"), &d, &double_bounds(1.0, f64::NAN)),
        "a NaN maximum is unknown"
    );

    let float = written(
        &batch(vec![(
            "f",
            Arc::new(Float32Array::from(vec![1.0f32, 4.0])) as ArrayRef,
        )]),
        4,
    );
    let f = [("f", DataType::Float32)];
    assert!(keeps(&less("F", "4.5"), &f, &float));
    assert!(
        !keeps(&greater("F", "4.5"), &f, &float),
        "FLOAT bounds widen to DOUBLE against the scan's DOUBLE literal"
    );
    assert!(
        !keeps(&greater("F", "16777216"), &f, &float),
        "an integer FLOAT holds exactly converts"
    );
}

#[test]
fn a_filter_on_a_declared_field_name_binds_by_the_uppercase_fold() {
    let ids = written(&batch(vec![("Id", int64s(&[1, 2, 3, 4]))]), 4);
    assert!(!keeps(
        &greater("ID", "100"),
        &[("Id", DataType::Int64)],
        &ids
    ));
}
