use crate::scan::raw_scan::{build_scan_sql, register_files};
use crate::scan::session_config_for_spec;
use crate::scan::spec::{FileEntry, LogicalField, ScanSpec};
use crate::scan::test_support::{local_file_size, minimal_spec};
use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use std::path::Path;
use std::sync::Arc;

/// Every physical-to-logical Arrow pair `datafusion-scan/type-relaxation`'s supported-set table
/// declares readable, one entry per table row (rows 9 and 10 each cover three physical types under
/// one row, so they contribute three entries here). `can_cast_types` decides castability from the
/// two `DataType` variants alone, so a representative precision/scale stands in for every concrete
/// decimal the rows describe.
fn supported_relaxation_pairs() -> Vec<(&'static str, DataType, DataType)> {
    vec![
        ("1: int -> long", DataType::Int32, DataType::Int64),
        ("2: float -> double", DataType::Float32, DataType::Float64),
        (
            "3: decimal(P,S) -> decimal(P',S), P' > P",
            DataType::Decimal128(10, 2),
            DataType::Decimal128(20, 2),
        ),
        ("4: byte -> short", DataType::Int8, DataType::Int32),
        ("5: byte -> int", DataType::Int8, DataType::Int32),
        ("6: byte -> long", DataType::Int8, DataType::Int64),
        ("7: short -> int", DataType::Int16, DataType::Int32),
        ("8: short -> long", DataType::Int16, DataType::Int64),
        ("9: byte -> double", DataType::Int8, DataType::Float64),
        ("9: short -> double", DataType::Int16, DataType::Float64),
        ("9: int -> double", DataType::Int32, DataType::Float64),
        (
            "10: byte -> decimal(10+k1,k2)",
            DataType::Int8,
            DataType::Decimal128(11, 1),
        ),
        (
            "10: short -> decimal(10+k1,k2)",
            DataType::Int16,
            DataType::Decimal128(11, 1),
        ),
        (
            "10: int -> decimal(10+k1,k2)",
            DataType::Int32,
            DataType::Decimal128(11, 1),
        ),
        (
            "11: long -> decimal(20+k1,k2)",
            DataType::Int64,
            DataType::Decimal128(21, 1),
        ),
        (
            "12: decimal(p,s) -> decimal(p+k1,s+k2), k1 >= k2 > 0",
            DataType::Decimal128(10, 2),
            DataType::Decimal128(20, 5),
        ),
        (
            "13: date -> timestamp without time zone",
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
    ]
}

// Scenario Coverage (type-relaxation): Every supported relaxation pair is proven castable rather
// than assumed
#[test]
fn arrow_castability_pins_every_supported_relaxation_pair() {
    for (row, physical, logical) in supported_relaxation_pairs() {
        assert!(
            can_cast_types(&physical, &logical),
            "row {row}: expected can_cast_types({physical:?}, {logical:?}) to be true; an \
             arrow-cast upgrade that withdraws this pair must fail here rather than silently \
             re-partitioning the supported set"
        );
    }
}

// Scenario Coverage (type-relaxation): Every supported relaxation pair is proven castable rather
// than assumed
#[test]
fn long_to_double_is_absent_from_the_supported_relaxation_set() {
    assert!(
        can_cast_types(&DataType::Int64, &DataType::Float64),
        "arrow-cast permits Int64 -> Float64 directly, which is exactly why the supported set \
         must exclude the pair by name rather than relying on can_cast_types to gate it"
    );

    let excludes_long_to_double = !supported_relaxation_pairs()
        .into_iter()
        .any(|(_, physical, logical)| physical == DataType::Int64 && logical == DataType::Float64);
    assert!(
        excludes_long_to_double,
        "long -> double is in neither format's promotion/widening rules and must not appear in \
         the supported set"
    );
}

/// One supported-set row read end to end. `expected` is written out by hand rather than derived
/// with `arrow::compute::cast`, so a pair that is castable in principle but wrong in practice fails
/// here instead of agreeing with the kernel under test.
struct RelaxationRead {
    row: &'static str,
    physical: ArrayRef,
    logical_tag: &'static str,
    expected: ArrayRef,
}

fn decimal128(values: Vec<i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("values fit the declared precision and scale"),
    )
}

/// A read case per row of [`supported_relaxation_pairs`], in the same order, carrying the narrow
/// type's boundary values so a cast that truncates or wraps cannot pass.
fn supported_relaxation_reads() -> Vec<RelaxationRead> {
    vec![
        RelaxationRead {
            row: "1: int -> long",
            physical: Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
            logical_tag: "int64",
            expected: Arc::new(Int64Array::from(vec![-2_147_483_648i64, 0, 2_147_483_647])),
        },
        RelaxationRead {
            row: "2: float -> double",
            physical: Arc::new(Float32Array::from(vec![f32::MIN, 0.0, 3.4, f32::MAX])),
            logical_tag: "float64",
            expected: Arc::new(Float64Array::from(vec![
                f64::from(f32::MIN),
                0.0,
                f64::from(3.4f32),
                f64::from(f32::MAX),
            ])),
        },
        RelaxationRead {
            row: "3: decimal(P,S) -> decimal(P',S), P' > P",
            physical: decimal128(vec![-9_999_999_999, 0, 9_999_999_999], 10, 2),
            logical_tag: "decimal128(20,2)",
            expected: decimal128(vec![-9_999_999_999, 0, 9_999_999_999], 20, 2),
        },
        RelaxationRead {
            row: "4: byte -> short",
            physical: Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            logical_tag: "int32",
            expected: Arc::new(Int32Array::from(vec![-128i32, 0, 127])),
        },
        RelaxationRead {
            row: "5: byte -> int",
            physical: Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            logical_tag: "int32",
            expected: Arc::new(Int32Array::from(vec![-128i32, 0, 127])),
        },
        RelaxationRead {
            row: "6: byte -> long",
            physical: Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            logical_tag: "int64",
            expected: Arc::new(Int64Array::from(vec![-128i64, 0, 127])),
        },
        RelaxationRead {
            row: "7: short -> int",
            physical: Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
            logical_tag: "int32",
            expected: Arc::new(Int32Array::from(vec![-32_768i32, 0, 32_767])),
        },
        RelaxationRead {
            row: "8: short -> long",
            physical: Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
            logical_tag: "int64",
            expected: Arc::new(Int64Array::from(vec![-32_768i64, 0, 32_767])),
        },
        RelaxationRead {
            row: "9: byte -> double",
            physical: Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            logical_tag: "float64",
            expected: Arc::new(Float64Array::from(vec![-128.0f64, 0.0, 127.0])),
        },
        RelaxationRead {
            row: "9: short -> double",
            physical: Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
            logical_tag: "float64",
            expected: Arc::new(Float64Array::from(vec![-32_768.0f64, 0.0, 32_767.0])),
        },
        RelaxationRead {
            row: "9: int -> double",
            physical: Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
            logical_tag: "float64",
            expected: Arc::new(Float64Array::from(vec![
                -2_147_483_648.0f64,
                0.0,
                2_147_483_647.0,
            ])),
        },
        RelaxationRead {
            row: "10: byte -> decimal(10+k1,k2)",
            physical: Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            logical_tag: "decimal128(11,1)",
            expected: decimal128(vec![-1_280, 0, 1_270], 11, 1),
        },
        RelaxationRead {
            row: "10: short -> decimal(10+k1,k2)",
            physical: Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
            logical_tag: "decimal128(11,1)",
            expected: decimal128(vec![-327_680, 0, 327_670], 11, 1),
        },
        RelaxationRead {
            row: "10: int -> decimal(10+k1,k2)",
            physical: Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
            logical_tag: "decimal128(11,1)",
            expected: decimal128(vec![-21_474_836_480, 0, 21_474_836_470], 11, 1),
        },
        RelaxationRead {
            row: "11: long -> decimal(20+k1,k2)",
            physical: Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
            logical_tag: "decimal128(21,1)",
            expected: decimal128(
                vec![-92_233_720_368_547_758_080, 0, 92_233_720_368_547_758_070],
                21,
                1,
            ),
        },
        RelaxationRead {
            row: "12: decimal(p,s) -> decimal(p+k1,s+k2), k1 >= k2 > 0",
            physical: decimal128(vec![-9_999_999_999, 0, 9_999_999_999], 10, 2),
            logical_tag: "decimal128(20,5)",
            expected: decimal128(vec![-9_999_999_999_000, 0, 9_999_999_999_000], 20, 5),
        },
        RelaxationRead {
            row: "13: date -> timestamp without time zone",
            physical: Arc::new(Date32Array::from(vec![-1, 0, 19_737])),
            logical_tag: "timestamp_us",
            expected: Arc::new(TimestampMicrosecondArray::from(vec![
                -86_400_000_000i64,
                0,
                1_705_276_800_000_000,
            ])),
        },
    ]
}

fn logical_field(name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: false,
        initial_default: None,
        physical_name: None,
    }
}

fn write_parquet(path: &Path, columns: Vec<(&str, ArrayRef)>) -> String {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, array)| array).collect();
    let file = std::fs::File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("arrow writer");
    let batch = RecordBatch::try_new(schema, arrays).expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

/// The Arrow types a written fixture file actually carries. A Parquet round-trip that silently
/// widened the narrow column would leave the read assertions proving nothing about relaxation.
fn parquet_column_types(path: &Path) -> Vec<DataType> {
    let file = std::fs::File::open(path).expect("open parquet file");
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader");
    builder
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect()
}

/// Drive the exact production scan path — `register_files` then `build_scan_sql` — so the cast under
/// test is the one `FieldIdExprAdapterFactory` delegates to, not one the test performs.
async fn run_scan(spec: &ScanSpec) -> Vec<RecordBatch> {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    register_files(&ctx, "scan_target", spec)
        .await
        .expect("register_files must succeed with a logical schema");
    let sql = build_scan_sql(&ctx, "scan_target", spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    df.collect()
        .await
        .expect("scan must read the assigned files")
}

// Scenario Coverage (type-relaxation): Every supported relaxation pair is proven castable rather
// than assumed
#[tokio::test]
async fn every_supported_relaxation_pair_reads_its_real_values_from_a_narrow_parquet_file() {
    let cases = supported_relaxation_reads();

    let covered: Vec<(&'static str, DataType, DataType)> = cases
        .iter()
        .map(|case| {
            (
                case.row,
                case.physical.data_type().clone(),
                case.expected.data_type().clone(),
            )
        })
        .collect();
    assert_eq!(
        covered,
        supported_relaxation_pairs(),
        "the read cases must stay in lockstep with the castability pins, so a supported-set row \
         added without a real read fails here rather than shipping unread"
    );

    let dir = std::env::temp_dir().join(format!("lh_type_relaxation_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");

    for (index, case) in cases.into_iter().enumerate() {
        let path = dir.join(format!("pair_{index}.parquet"));
        let file_url = write_parquet(&path, vec![("val", Arc::clone(&case.physical))]);
        assert_eq!(
            parquet_column_types(&path),
            vec![case.physical.data_type().clone()],
            "row {}: the fixture file must round-trip at the NARROW physical type, or the read \
             below is an identity read that proves nothing about relaxation",
            case.row
        );

        let mut spec = minimal_spec();
        let file_size = local_file_size(&file_url);
        spec.files = vec![FileEntry::new(file_url, file_size)];
        spec.common.logical_schema = vec![logical_field("val", case.logical_tag)];
        spec.common.projection = vec!["VAL".into()];

        let batches = run_scan(&spec).await;
        let columns: Vec<ArrayRef> = batches
            .iter()
            .map(|batch| Arc::clone(batch.column(0)))
            .collect();
        assert!(
            !columns.is_empty(),
            "row {}: the scan returned no batches",
            case.row
        );
        let column_refs: Vec<&dyn Array> = columns.iter().map(|column| column.as_ref()).collect();
        let got = arrow::compute::concat(&column_refs).expect("concat scan output");

        assert_eq!(
            got.as_ref(),
            case.expected.as_ref(),
            "row {}: a Parquet column written at {:?} and registered under a logical schema \
             declaring {:?} must return its real values at the logical type",
            case.row,
            case.physical.data_type(),
            case.expected.data_type()
        );
    }
}

// Scenario Coverage (type-relaxation): A narrow physical column binds to the current wider logical
// type and is cast per file
#[tokio::test]
async fn a_narrow_physical_column_is_cast_to_the_current_logical_type_per_file() {
    let dir = std::env::temp_dir().join(format!(
        "lh_type_relaxation_per_file_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");

    let pre_widening_path = dir.join("pre_widening_int32.parquet");
    let pre_widening = write_parquet(
        &pre_widening_path,
        vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            ),
            (
                "val",
                Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])) as ArrayRef,
            ),
        ],
    );
    let post_widening = write_parquet(
        &dir.join("post_widening_int64.parquet"),
        vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![4i64, 5, 6])) as ArrayRef,
            ),
            (
                "val",
                Arc::new(Int64Array::from(vec![
                    -2_147_483_649i64,
                    4_294_967_296,
                    i64::MAX,
                ])) as ArrayRef,
            ),
        ],
    );

    assert_eq!(
        parquet_column_types(&pre_widening_path),
        vec![DataType::Int64, DataType::Int32],
        "the pre-widening file must carry the NARROW int32 column, or the two-file read below \
         straddles no widening at all"
    );

    let mut spec = minimal_spec();
    let pre_size = local_file_size(&pre_widening);
    let post_size = local_file_size(&post_widening);
    spec.files = vec![
        FileEntry::new(pre_widening, pre_size),
        FileEntry::new(post_widening, post_size),
    ];
    spec.common.logical_schema = vec![logical_field("id", "int64"), logical_field("val", "int64")];
    spec.common.projection = vec!["ID".into(), "VAL".into()];

    let batches = run_scan(&spec).await;

    let mut got: Vec<(i64, i64)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val must arrive at the logical int64 type for both physical layouts");
        for row in 0..batch.num_rows() {
            assert!(!values.is_null(row), "no widened value may arrive as NULL");
            got.push((ids.value(row), values.value(row)));
        }
    }
    got.sort_by_key(|(id, _)| *id);

    let expected: Vec<(i64, i64)> = vec![
        (1, -2_147_483_648),
        (2, 0),
        (3, 2_147_483_647),
        (4, -2_147_483_649),
        (5, 4_294_967_296),
        (6, i64::MAX),
    ];
    assert_eq!(
        got, expected,
        "the pre-widening int32 file must be cast up per file while the post-widening int64 file \
         keeps values outside the 32-bit range, in one result set"
    );
}
