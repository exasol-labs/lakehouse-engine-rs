use super::*;
use crate::export::test_support::{date_columns, partition_spec};
use arrow::array::{ArrayRef, Int32Array, StringArray};
use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Struct, Transform, Type};
use std::collections::HashMap;
use std::sync::Arc;

const MIB: u64 = 1024 * 1024;
const MB_200: u64 = 200 * MIB;

#[test]
fn the_budget_is_half_the_limit_after_overhead_within_64_and_512_mib() {
    let cases = [
        ("unknown", 0, 512 * MIB),
        ("small", 300 * MIB, 64 * MIB),
        ("below the overhead", 100 * MIB, 64 * MIB),
        ("known", MB_200 + 600 * MIB, 300 * MIB),
        ("large", 8 * 1024 * MIB, 512 * MIB),
    ];

    for (case, limit, expected) in cases {
        assert_eq!(buffer_budget(limit) as u64, expected, "{case}");
    }
}

#[test]
fn the_largest_partitions_go_out_first_until_the_rest_fit() {
    let cases = [
        ("within the budget", vec![40, 30, 30], 100, vec![]),
        ("one over", vec![40, 50, 30], 100, vec![1]),
        ("several over", vec![40, 50, 30, 45], 100, vec![1, 3]),
        ("a single partition", vec![150], 100, vec![0]),
        ("exactly the budget", vec![60, 40], 100, vec![]),
    ];

    for (case, sizes, budget, expected) in cases {
        assert_eq!(write_out(&sizes, budget), expected, "{case}");
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_fields([
                NestedField::optional(1, "K", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "V", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn batch(keys: &[Option<i32>]) -> RecordBatch {
    let arrow = iceberg::arrow::schema_to_arrow_schema(&schema()).unwrap();
    let values: Vec<String> = keys.iter().map(|_| "x".repeat(100)).collect();
    RecordBatch::try_new(
        Arc::new(arrow),
        vec![
            Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// The row count of each written partition, keyed by its value of `K`.
fn rows_by_key(written: &[(PartitionKey, Vec<RecordBatch>)]) -> HashMap<Option<i32>, usize> {
    written
        .iter()
        .map(|(key, batches)| {
            let value = match key.data().fields().first() {
                Some(Some(Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(v)))) => Some(*v),
                _ => None,
            };
            (value, batches.iter().map(RecordBatch::num_rows).sum())
        })
        .collect()
}

#[test]
fn rows_wait_per_partition_until_the_budget_writes_the_largest_one_out() {
    let spec = PartitionSpec::builder(schema())
        .add_partition_field("K", "K", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let one_row = batch(&[Some(1)]).get_array_memory_size();
    let mut buffer = PartitionBuffer::new(schema(), &spec, one_row * 3).unwrap();

    let first = buffer.push(&batch(&[Some(1), Some(2), Some(1)])).unwrap();
    let second = buffer.push(&batch(&[Some(1), None, Some(1)])).unwrap();
    let rest = buffer.drain();

    assert!(first.is_empty());
    assert_eq!(rows_by_key(&second), HashMap::from([(Some(1), 4)]));
    assert_eq!(rows_by_key(&rest), HashMap::from([(Some(2), 1), (None, 1)]));
}

/// Writers use the table's default spec, and iceberg-rust's fast append refuses a data file whose
/// partition arity differs from its spec's.
#[test]
fn every_data_file_carries_the_default_spec_and_a_null_per_void_field() {
    let cases = [
        ("no fields, spec ID 0", 0, &[][..]),
        ("no fields, spec ID 1", 1, &[]),
        ("only void fields", 1, &[Transform::Void, Transform::Void]),
    ];

    for (case, spec_id, transforms) in cases {
        let key = null_key(
            &partition_spec(spec_id, transforms),
            date_columns(transforms.len()),
        );

        assert_eq!(key.spec().spec_id(), spec_id, "{case}");
        assert_eq!(
            key.data(),
            &transforms.iter().map(|_| None).collect::<Struct>(),
            "{case}"
        );
    }
}
