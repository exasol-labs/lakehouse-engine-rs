use super::*;
use crate::scan::type_relaxation::supported_relaxation_pairs;

fn unsupported_pairs() -> Vec<(&'static str, DataType, DataType)> {
    vec![
        ("long -> double", DataType::Int64, DataType::Float64),
        (
            "decimal(12,2) -> decimal(10,5): neither ordering satisfies a row",
            DataType::Decimal128(12, 2),
            DataType::Decimal128(10, 5),
        ),
        (
            "byte -> short renders as Int8 -> Int32, so Int8 -> Int16 is no row",
            DataType::Int8,
            DataType::Int16,
        ),
        ("string and a number", DataType::Utf8, DataType::Int32),
        ("boolean and a number", DataType::Boolean, DataType::Int32),
        (
            "double -> decimal",
            DataType::Float64,
            DataType::Decimal128(20, 2),
        ),
        (
            "date -> timestamp at a unit row 13 does not name",
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
        (
            "mixed timestamp units",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
        (
            "naive and tz-aware timestamps",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ]
}

/// Scenario: The supported pair set answers a plan-time widening question from one production owner
#[test]
fn widening_owner_answers_every_supported_pair_and_refuses_the_rest() {
    for (row, narrow, wide) in supported_relaxation_pairs() {
        assert_eq!(
            widen(&narrow, &wide),
            Some(wide.clone()),
            "row {row}: the supported-set table records this pair, so the owner must resolve \
             {narrow:?} and {wide:?} to {wide:?}"
        );
        assert_eq!(
            widen(&wide, &narrow),
            Some(wide.clone()),
            "row {row}: the answer must not depend on the argument order, because the fold reads \
             footers in listing order and either type can appear first"
        );
    }

    for (pair, left, right) in unsupported_pairs() {
        assert_eq!(
            widen(&left, &right),
            None,
            "{pair}: no row of the supported set covers this pair, so the owner must return no \
             answer rather than a type the scan could not cast a file up to"
        );
        assert_eq!(
            widen(&right, &left),
            None,
            "{pair}: the refusal must not depend on the argument order"
        );
    }
}

/// Scenario: The supported pair set answers a plan-time widening question from one production owner
#[test]
fn two_equal_types_resolve_to_the_shared_type() {
    for shared in [
        DataType::Int32,
        DataType::Utf8,
        DataType::Decimal128(10, 2),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        assert_eq!(
            widen(&shared, &shared),
            Some(shared.clone()),
            "an unevolved column must cost the caller no special case, including for {shared:?}, \
             a type no row of the supported set names"
        );
    }
}

/// Scenario: The supported pair set answers a plan-time widening question from one production owner
#[test]
fn decimal_rows_are_evaluated_on_the_concrete_precision_and_scale() {
    assert_eq!(
        widen(&DataType::Decimal128(10, 2), &DataType::Decimal128(12, 2)),
        Some(DataType::Decimal128(12, 2)),
        "row 3: same scale and a greater precision"
    );
    assert_eq!(
        widen(&DataType::Decimal128(10, 2), &DataType::Decimal128(20, 5)),
        Some(DataType::Decimal128(20, 5)),
        "row 12: the precision grows by at least the scale's growth"
    );
    assert_eq!(
        widen(&DataType::Decimal128(10, 1), &DataType::Decimal128(11, 3)),
        None,
        "row 12 requires k1 >= k2: a scale growing faster than the precision loses integral digits"
    );
    assert_eq!(
        widen(&DataType::Int32, &DataType::Decimal128(11, 1)),
        Some(DataType::Decimal128(11, 1)),
        "row 10: an Int32 source is measured against the decimal(10 + k1, k2) base"
    );
    assert_eq!(
        widen(&DataType::Int32, &DataType::Decimal128(10, 5)),
        None,
        "row 10: five integral digits cannot hold an Int32"
    );
    assert_eq!(
        widen(&DataType::Int64, &DataType::Decimal128(21, 1)),
        Some(DataType::Decimal128(21, 1)),
        "row 11: an Int64 source is measured against the decimal(20 + k1, k2) base"
    );
    assert_eq!(
        widen(&DataType::Int64, &DataType::Decimal128(11, 1)),
        None,
        "row 11: ten integral digits cannot hold an Int64"
    );
}
