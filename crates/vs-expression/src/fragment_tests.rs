use super::*;

#[test]
fn a_fragment_of_the_declared_type_is_not_cast() {
    let fragment = SqlFragment::new(r#""GK_0""#, Some(ExasolType::Date));
    assert_eq!(fragment.as_type(ExasolType::Date), r#""GK_0""#);
}

#[test]
fn a_fragment_of_another_type_is_cast_to_the_declared_type() {
    let fragment = SqlFragment::new(r#"SUM("PARTIAL_sum_0")"#, Some(ExasolType::decimal(36, 2)));
    assert_eq!(
        fragment.as_type(ExasolType::decimal(29, 2)),
        r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(29,2))"#
    );
}

#[test]
fn an_untyped_fragment_is_always_cast() {
    let fragment = SqlFragment::new("NULL", None);
    assert_eq!(
        fragment.as_type(ExasolType::WIDEST_VARCHAR),
        "CAST(NULL AS VARCHAR(2000000))"
    );
}
