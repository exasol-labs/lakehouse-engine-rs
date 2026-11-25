use super::super::super::test_support::*;
use super::*;
use crate::types::exasol_type::ExasolType;

#[test]
fn a_split_keeps_each_declined_conjunct_as_exasol_sent_it() {
    // `SECOND(ts, 3)` is a DataFusion arity decline Exasol renders; `LENGTH(amount) > 5`
    // is rewritten, so the equality assertion distinguishes original from rewritten.
    let id_above_five = compare("predicate_greater", col("id"), num_lit(5));
    let second = declined_second_predicate(col("ts"));
    let length = compare(
        "predicate_greater",
        scalar_fn("LENGTH", [col("amount")]),
        num_lit(5),
    );
    let filter = and([id_above_five.clone(), second.clone(), length.clone()]);
    let col_types = column_types(&[
        ("ID", ExasolType::decimal(20, 0)),
        ("TS", ExasolType::BARE_TIMESTAMP),
        ("AMOUNT", ExasolType::decimal(18, 2)),
    ]);
    let placement =
        WherePlacement::single_scan(scan_over(&col_types), &filter_only_request(&filter));

    assert_eq!(
        placement.scan_filter(0),
        Some(
            scan_over(&col_types)
                .filter(&ast(&id_above_five))
                .expect("the bare id comparison renders")
        ),
    );
    assert_eq!(
        placement.wrapper(),
        Some(&ast(&and([second, length]))),
        "the declined half must hold the declined conjuncts as Exasol sent them, not their scan \
         rewrites"
    );
}
