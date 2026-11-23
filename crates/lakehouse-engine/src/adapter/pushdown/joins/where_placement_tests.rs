use super::super::super::test_support::*;
use super::*;
use crate::types::exasol_type::ExasolType;

/// A placement whose one scan applies `sql` and whose wrapper applies nothing, for a golden that
/// renders a scan filter its fixture does not carry.
pub(in super::super::super) fn scan_applies(sql: Option<String>) -> WherePlacement {
    WherePlacement::one_scan(sql, None)
}

fn id_and_amount() -> crate::adapter::pushdown::ColumnTypes {
    column_types(&[
        ("ID", ExasolType::decimal(20, 0)),
        ("AMOUNT", ExasolType::decimal(18, 2)),
    ])
}

#[test]
fn a_single_scan_applies_a_filter_that_renders_and_the_wrapper_one_that_declines() {
    let renders = compare("predicate_greater", col("ID"), num_lit(5));
    let declines = like(col("AMOUNT"), "1%");
    let columns = id_and_amount();
    let cases = [
        ("absent", None, (false, None)),
        ("pushed", Some(renders.clone()), (true, None)),
        (
            "declined",
            Some(declines.clone()),
            (false, Some(declines.clone())),
        ),
        (
            "declined OR",
            Some(or([renders.clone(), declines.clone()])),
            (false, Some(or([renders, declines]))),
        ),
    ];
    for (case, filter, (scan_filters, wrapper)) in cases {
        let request = match &filter {
            Some(filter) => filter_only_request(filter),
            None => filter_only_request(&Json::Null),
        };
        let placement = WherePlacement::single_scan(scan_over(&columns), &request);
        assert_eq!(placement.scan_filter(0).is_some(), scan_filters, "{case}");
        assert_eq!(placement.wrapper(), wrapper.as_ref(), "{case}");
    }
}

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
                .filter(&id_above_five)
                .expect("the bare id comparison renders")
        ),
    );
    assert_eq!(
        placement.wrapper(),
        Some(&and([second, length])),
        "the declined half must hold the declined conjuncts as Exasol sent them, not their scan \
         rewrites"
    );
}

#[test]
fn the_whole_placement_leaves_every_conjunct_to_the_wrapper() {
    let filter = and([
        compare("predicate_greater", col("ID"), num_lit(5)),
        like(col("AMOUNT"), "1%"),
    ]);
    let placement = WherePlacement::whole(&filter_only_request(&filter));
    assert_eq!(placement.scan_filter(0), None);
    assert_eq!(placement.wrapper(), Some(&filter));
}
