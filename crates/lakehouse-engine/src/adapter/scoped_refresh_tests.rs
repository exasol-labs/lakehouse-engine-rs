use super::*;
use crate::adapter::skipped_tables::{SkipEntry, insert_skipped_tables};
use lakehouse_catalog::SkipReason;
use serde_json::{Map, json};

fn ident(namespace: &[&str], name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: namespace
            .iter()
            .map(|segment| segment.to_string())
            .collect(),
        name: name.to_string(),
    }
}

fn requested(names: &[&str]) -> RequestedTables {
    RequestedTables::from_request(&json!({"type": "refresh", "requestedTables": names}))
        .expect("a well-formed requestedTables")
        .expect("a refresh that carries requestedTables")
}

fn entry(table: &str, reason: &str) -> SkipEntry {
    SkipEntry {
        table: table.to_string(),
        reason: reason.to_string(),
    }
}

fn no_data_file(namespace: &[&str], name: &str) -> SkippedTable {
    SkippedTable {
        ident: ident(namespace, name),
        reason: SkipReason::NoDataFile,
    }
}

/// Scenario: A partial refresh loads the metadata of the requested tables only
#[test]
fn only_requested_identifiers_are_selected_for_loading() {
    let configured_ns = ["sales".to_string()];
    let listed = [
        ident(&["sales"], "orders"),
        ident(&["sales"], "payments"),
        ident(&["sales"], "refunds"),
    ];
    let requested = requested(&["REFUNDS", "ORDERS"]);

    let selection = ScopedRefresh::new(&requested, &configured_ns, &listed).selection();

    assert_eq!(
        selection,
        vec![ident(&["sales"], "orders"), ident(&["sales"], "refunds")]
    );
}

#[test]
fn an_empty_requested_tables_array_is_a_partial_refresh_that_selects_nothing() {
    let requested = requested(&[]);

    let selection = ScopedRefresh::new(&requested, &[], &[ident(&[], "orders")]).selection();

    assert!(selection.is_empty());
}

/// Scenario: A partial refresh resolves each requested name as a full refresh would
#[test]
fn every_identifier_that_flattens_to_a_requested_name_is_selected() {
    let configured_ns = ["sales".to_string()];
    let listed = [
        ident(&["sales"], "Orders"),
        ident(&["sales"], "eu__orders"),
        ident(&["sales"], "other"),
        ident(&["sales", "eu"], "orders"),
        ident(&["sales"], "orders"),
    ];
    let requested = requested(&["ORDERS", "EU__ORDERS"]);

    let selection = ScopedRefresh::new(&requested, &configured_ns, &listed).selection();

    assert_eq!(
        selection,
        vec![
            ident(&["sales"], "Orders"),
            ident(&["sales"], "eu__orders"),
            ident(&["sales", "eu"], "orders"),
            ident(&["sales"], "orders"),
        ]
    );
}

#[test]
fn a_requested_name_matching_no_identifier_selects_nothing() {
    let listed = [ident(&[], "orders")];
    let requested = requested(&["NO_SUCH_TABLE", "orders"]);

    let selection = ScopedRefresh::new(&requested, &[], &listed).selection();

    assert!(selection.is_empty(), "selected {selection:?}");
}

#[test]
fn a_request_without_requested_tables_is_no_partial_refresh() {
    for request in [
        json!({"type": "refresh"}),
        json!({"type": "setProperties", "requestedTables": ["T"]}),
        json!({"type": "createVirtualSchema"}),
    ] {
        assert!(
            RequestedTables::from_request(&request)
                .expect("a well-formed request")
                .is_none(),
            "{request} must refresh everything"
        );
    }
}

#[test]
fn a_malformed_requested_tables_value_fails_naming_it() {
    for value in [json!("T1"), json!(["T1", 2])] {
        let request = json!({"type": "refresh", "requestedTables": value});

        let error = RequestedTables::from_request(&request)
            .expect_err("a malformed requestedTables must fail")
            .to_string();

        assert!(
            error.contains("requestedTables") && error.contains(&value.to_string()),
            "{error}"
        );
    }
}

#[test]
fn a_requested_name_matches_only_the_exact_exasol_name() {
    let requested = requested(&["T2", "Mixed"]);

    assert!(requested.contains("T2"));
    assert!(requested.contains("Mixed"));
    assert!(!requested.contains("t2"));
    assert!(!requested.contains("MIXED"));
}

/// Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list
#[test]
fn merge_keeps_unrequested_listed_entries_in_listing_order() {
    let listed = [
        ident(&[], "a_unrequested"),
        ident(&[], "b_now_loads"),
        ident(&[], "c_still_skipped"),
        ident(&[], "d_newly_broken"),
        ident(&[], "e_unrequested"),
    ];
    let requested = requested(&["B_NOW_LOADS", "C_STILL_SKIPPED"]);
    let persisted = SkippedTables {
        entries: vec![
            entry("e_unrequested", "holds no data file"),
            entry("c_still_skipped", "an earlier reason"),
            entry("b_now_loads", "holds no data file"),
            entry("a_unrequested", "table_type=VIEW"),
        ],
        omitted: 0,
    };
    let loaded = [no_data_file(&[], "c_still_skipped")];

    let merged =
        ScopedRefresh::new(&requested, &[], &listed).merge_skipped_tables(&loaded, &persisted);

    assert_eq!(
        merged,
        SkippedTables {
            entries: vec![
                entry("a_unrequested", "table_type=VIEW"),
                entry("c_still_skipped", "holds no data file"),
                entry("e_unrequested", "holds no data file"),
            ],
            omitted: 0,
        }
    );
}

/// Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list
#[test]
fn merge_drops_entries_the_source_no_longer_lists() {
    let configured_ns = ["sales".to_string()];
    let listed = [
        ident(&["sales"], "unrequested"),
        ident(&["sales"], "still_skipped"),
    ];
    let requested = requested(&["STILL_SKIPPED", "DROPPED_A"]);
    let persisted = SkippedTables {
        entries: vec![
            entry("sales.dropped_a", "holds no data file"),
            entry("sales.unrequested", "holds no data file"),
            entry("sales.dropped_b", "holds no data file"),
            entry("sales.still_skipped", "holds no data file"),
        ],
        omitted: 0,
    };
    let loaded = [no_data_file(&["sales"], "still_skipped")];

    let merged = ScopedRefresh::new(&requested, &configured_ns, &listed)
        .merge_skipped_tables(&loaded, &persisted);

    assert_eq!(
        merged.entries,
        vec![
            entry("sales.unrequested", "holds no data file"),
            entry("sales.still_skipped", "holds no data file"),
        ]
    );
}

/// Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list
#[test]
fn merge_carries_the_persisted_omitted_count() {
    let listed = [ident(&[], "unrequested"), ident(&[], "requested")];
    let requested = requested(&["REQUESTED"]);
    let mut notes = Map::new();
    insert_skipped_tables(
        &mut notes,
        &SkippedTables {
            entries: vec![entry("unrequested", "holds no data file")],
            omitted: 3,
        },
    );
    let loaded = [no_data_file(&[], "requested")];

    let merged = ScopedRefresh::new(&requested, &[], &listed)
        .merge_skipped_tables(&loaded, &SkippedTables::persisted(&notes));
    insert_skipped_tables(&mut notes, &merged);

    assert_eq!(notes["SKIPPED_TABLES_OMITTED"], json!("3"));
    assert_eq!(
        notes["SKIPPED_TABLES"],
        json!([
            {"table": "unrequested", "reason": "holds no data file"},
            {"table": "requested", "reason": "holds no data file"},
        ])
    );
}

/// Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list
#[test]
fn merge_writes_no_omitted_count_when_the_sum_is_zero() {
    let listed = [ident(&[], "unrequested"), ident(&[], "requested")];
    let requested = requested(&["REQUESTED"]);
    let mut notes = Map::new();
    insert_skipped_tables(
        &mut notes,
        &SkippedTables {
            entries: vec![entry("unrequested", "holds no data file")],
            omitted: 0,
        },
    );

    let merged = ScopedRefresh::new(&requested, &[], &listed)
        .merge_skipped_tables(&[], &SkippedTables::persisted(&notes));
    insert_skipped_tables(&mut notes, &merged);

    assert!(
        notes.get("SKIPPED_TABLES_OMITTED").is_none(),
        "no count when nothing was ever omitted: {notes:?}"
    );
    assert_eq!(
        notes["SKIPPED_TABLES"],
        json!([{"table": "unrequested", "reason": "holds no data file"}])
    );
}
