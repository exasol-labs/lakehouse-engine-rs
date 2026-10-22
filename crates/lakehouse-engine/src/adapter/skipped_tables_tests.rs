use super::*;
use lakehouse_catalog::CatalogTableIdent;

fn skipped_entry(table: &str, reason: SkipReason) -> SkippedTable {
    SkippedTable {
        ident: CatalogTableIdent {
            namespace: vec!["sales".to_string()],
            name: table.to_string(),
        },
        reason,
    }
}

fn notes_of(value: Json) -> Map<String, Json> {
    match value {
        Json::Object(notes) => notes,
        other => panic!("notes must be a JSON object: {other}"),
    }
}

/// `count` entries whose identifiers hold `filler`, long enough to overflow the notes limit.
fn oversized_listing(count: usize, filler: &str) -> Vec<SkippedTable> {
    (0..count)
        .map(|i| {
            skipped_entry(
                &format!("{i:0>80}{}", filler.repeat(40)),
                SkipReason::NoDataFile,
            )
        })
        .collect()
}

/// Scenario: A skipped-table list too long for adapterNotes is capped to the longest prefix that fits
#[test]
fn an_oversized_skipped_list_is_capped_to_the_longest_prefix_that_fits_the_exasol_limit() {
    for filler in ["0", "é"] {
        let total = 15_000;
        let skipped = oversized_listing(total, filler);
        let mut notes = notes_of(json!({"PARALLELISM_FACTOR": "8"}));

        insert_skipped_tables(&mut notes, &SkippedTables::listed(&skipped));
        let text = Json::Object(notes.clone()).to_string();
        let parsed = Json::Object(notes);

        let recorded = parsed["SKIPPED_TABLES"].as_array().expect("an array");
        let kept = recorded.len();
        assert!(kept > 0 && kept < total, "{filler}: kept {kept} of {total}");
        assert!(
            text.len() <= ADAPTER_NOTES_MAX_BYTES,
            "{filler}: {} bytes",
            text.len()
        );
        assert_eq!(
            parsed["SKIPPED_TABLES_OMITTED"],
            json!((total - kept).to_string())
        );
        for (entry, source) in recorded.iter().zip(&skipped) {
            assert_eq!(entry["table"], catalog_identifier_string(&source.ident));
        }
        let mut one_more = parsed.clone();
        one_more["SKIPPED_TABLES"]
            .as_array_mut()
            .expect("an array")
            .push(json!({
                "table": catalog_identifier_string(&skipped[kept].ident),
                "reason": skip_kind_and_reason(&skipped[kept]).1,
            }));
        let still_omitted = total - kept - 1;
        if still_omitted == 0 {
            one_more
                .as_object_mut()
                .expect("an object")
                .remove("SKIPPED_TABLES_OMITTED");
        } else {
            one_more["SKIPPED_TABLES_OMITTED"] = json!(still_omitted.to_string());
        }
        let with_one_more = one_more.to_string().len();
        assert!(
            with_one_more > ADAPTER_NOTES_MAX_BYTES,
            "{filler}: one more entry would still fit ({with_one_more} bytes)"
        );
    }
}

/// Scenario: A skipped-table list too long for adapterNotes is capped to the longest prefix that fits
#[test]
fn a_skipped_list_that_fits_keeps_every_entry_and_drops_a_stale_omitted_count() {
    let mut notes = notes_of(json!({ "SKIPPED_TABLES_OMITTED": "7" }));
    let skipped = vec![skipped_entry("a", SkipReason::NoDataFile)];

    insert_skipped_tables(&mut notes, &SkippedTables::listed(&skipped));

    assert_eq!(notes["SKIPPED_TABLES"].as_array().unwrap().len(), 1);
    assert!(notes.get("SKIPPED_TABLES_OMITTED").is_none());
}

#[test]
fn a_carried_omitted_count_counts_toward_the_cap_and_adds_to_its_drops() {
    let carried = 9_999_999_999;
    let skipped = SkippedTables {
        omitted: carried,
        ..SkippedTables::listed(&[
            skipped_entry("a", SkipReason::NoDataFile),
            skipped_entry("b", SkipReason::NoDataFile),
        ])
    };
    let both_entries_len = skipped
        .entries
        .iter()
        .map(|entry| entry.to_json().to_string().len())
        .sum::<usize>()
        + ",".len();
    let carried_count_len = format!(",\"SKIPPED_TABLES_OMITTED\":\"{carried}\"").len();
    let empty_notes_len = json!({"F": "", "SKIPPED_TABLES": []}).to_string().len();
    // Both entries miss the limit by one byte once the carried count is counted.
    let filler = "x".repeat(
        ADAPTER_NOTES_MAX_BYTES - empty_notes_len - both_entries_len - carried_count_len + 1,
    );
    let mut notes = notes_of(json!({ "F": filler }));

    insert_skipped_tables(&mut notes, &skipped);

    assert_eq!(
        notes["SKIPPED_TABLES"].as_array().expect("an array").len(),
        1
    );
    assert_eq!(
        notes["SKIPPED_TABLES_OMITTED"],
        json!((carried + 1).to_string())
    );
    let text_len = Json::Object(notes).to_string().len();
    assert!(text_len <= ADAPTER_NOTES_MAX_BYTES, "{text_len} bytes");
}

#[test]
fn the_persisted_list_reads_back_the_entries_and_count_a_response_recorded() {
    let skipped = vec![
        skipped_entry("hive_events", SkipReason::NotLoadableIcebergTable),
        skipped_entry(
            "summary",
            SkipReason::NotDeltaBaseTable {
                detail: "table_type=VIEW".to_string(),
            },
        ),
    ];
    let recorded = SkippedTables {
        omitted: 4,
        ..SkippedTables::listed(&skipped)
    };
    let mut notes = notes_of(json!({"PARALLELISM_FACTOR": "8"}));
    insert_skipped_tables(&mut notes, &recorded);

    assert_eq!(SkippedTables::persisted(&notes), recorded);
    assert_eq!(
        recorded.entries,
        vec![
            SkipEntry {
                table: "sales.hive_events".to_string(),
                reason: "catalog reported it is not a loadable Iceberg table".to_string(),
            },
            SkipEntry {
                table: "sales.summary".to_string(),
                reason: "table_type=VIEW".to_string(),
            },
        ]
    );
}

#[test]
fn notes_without_a_skipped_list_read_as_an_empty_list_with_no_omission() {
    let notes = notes_of(json!({"PARALLELISM_FACTOR": "8"}));

    assert_eq!(SkippedTables::persisted(&notes), SkippedTables::default());
}

#[test]
fn a_malformed_persisted_entry_or_count_reads_as_absent() {
    let notes = notes_of(json!({
        "SKIPPED_TABLES": [
            {"table": "sales.kept", "reason": "holds no data file"},
            {"table": "sales.no_reason"},
            {"reason": "no table"},
            "not an object",
        ],
        "SKIPPED_TABLES_OMITTED": "many",
    }));

    assert_eq!(
        SkippedTables::persisted(&notes),
        SkippedTables {
            entries: vec![SkipEntry {
                table: "sales.kept".to_string(),
                reason: "holds no data file".to_string(),
            }],
            omitted: 0,
        }
    );
}

#[test]
fn a_skipped_list_that_is_not_an_array_reads_as_empty() {
    let notes = notes_of(json!({"SKIPPED_TABLES": "sales.t", "SKIPPED_TABLES_OMITTED": "2"}));

    assert_eq!(
        SkippedTables::persisted(&notes),
        SkippedTables {
            entries: Vec::new(),
            omitted: 2,
        }
    );
}
