use super::*;
use crate::adapter::parquet_directory::MergeMode;
use serde_json::json;

#[test]
fn absent_namespace_resolves_base_path_to_the_connection_address_alone() {
    let props = json!({});

    let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect("absent properties must resolve, not error");

    assert_eq!(resolved.base_path, "s3://bucket/lake");
    assert!(resolved.merge_schema, "MERGE_SCHEMA must default to TRUE");
    assert!(
        resolved.hive_partitioning,
        "HIVE_PARTITIONING must default to TRUE"
    );
}

#[test]
fn namespace_joins_the_connection_address_with_exactly_one_slash() {
    let cases = [
        ("s3://bucket/lake/", "finance", "s3://bucket/lake/finance"),
        ("s3://bucket/lake", "finance/", "s3://bucket/lake/finance/"),
        ("s3://bucket/lake", "finance", "s3://bucket/lake/finance"),
    ];

    for (address, namespace, expected) in cases {
        let props = json!({ "NAMESPACE": namespace });
        let resolved = resolve_direct_storage_properties(&props, address)
            .unwrap_or_else(|err| panic!("must resolve for {address} + {namespace}: {err}"));

        assert_eq!(
            resolved.base_path, expected,
            "address={address} namespace={namespace}"
        );
    }
}

#[test]
fn namespace_is_treated_as_slash_delimited_and_never_split_on_dot() {
    let props = json!({ "NAMESPACE": "finance.eu/subdir" });

    let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect("a dotted namespace segment must be accepted verbatim");

    assert_eq!(resolved.base_path, "s3://bucket/lake/finance.eu/subdir");
}

#[test]
fn namespace_carrying_a_uri_scheme_is_rejected() {
    let props = json!({ "NAMESPACE": "s3://other/finance" });

    let err = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect_err("a NAMESPACE carrying a URI scheme must be rejected");
    let msg = err.to_string();

    assert!(msg.contains("NAMESPACE"), "{msg}");
    assert!(msg.contains("s3://other/finance"), "{msg}");
    assert!(msg.to_lowercase().contains("relative"), "{msg}");
}

#[test]
fn namespace_beginning_with_a_leading_slash_is_rejected() {
    let props = json!({ "NAMESPACE": "/finance" });

    let err = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect_err("a NAMESPACE beginning with '/' must be rejected");
    let msg = err.to_string();

    assert!(msg.contains("NAMESPACE"), "{msg}");
    assert!(msg.contains("/finance"), "{msg}");
}

#[test]
fn merge_schema_false_spellings_are_accepted_case_insensitively() {
    for value in ["false", "False", "FALSE"] {
        let props = json!({ "MERGE_SCHEMA": value });
        let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
            .unwrap_or_else(|err| panic!("'{value}' must resolve, got error: {err}"));
        assert!(!resolved.merge_schema, "'{value}' must resolve to FALSE");
    }
}

#[test]
fn merge_schema_true_spellings_are_accepted_case_insensitively() {
    for value in ["true", "True", "TRUE"] {
        let props = json!({ "MERGE_SCHEMA": value });
        let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
            .unwrap_or_else(|err| panic!("'{value}' must resolve, got error: {err}"));
        assert!(resolved.merge_schema, "'{value}' must resolve to TRUE");
    }
}

#[test]
fn unparseable_merge_schema_is_rejected_naming_the_value_and_accepted_spellings() {
    let props = json!({ "MERGE_SCHEMA": "maybe" });

    let err = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect_err("an unparseable MERGE_SCHEMA value must be rejected, not defaulted");
    let msg = err.to_string();

    assert!(msg.contains("MERGE_SCHEMA"), "{msg}");
    assert!(msg.contains("maybe"), "{msg}");
    assert!(msg.contains("TRUE"), "{msg}");
    assert!(msg.contains("FALSE"), "{msg}");
}

#[test]
fn unparseable_hive_partitioning_is_rejected_naming_the_value_and_accepted_spellings() {
    let props = json!({ "HIVE_PARTITIONING": "yes" });

    let err = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect_err("an unparseable HIVE_PARTITIONING value must be rejected, not defaulted");
    let msg = err.to_string();

    assert!(msg.contains("HIVE_PARTITIONING"), "{msg}");
    assert!(msg.contains("yes"), "{msg}");
}

#[test]
fn empty_merge_schema_and_hive_partitioning_resolve_their_defaults() {
    let props = json!({ "MERGE_SCHEMA": "", "HIVE_PARTITIONING": "" });

    let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
        .expect("an empty value must resolve to the default, not be treated as unparseable");

    assert!(resolved.merge_schema);
    assert!(resolved.hive_partitioning);
}

#[test]
fn hive_partitioning_accepts_all_three_inputs_and_defaults_true() {
    let absent = json!({});
    let explicit_true = json!({ "HIVE_PARTITIONING": "TRUE" });
    let explicit_false = json!({ "HIVE_PARTITIONING": "FALSE" });

    let resolved_absent = resolve_direct_storage_properties(&absent, "s3://bucket/lake").unwrap();
    let resolved_true =
        resolve_direct_storage_properties(&explicit_true, "s3://bucket/lake").unwrap();
    let resolved_false =
        resolve_direct_storage_properties(&explicit_false, "s3://bucket/lake").unwrap();

    assert!(resolved_absent.hive_partitioning);
    assert!(resolved_true.hive_partitioning);
    assert!(!resolved_false.hive_partitioning);
}

#[test]
fn join_storage_path_returns_the_base_alone_when_the_segment_is_absent() {
    assert_eq!(
        join_storage_path("s3://bucket/lake", None),
        "s3://bucket/lake"
    );
}

/// A CONNECTION address may end in any number of separators; the join strips them all before
/// appending one, so enumeration and pushdown can't list a table from two different prefixes.
#[test]
fn a_base_path_with_repeated_trailing_separators_joins_to_one_separator() {
    for base in [
        "s3://bucket/lake",
        "s3://bucket/lake/",
        "s3://bucket/lake//",
    ] {
        assert_eq!(
            join_storage_path(base, Some("orders")),
            "s3://bucket/lake/orders",
            "base={base}"
        );
    }
}

#[test]
fn directory_options_derive_both_switches() {
    for (merge_schema, hive_partitioning, expected_mode) in [
        (true, true, MergeMode::FoldEveryFile),
        (false, true, MergeMode::SampleOneFile),
        (true, false, MergeMode::FoldEveryFile),
        (false, false, MergeMode::SampleOneFile),
    ] {
        let props = json!({
            "MERGE_SCHEMA": if merge_schema { "TRUE" } else { "FALSE" },
            "HIVE_PARTITIONING": if hive_partitioning { "TRUE" } else { "FALSE" },
        });
        let resolved = resolve_direct_storage_properties(&props, "s3://bucket/lake")
            .expect("both switches resolve");

        let options = resolved.directory_options();
        assert_eq!(
            options.merge_mode, expected_mode,
            "MERGE_SCHEMA={merge_schema}"
        );
        assert_eq!(
            options.hive_partitioning, hive_partitioning,
            "HIVE_PARTITIONING={hive_partitioning}"
        );
    }
}
