use super::*;
use crate::exported_test_support::{date_columns, partition_spec};

/// Scenario: A format-version 3 table is refused before any data is written
#[test]
fn only_a_table_of_format_version_1_or_2_takes_appends() {
    let cases = [
        ("no fields, spec ID 0", FormatVersion::V2, &[][..], 0, None),
        ("no fields, spec ID 1", FormatVersion::V2, &[], 1, None),
        (
            "only void fields",
            FormatVersion::V1,
            &[Transform::Void, Transform::Void],
            1,
            None,
        ),
        (
            "an identity and a day field",
            FormatVersion::V2,
            &[Transform::Identity, Transform::Void, Transform::Day],
            0,
            None,
        ),
        (
            "a bucket and a month field",
            FormatVersion::V1,
            &[Transform::Bucket(4), Transform::Month],
            0,
            None,
        ),
        ("format version 1", FormatVersion::V1, &[], 0, None),
        (
            "format version 3",
            FormatVersion::V3,
            &[],
            0,
            Some(Refusal::FormatVersion(3)),
        ),
    ];

    for (case, version, transforms, spec_id, refusal) in cases {
        let spec = partition_spec(spec_id, transforms);

        let refused = refusal_of(version, &spec, &date_columns(transforms.len())).err();

        assert_eq!(refused, refusal, "{case}");
    }
}

/// Scenario: A void partition field named like a column is refused before any data is written
#[test]
fn a_void_partition_field_named_like_a_column_is_refused_naming_it() {
    // The spec comes from JSON as a catalog serves it, because the partition spec builder refuses
    // a field named like another column.
    let spec = |first: &str, source: i32| -> PartitionSpec {
        serde_json::from_value(serde_json::json!({
            "spec-id": 1,
            "fields": [
                {"name": "c0", "transform": first, "source-id": source, "field-id": 1000},
                {"name": "p1", "transform": "void", "source-id": 2, "field-id": 1001},
            ],
        }))
        .unwrap()
    };
    let refused = |first: &str, source: i32| {
        refusal_of(FormatVersion::V1, &spec(first, source), &date_columns(2)).err()
    };

    assert_eq!(
        refused("void", 1),
        Some(Refusal::FieldsNamedLikeColumns(vec!["c0 (void)".into()]))
    );
    assert_eq!(
        refused("identity", 1),
        None,
        "an identity field is named like its column"
    );
    assert!(
        refused("identity", 2).is_some(),
        "an identity field of another column named like c0"
    );
}
