use super::*;
use crate::export::test_support::{date_columns, partition_spec, static_key_connection};
use iceberg::spec::Transform;

fn connection() -> ExportConnection {
    static_key_connection("https://catalog.example", false)
}

#[test]
fn a_table_name_without_a_namespace_is_refused_naming_it() {
    let refused = target(connection(), "orders").err().expect("a refusal");

    assert!(
        refused.to_string().contains("TABLE_NAME = 'orders'"),
        "{refused}"
    );
}

#[test]
fn a_table_name_with_a_namespace_of_several_levels_opens_a_target() {
    assert!(target(connection(), "sales.eu.orders").is_ok());
}

/// Scenario: A format-version 3 table is refused before any data is written
#[test]
fn only_a_table_of_format_version_1_or_2_takes_appends() {
    let version_3 = "table ns.t has format version 3; the export appends only to tables of \
                     format version 1 or 2";
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
            Some(version_3),
        ),
    ];
    let table = TableIdent::from_strs(["ns", "t"]).unwrap();

    for (case, version, transforms, spec_id, refusal) in cases {
        let spec = partition_spec(spec_id, transforms);

        let refused =
            refuse_unappendable(&table, version, &spec, &date_columns(transforms.len())).err();

        assert_eq!(
            refused.map(|error| error.to_string()),
            refusal.map(|text| format!("LAKEHOUSE_EXPORT: {text}")),
            "{case}"
        );
    }
}

#[test]
fn a_partition_field_named_like_a_column_is_refused_unless_it_is_its_identity_or_void() {
    // The spec comes from JSON as a catalog serves it, because the partition spec builder refuses
    // a field named like a column that is not its identity or void.
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
    let table = TableIdent::from_strs(["ns", "t"]).unwrap();
    let refused = |first: &str, source: i32| {
        refuse_unappendable(
            &table,
            FormatVersion::V1,
            &spec(first, source),
            &date_columns(2),
        )
        .err()
        .map(|error| error.to_string())
    };

    assert_eq!(
        refused("bucket[4]", 1),
        Some(
            "LAKEHOUSE_EXPORT: table ns.t has the partition field c0 (bucket[4]) named like a \
             column; the export appends only to tables whose partition fields are named like a \
             column only as that column's identity or void field"
                .to_string()
        )
    );
    for (transform, why) in [
        ("identity", "an identity field is named like its column"),
        (
            "void",
            "Spark keeps a dropped version 1 identity field as void under its name",
        ),
    ] {
        assert_eq!(refused(transform, 1), None, "{why}");
        assert!(
            refused(transform, 2).is_some(),
            "a {transform} field of another column named like c0"
        );
    }
}
