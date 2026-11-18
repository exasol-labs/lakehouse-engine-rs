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

/// Scenario: A partitioned or format-version 3 table is refused before any data is written
#[test]
fn only_an_unpartitioned_table_of_format_version_1_or_2_takes_appends() {
    let partitioned = "table ns.t is partitioned by p0 (identity), p2 (day); the export appends \
                       only to unpartitioned tables";
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
            Some(partitioned),
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

/// Scenario: A void partition field named like a column is refused before any data is written
#[test]
fn a_void_partition_field_named_like_a_column_is_refused_naming_it() {
    // Spark keeps a dropped version 1 identity field as `void` under its column's name, which the
    // partition spec builder refuses, so the spec comes from JSON as a catalog serves it.
    let spec: PartitionSpec = serde_json::from_value(serde_json::json!({
        "spec-id": 1,
        "fields": [
            {"name": "c0", "transform": "void", "source-id": 1, "field-id": 1000},
            {"name": "p1", "transform": "void", "source-id": 2, "field-id": 1001},
        ],
    }))
    .unwrap();
    let table = TableIdent::from_strs(["ns", "t"]).unwrap();

    let refused = refuse_unappendable(&table, FormatVersion::V1, &spec, &date_columns(2)).err();

    assert_eq!(
        refused.map(|error| error.to_string()),
        Some(
            "LAKEHOUSE_EXPORT: table ns.t keeps the dropped partition field c0 (void) under the \
             name of a column; the export appends only to tables whose partition fields are not \
             named like a column"
                .to_string()
        )
    );
}
