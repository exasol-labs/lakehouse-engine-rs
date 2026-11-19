use super::*;
use crate::export::batch::{SourceColumns, arrow_schema};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use vs_expression::udf_column_test_support::measured_column;

fn names(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|name| name.to_string()).collect()
}

fn field(transform: Transform, column: &str, name: &str) -> PartitionField {
    PartitionField {
        transform,
        column: column.to_string(),
        name: name.to_string(),
    }
}

fn source(columns: &[(&str, &str)]) -> SourceColumns {
    SourceColumns {
        names: columns.iter().map(|(name, _)| name.to_string()).collect(),
        columns: columns
            .iter()
            .map(|(_, typ)| measured_column(typ))
            .collect(),
    }
}

#[test]
fn a_valid_partition_by_resolves_to_named_fields_in_order() {
    let source = names(&["ID", "Region", "TS", "D", "my col", "q\"t", "code", "CODE"]);
    let cases = [
        ("ID", vec![field(Transform::Identity, "ID", "ID")]),
        (
            " bucket ( 16 , id ) ,TRUNCATE(4,Region) ",
            vec![
                field(Transform::Bucket(16), "ID", "ID_bucket"),
                field(Transform::Truncate(4), "Region", "Region_trunc"),
            ],
        ),
        (
            "Year(TS), month(D), DAY(\"my col\")",
            vec![
                field(Transform::Year, "TS", "TS_year"),
                field(Transform::Month, "D", "D_month"),
                field(Transform::Day, "my col", "my col_day"),
            ],
        ),
        ("hour(ts)", vec![field(Transform::Hour, "TS", "TS_hour")]),
        (
            "\"my col\", bucket(2, \"q\"\"t\")",
            vec![
                field(Transform::Identity, "my col", "my col"),
                field(Transform::Bucket(2), "q\"t", "q\"t_bucket"),
            ],
        ),
        // An exact match wins over the letter-case matches it has.
        ("code", vec![field(Transform::Identity, "code", "code")]),
    ];

    for (value, expected) in cases {
        assert_eq!(parse(value, &source).unwrap(), expected, "{value}");
    }
}

/// Scenario: An invalid PARTITION_BY fails the export before any data is read
#[test]
fn an_invalid_partition_by_is_refused_naming_the_field_and_the_form() {
    let cases = [
        (
            "",
            &["ID"][..],
            "PARTITION_BY = '' lists no partition field",
        ),
        (" ", &["ID"], "PARTITION_BY = ' ' lists no partition field"),
        (
            "ID,",
            &["ID"],
            "PARTITION_BY field '' is not a partition field",
        ),
        (
            "bucket(16 ID)",
            &["ID"],
            "PARTITION_BY field 'bucket(16 ID)' is not a partition field",
        ),
        (
            "ID)",
            &["ID"],
            "PARTITION_BY field 'ID)' is not a partition field",
        ),
        (
            "\"ID",
            &["ID"],
            "PARTITION_BY field '\"ID' is not a partition field",
        ),
        (
            "\"ID\"(1)",
            &["ID"],
            "PARTITION_BY field '\"ID\"(1)' is not a partition field",
        ),
        (
            "truncate(x, ID)",
            &["ID"],
            "PARTITION_BY field 'truncate(x, ID)' is not a partition field",
        ),
        (
            "ID, zorder(ID)",
            &["ID"],
            "PARTITION_BY field 'zorder(ID)' names the unknown transform zorder",
        ),
        (
            "bucket(0, ID)",
            &["ID"],
            "PARTITION_BY field 'bucket(0, ID)' has width 0, which is below 1",
        ),
        (
            "NOPE",
            &["ID", "D"],
            "PARTITION_BY field 'NOPE' names column NOPE, which the export source lacks; the \
             source columns are ID, D",
        ),
        (
            "day(code)",
            &["CODE", "Code"],
            "PARTITION_BY field 'day(code)' names column code, which matches the source columns \
             CODE and Code only by letter case",
        ),
        (
            "ID, id",
            &["ID"],
            "PARTITION_BY field 'id' has the name ID of an earlier field",
        ),
        (
            "bucket(4, ID), bucket(8, ID)",
            &["ID"],
            "PARTITION_BY field 'bucket(8, ID)' has the name ID_bucket of an earlier field",
        ),
        (
            "bucket(4, ID)",
            &["ID", "ID_bucket"],
            "PARTITION_BY field 'bucket(4, ID)' has the name ID_bucket of another source column",
        ),
        (
            "year(TS), day(TS)",
            &["TS"],
            "PARTITION_BY field 'day(TS)' puts a second one of year, month, day, and hour on \
             column TS",
        ),
    ];

    for (value, columns, expected) in cases {
        let message = parse(value, &names(columns))
            .expect_err("an invalid PARTITION_BY")
            .to_string();
        assert!(
            message.starts_with(&format!("LAKEHOUSE_EXPORT: {expected}"))
                && message.ends_with(FORM),
            "{value}: {message}"
        );
    }
}

/// Scenario: A transform that the column's type does not take fails the export before any data is written
#[test]
fn each_transform_takes_only_its_source_types() {
    let types = [
        "DECIMAL(18,0)",
        "DECIMAL(12,2)",
        "DOUBLE",
        "BOOLEAN",
        "VARCHAR(10) UTF8",
        "CHAR(3) UTF8",
        "DATE",
        "TIMESTAMP(3)",
    ];
    let dates = vec!["DATE", "TIMESTAMP(3)"];
    let cases: [(Transform, Vec<&str>); 7] = [
        (Transform::Identity, types.to_vec()),
        (
            Transform::Bucket(4),
            vec![
                "DECIMAL(18,0)",
                "DECIMAL(12,2)",
                "VARCHAR(10) UTF8",
                "CHAR(3) UTF8",
                "DATE",
                "TIMESTAMP(3)",
            ],
        ),
        (
            Transform::Truncate(4),
            vec![
                "DECIMAL(18,0)",
                "DECIMAL(12,2)",
                "VARCHAR(10) UTF8",
                "CHAR(3) UTF8",
            ],
        ),
        (Transform::Year, dates.clone()),
        (Transform::Month, dates.clone()),
        (Transform::Day, dates),
        (Transform::Hour, vec!["TIMESTAMP(3)"]),
    ];

    for (transform, expected) in cases {
        let accepted: Vec<&str> = types
            .into_iter()
            .filter(|typ| {
                let fields = [field(transform, "C", "P")];
                check_types(&fields, &source(&[("C", typ)])).is_ok()
            })
            .collect();
        assert_eq!(accepted, expected, "{transform}");
    }
}

#[test]
fn a_type_mismatch_is_refused_naming_the_field_its_column_and_its_type() {
    let fields = [
        field(Transform::Day, "D", "D_day"),
        field(Transform::Hour, "D", "D_hour"),
    ];

    let message = check_types(&fields, &source(&[("ID", "DECIMAL(9,0)"), ("D", "DATE")]))
        .expect_err("hour of a DATE")
        .to_string();

    assert_eq!(
        message,
        "LAKEHOUSE_EXPORT: PARTITION_BY field D_hour applies hour to column D of type DATE, \
         which hour does not take; hour takes TIMESTAMP"
    );
}

#[test]
fn the_unbound_spec_holds_the_fields_in_order_over_the_schema_field_ids() {
    let source = source(&[("ID", "DECIMAL(18,0)"), ("TS", "TIMESTAMP(6)")]);
    let arrow = arrow_schema(&source.names, &source.columns).unwrap();
    let schema = arrow_schema_to_schema_auto_assign_ids(&arrow).unwrap();
    let fields = [
        field(Transform::Hour, "TS", "TS_hour"),
        field(Transform::Identity, "ID", "ID"),
    ];

    let spec = unbound_spec(&fields, &schema).unwrap();

    let shape: Vec<(&[i32], &str, Transform)> = spec
        .fields()
        .iter()
        .map(|field| (field.source_ids(), field.name(), field.transform()))
        .collect();
    assert_eq!(
        shape,
        [
            (&[2][..], "TS_hour", Transform::Hour),
            (&[1][..], "ID", Transform::Identity)
        ]
    );
}
