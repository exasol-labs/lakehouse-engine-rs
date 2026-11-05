use super::super::test_support::{
    SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, declared_columns, glue_catalog_table, nullable_field,
    user_message,
};
use super::*;
use lakehouse_catalog::TableFormat;

#[test]
fn declare_columns_notes_each_catalog_parquet_column_with_its_partition_position_and_refusal() {
    let table = CatalogTable {
        partition_columns: vec!["year".into(), "region".into()],
        ..glue_catalog_table(
            "sales",
            "orders",
            TableFormat::Parquet,
            "s3://bucket/sales/orders",
            &[
                ("id", "bigint"),
                ("region", "string"),
                ("year", "int"),
                ("u", "uniontype<int>"),
            ],
        )
    };

    let declarations = declared_columns(&table);

    let declared: Vec<(&str, Option<usize>, bool)> = declarations
        .iter()
        .map(|declaration| {
            let field = &declaration.source_type;
            (
                field.name.as_str(),
                declaration.partition,
                declaration.refused.is_some(),
            )
        })
        .collect();
    assert_eq!(
        declared,
        vec![
            ("id", None, false),
            ("region", Some(1), false),
            ("year", Some(0), false),
            ("u", None, true),
        ]
    );
    assert!(
        declarations
            .iter()
            .all(|declaration| declaration.source_type.nullable)
    );
}

/// Scenario: A binary partition value travels as lowercase hex
#[test]
fn a_delta_or_hive_binary_partition_value_travels_as_the_hex_of_its_utf8_bytes() {
    let logical_schema = [nullable_field("b", "binary"), nullable_field("s", "utf8")];
    let mut files = vec![FileEntry::with_partition_values(
        "part-0.parquet",
        1,
        [
            ("b".to_string(), Some("\u{1}é".to_string())),
            ("s".to_string(), Some("ab".to_string())),
        ]
        .into_iter()
        .collect(),
    )];
    let mut null_file = files[0].clone();
    null_file.partition_values.insert("b".to_string(), None);
    files.push(null_file);

    hex_binary_partition_values(&mut files, &logical_schema);

    assert_eq!(files[0].partition_values["b"].as_deref(), Some("01c3a9"));
    assert_eq!(
        files[0].partition_values["s"].as_deref(),
        Some("ab"),
        "only a binary column's value is encoded"
    );
    assert_eq!(files[1].partition_values["b"], None);
}

#[test]
fn redacted_masks_every_effective_storage_secret_in_a_raised_error() {
    let readable = "failed to resolve the current Delta version for table root \
                    's3://bucket/cat/sch/orders': signature mismatch for ";
    let raised = UdfError::User(format!(
        "{readable}{SENTINEL_ACCESS_KEY} signed with {SENTINEL_SECRET_KEY}"
    ));

    let message = user_message(redacted(
        raised,
        &[SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY],
    ));

    assert!(message.starts_with(readable), "{message}");
    assert!(
        !message.contains(SENTINEL_ACCESS_KEY) && !message.contains(SENTINEL_SECRET_KEY),
        "{message}"
    );
}
