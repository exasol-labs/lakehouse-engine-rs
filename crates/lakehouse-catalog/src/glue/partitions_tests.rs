use std::collections::BTreeMap;

use super::*;

use crate::test_support::ORC_INPUT_FORMAT;
const TABLE: &str = "sales.events";

fn partition_keys() -> Vec<String> {
    vec![
        "p_int".to_string(),
        "p_date".to_string(),
        "p_str".to_string(),
    ]
}

fn partition(values: &[&str], location: Option<&str>, input_format: Option<&str>) -> GluePartition {
    GluePartition {
        values: values.iter().map(|value| value.to_string()).collect(),
        location: location.map(str::to_string),
        input_format: input_format.map(str::to_string),
    }
}

fn values(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
        .collect()
}

fn neutral(partition: GluePartition) -> Result<CatalogPartition, String> {
    neutral_partition(TABLE, &partition_keys(), partition)
}

/// Scenario: Partitions carry their Glue values, location, and format
#[test]
fn partitions_carry_glue_values_the_default_partition_as_null_and_their_own_format() {
    let cases = [
        (
            partition(
                &["1", "2024-01-01", "a"],
                Some("s3://bucket/events/p_int=7/p_date=2024-07-07/p_str=z/"),
                Some(PARQUET_INPUT_FORMAT),
            ),
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("1")),
                    ("p_date", Some("2024-01-01")),
                    ("p_str", Some("a")),
                ]),
                location: "s3://bucket/events/p_int=7/p_date=2024-07-07/p_str=z".to_string(),
                format: PartitionFormat::Parquet,
            },
        ),
        (
            partition(
                &["__HIVE_DEFAULT_PARTITION__", "2024-01-02", "b"],
                Some("s3://bucket/events/p_int=__HIVE_DEFAULT_PARTITION__/"),
                Some(PARQUET_INPUT_FORMAT),
            ),
            CatalogPartition {
                values: values(&[
                    ("p_int", None),
                    ("p_date", Some("2024-01-02")),
                    ("p_str", Some("b")),
                ]),
                location: "s3://bucket/events/p_int=__HIVE_DEFAULT_PARTITION__".to_string(),
                format: PartitionFormat::Parquet,
            },
        ),
        (
            partition(
                &["3", "2024-01-03", "c"],
                Some("s3://other/elsewhere"),
                None,
            ),
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("3")),
                    ("p_date", Some("2024-01-03")),
                    ("p_str", Some("c")),
                ]),
                location: "s3://other/elsewhere".to_string(),
                format: PartitionFormat::Parquet,
            },
        ),
        (
            partition(
                &["9", "2024-01-09", "z"],
                Some("s3://bucket/events/p_int=9/"),
                Some(ORC_INPUT_FORMAT),
            ),
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("9")),
                    ("p_date", Some("2024-01-09")),
                    ("p_str", Some("z")),
                ]),
                location: "s3://bucket/events/p_int=9".to_string(),
                format: PartitionFormat::Unsupported {
                    input_format: ORC_INPUT_FORMAT.to_string(),
                },
            },
        ),
    ];

    for (glue, expected) in cases {
        assert_eq!(
            neutral(glue).expect("a well-formed partition converts"),
            expected
        );
    }
}

#[test]
fn a_partition_without_a_location_fails_naming_its_values() {
    let message = neutral(partition(
        &["1", "2024-01-01", "a"],
        None,
        Some(PARQUET_INPUT_FORMAT),
    ))
    .expect_err("a partition without a location fails");

    assert!(message.contains(TABLE), "{message}");
    assert!(message.contains("2024-01-01"), "{message}");
}
