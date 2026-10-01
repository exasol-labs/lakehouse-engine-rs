use super::*;
use std::collections::HashMap;

const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
const METADATA: &str = "s3://bucket/orders/metadata/00001-abc.metadata.json";

fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn skip(detail: &str) -> Route {
    Route::Skip(detail.to_string())
}

#[test]
fn route_admits_an_iceberg_table_type_case_insensitively_with_its_metadata_location() {
    for table_type in ["ICEBERG", "iceberg", "Iceberg"] {
        let parameters = params(&[("table_type", table_type), ("metadata_location", METADATA)]);

        assert_eq!(
            route(Some("EXTERNAL_TABLE"), &parameters, None),
            Route::Iceberg {
                metadata_location: METADATA.to_string()
            },
            "table_type={table_type}"
        );
    }
}

#[test]
fn route_skips_an_iceberg_table_without_a_metadata_location_naming_the_parameter() {
    for parameters in [
        params(&[("table_type", "ICEBERG")]),
        params(&[("table_type", "ICEBERG"), ("metadata_location", "")]),
        params(&[("table_type", "ICEBERG"), ("metadata_location", "  ")]),
    ] {
        let Route::Skip(detail) = route(Some("EXTERNAL_TABLE"), &parameters, None) else {
            panic!("an Iceberg table without metadata_location must be skipped: {parameters:?}");
        };
        assert!(detail.contains("metadata_location"), "{detail}");
    }
}

#[test]
fn route_skips_a_present_non_iceberg_table_type_before_reading_the_input_format() {
    let athena_delta = params(&[("table_type", "delta")]);

    assert_eq!(
        route(
            Some("EXTERNAL_TABLE"),
            &athena_delta,
            Some(PARQUET_INPUT_FORMAT)
        ),
        skip("table_type=delta"),
        "a present table_type decides the route even over a Parquet input format"
    );
    assert_eq!(
        route(
            Some("EXTERNAL_TABLE"),
            &athena_delta,
            Some("org.apache.hadoop.mapred.SequenceFileInputFormat")
        ),
        skip("table_type=delta")
    );
}

#[test]
fn route_admits_a_hive_table_only_for_the_mapred_parquet_input_format() {
    assert_eq!(
        route(
            Some("EXTERNAL_TABLE"),
            &params(&[]),
            Some(PARQUET_INPUT_FORMAT)
        ),
        Route::Parquet
    );
    assert_eq!(
        route(None, &params(&[]), Some(PARQUET_INPUT_FORMAT)),
        Route::Parquet
    );
}

#[test]
fn route_skips_every_other_input_format_naming_it() {
    for input_format in [
        ORC_INPUT_FORMAT,
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat",
    ] {
        assert_eq!(
            route(Some("EXTERNAL_TABLE"), &params(&[]), Some(input_format)),
            skip(&format!("InputFormat={input_format}"))
        );
    }
    assert_eq!(
        route(Some("EXTERNAL_TABLE"), &params(&[]), None),
        skip("InputFormat=absent")
    );
}

#[test]
fn route_skips_a_view_naming_its_table_type_before_any_parameter() {
    let iceberg = params(&[("table_type", "ICEBERG"), ("metadata_location", METADATA)]);

    for table_type in ["VIRTUAL_VIEW", "virtual_view"] {
        assert_eq!(
            route(Some(table_type), &params(&[]), None),
            skip(&format!("TableType={table_type}"))
        );
        assert_eq!(
            route(Some(table_type), &iceberg, Some(PARQUET_INPUT_FORMAT)),
            skip(&format!("TableType={table_type}"))
        );
    }
}

#[test]
fn route_skips_a_projection_enabled_parquet_table_stating_it_would_read_no_rows() {
    for enabled in ["true", "TRUE", "True"] {
        let parameters = params(&[("projection.enabled", enabled)]);

        let Route::Skip(detail) = route(
            Some("EXTERNAL_TABLE"),
            &parameters,
            Some(PARQUET_INPUT_FORMAT),
        ) else {
            panic!("projection.enabled={enabled} must skip");
        };
        assert!(detail.starts_with("projection.enabled="), "{detail}");
        assert!(detail.contains("zero rows"), "{detail}");
    }

    assert_eq!(
        route(
            Some("EXTERNAL_TABLE"),
            &params(&[("projection.enabled", "false")]),
            Some(PARQUET_INPUT_FORMAT)
        ),
        Route::Parquet
    );
}

#[test]
fn input_format_names_the_parquet_format_only_for_the_mapred_parquet_input_format() {
    assert_eq!(
        format_of_input_format(PARQUET_INPUT_FORMAT),
        Some(TableFormat::Parquet)
    );
    assert_eq!(format_of_input_format(ORC_INPUT_FORMAT), None);
    assert_eq!(format_of_input_format(""), None);
}
