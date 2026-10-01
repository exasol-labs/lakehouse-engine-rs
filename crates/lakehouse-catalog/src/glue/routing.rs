use std::collections::HashMap;

use crate::TableFormat;

pub(super) const PARQUET_INPUT_FORMAT: &str =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

const VIEW_TABLE_TYPE: &str = "VIRTUAL_VIEW";
const TABLE_TYPE_PARAMETER: &str = "table_type";
const ICEBERG_TABLE_TYPE: &str = "ICEBERG";
const METADATA_LOCATION_PARAMETER: &str = "metadata_location";
const PROJECTION_ENABLED_PARAMETER: &str = "projection.enabled";

/// The reader a Glue table routes to, or the skip detail naming the Glue value that decided it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Route {
    Iceberg { metadata_location: String },
    Parquet,
    Skip(String),
}

/// A view and a present `table_type` decide before the storage descriptor, because a
/// registration that names its format in `table_type` may carry any descriptor.
pub(super) fn route(
    table_type: Option<&str>,
    parameters: &HashMap<String, String>,
    input_format: Option<&str>,
) -> Route {
    if let Some(view) = table_type.filter(|kind| kind.eq_ignore_ascii_case(VIEW_TABLE_TYPE)) {
        return Route::Skip(format!("TableType={view}"));
    }
    let parameter = |key: &str| parameters.get(key).map(String::as_str);
    match parameter(TABLE_TYPE_PARAMETER) {
        Some(kind) if kind.eq_ignore_ascii_case(ICEBERG_TABLE_TYPE) => {
            iceberg_route(kind, parameter(METADATA_LOCATION_PARAMETER))
        }
        Some(kind) => Route::Skip(format!("{TABLE_TYPE_PARAMETER}={kind}")),
        None => hive_route(input_format, parameter(PROJECTION_ENABLED_PARAMETER)),
    }
}

pub(super) fn format_of_input_format(input_format: &str) -> Option<TableFormat> {
    (input_format == PARQUET_INPUT_FORMAT).then_some(TableFormat::Parquet)
}

fn iceberg_route(table_type: &str, metadata_location: Option<&str>) -> Route {
    match metadata_location.filter(|location| !location.trim().is_empty()) {
        Some(location) => Route::Iceberg {
            metadata_location: location.to_string(),
        },
        None => Route::Skip(format!(
            "{TABLE_TYPE_PARAMETER}={table_type} with an absent or empty \
             {METADATA_LOCATION_PARAMETER}"
        )),
    }
}

fn hive_route(input_format: Option<&str>, projection_enabled: Option<&str>) -> Route {
    let Some(TableFormat::Parquet) = input_format.and_then(format_of_input_format) else {
        return Route::Skip(format!("InputFormat={}", input_format.unwrap_or("absent")));
    };
    match projection_enabled {
        Some(enabled) if enabled.eq_ignore_ascii_case("true") => Route::Skip(format!(
            "{PROJECTION_ENABLED_PARAMETER}={enabled}: partition projection registers no \
             partition in Glue, so reading the table would return zero rows"
        )),
        _ => Route::Parquet,
    }
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;
