mod client;
mod partitions;
mod routing;
mod sdk;
mod source;

pub use client::GlueCatalogSession;
pub(crate) use routing::PARQUET_INPUT_FORMAT;

/// Glue records a directory location with a trailing `/`; the neutral types carry none.
fn trim_location(location: &str) -> &str {
    location.strip_suffix('/').unwrap_or(location)
}
