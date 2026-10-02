mod client;
mod partitions;
mod routing;
mod sdk;

pub use client::{GlueCatalogSession, parse_glue_table_ident};

/// Glue records a directory location with a trailing `/`; the neutral types carry none.
fn trim_location(location: &str) -> &str {
    location.strip_suffix('/').unwrap_or(location)
}
