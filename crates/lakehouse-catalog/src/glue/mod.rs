mod client;
mod partitions;
mod routing;

#[cfg(test)]
#[path = "mock_glue_tests.rs"]
mod mock_glue;

pub use client::GlueCatalogSession;

/// Glue records a directory location with a trailing `/`; the neutral types carry none.
fn trim_location(location: &str) -> &str {
    location.strip_suffix('/').unwrap_or(location)
}
