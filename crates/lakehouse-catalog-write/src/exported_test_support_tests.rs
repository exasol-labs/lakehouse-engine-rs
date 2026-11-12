//! The fake Iceberg REST catalog and the recorded answers that the write crate's tests share
//! with its dependents' tests.

use lakehouse_catalog::exported_test_support::spawn_server;
use std::sync::{Arc, Mutex};

/// A freshly created table of the vended warehouse, loaded with `snapshots=all`.
pub const LOAD_TABLE_NO_SNAPSHOTS: &str = include_str!("testdata/load_table_no_snapshots.json");

/// The `/v1/config` answer that gives the catalog the prefix `p`.
pub const PREFIX_CONFIG: &str = r#"{"defaults":{"prefix":"p"}}"#;

/// A fake catalog: `/v1/config` answers `config`, and every other request the answer `route`
/// gives it, by request line.
pub async fn catalog(
    config: &'static str,
    route: impl Fn(&str) -> (u16, String) + Send + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_server("application/json", move |request| {
        if request.starts_with("GET /v1/config") {
            (200, config.to_string())
        } else {
            route(request.lines().next().unwrap_or_default())
        }
    })
    .await
}
