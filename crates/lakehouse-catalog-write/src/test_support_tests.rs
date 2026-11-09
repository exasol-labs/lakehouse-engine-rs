//! Write-crate test inputs: answers recorded from Lakekeeper with every credential value replaced
//! by a placeholder, tables over in-memory storage, data files, and a fake catalog session.

use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct};
use iceberg::table::Table;
use iceberg::{Runtime, TableIdent};
use iceberg_catalog_rest::LoadTableResult;
use lakehouse_catalog::exported_test_support::spawn_server;
use lakehouse_catalog::{CatalogSession, ConnectionCreds};
use std::sync::{Arc, Mutex};

/// A freshly created table of the vended warehouse, loaded with `snapshots=all`.
pub(crate) const LOAD_TABLE_NO_SNAPSHOTS: &str =
    include_str!("testdata/load_table_no_snapshots.json");
/// [`LOAD_TABLE_NO_SNAPSHOTS`] after `append_files` committed one snapshot tagged with
/// [`TAGGED_EXPORT_ID`] and [`TAGGED_WRITER_ID`].
pub(crate) const LOAD_TABLE_TAGGED: &str = include_str!("testdata/load_table_tagged_snapshot.json");
/// The stage-create answer of a table on the vended warehouse.
pub(crate) const STAGE_TABLE: &str = include_str!("testdata/stage_table.json");
/// Lakekeeper's HTTP 409 answer to a commit whose requirement no longer holds.
pub(crate) const COMMIT_CONFLICT: &str = include_str!("testdata/commit_conflict.json");

pub(crate) const TAGGED_EXPORT_ID: &str = "0123456789abcdef0123456789abcdef";
pub(crate) const TAGGED_WRITER_ID: &str = "00112233aabbccdd";
/// The placeholders that stand for vended credential values in the recorded answers.
pub(crate) const RECORDED_PLACEHOLDERS: [&str; 6] = [
    "LOADED_ACCESS_KEY_ID_PLACEHOLDER",
    "LOADED_SECRET_ACCESS_KEY_PLACEHOLDER",
    "LOADED_SESSION_TOKEN_PLACEHOLDER",
    "STAGED_ACCESS_KEY_ID_PLACEHOLDER",
    "STAGED_SECRET_ACCESS_KEY_PLACEHOLDER",
    "STAGED_SESSION_TOKEN_PLACEHOLDER",
];

/// The location of the table in [`STAGE_TABLE`], which the recorded `loadCredentials` answer's
/// only `storage-credentials` entry covers.
pub(crate) const STAGED_LOCATION: &str =
    "s3://warehouse/lakehouse_vended/01a1bf3b-af23-7020-ac26-0a0912b927ee";

pub(crate) const PREFIX_CONFIG: &str = r#"{"defaults":{"prefix":"p"}}"#;

pub(crate) fn ident(namespace: &str, name: &str) -> TableIdent {
    TableIdent::from_strs([namespace, name]).unwrap()
}

/// The table of a recorded load or stage `answer`, named `table`, whose FileIO writes in memory.
pub(crate) fn memory_table(answer: &str, table: &TableIdent) -> Table {
    let answer: LoadTableResult = serde_json::from_str(answer).unwrap();
    let mut builder = Table::builder()
        .identifier(table.clone())
        .metadata(answer.metadata)
        .file_io(FileIO::new_with_memory())
        .runtime(Runtime::try_current().unwrap());
    if let Some(location) = answer.metadata_location {
        builder = builder.metadata_location(location);
    }
    builder.build().unwrap()
}

/// One Parquet data file named `name` under `table`'s data directory.
pub(crate) fn data_file(table: &Table, name: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/{name}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1024)
        .record_count(10)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::empty())
        .build()
        .unwrap()
}

/// A fake catalog: `/v1/config` answers `config`, and every other request the answer `route`
/// gives it, by request line.
pub(crate) async fn catalog(
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

/// A session over the catalog at `uri` for a CONNECTION that does or does not vend.
pub(crate) async fn session(uri: &str, use_vended_credentials: bool) -> CatalogSession {
    let creds = ConnectionCreds {
        warehouse: "wh".into(),
        use_vended_credentials,
        ..Default::default()
    };
    CatalogSession::resolve(uri, "wh", &creds)
        .await
        .expect("a session over the fake catalog")
}

/// The recorded requests whose request line starts with `start`, such as `POST /v1/p/namespaces`.
pub(crate) fn requests_starting(requests: &Mutex<Vec<String>>, start: &str) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.starts_with(start))
        .cloned()
        .collect()
}

pub(crate) fn assert_free_of(text: &str, secrets: &[&str]) {
    for secret in secrets {
        assert!(!text.contains(secret), "{secret} leaked into: {text}");
    }
}
