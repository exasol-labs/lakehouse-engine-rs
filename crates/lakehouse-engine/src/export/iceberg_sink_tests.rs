use super::super::iceberg_rest::target;
use super::super::target::ExportConnection;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use lakehouse_catalog::ConnectionCreds;
use lakehouse_catalog::exported_test_support::spawn_server;
use lakehouse_catalog_write::CommitTag;
use std::sync::{Arc, Mutex};

const CREATE: &str = "POST /v1/p/namespaces/ns/tables ";
const RELOAD: &str = "GET /v1/p/namespaces/ns/tables/t?snapshots=all ";
const COMMIT: &str = "POST /v1/p/namespaces/ns/tables/t ";
const DATA_FILES: &str = "/warehouse/ns/t/data/";

/// A table at `s3://warehouse/ns/t` that allows no commit retry, so one answer decides a commit.
fn load_answer() -> String {
    serde_json::json!({
        "metadata-location": "s3://warehouse/ns/t/metadata/00000-0.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://warehouse/ns/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "ID", "required": false, "type": "long"}
            ]}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "properties": {"commit.retry.num-retries": "0"}
        },
        "config": {}
    })
    .to_string()
}

/// One fake for the catalog and its S3 storage: it creates and reloads the table, accepts every
/// storage write and delete, and answers the commit with `commit`.
async fn catalog_and_storage(commit: (u16, &'static str)) -> (String, Arc<Mutex<Vec<String>>>) {
    let load = load_answer();
    spawn_server("application/json", move |request| {
        let line = request.lines().next().unwrap_or_default();
        if line.starts_with("GET /v1/config") {
            (200, r#"{"defaults":{"prefix":"p"}}"#.into())
        } else if line.starts_with(CREATE) || line.starts_with(RELOAD) {
            (200, load.clone())
        } else if line.starts_with(COMMIT) {
            (commit.0, commit.1.into())
        } else if line.starts_with("PUT /warehouse/") {
            (200, String::new())
        } else if line.starts_with("DELETE /warehouse/") {
            (204, String::new())
        } else {
            (404, "{}".into())
        }
    })
    .await
}

/// A static-key CONNECTION whose catalog and storage are both the fake at `uri`.
fn connection(uri: &str) -> ExportConnection {
    ExportConnection {
        address: uri.to_string(),
        creds: ConnectionCreds {
            warehouse: "wh".into(),
            endpoint: uri.to_string(),
            region: "us-east-1".into(),
            access_key: "STATIC_ACCESS_KEY".into(),
            secret_key: "STATIC_SECRET_KEY".into(),
            path_style: Some(true),
            ..Default::default()
        },
        allow_http: true,
    }
}

/// The data file paths of the recorded requests whose method is `method`.
fn data_file_paths(requests: &Mutex<Vec<String>>, method: &str) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter_map(|request| {
            let mut line = request.lines().next()?.split(' ');
            let path = (line.next()? == method).then(|| line.next())??;
            let path = path.split('?').next()?;
            path.starts_with(DATA_FILES).then(|| path.to_string())
        })
        .collect()
}

/// The outcome of a 504 commit is unknown, so the data files stay: the catalog may have applied it.
#[tokio::test]
async fn abort_keeps_the_data_files_while_the_commit_outcome_is_unknown() {
    let schema = Schema::new(vec![Field::new("ID", DataType::Int64, true)]);
    let tag = CommitTag {
        export_id: "e1".into(),
        writer_id: "w1".into(),
    };
    let (uri, requests) = catalog_and_storage((504, "{}")).await;
    let target = target(connection(&uri), "ns.t").unwrap();
    let mut sink = target.open_table(&schema, &tag, false).await.unwrap();
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let batch = RecordBatch::try_new(sink.schema(), vec![ids]).unwrap();

    sink.write(&batch).await.unwrap();
    let finished = sink.finish().await.unwrap_err().to_string();
    sink.abort().await;

    assert!(
        finished.contains("the outcome of its last request is unknown"),
        "{finished}"
    );
    assert_eq!(data_file_paths(&requests, "PUT").len(), 1);
    assert!(data_file_paths(&requests, "DELETE").is_empty());
}
