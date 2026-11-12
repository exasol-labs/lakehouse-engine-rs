use super::super::iceberg_rest::target;
use crate::export::test_support::static_key_connection;
use arrow::array::{ArrayRef, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use lakehouse_catalog_write::CommitTag;
use lakehouse_catalog_write::exported_test_support::{
    LOAD_TABLE_NO_SNAPSHOTS, PREFIX_CONFIG, catalog,
};
use std::sync::{Arc, Mutex};

const TABLE_LOCATION: &str = "/warehouse/lakehouse_vended/";

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
            (path.starts_with(TABLE_LOCATION) && path.contains("/data/")).then(|| path.to_string())
        })
        .collect()
}

/// The outcome of a 504 commit is unknown, so the data files stay: the catalog may have applied it.
#[tokio::test]
async fn abort_keeps_the_data_files_while_the_commit_outcome_is_unknown() {
    // The recorded table allows 20 commit retries; none keeps the one 504 answer decisive.
    let table = LOAD_TABLE_NO_SNAPSHOTS.replace(
        r#""commit.retry.num-retries": "20""#,
        r#""commit.retry.num-retries": "0""#,
    );
    let (uri, requests) = catalog(PREFIX_CONFIG, move |line| {
        if line.starts_with("POST /v1/p/namespaces/ns/tables/t ") {
            (504, "{}".into())
        } else if line.starts_with("POST /v1/p/namespaces/ns/tables ")
            || line.starts_with("GET /v1/p/namespaces/ns/tables/t?snapshots=all ")
        {
            (200, table.clone())
        } else if line.starts_with("PUT /warehouse/") {
            (200, String::new())
        } else if line.starts_with("DELETE /warehouse/") {
            (204, String::new())
        } else {
            (404, "{}".into())
        }
    })
    .await;
    let schema = Schema::new(vec![Field::new("ID", DataType::Int64, true)]);
    let tag = CommitTag {
        export_id: "e1".into(),
        writer_id: "w1".into(),
    };
    let target = target(static_key_connection(&uri, true), "ns.t").unwrap();
    let mut sink = target.open_table(&schema, &tag, false).await.unwrap();
    let ids: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1, 2])
            .with_precision_and_scale(18, 0)
            .unwrap(),
    );
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let batch = RecordBatch::try_new(sink.schema(), vec![ids, names]).unwrap();

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
