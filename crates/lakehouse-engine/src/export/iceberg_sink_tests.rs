use super::super::iceberg_rest::target;
use super::data_file_settings;
use crate::export::test_support::{date_columns, static_key_connection};
use arrow::array::{ArrayRef, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use lakehouse_catalog_write::CommitTag;
use lakehouse_catalog_write::exported_test_support::{
    LOAD_TABLE_NO_SNAPSHOTS, PREFIX_CONFIG, catalog, with_short_commit_waits,
};
use std::collections::HashMap;
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
    let table = with_short_commit_waits(LOAD_TABLE_NO_SNAPSHOTS);
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
    let mut sink = target
        .open_new_table(&schema, &[], &tag, false, 1 << 20)
        .await
        .unwrap();
    let ids: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1, 2])
            .with_precision_and_scale(18, 0)
            .unwrap(),
    );
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let batch = RecordBatch::try_new(sink.columns().schema.clone(), vec![ids, names]).unwrap();

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

fn properties(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn the_target_file_size_follows_the_table_or_iceberg_default() {
    let cases = [
        (properties(&[]), 512 << 20),
        (
            properties(&[
                ("write.target-file-size-bytes", "4194304"),
                ("write.parquet.compression-codec", "snappy"),
                ("write.parquet.row-group-size-bytes", "1048576"),
            ]),
            4 << 20,
        ),
        (
            properties(&[("write.parquet.content-defined-chunking.enabled", "true")]),
            512 << 20,
        ),
    ];
    for (table, target) in cases {
        let (_, size) = data_file_settings(&table, date_columns(1)).unwrap();
        assert_eq!(size, target, "{table:?}");
    }
}

#[test]
fn an_unusable_write_property_is_named_with_its_value() {
    let cases = [
        (
            vec![("write.parquet.compression-codec", "lzo")],
            "write.parquet.compression-codec = 'lzo'",
        ),
        (
            vec![("write.parquet.compression-codec", "foo")],
            "write.parquet.compression-codec = 'foo'",
        ),
        (
            vec![("write.parquet.compression-level", "x")],
            "write.parquet.compression-level = 'x'",
        ),
        (
            vec![("write.parquet.compression-level", "99")],
            "write.parquet.compression-level = '99'",
        ),
        (
            vec![("write.parquet.row-group-size-bytes", "0")],
            "write.parquet.row-group-size-bytes = '0'",
        ),
        (
            vec![("write.parquet.page-size-bytes", "-1")],
            "write.parquet.page-size-bytes = '-1'",
        ),
        (
            vec![("write.target-file-size-bytes", "big")],
            "write.target-file-size-bytes = 'big'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                ("write.parquet.content-defined-chunking.min-chunk-size", "0"),
            ],
            "write.parquet.content-defined-chunking.min-chunk-size = '0'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                (
                    "write.parquet.content-defined-chunking.min-chunk-size",
                    "1024",
                ),
                (
                    "write.parquet.content-defined-chunking.max-chunk-size",
                    "1024",
                ),
            ],
            "write.parquet.content-defined-chunking.max-chunk-size = '1024'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                ("write.parquet.content-defined-chunking.norm-level", "40"),
            ],
            "write.parquet.content-defined-chunking.norm-level = '40'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                ("write.parquet.content-defined-chunking.min-chunk-size", "x"),
            ],
            "write.parquet.content-defined-chunking.min-chunk-size = 'x'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                (
                    "write.parquet.content-defined-chunking.min-chunk-size",
                    "9223372036854775808",
                ),
                (
                    "write.parquet.content-defined-chunking.max-chunk-size",
                    "9223372036854775810",
                ),
            ],
            "write.parquet.content-defined-chunking.norm-level = ''",
        ),
        (
            vec![("write.parquet.content-defined-chunking.enabled", "maybe")],
            "write.parquet.content-defined-chunking.enabled = 'maybe'",
        ),
    ];
    for (pairs, named) in cases {
        let error = data_file_settings(&properties(&pairs), date_columns(1))
            .err()
            .unwrap_or_else(|| panic!("{pairs:?} was accepted"));
        assert!(
            error.starts_with(&format!("invalid write property {named}: ")),
            "{pairs:?}: {error}"
        );
    }
}
