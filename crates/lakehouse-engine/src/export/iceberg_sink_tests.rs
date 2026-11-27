use super::super::iceberg_rest::target;
use super::data_file_settings;
use crate::export::test_support::{date_columns, static_key_connection};
use arrow::array::{ArrayRef, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use lakehouse_catalog::exported_test_support::spawn_server_with_headers;
use lakehouse_catalog_write::CommitTag;
use lakehouse_catalog_write::exported_test_support::{
    LOAD_TABLE_NO_SNAPSHOTS, PREFIX_CONFIG, with_short_commit_waits,
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

/// The recorded S3 DeleteObjects requests, which object_store sends for every delete.
fn bulk_deletes(requests: &Mutex<Vec<String>>) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.starts_with("POST /warehouse?delete "))
        .cloned()
        .collect()
}

/// A sink that writes into a new unpartitioned table, whose catalog answers a commit with a 504,
/// the requests the catalog received, and a batch of two rows for it.
async fn new_table_sink() -> (
    Box<dyn crate::export::target::ExportSink>,
    Arc<Mutex<Vec<String>>>,
    RecordBatch,
) {
    let table = with_short_commit_waits(LOAD_TABLE_NO_SNAPSHOTS);
    let (uri, requests) = spawn_server_with_headers("application/json", move |request| {
        let line = request.lines().next().unwrap_or_default();
        if line.starts_with("GET /v1/config") {
            (200, Vec::new(), PREFIX_CONFIG.to_string())
        } else if line.starts_with("POST /v1/p/namespaces/ns/tables/t ") {
            (504, Vec::new(), "{}".into())
        } else if line.starts_with("POST /v1/p/namespaces/ns/tables ")
            || line.starts_with("GET /v1/p/namespaces/ns/tables/t?snapshots=all ")
        {
            (200, Vec::new(), table.clone())
        } else if line.starts_with("PUT /warehouse/") {
            // S3 answers every PUT with the object's ETag, which object_store requires.
            (200, vec![("ETag", "\"etag\"".to_string())], String::new())
        } else if line.starts_with("DELETE /warehouse/") {
            (204, Vec::new(), String::new())
        } else {
            (404, Vec::new(), "{}".into())
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
    (sink, requests, batch)
}

/// The outcome of a 504 commit is unknown, so the data files stay: the catalog may have applied it.
#[tokio::test]
async fn abort_keeps_the_data_files_while_the_commit_outcome_is_unknown() {
    let (mut sink, requests, batch) = new_table_sink().await;
    sink.write(&batch).await.unwrap();
    let finished = sink.finish().await.unwrap_err().to_string();
    sink.abort().await;

    assert!(
        finished.contains("the outcome of its last request is unknown"),
        "{finished}"
    );
    assert_eq!(data_file_paths(&requests, "PUT").len(), 1);
    assert!(bulk_deletes(&requests).is_empty());
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
fn a_codec_without_a_level_ignores_the_level_property() {
    for codec in ["snappy", "uncompressed", "lz4_raw"] {
        let table = properties(&[
            ("write.parquet.compression-codec", codec),
            ("write.parquet.compression-level", "abc"),
        ]);
        assert!(
            data_file_settings(&table, date_columns(1)).is_ok(),
            "{codec}"
        );
    }
}

#[test]
fn an_unknown_codec_or_a_negative_size_gets_a_plain_reason() {
    let cases = [
        (
            ("write.parquet.compression-codec", "foo"),
            "the export writes zstd, snappy, gzip, brotli, lz4_raw, uncompressed, or lz4",
        ),
        (
            ("write.parquet.page-size-bytes", "-1"),
            "the value must not be negative",
        ),
    ];
    for (pair, reason) in cases {
        let error = data_file_settings(&properties(&[pair]), date_columns(1)).unwrap_err();
        assert!(error.ends_with(reason), "{pair:?}: {error}");
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
            vec![("write.parquet.page-row-limit", "0")],
            "write.parquet.page-row-limit = '0'",
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
                    "9223372036855824384",
                ),
            ],
            "write.parquet.content-defined-chunking.min-chunk-size = '9223372036854775808'",
        ),
        (
            vec![
                ("write.parquet.content-defined-chunking.enabled", "true"),
                (
                    "write.parquet.content-defined-chunking.max-chunk-size",
                    "9223372036854775808",
                ),
            ],
            "write.parquet.content-defined-chunking.max-chunk-size = '9223372036854775808'",
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
        let prefix = format!("invalid write property {named}: ");
        assert!(error.starts_with(&prefix), "{pairs:?}: {error}");
        let key = named.split(" = ").next().unwrap();
        assert!(
            !error[prefix.len()..].replace(key, "").contains("write."),
            "{pairs:?} names another property: {error}"
        );
    }
}

/// The chunk sizes and normalization levels the export refuses are exactly those parquet-rs
/// fails to write with.
#[test]
fn a_refused_chunking_setting_is_one_parquet_rs_cannot_write() {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{CdcOptions, WriterProperties};
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let column: ArrayRef = Arc::new(StringArray::from(vec!["x"; 64]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    for min in [1_usize, 1_000, 256 * 1024] {
        for max in [min + 1, min + 16, min + 1_000, 1 << 20, 1 << 40, 1 << 63] {
            for norm in [-60, -3, 0, 3, 20, 40] {
                let options = CdcOptions {
                    min_chunk_size: min,
                    max_chunk_size: max,
                    norm_level: norm,
                };
                let props = WriterProperties::builder()
                    .set_content_defined_chunking(Some(options))
                    .build();
                let parquet_fails = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props))
                    .and_then(|mut writer| writer.write(&batch).and_then(|()| writer.close()))
                    .is_err();
                let (min, max, norm) = (min.to_string(), max.to_string(), norm.to_string());
                let refused = data_file_settings(
                    &properties(&[
                        ("write.parquet.content-defined-chunking.enabled", "true"),
                        (
                            "write.parquet.content-defined-chunking.min-chunk-size",
                            &min,
                        ),
                        (
                            "write.parquet.content-defined-chunking.max-chunk-size",
                            &max,
                        ),
                        ("write.parquet.content-defined-chunking.norm-level", &norm),
                    ]),
                    date_columns(1),
                )
                .is_err();
                assert_eq!(refused, parquet_fails, "min {min}, max {max}, norm {norm}");
            }
        }
    }
}

/// An open data file becomes an object on abort, so its delete removes it and no upload stays open.
#[tokio::test]
async fn abort_closes_the_open_data_file_before_deleting_it() {
    let (mut sink, requests, batch) = new_table_sink().await;

    sink.write(&batch).await.unwrap();
    sink.abort().await;

    let written = data_file_paths(&requests, "PUT");
    assert_eq!(written.len(), 1);
    let key = written[0].trim_start_matches("/warehouse/");
    let deletes = bulk_deletes(&requests);
    assert_eq!(deletes.len(), 1);
    assert!(
        deletes[0].contains(&format!("<Key>{key}</Key>")),
        "{}",
        deletes[0]
    );
}
