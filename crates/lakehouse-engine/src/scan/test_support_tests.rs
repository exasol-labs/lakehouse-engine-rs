use crate::scan::field_id_projection::build_logical_arrow_schema;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, ScanSpec, ScanStorage, StorageBackend, StorageProps,
};
use crate::scan::{FieldIdExprAdapterFactory, FieldIdResolution, ResolvedScanStorage};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::path::Path;
use std::sync::Arc;

/// Write one single-batch Parquet file of non-nullable columns, returning its `file://` URL.
pub(super) fn write_parquet(path: &Path, columns: Vec<(&str, ArrayRef)>) -> String {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, array)| array).collect();
    let file = std::fs::File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("arrow writer");
    let batch = RecordBatch::try_new(schema, arrays).expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

pub(super) fn local_file_size(file_url: &str) -> u64 {
    let path = url::Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

/// The logical file schema and column-binding adapter factory the raw scan installs for an
/// unpartitioned table declaring `logical_schema` with no name mapping.
pub(crate) fn column_binding_for(
    logical_schema: &[LogicalField],
    table_root: &str,
) -> (SchemaRef, FieldIdExprAdapterFactory) {
    let resolution = FieldIdResolution::for_logical_schema(logical_schema, &[])
        .expect("the logical schema's initial defaults reconstruct");
    (
        build_logical_arrow_schema(logical_schema),
        FieldIdExprAdapterFactory {
            resolution,
            table_root: table_root.to_string(),
        },
    )
}

pub(super) fn minimal_spec() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "testkey".into(),
                secret_key: "testsecret".into(),
                allow_http: true,
                ..Default::default()
            })),
            ..Default::default()
        },
        files: vec![FileEntry::new("s3://test-bucket/data/part-0.parquet", 1024)],
    }
}

pub(super) fn inline_resolved(spec: &ScanSpec) -> ResolvedScanStorage {
    ResolvedScanStorage::from_backends(
        inline_backend(&spec.common.storage),
        spec.common
            .join
            .as_ref()
            .map(|join| inline_backend(&join.storage)),
    )
}

fn inline_backend(storage: &ScanStorage) -> StorageBackend {
    match storage {
        ScanStorage::Inline(backend) => backend.clone(),
        other => panic!("this fixture shortcut needs an inline storage value, not {other:?}"),
    }
}

pub(super) const TEST_CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

pub(super) fn refusing_endpoint(message: &str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let body =
        format!("<Error><Code>SignatureDoesNotMatch</Code><Message>{message}</Message></Error>");
    let resp = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let _ = std::io::Read::read(&mut s, &mut [0u8; 4096]);
            let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
        }
    });
    url
}

pub(super) fn refusing_backend(endpoint: &str, secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        access_key: "testkey".into(),
        secret_key: secret.into(),
        allow_http: true,
        ..Default::default()
    })
}

pub(super) struct SinkCtx;

impl exasol_udf_sdk::context::UdfContext for SinkCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(
        &self,
        _: usize,
    ) -> Result<&exasol_udf_sdk::value::Value, exasol_udf_sdk::error::UdfError> {
        unimplemented!()
    }
    fn input_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::Type(format!(
            "input column {idx} out of range"
        )))
    }
    fn output_column_count(&self) -> usize {
        0
    }
    fn output_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::Type(format!(
            "output column {idx} out of range"
        )))
    }
    fn emit(
        &mut self,
        _: Vec<exasol_udf_sdk::value::Value>,
    ) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
        Ok(false)
    }
    fn emit_record_batch_ipc(&mut self, _: &[u8]) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
}
