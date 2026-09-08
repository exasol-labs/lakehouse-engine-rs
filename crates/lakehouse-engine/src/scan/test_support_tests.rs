use crate::scan::ResolvedScanStorage;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, ScanSpec, ScanStorage, StorageBackend, StorageProps,
};

pub(super) fn local_file_size(file_url: &str) -> u64 {
    let path = url::Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
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
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback endpoint");
    let url = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("bound endpoint has an address")
    );
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>SignatureDoesNotMatch</Code><Message>{message}</Message></Error>"
    );
    let response = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut request_head = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut request_head);
            let _ = std::io::Write::write_all(&mut stream, response.as_bytes());
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
        _col: usize,
    ) -> Result<&exasol_udf_sdk::value::Value, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::User(
            "this sink reads no input column".into(),
        ))
    }
    fn emit(
        &mut self,
        _values: &[exasol_udf_sdk::value::Value],
    ) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
        Ok(false)
    }
    fn emit_record_batch_ipc(
        &mut self,
        _ipc: &[u8],
    ) -> Result<(), exasol_udf_sdk::error::UdfError> {
        Ok(())
    }
}
