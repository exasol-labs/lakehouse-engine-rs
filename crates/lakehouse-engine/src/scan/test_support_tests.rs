//! Test-only fixtures shared across the `scan` submodule test modules.
//!
//! Extracted verbatim from the former flat `mod tests` helpers block. Each
//! functional submodule's `#[cfg(test)] mod tests` reaches these through
//! `super::test_support` (or `crate::scan::test_support` from a nested module).

use crate::scan::ResolvedScanStorage;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, ScanSpec, ScanStorage, StorageBackend, StorageProps,
};

/// The byte size of the local file behind a `file://` URL.
///
/// The custom `ParquetSource`-backed provider builds each file's `ObjectMeta`
/// from the spec-supplied size (the no-HEAD design), so tests that register a
/// local Parquet file must supply its real size instead of a `0` placeholder.
pub(super) fn local_file_size(file_url: &str) -> u64 {
    let path = url::Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

/// Minimal ScanSpec with a valid-looking S3 URI for build_session_context tests.
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

/// The [`ResolvedScanStorage`] an already-inline host-test spec stands for.
///
/// A fixture spec constructs its backends directly and wraps them in
/// [`ScanStorage::Inline`], so no CONNECTION is involved and the resolved pair is
/// just those same backends lifted out. Shared here rather than duplicated per
/// sibling test module: the pair every scan path now takes must agree with the
/// spec each test hands alongside it, and one derivation is what keeps them
/// agreeing.
///
/// Panics on a non-inline fixture — a spec referencing a CONNECTION belongs with
/// a stub context and `resolve_scan_storage`, not with this shortcut.
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

/// The CONNECTION name a fixture spec references when the point of the test is
/// that the spec carries NO credential of its own.
pub(super) const TEST_CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

/// A loopback endpoint refusing every request with a 403 whose XML body QUOTES
/// `message`, and the URL to reach it at.
///
/// The refusal BODY is the whole point: `object_store` folds a non-2xx response
/// body into the error it surfaces, so an endpoint quoting a credential in its
/// refusal — the real shape of an S3 `SignatureDoesNotMatch` — is what makes
/// value-based redaction OBSERVABLE rather than vacuous. Without it, a test
/// asserting a secret's absence would pass on a build whose redaction set was
/// empty.
///
/// A 4xx is never retried, so each read reaches the endpoint exactly once and
/// fails fast.
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

/// An S3 backend reaching `endpoint` and carrying `secret` as its secret key —
/// the resolved credential an error must be redacted against.
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

/// A no-op `UdfContext` sink: it accepts every emitted row and batch and reads no
/// input, for a test driving a scan path whose emitted output is not what is
/// under assertion.
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
