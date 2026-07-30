//! External-vantage proof that the engine's re-exported catalog types are the
//! catalog crate's own types, not merely structurally-similar look-alikes, plus
//! an independent golden-string pin on `StorageProps`' wire encoding.
//!
//! `StorageProps`, `CatalogProps`, and `ConnectionCreds` are declared in
//! `lakehouse-catalog` and re-exported at their pre-move engine path
//! (`scan::spec::{StorageProps, CatalogProps}`,
//! `adapter::connection::ConnectionCreds`) so no consumer `use` path had to
//! change. A `pub use` re-export does not create a new type — it adds another
//! import path to the same one — but Rust's nominal (non-structural) type
//! system makes that easy to verify: a function that names
//! `lakehouse_catalog::StorageProps` in its signature accepts a value built via
//! `lakehouse_engine::scan::spec::StorageProps` only if the two paths name the
//! identical type. No `From`/`Into` conversion or `TypeId` machinery is
//! involved anywhere below; the proof is that this file compiles at all.

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::scan::spec::{CatalogProps, StorageProps};

/// Accepts only the catalog crate's own `StorageProps` — never a re-export.
fn accepts_catalog_crate_storage_props(_props: lakehouse_catalog::StorageProps) {}

/// Accepts only the catalog crate's own `CatalogProps` — never a re-export.
fn accepts_catalog_crate_catalog_props(_props: lakehouse_catalog::CatalogProps) {}

/// Accepts only the catalog crate's own `ConnectionCreds` — never a re-export.
fn accepts_catalog_crate_connection_creds(_creds: lakehouse_catalog::ConnectionCreds) {}

#[test]
fn reexported_paths_resolve_to_the_catalog_crate_types() {
    // Built via the engine's re-exported `scan::spec` path.
    let storage = StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    };
    accepts_catalog_crate_storage_props(storage);

    // Built via the engine's re-exported `scan::spec` path.
    let catalog = CatalogProps {
        warehouse: "warehouse".into(),
        table: "ns.table".into(),
    };
    accepts_catalog_crate_catalog_props(catalog);

    // Built via the engine's re-exported `adapter::connection` path.
    let creds = ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
    };
    accepts_catalog_crate_connection_creds(creds);
}

/// Independent, integration-level pin on `StorageProps`' serde encoding,
/// asserting the exact same field values and byte-for-byte JSON that the
/// crate-internal unit test `common_blob_wire_is_byte_stable`
/// (`crates/lakehouse-engine/src/scan/spec.rs`) pins as the embedded `storage`
/// segment of its `CommonScanSpec` golden string. That unit test proves the
/// wire format is stable inside `CommonScanSpec`; this test proves the same
/// thing for `StorageProps` alone, from outside the crate, so the guarantee
/// survives even if the unit test were ever deleted.
#[test]
fn storage_props_wire_encoding_unchanged() {
    let storage = StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    };

    let golden = r#"{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}"#;

    assert_eq!(serde_json::to_string(&storage).unwrap(), golden);
}

/// Companion pin to [`storage_props_wire_encoding_unchanged`]: `StorageBackend`
/// wraps [`StorageProps`] in an externally-tagged, lowercase-keyed `s3` variant,
/// so the exact same field values now round-trip under `{"s3": {...}}` rather
/// than the bare object above. This is the one deliberate byte-level wire
/// change the storage-backend-enum refactor makes.
#[test]
fn storage_backend_wire_encoding_tags_the_s3_payload() {
    use lakehouse_engine::scan::spec::StorageBackend;

    let backend = StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    });

    let golden = r#"{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}}"#;

    let encoded = serde_json::to_string(&backend).unwrap();
    assert_eq!(encoded, golden);
    assert_eq!(
        serde_json::from_str::<StorageBackend>(&encoded).unwrap(),
        backend
    );
}
