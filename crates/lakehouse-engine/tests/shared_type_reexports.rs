//! The engine's re-exported catalog types are the catalog crate's own types: nominal typing
//! means this file compiles only if both paths name the identical type.

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::scan::spec::{AdlsCred, CatalogProps, StorageProps};

fn accepts_catalog_crate_storage_props(_props: lakehouse_catalog::StorageProps) {}

fn accepts_catalog_crate_catalog_props(_props: lakehouse_catalog::CatalogProps) {}

fn accepts_catalog_crate_connection_creds(_creds: lakehouse_catalog::ConnectionCreds) {}

fn accepts_catalog_crate_adls_cred(_cred: lakehouse_catalog::AdlsCred) {}

#[test]
fn reexported_paths_resolve_to_the_catalog_crate_types() {
    let storage = StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    };
    accepts_catalog_crate_storage_props(storage);

    let catalog = CatalogProps {
        warehouse: "warehouse".into(),
        table: "ns.table".into(),
    };
    accepts_catalog_crate_catalog_props(catalog);

    let creds = ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    };
    accepts_catalog_crate_connection_creds(creds);

    let adls_cred = AdlsCred::AccountKey("k".into());
    accepts_catalog_crate_adls_cred(adls_cred);
}

/// Scenario: `StorageProps` serializes byte-for-byte to the golden wire encoding
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

/// Scenario: `StorageBackend::S3` round-trips under an externally-tagged `{"s3": {...}}` key
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
