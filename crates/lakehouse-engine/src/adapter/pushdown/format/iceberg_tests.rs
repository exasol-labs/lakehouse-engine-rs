use super::super::super::test_support::sample_storage;
use super::*;
use lakehouse_catalog::ConnectionCreds;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Credentials whose SigV4 mode makes the `loadTable` GET the only request the
/// resolution issues, so a single-shot loopback catalog answers the whole run.
fn one_request_sigv4_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "signing-access-key".into(),
        secret_key: "signing-secret-key".into(),
        session_token: None,
        path_style: true,
        use_sigv4: true,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// A `loadTable` response for a table with a location, a two-field schema, a
/// name-mapping property, and NO snapshot.
///
/// The absent snapshot is what keeps this a unit test: `TableScanBuilder::build`
/// answers an empty `TableScan` when `current_snapshot()` is `None`, so the
/// resolution returns its schema, root, and name mapping without reading a single
/// object from the store the location names.
fn snapshotless_load_table_body() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000003",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "label", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": [],
            "properties": {
                "schema.name-mapping.default":
                    "[{\"field-id\":1,\"names\":[\"id\"]},{\"field-id\":2,\"names\":[\"label\"]}]"
            }
        }
    })
    .to_string()
}

/// A single-shot loopback catalog serving `body` as its one `loadTable`
/// response; answers its own base URI and the task serving it.
async fn loopback_catalog(body: String) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let port = listener.local_addr().expect("local_addr").port();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _n = stream.read(&mut buf).await.expect("read");

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.expect("write");
    });

    (format!("http://127.0.0.1:{port}"), server)
}

/// Scenario: Iceberg planning is byte-identical through the new seam.
///
/// Driven twice against loopback catalogs serving the IDENTICAL `loadTable`
/// response — once through `resolve_file_list` directly and once through the
/// reader — every resolved value matches field for field and the Delta block is
/// absent, so an Iceberg request's serialized spec cannot change through the
/// seam.
#[tokio::test]
async fn iceberg_reader_returns_resolve_file_lists_result_with_no_delta_block() {
    let creds = one_request_sigv4_creds();
    let storage = sample_storage();
    let catalog_props = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.t".into(),
    };

    let (uri, server) = loopback_catalog(snapshotless_load_table_body()).await;
    let session = CatalogSession::resolve(&uri, &creds.warehouse, &creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(&session, &catalog_props, &storage, &creds, true, None)
            .await
            .expect("the shipped Iceberg path must resolve the loopback catalog's table");
    server
        .await
        .expect("the loopback catalog fake must serve its one response without panicking");

    let (seam_uri, seam_server) = loopback_catalog(snapshotless_load_table_body()).await;
    let seam_session = CatalogSession::resolve(&seam_uri, &creds.warehouse, &creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let reader = IcebergFormatReader {
        session: &seam_session,
        catalog_props: &catalog_props,
        connection: ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    };
    let resolved = reader
        .resolve_scan(None)
        .await
        .expect("the seam must resolve the same table the shipped path resolved");

    assert_eq!(
        resolved.logical_schema, logical_schema,
        "the seam must carry the shipped path's logical schema verbatim"
    );
    assert!(
        !resolved.logical_schema.is_empty(),
        "the fixture's two-field schema must reach the comparison, or it proves nothing"
    );
    assert_eq!(
        resolved.files, files,
        "the seam must carry the shipped path's file list verbatim"
    );
    assert_eq!(
        resolved.effective_storage, effective_storage,
        "the seam must carry the shipped path's effective storage verbatim"
    );
    assert_eq!(
        resolved.table_root, table_root,
        "the seam must carry the shipped path's table root verbatim"
    );
    assert_eq!(
        resolved.name_mapping, name_mapping,
        "the seam must carry the shipped path's name mapping verbatim"
    );
    assert!(
        !resolved.name_mapping.is_empty(),
        "the fixture's name-mapping property must reach the comparison, or it proves nothing"
    );
    assert_eq!(
        resolved.delta, None,
        "an Iceberg scan must carry no Delta block, so its encoding stays byte-identical"
    );

    // Joined LAST: a reader that resolved nothing fails an assertion above rather
    // than blocking here on a catalog request it never issued.
    seam_server
        .await
        .expect("the loopback catalog fake must serve its one response without panicking");
}
