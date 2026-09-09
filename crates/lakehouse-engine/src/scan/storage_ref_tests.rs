use super::*;
use crate::scan::sealed::seal_storage;
use crate::scan::spec::{
    CommonScanSpec, JoinSpec, JoinType, ScanStorage, StorageBackend, StorageProps,
};
use exasol_udf_sdk::connect_back::ConnectionObject;
use exasol_udf_sdk::test_support::TestContext;

const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

fn password(secret: &str) -> String {
    serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://minio:9000",
        "region": "us-east-1",
        "access_key": "AKIDEXAMPLE",
        "secret_key": secret,
        "path_style": true,
    })
    .to_string()
}

fn s3_backend(secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "AKIDEXAMPLE".into(),
        secret_key: secret.into(),
        ..Default::default()
    })
}

fn common_with(storage: ScanStorage) -> CommonScanSpec {
    CommonScanSpec {
        storage,
        ..Default::default()
    }
}

fn conn_obj(password: String) -> ConnectionObject {
    ConnectionObject {
        kind: "PASSWORD".into(),
        address: "http://catalog.example.com".into(),
        user: String::new(),
        password,
    }
}

#[test]
fn scan_storage_variants_resolve_or_fail_correctly() {
    let backend = s3_backend("INLINESECRET");

    let inline = resolve_scan_storage(
        &common_with(ScanStorage::Inline(backend.clone())),
        &TestContext::scalar(vec![]),
    )
    .expect("inline");
    assert_eq!(inline.primary(), &backend);
    assert!(inline.join().is_none());

    let conn = resolve_scan_storage(
        &common_with(ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: true,
        }),
        &TestContext::scalar(vec![])
            .with_connection(CONNECTION, conn_obj(password("RESOLVEDSECRET"))),
    )
    .expect("connection");
    let StorageBackend::S3(props) = conn.primary() else {
        panic!("expected S3")
    };
    assert_eq!(props.secret_key, "RESOLVEDSECRET");
    assert!(props.allow_http);

    let conn_spec = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });
    let err =
        resolve_scan_storage(&conn_spec, &TestContext::scalar(vec![])).expect_err("unreadable");
    let text = err.to_string();
    assert!(
        text.contains(CONNECTION) && text.contains("ACCESS ON CONNECTION"),
        "{text}"
    );

    let deployed = resolve_scan_storage(
        &conn_spec,
        &TestContext::scalar(vec![])
            .with_script_schema("LAKEHOUSE_OPS")
            .with_script_name("LH_SCAN_V2"),
    )
    .expect_err("unreadable");
    assert!(
        deployed
            .to_string()
            .contains("FOR SCRIPT LAKEHOUSE_OPS.LH_SCAN_V2"),
        "{deployed}"
    );
    let fallback =
        resolve_scan_storage(&conn_spec, &TestContext::scalar(vec![])).expect_err("unreadable");
    assert!(
        fallback
            .to_string()
            .contains("FOR SCRIPT <schema>.LAKEHOUSE_SCAN"),
        "{fallback}"
    );

    for bad in ["not json at all", "[]"] {
        let err = resolve_scan_storage(
            &common_with(ScanStorage::Connection {
                name: CONNECTION.into(),
                allow_http: false,
            }),
            &TestContext::scalar(vec![]).with_connection(CONNECTION, conn_obj(bad.into())),
        )
        .expect_err("non-object");
        let text = err.to_string();
        assert!(
            text.contains(CONNECTION) && text.contains("not a JSON object") && !text.contains(bad),
            "{text}"
        );
    }

    let vended = s3_backend("VENDEDSECRET");
    let raw = password("STANDINGSECRET");
    let payload = seal_storage(&vended, &derive_sealed_storage_key(&raw)).expect("seal");
    let sealed = resolve_scan_storage(
        &common_with(ScanStorage::Sealed {
            name: CONNECTION.into(),
            payload,
        }),
        &TestContext::scalar(vec![]).with_connection(CONNECTION, conn_obj(raw)),
    )
    .expect("sealed");
    assert_eq!(sealed.primary(), &vended);
    let secrets = sealed.all_secret_values();
    assert!(
        secrets.contains(&"VENDEDSECRET") && !secrets.contains(&"STANDINGSECRET"),
        "{secrets:?}"
    );
}

#[test]
fn join_spec_resolves_two_sides_independently() {
    let dim_conn = "LAKEHOUSE_DIM_CREDS";
    let ctx = TestContext::scalar(vec![])
        .with_connection(CONNECTION, conn_obj(password("FACTSECRET")))
        .with_connection(dim_conn, conn_obj(password("DIMSECRET")));
    let mut common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });
    common.join = Some(JoinSpec {
        storage: ScanStorage::Connection {
            name: dim_conn.into(),
            allow_http: false,
        },
        table_root: "s3://dim/db/dim".into(),
        condition: "\"F\" = \"D\"".into(),
        join_type: JoinType::Inner,
        files: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        post_join_limit: None,
        partition_columns: Vec::new(),
    });

    let resolved = resolve_scan_storage(&common, &ctx).expect("both resolve");
    assert_eq!(secret_key_of(resolved.primary()), "FACTSECRET");
    assert_eq!(secret_key_of(resolved.join().expect("dim")), "DIMSECRET");
    let secrets = resolved.all_secret_values();
    assert!(
        secrets.contains(&"FACTSECRET") && secrets.contains(&"DIMSECRET"),
        "{secrets:?}"
    );
}

fn secret_key_of(backend: &StorageBackend) -> &str {
    let StorageBackend::S3(props) = backend else {
        panic!("these fixtures resolve S3 backends");
    };
    &props.secret_key
}
