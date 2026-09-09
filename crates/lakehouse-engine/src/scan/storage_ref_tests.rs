use super::*;
use crate::scan::sealed::seal_storage;
use crate::scan::spec::{
    CommonScanSpec, JoinSpec, JoinType, ScanStorage, StorageBackend, StorageProps,
};
use exasol_udf_sdk::connect_back::ConnectionObject;

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

struct StubConnections {
    connections: Vec<(&'static str, String)>,
    script_schema: Option<&'static str>,
    script_name: Option<&'static str>,
}

impl StubConnections {
    fn one(name: &'static str, password: String) -> Self {
        Self {
            connections: vec![(name, password)],
            script_schema: None,
            script_name: None,
        }
    }

    fn none() -> Self {
        Self {
            connections: Vec::new(),
            script_schema: None,
            script_name: None,
        }
    }

    fn with_script(mut self, schema: &'static str, name: &'static str) -> Self {
        self.script_schema = Some(schema);
        self.script_name = Some(name);
        self
    }
}

impl UdfContext for StubConnections {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::User("stub".into()))
    }
    fn emit(&mut self, _: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Err(UdfError::User("stub".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(&self, name: &str) -> Result<ConnectionObject, UdfError> {
        self.connections
            .iter()
            .find(|(h, _)| *h == name)
            .map(|(_, pw)| ConnectionObject {
                kind: "PASSWORD".into(),
                address: "http://catalog.example.com".into(),
                user: String::new(),
                password: pw.clone(),
            })
            .ok_or_else(|| {
                UdfError::ConnectBack(format!(
                    "insufficient privileges for using connection {name} in script LAKEHOUSE_SCAN"
                ))
            })
    }
    fn script_schema(&self) -> String {
        self.script_schema.unwrap_or_default().to_string()
    }
    fn script_name(&self) -> String {
        self.script_name.unwrap_or_default().to_string()
    }
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

#[test]
fn scan_storage_variants_resolve_or_fail_correctly() {
    let backend = s3_backend("INLINESECRET");

    let inline = resolve_scan_storage(
        &common_with(ScanStorage::Inline(backend.clone())),
        &StubConnections::none(),
    )
    .expect("inline");
    assert_eq!(inline.primary(), &backend);
    assert!(inline.join().is_none());

    let conn = resolve_scan_storage(
        &common_with(ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: true,
        }),
        &StubConnections::one(CONNECTION, password("RESOLVEDSECRET")),
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
    let err = resolve_scan_storage(&conn_spec, &StubConnections::none()).expect_err("unreadable");
    let text = err.to_string();
    assert!(
        text.contains(CONNECTION) && text.contains("ACCESS ON CONNECTION"),
        "{text}"
    );

    let deployed = resolve_scan_storage(
        &conn_spec,
        &StubConnections::none().with_script("LAKEHOUSE_OPS", "LH_SCAN_V2"),
    )
    .expect_err("unreadable");
    assert!(
        deployed
            .to_string()
            .contains("FOR SCRIPT LAKEHOUSE_OPS.LH_SCAN_V2"),
        "{deployed}"
    );
    let fallback =
        resolve_scan_storage(&conn_spec, &StubConnections::none()).expect_err("unreadable");
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
            &StubConnections::one(CONNECTION, bad.into()),
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
        &StubConnections::one(CONNECTION, raw),
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
    let ctx = StubConnections {
        connections: vec![
            (CONNECTION, password("FACTSECRET")),
            (dim_conn, password("DIMSECRET")),
        ],
        script_schema: None,
        script_name: None,
    };
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
