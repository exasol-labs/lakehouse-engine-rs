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
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::User("the resolution path reads no column".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Err(UdfError::User("the resolution path emits no row".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(&self, name: &str) -> Result<ConnectionObject, UdfError> {
        self.connections
            .iter()
            .find(|(held, _)| *held == name)
            .map(|(_, password)| ConnectionObject {
                kind: "PASSWORD".into(),
                address: "http://catalog.example.com".into(),
                user: String::new(),
                password: password.clone(),
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
fn an_inline_variant_resolves_to_its_own_backend() {
    let backend = s3_backend("INLINESECRET");
    let resolved = resolve_scan_storage(
        &common_with(ScanStorage::Inline(backend.clone())),
        &StubConnections::none(),
    )
    .expect("an inline variant needs no CONNECTION");

    assert_eq!(resolved.primary(), &backend);
    assert!(resolved.join().is_none());
}

#[test]
fn connection_reference_resolves_through_ctx_connection() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: true,
    });
    let resolved = resolve_scan_storage(
        &common,
        &StubConnections::one(CONNECTION, password("RESOLVEDSECRET")),
    )
    .expect("the referenced CONNECTION resolves");

    let StorageBackend::S3(props) = resolved.primary() else {
        panic!("an S3 password must resolve to an S3 backend");
    };
    assert_eq!(props.secret_key, "RESOLVEDSECRET");
    assert_eq!(props.endpoint, "http://minio:9000");
    assert!(props.allow_http);
}

#[test]
fn unresolvable_connection_errors_without_falling_back() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });
    let error = resolve_scan_storage(&common, &StubConnections::none())
        .expect_err("an unreadable CONNECTION must fail the resolution");

    let text = error.to_string();
    assert!(text.contains(CONNECTION), "{text}");
    assert!(text.contains("ACCESS ON CONNECTION"), "{text}");
}

#[test]
fn a_non_object_connection_password_is_refused_without_falling_back() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });
    for bad_password in ["not json at all", "[]"] {
        let error = resolve_scan_storage(
            &common,
            &StubConnections::one(CONNECTION, bad_password.into()),
        )
        .expect_err(&format!("non-object password ({bad_password:?}) must not resolve"));

        let text = error.to_string();
        assert!(text.contains(CONNECTION), "{text}");
        assert!(text.contains("not a JSON object"), "{text}");
        assert!(!text.contains(bad_password), "{text}");
    }
}

const DEPLOYED_SCRIPT_SCHEMA: &str = "LAKEHOUSE_OPS";
const DEPLOYED_SCRIPT_NAME: &str = "LH_SCAN_V2";

#[test]
fn the_missing_grant_refusal_names_the_deployed_scan_script() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });

    let reported = resolve_scan_storage(
        &common,
        &StubConnections::none().with_script(DEPLOYED_SCRIPT_SCHEMA, DEPLOYED_SCRIPT_NAME),
    )
    .expect_err("an unreadable CONNECTION must fail the resolution");
    assert!(
        reported.to_string().contains(&format!(
            "FOR SCRIPT {DEPLOYED_SCRIPT_SCHEMA}.{DEPLOYED_SCRIPT_NAME}"
        )),
        "{reported}"
    );

    let unreported = resolve_scan_storage(&common, &StubConnections::none())
        .expect_err("an unreadable CONNECTION must fail the resolution");
    assert!(
        unreported
            .to_string()
            .contains("FOR SCRIPT <schema>.LAKEHOUSE_SCAN"),
        "{unreported}"
    );
}

// --- Both sides, in one step ---

const DIM_CONNECTION: &str = "LAKEHOUSE_DIM_CREDS";

fn common_with_join(fact: ScanStorage, dimension: ScanStorage) -> CommonScanSpec {
    let mut common = common_with(fact);
    common.join = Some(JoinSpec {
        table_root: "s3://dim-bucket/db/dim".into(),
        files: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: dimension,
    });
    common
}

fn two_side_ctx() -> StubConnections {
    StubConnections {
        connections: vec![
            (CONNECTION, password("FACTSIDESECRET")),
            (DIM_CONNECTION, password("DIMSIDESECRET")),
        ],
        script_schema: None,
        script_name: None,
    }
}

fn two_side_common() -> CommonScanSpec {
    common_with_join(
        ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: false,
        },
        ScanStorage::Connection {
            name: DIM_CONNECTION.into(),
            allow_http: false,
        },
    )
}

#[test]
fn join_spec_resolves_two_sides_independently() {
    let resolved =
        resolve_scan_storage(&two_side_common(), &two_side_ctx()).expect("both references resolve");

    assert_eq!(secret_key_of(resolved.primary()), "FACTSIDESECRET");
    assert_eq!(
        secret_key_of(resolved.join().expect("dimension side must resolve")),
        "DIMSIDESECRET",
    );

    // Union secret set covers both sides.
    let secrets = resolved.all_secret_values();
    assert!(secrets.contains(&"FACTSIDESECRET"), "{secrets:?}");
    assert!(secrets.contains(&"DIMSIDESECRET"), "{secrets:?}");
}

// --- Adapter/scan parity ---

#[test]
fn resolved_backend_equals_the_adapter_storage_block() {
    use crate::adapter::catalog_kind::CatalogKind;
    use crate::adapter::connection::{read_connection, storage_block};

    for allow_http in [false, true] {
        let raw = password("SHAREDSECRET");
        let ctx = StubConnections::one(CONNECTION, raw);
        let adapter_side = storage_block(
            &read_connection(&ctx, Some(CONNECTION), CatalogKind::IcebergRest)
                .expect("the fixture password is an acceptable CONNECTION")
                .creds,
            allow_http,
        );
        let scan_side = resolve_scan_storage(
            &common_with(ScanStorage::Connection {
                name: CONNECTION.into(),
                allow_http,
            }),
            &ctx,
        )
        .expect("the reference resolves");

        assert_eq!(scan_side.primary(), &adapter_side);
    }
}

#[test]
fn resolved_variants_yield_correct_backends_and_secret_sets() {
    // Connection variant: resolves and has non-empty secret set.
    let conn_resolved = resolve_scan_storage(
        &common_with(ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: false,
        }),
        &StubConnections::one(CONNECTION, password("REDACTABLESECRET")),
    )
    .expect("the reference resolves");
    assert!(conn_resolved.all_secret_values().contains(&"REDACTABLESECRET"));

    // Sealed variant: resolves to the sealed backend; secret set carries the
    // unsealed credential, not the standing password.
    let vended = s3_backend("VENDEDSECRET");
    let raw = password("STANDINGSECRET");
    let payload = seal_storage(&vended, &derive_sealed_storage_key(&raw))
        .expect("sealing must succeed");
    let sealed_resolved = resolve_scan_storage(
        &common_with(ScanStorage::Sealed {
            name: CONNECTION.into(),
            payload,
        }),
        &StubConnections::one(CONNECTION, raw),
    )
    .expect("the sealing password must open the envelope");
    assert_eq!(sealed_resolved.primary(), &vended);
    let secrets = sealed_resolved.all_secret_values();
    assert!(secrets.contains(&"VENDEDSECRET"), "{secrets:?}");
    assert!(!secrets.contains(&"STANDINGSECRET"), "{secrets:?}");
}


fn secret_key_of(backend: &StorageBackend) -> &str {
    let StorageBackend::S3(props) = backend else {
        panic!("these fixtures resolve S3 backends");
    };
    &props.secret_key
}
