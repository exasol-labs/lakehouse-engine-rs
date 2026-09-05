use super::*;
use crate::scan::sealed::seal_storage;
use crate::scan::spec::{
    CommonScanSpec, JoinSpec, JoinType, ScanStorage, StorageBackend, StorageProps,
};
use exasol_udf_sdk::connect_back::ConnectionObject;
use std::cell::RefCell;

/// The CONNECTION name every fixture references unless it is testing a second
/// side or a missing grant.
const CONNECTION: &str = "LAKEHOUSE_CATALOG_CREDS";

/// A CONNECTION password carrying every storage field the nine-field projection
/// reads, so a resolved backend is distinguishable from a partially-populated one.
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

/// Stub `UdfContext` resolving each CONNECTION name it was built with and
/// REFUSING every other name — the two outcomes the resolution path branches on.
///
/// It answers `connection()` alone: resolution reads no input column and emits
/// no row, so every other trait method is a trap rather than a fixture.
struct StubConnections(Vec<(&'static str, String)>);

impl StubConnections {
    fn one(name: &'static str, password: String) -> Self {
        Self(vec![(name, password)])
    }

    /// A context holding no CONNECTION at all — every read is refused, the shape
    /// a deployment without the script-scoped grant presents.
    fn none() -> Self {
        Self(Vec::new())
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
        self.0
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
}

/// An S3 backend carrying `secret`, the value a resolved side is recognised by.
fn s3_backend(secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "AKIDEXAMPLE".into(),
        secret_key: secret.into(),
        ..Default::default()
    })
}

/// A single-table common spec whose fact side carries `storage`.
fn common_with(storage: ScanStorage) -> CommonScanSpec {
    CommonScanSpec {
        storage,
        ..Default::default()
    }
}

/// An `Inline` variant is already resolved: it passes its own backend through
/// untouched, contacting no CONNECTION at all (the stub holds none, so any read
/// would fail).
#[test]
fn an_inline_variant_resolves_to_its_own_backend() {
    let backend = s3_backend("INLINESECRET");

    let resolved = resolve_scan_storage(
        &common_with(ScanStorage::Inline(backend.clone())),
        &StubConnections::none(),
    )
    .expect("an inline variant needs no CONNECTION");

    assert_eq!(
        resolved.primary(),
        &backend,
        "an inline variant must resolve to the backend it carries"
    );
    assert!(
        resolved.join().is_none(),
        "a single-table spec resolves no dimension side"
    );
}

/// A `Connection` variant carries no credential: the backend comes from the
/// password `ctx.connection()` returns for the referenced name.
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
    assert_eq!(
        props.secret_key, "RESOLVEDSECRET",
        "the secret must come from the CONNECTION password, not the wire spec"
    );
    assert_eq!(
        props.endpoint, "http://minio:9000",
        "the addressing must be re-derived from the same CONNECTION read"
    );
    assert!(
        props.allow_http,
        "the wire's allow_http consent gate must reach the resolved backend"
    );
}

/// A CONNECTION the scan cannot read fails the whole resolution with an error
/// naming the connection and the missing access — never a fallback to a partial
/// or empty credential, and never a panic.
#[test]
fn unresolvable_connection_errors_without_falling_back() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });

    let error = resolve_scan_storage(&common, &StubConnections::none())
        .expect_err("an unreadable CONNECTION must fail the resolution");

    let text = error.to_string();
    assert!(
        text.contains(CONNECTION),
        "the error must name the connection it could not read: {text}"
    );
    assert!(
        text.contains("ACCESS ON CONNECTION"),
        "the error must name the grant the scan script is missing: {text}"
    );
}

/// A CONNECTION password that is not a JSON object — either not JSON at all, or
/// valid JSON of some other shape — is refused rather than read as an
/// absent-everything credential, the partial-credential fallback the module doc
/// forbids. The error names the connection and states the actual defect, and
/// echoes no part of the password.
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
        .expect_err(&format!(
            "a non-object password ({bad_password:?}) must not resolve"
        ));

        let text = error.to_string();
        assert!(
            text.contains(CONNECTION),
            "the error must name the connection: {text}"
        );
        assert!(
            text.contains("not a JSON object"),
            "the error must state the password is not a JSON object: {text}"
        );
        assert!(
            !text.contains(bad_password),
            "the error must not echo the password: {text}"
        );
    }
}

/// The schema and name a deployment that installed the scan script under its own
/// identity reports through the handshake metadata.
const DEPLOYED_SCRIPT_SCHEMA: &str = "LAKEHOUSE_OPS";
const DEPLOYED_SCRIPT_NAME: &str = "LH_SCAN_V2";

/// [`StubConnections`] plus a reported script identity, and NOTHING else.
///
/// Wrapping rather than restating the trait impl keeps the reported script
/// schema/name the only difference between this fixture and the one the
/// placeholder-fallback half of the test drives — so the two halves cannot
/// diverge in any other respect.
struct DeployedScript(StubConnections);

impl UdfContext for DeployedScript {
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
    fn get(&self, col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        self.0.get(col)
    }
    fn emit(&mut self, values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        self.0.emit(values)
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        self.0.next()
    }
    fn connection(&self, name: &str) -> Result<ConnectionObject, UdfError> {
        self.0.connection(name)
    }
    fn script_schema(&self) -> String {
        DEPLOYED_SCRIPT_SCHEMA.to_string()
    }
    fn script_name(&self) -> String {
        DEPLOYED_SCRIPT_NAME.to_string()
    }
}

/// The refusal's `FOR SCRIPT` fragment names the deployment's OWN scan script,
/// and keeps the placeholder pair only when the host reports neither value.
///
/// The message exists to be RUN, so an unresolved `<schema>.LAKEHOUSE_SCAN`
/// handed to a deployment that renamed the script or installed it in a non-default
/// schema is guidance naming a script that does not exist there. Both values ride
/// on the very context this path already holds, and both default to the empty
/// string on a context that does not report them — which is why the placeholder
/// stays the fallback rather than being dropped.
#[test]
fn the_missing_grant_refusal_names_the_deployed_scan_script() {
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });

    let reported = resolve_scan_storage(&common, &DeployedScript(StubConnections::none()))
        .expect_err("an unreadable CONNECTION must fail the resolution");
    assert!(
        reported.to_string().contains(&format!(
            "FOR SCRIPT {DEPLOYED_SCRIPT_SCHEMA}.{DEPLOYED_SCRIPT_NAME}"
        )),
        "the grant must name the script the host reports: {reported}"
    );

    let unreported = resolve_scan_storage(&common, &StubConnections::none())
        .expect_err("an unreadable CONNECTION must fail the resolution");
    assert!(
        unreported
            .to_string()
            .contains("FOR SCRIPT <schema>.LAKEHOUSE_SCAN"),
        "a host reporting neither value must leave the placeholder pair intact: {unreported}"
    );
}

// ---------------------------------------------------------------------------
// Both sides, in one step
// ---------------------------------------------------------------------------

/// The dimension side's CONNECTION, distinct from the fact side's — a vended or
/// per-table credential is scoped to the table it was resolved for.
const DIM_CONNECTION: &str = "LAKEHOUSE_DIM_CREDS";

/// A join common spec whose two sides carry their own `ScanStorage` values.
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

/// A join spec's two sides resolve in ONE step, EACH through its own reference —
/// so a dimension side is never served the fact side's credential.
#[test]
fn join_spec_resolves_two_sides_in_one_step() {
    let common = common_with_join(
        ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: false,
        },
        ScanStorage::Connection {
            name: DIM_CONNECTION.into(),
            allow_http: false,
        },
    );
    let ctx = StubConnections(vec![
        (CONNECTION, password("FACTSIDESECRET")),
        (DIM_CONNECTION, password("DIMSIDESECRET")),
    ]);

    let resolved = resolve_scan_storage(&common, &ctx).expect("both references resolve");

    assert_eq!(
        secret_key_of(resolved.primary()),
        "FACTSIDESECRET",
        "the fact side must resolve through its own reference"
    );
    assert_eq!(
        secret_key_of(
            resolved
                .join()
                .expect("a join spec resolves a dimension side")
        ),
        "DIMSIDESECRET",
        "the dimension side must resolve through ITS own reference, not the fact side's"
    );
}

/// The redaction set a join scan reads is the UNION of both RESOLVED sides. A
/// fact-side-only set would leave the dimension side's credential printable in an
/// error raised while that side's store was in scope.
#[test]
fn join_secret_set_is_the_union_of_both_resolved_sides() {
    let common = common_with_join(
        ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: false,
        },
        ScanStorage::Connection {
            name: DIM_CONNECTION.into(),
            allow_http: false,
        },
    );
    let ctx = StubConnections(vec![
        (CONNECTION, password("FACTSIDESECRET")),
        (DIM_CONNECTION, password("DIMSIDESECRET")),
    ]);

    let resolved = resolve_scan_storage(&common, &ctx).expect("both references resolve");
    let secrets = resolved.all_secret_values();

    assert!(
        secrets.contains(&"FACTSIDESECRET"),
        "the union must carry the fact side's secret: {secrets:?}"
    );
    assert!(
        secrets.contains(&"DIMSIDESECRET"),
        "the union must carry the dimension side's secret: {secrets:?}"
    );
}

// ---------------------------------------------------------------------------
// The resolved backend is the adapter's own selection
// ---------------------------------------------------------------------------

/// The backend the scan derives from a CONNECTION is FIELD-FOR-FIELD the one the
/// adapter's `storage_block` derives from the same password.
///
/// The two sides of the wire now build the store from independent reads of one
/// CONNECTION. If they applied different rules, the scan would read through a
/// different store than the adapter planned against — the failure mode that made
/// carrying no addressing on the wire the right decision, and the one this test
/// pins.
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

        assert_eq!(
            scan_side.primary(),
            &adapter_side,
            "the scan's derivation must equal the adapter's for allow_http={allow_http}"
        );
    }
}

/// A resolved `Connection` variant yields a NON-EMPTY secret set. An empty one
/// would disarm every redaction feed site while every one of them still compiled.
#[test]
fn connection_variant_yields_a_non_empty_secret_set_after_resolution() {
    let resolved = resolve_scan_storage(
        &common_with(ScanStorage::Connection {
            name: CONNECTION.into(),
            allow_http: false,
        }),
        &StubConnections::one(CONNECTION, password("REDACTABLESECRET")),
    )
    .expect("the reference resolves");

    let secrets = resolved.all_secret_values();
    assert!(
        !secrets.is_empty(),
        "an empty set here would disarm every redaction feed site silently"
    );
    assert!(
        secrets.contains(&"REDACTABLESECRET"),
        "a resolved reference must contribute its secret to the redaction set: {secrets:?}"
    );
}

// ---------------------------------------------------------------------------
// The sealed vended envelope
// ---------------------------------------------------------------------------

/// A `Sealed` variant resolves to exactly the backend that was sealed, opened
/// with the key derived from the SAME CONNECTION password the adapter sealed
/// under.
///
/// The vended secret (`VENDEDSECRET`) is deliberately different from the
/// CONNECTION's own standing one (`STANDINGSECRET`): the envelope's plaintext is
/// the catalog-vended credential, and the password is only key material.
#[test]
fn sealed_variant_resolves_to_the_backend_that_was_sealed() {
    let vended = s3_backend("VENDEDSECRET");
    let raw = password("STANDINGSECRET");
    let payload = seal_storage(&vended, &derive_sealed_storage_key(&raw))
        .expect("sealing a backend must succeed");

    let resolved = resolve_scan_storage(
        &common_with(ScanStorage::Sealed {
            name: CONNECTION.into(),
            payload,
        }),
        &StubConnections::one(CONNECTION, raw),
    )
    .expect("the sealing password must open the envelope");

    assert_eq!(
        resolved.primary(),
        &vended,
        "the envelope must open to the vended backend, not to the CONNECTION's own"
    );
}

/// A CONNECTION rotated between the adapter's read and the scan's own read
/// derives a different key, so the envelope will not open.
///
/// The outcome must be a NAMED failure and never a fallback: reading a stale or
/// partial credential is the silent degradation the sealed path exists to
/// prevent. The error names the connection and the operation that failed, and
/// quotes neither password, nor the payload, nor the sealed credential.
#[test]
fn a_rotated_connection_fails_sealed_decryption_with_a_named_error() {
    let vended = s3_backend("VENDEDSECRET");
    let before_rotation = password("BEFOREROTATION");
    let payload = seal_storage(&vended, &derive_sealed_storage_key(&before_rotation))
        .expect("sealing a backend must succeed");

    let error = resolve_scan_storage(
        &common_with(ScanStorage::Sealed {
            name: CONNECTION.into(),
            payload: payload.clone(),
        }),
        &StubConnections::one(CONNECTION, password("AFTERROTATION")),
    )
    .expect_err("a rotated password must not open the envelope");

    let text = error.to_string();
    assert!(
        text.contains(CONNECTION),
        "the error must name the connection whose envelope would not open: {text}"
    );
    assert!(
        text.contains("AES-256-GCM authentication"),
        "the error must name the unseal operation that failed: {text}"
    );
    for leaked in ["BEFOREROTATION", "AFTERROTATION", "VENDEDSECRET", &payload] {
        assert!(
            !text.contains(leaked),
            "the error must not echo a password, a payload, or a credential: {text}"
        );
    }
}

/// A resolved `Sealed` variant yields a NON-EMPTY secret set: the vended
/// credential the envelope carried is exactly what an error on the vended path
/// must be redacted against.
#[test]
fn sealed_variant_yields_a_non_empty_secret_set_after_resolution() {
    let vended = s3_backend("VENDEDSECRET");
    let raw = password("STANDINGSECRET");
    let payload = seal_storage(&vended, &derive_sealed_storage_key(&raw))
        .expect("sealing a backend must succeed");

    let resolved = resolve_scan_storage(
        &common_with(ScanStorage::Sealed {
            name: CONNECTION.into(),
            payload,
        }),
        &StubConnections::one(CONNECTION, raw),
    )
    .expect("the sealing password must open the envelope");

    let secrets = resolved.all_secret_values();
    assert!(
        !secrets.is_empty(),
        "an empty set here would leave a vended credential printable in an error"
    );
    assert!(
        secrets.contains(&"VENDEDSECRET"),
        "the UNSEALED credential is what the redaction set must carry: {secrets:?}"
    );
    assert!(
        !secrets.contains(&"STANDINGSECRET"),
        "the CONNECTION's own standing secret was never resolved into a backend here, \
         so it must not appear in the set: {secrets:?}"
    );
}

// ---------------------------------------------------------------------------
// A rotation mid-query is observed per resolution
// ---------------------------------------------------------------------------

/// Stub `UdfContext` serving a DIFFERENT password on each successive read, the
/// last one for every read thereafter — a CONNECTION rotated between two shards'
/// resolutions.
struct RotatingConnection(RefCell<Vec<String>>);

impl UdfContext for RotatingConnection {
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
    fn connection(&self, _name: &str) -> Result<ConnectionObject, UdfError> {
        let mut remaining = self.0.borrow_mut();
        let password = if remaining.len() > 1 {
            remaining.remove(0)
        } else {
            remaining
                .first()
                .cloned()
                .expect("the rotating fixture must hold at least one password")
        };
        Ok(ConnectionObject {
            kind: "PASSWORD".into(),
            address: "http://catalog.example.com".into(),
            user: String::new(),
            password,
        })
    }
}

/// Each resolution reads the CONNECTION's value CURRENT AT THAT MOMENT, so a
/// rotation landing between two shards' reads is observable rather than hidden.
///
/// The engine cannot close that window — every shard resolves the CONNECTION
/// itself, which is what removes the second source for the backend — so the
/// behaviour is specified and tested rather than papered over.
#[test]
fn each_resolution_reads_the_connection_value_current_at_that_moment() {
    let ctx = RotatingConnection(RefCell::new(vec![
        password("BEFOREROTATION"),
        password("AFTERROTATION"),
    ]));
    let common = common_with(ScanStorage::Connection {
        name: CONNECTION.into(),
        allow_http: false,
    });

    let first = resolve_scan_storage(&common, &ctx).expect("the first read resolves");
    let second = resolve_scan_storage(&common, &ctx).expect("the second read resolves");

    assert_eq!(secret_key_of(first.primary()), "BEFOREROTATION");
    assert_eq!(
        secret_key_of(second.primary()),
        "AFTERROTATION",
        "a resolution after the rotation must observe the rotated value"
    );
}

/// The `secret_key` an S3 backend carries — the field a resolved side is told
/// apart by.
fn secret_key_of(backend: &StorageBackend) -> &str {
    let StorageBackend::S3(props) = backend else {
        panic!("these fixtures resolve S3 backends");
    };
    &props.secret_key
}
