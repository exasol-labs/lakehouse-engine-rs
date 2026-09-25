use super::*;
use crate::scan::runtime::{DEFAULT_BUDGET_BYTES, MIN_POOL_FLOOR_BYTES};
use crate::scan::spec::{
    DeleteMechanism, DeltaDeletionVectorStorage, JoinSpec, JoinType, ScanStorage, StorageProps,
};
use crate::scan::test_support::{inline_resolved, minimal_spec};
use ::object_store::ClientConfigKey;
use arrow::datatypes::DataType;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::memory_pool::MemoryLimit;

fn bucket_url(bucket: &str) -> Url {
    Url::parse(&format!("s3://{bucket}")).expect("bucket URL must parse")
}

fn store_registered(ctx: &SessionContext, bucket: &str) -> bool {
    ctx.runtime_env()
        .object_store_registry
        .get_store(&bucket_url(bucket))
        .is_ok()
}

/// Same whole-spec budget and union redaction set `build_session_context` passes every side.
fn build_fact_side(spec: &ScanSpec) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let resolved = inline_resolved(spec);
    let sides = present_sides(spec, &resolved);
    build_side_store(
        &sides[0],
        spec.common.s3_max_connections,
        &resolved.all_secret_values(),
    )
}

fn abfss_spec(fact_root: &str, dim_root: &str) -> ScanSpec {
    let mut spec = spec_with_join(dim_root, vec![FileEntry::new("data/dim-0.parquet", 64)]);
    spec.common.table_root = fact_root.into();
    spec.files = vec![FileEntry::new("data/fact-0.parquet", 128)];
    spec
}

fn spec_with_join(dim_root: &str, dim_files: Vec<FileEntry>) -> ScanSpec {
    let mut spec = minimal_spec();
    spec.common.join = Some(JoinSpec {
        table_root: dim_root.into(),
        files: dim_files,
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: spec.common.storage.clone(),
    });
    spec
}

/// Path-style (the `StorageProps` default) is required: the S3 arm applies `endpoint` only then.
fn s3_backend(endpoint: &str, secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        access_key: "testkey".into(),
        secret_key: secret.into(),
        allow_http: true,
        ..Default::default()
    })
}

/// Records which endpoint a read reached; every request is refused, as only the destination is tested.
struct RecordingEndpoint {
    url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
}

impl RecordingEndpoint {
    async fn bind() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A 4xx is not retried, so each read reaches the endpoint once and fails fast.
        const REFUSAL: &[u8] =
            b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the loopback endpoint must bind");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("bound endpoint has an address")
        );
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));

        let recorded = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut head = vec![0u8; 4096];
                let read = stream.read(&mut head).await.unwrap_or(0);
                recorded
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(String::from_utf8_lossy(&head[..read]).into_owned());
                let _ = stream.write_all(REFUSAL).await;
            }
        });

        Self { url, requests }
    }

    fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("the recorder must not be poisoned")
            .clone()
    }
}

/// Scenario: a positive memory limit sizes the pool at fraction × (limit − overhead).
#[test]
fn session_context_sizes_pool_from_ctx_limit() {
    let limit: u64 = 2 * 1024 * 1024 * 1024;
    let spec = minimal_spec();
    let overhead_bytes = spec.common.instance_overhead_mb * 1024 * 1024;
    let net = limit - overhead_bytes;
    let expected_budget = (net as f64 * spec.common.memory_pool_fraction) as usize;
    let ctx =
        build_session_context(&spec, &inline_resolved(&spec), limit).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, expected_budget,
            "pool budget must be fraction × (limit − overhead)"
        ),
        _ => panic!("expected Finite pool limit"),
    }
}

/// Scenario: a zero memory limit uses the default pool budget.
#[test]
fn session_context_uses_default_budget_on_zero_limit() {
    let spec = minimal_spec();
    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, DEFAULT_BUDGET_BYTES as usize,
            "pool budget must equal the 1 GiB default when limit is unknown (0)"
        ),
        _ => panic!("expected Finite pool limit"),
    }
}

/// Scenario: non-default fraction and overhead in the spec flow through to pool sizing.
#[test]
fn memory_budget_round_trips_into_scan_spec() {
    let mut spec = minimal_spec();
    spec.common.memory_pool_fraction = 0.5;
    spec.common.instance_overhead_mb = 256;
    let limit: u64 = 4 * 1024 * 1024 * 1024;
    let overhead_bytes = 256_u64 * 1024 * 1024;
    let net = limit - overhead_bytes;
    let expected = (net as f64 * 0.5_f64) as usize;
    let ctx =
        build_session_context(&spec, &inline_resolved(&spec), limit).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, expected,
            "pool budget must be 0.5 × (4 GiB − 256 MiB); got {actual}, expected {expected}"
        ),
        _ => panic!("expected Finite pool limit"),
    }
    assert!(
        expected > MIN_POOL_FLOOR_BYTES as usize,
        "expected budget must exceed the floor"
    );
}

/// Scenario: the connection budget becomes the per-host warm-connection-pool ceiling.
#[test]
fn client_options_carry_connection_budget() {
    let opts = client_options_for(32);
    assert_eq!(
        opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
        Some("32".to_string()),
        "client options must carry the resolved connection budget as pool_max_idle_per_host"
    );
}

/// Scenario: a zero budget clamps to at least 1.
#[test]
fn client_options_clamp_budget_to_at_least_one() {
    let opts = client_options_for(0);
    assert_eq!(
        opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
        Some("1".to_string()),
        "a zero budget must clamp to at least 1"
    );
}

/// Scenario: every side's store gets the whole-spec connection budget, never a per-side share.
#[test]
fn each_side_store_gets_the_full_connection_budget() {
    // Non-default: the shared fixture uses 8.
    const BUDGET: usize = 16;

    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    spec.common.s3_max_connections = BUDGET;

    let resolved = inline_resolved(&spec);
    let sides = present_sides(&spec, &resolved);
    assert_eq!(sides.len(), 2, "a join spec must present both sides");
    let all_secrets = resolved.all_secret_values();

    for side in &sides {
        let budget = spec.common.s3_max_connections;
        let described = build_side_store(side, budget, &all_secrets)
            .unwrap_or_else(|e| {
                panic!(
                    "the {} side's store must build with the whole-spec budget: {e}",
                    side.label
                )
            })
            .to_string();

        assert!(
            described.starts_with("SpecSizedObjectStore(AmazonS3("),
            "the {} side must get its own sized S3 store: {described}",
            side.label
        );
        assert_eq!(
            client_options_for(budget).get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
            Some(BUDGET.to_string()),
            "the {} side must receive the whole-spec budget of {BUDGET}, not a per-side share",
            side.label
        );
    }
}

/// Scenario: `build_table_root_store` returns the unwrapped store; `build_side_store` wraps the same store.
#[test]
fn the_table_root_store_is_the_unwrapped_store_a_scan_side_wraps() {
    let spec = minimal_spec();
    let resolved = inline_resolved(&spec);
    let sides = present_sides(&spec, &resolved);
    let all_secrets = resolved.all_secret_values();
    let budget = spec.common.s3_max_connections;
    let table_root = "s3://test-bucket/data";

    let raw = build_table_root_store(sides[0].backend, table_root, budget, &all_secrets)
        .expect("a table root alone must build a store");
    let decorated =
        build_side_store(&sides[0], budget, &all_secrets).expect("decorated store must build");

    assert!(
        !raw.to_string().starts_with("SpecSizedObjectStore("),
        "the undecorated builder must not wrap in SpecSizedObjectStore: {raw}"
    );
    assert_eq!(
        decorated.to_string(),
        format!("SpecSizedObjectStore({raw})"),
        "build_side_store must wrap the undecorated store unchanged"
    );
}

/// Scenario: `build_session_context` registers the checked division under vs-expression's exported name.
#[test]
fn build_session_context_registers_the_checked_float_div_function() {
    let spec = minimal_spec();

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    let registered = ctx
        .state()
        .udf(vs_expression::CHECKED_FLOAT_DIV_FN)
        .unwrap_or_else(|e| {
            panic!(
                "a session built for a spec must resolve {}: {e}",
                vs_expression::CHECKED_FLOAT_DIV_FN
            )
        });
    assert_eq!(
        registered
            .return_type(&[DataType::Float64, DataType::Float64])
            .expect("the checked division must declare a return type"),
        DataType::Float64,
        "the registered function must return DOUBLE, so the renderer needs no \
         CAST of its own around the call"
    );
}

/// Scenario: two sides in distinct buckets each get their own registered store.
#[test]
fn join_sides_in_two_buckets_register_two_stores() {
    let spec = spec_with_join(
        "s3://dim-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    for bucket in ["test-bucket", "dim-bucket"] {
        assert!(
            store_registered(&ctx, bucket),
            "a store must be resolvable for {bucket}"
        );
    }
}

/// Scenario: an `s3a://` side registers under its own `s3a://` registry URL.
#[test]
fn an_s3a_scheme_side_registers_a_store_under_its_own_key() {
    let mut spec = minimal_spec();
    spec.files = vec![FileEntry::new(
        "s3a://test-bucket/data/part-0.parquet",
        1024,
    )];
    let expected = Url::parse("s3a://test-bucket").expect("URL must parse");

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&expected)
            .is_ok(),
        "the s3a:// side's store must be resolvable under its own registered key"
    );
}

/// `AzureAccessKey::try_new` rejects non-base64 keys before the store registers.
const VALID_ACCOUNT_KEY: &str = "c3RhdGljLWFjY291bnQta2V5";

fn adls_backend(cred: AdlsCred) -> StorageBackend {
    StorageBackend::Adls {
        account_name: "acct".into(),
        cred,
    }
}

fn adls_spec(table_root: &str, cred: AdlsCred) -> ScanSpec {
    let mut spec = minimal_spec();
    spec.common.storage = ScanStorage::Inline(adls_backend(cred));
    spec.common.table_root = table_root.into();
    spec.files = vec![FileEntry::new("data/part-0.parquet", 1)];
    spec
}

fn adls_spec_with_join(fact_root: &str, dim_root: &str, cred: AdlsCred) -> ScanSpec {
    let mut spec = adls_spec(fact_root, cred);
    spec.common.join = Some(JoinSpec {
        table_root: dim_root.into(),
        files: vec![FileEntry::new("data/dim-0.parquet", 64)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: spec.common.storage.clone(),
    });
    spec
}

/// Scenario: DataFusion's registry drops the container, so `get_store` succeeds for any container of the host.
#[test]
fn an_azure_side_registers_under_a_container_qualified_url_the_registry_key_drops() {
    let spec = adls_spec(
        "abfss://container@acct.dfs.core.windows.net/db/table",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    let registered =
        Url::parse("abfss://container@acct.dfs.core.windows.net").expect("URL must parse");
    assert_eq!(
        side_store_url(&spec.files, &spec.common.table_root).expect("store URL must derive"),
        registered,
        "the URL a side is registered under must keep its container"
    );

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&registered)
            .is_ok(),
        "the container-qualified store must be resolvable"
    );

    let other_container_same_account =
        Url::parse("abfss://other@acct.dfs.core.windows.net").expect("URL must parse");
    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&other_container_same_account)
            .is_ok(),
        "the registry key drops the container, so a DIFFERENT container of the same \
         account resolves to the SAME store — exactly the collision \
         validate_sides_share_one_store exists to reject"
    );
}

/// Scenario: two sides in the same container register one routing store.
#[test]
fn azure_sides_in_one_container_share_one_routing_store() {
    let spec = adls_spec_with_join(
        "abfss://container@acct.dfs.core.windows.net/db/fact",
        "abfss://container@acct.dfs.core.windows.net/db/dim",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    let described = ctx
        .runtime_env()
        .object_store_registry
        .get_store(
            &Url::parse("abfss://container@acct.dfs.core.windows.net").expect("URL must parse"),
        )
        .expect("the container's store must be registered")
        .to_string();

    assert!(
        described.starts_with("PrefixRoutingObjectStore("),
        "both sides of the container must be served by one routing store: {described}"
    );
    for side in ["fact=", "dimension="] {
        assert!(
            described.contains(side),
            "the router must hold an inner store for {side}: {described}"
        );
    }
}

/// Scenario: two sides in different storage accounts each register their own store.
#[test]
fn azure_sides_in_different_accounts_register_two_stores() {
    let spec = adls_spec_with_join(
        "abfss://facts@acct1.dfs.core.windows.net/db/fact",
        "abfss://dims@acct2.dfs.core.windows.net/db/dim",
        AdlsCred::Sas("sv=2021&sig=static-sas-signature".into()),
    );

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    for account in [
        "abfss://facts@acct1.dfs.core.windows.net",
        "abfss://dims@acct2.dfs.core.windows.net",
    ] {
        assert!(
            ctx.runtime_env()
                .object_store_registry
                .get_store(&Url::parse(account).expect("URL must parse"))
                .is_ok(),
            "each account's side must register its own store: {account}"
        );
    }
}

/// Scenario: an `s3://` fact side and an `abfss://` dimension side each register their own store.
#[test]
fn sides_on_different_backends_each_register_their_own_store() {
    const DIM_ROOT: &str = "abfss://dims@acct.dfs.core.windows.net/db/dim";
    let mut spec = spec_with_join(DIM_ROOT, vec![FileEntry::new("data/dim-0.parquet", 64)]);
    spec.common
        .join
        .as_mut()
        .expect("spec_with_join sets a join block")
        .storage = ScanStorage::Inline(adls_backend(AdlsCred::Sas(
        "sv=2021&sig=static-sas-signature".into(),
    )));

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    assert!(
        store_registered(&ctx, "test-bucket"),
        "the S3 fact side must register under its own bucket key"
    );
    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(
                &Url::parse("abfss://dims@acct.dfs.core.windows.net").expect("URL must parse")
            )
            .is_ok(),
        "the Azure dimension side must register under its own account key"
    );
}

/// Scenario: an `abfs://` side registers a store like `abfss://`.
#[test]
fn an_abfs_scheme_side_registers_a_store_under_its_own_key() {
    let spec = adls_spec(
        "abfs://container@acct.dfs.core.windows.net/db/table",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    let expected =
        Url::parse("abfs://container@acct.dfs.core.windows.net").expect("URL must parse");

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&expected)
            .is_ok(),
        "the abfs:// side's store must be resolvable under its own registered key"
    );
}

/// Scenario: an unrecognised Azure host fails loud at `build()` with no credential in the error.
#[test]
fn an_unrecognised_azure_host_is_rejected_redacted() {
    let secret = "static-account-key";
    let spec = adls_spec(
        "abfss://container@sovereign.example.com/db/table",
        AdlsCred::AccountKey(secret.into()),
    );

    let err =
        build_fact_side(&spec).expect_err("an unrecognised Azure host suffix must be rejected");

    let UdfError::User(msg) = err else {
        panic!("an unrecognised host is caller input, not an internal fault");
    };
    assert!(
        !msg.contains(secret),
        "the error must not leak the account key: {msg}"
    );
}

/// Scenario: an empty dimension file list registers only the fact side.
#[test]
fn join_with_empty_dimension_file_list_registers_only_the_fact_side() {
    let spec = spec_with_join("s3://dim-bucket/db/dim", Vec::new());

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0)
        .expect("an empty dimension file list must not fail the session build");

    assert!(
        store_registered(&ctx, "test-bucket"),
        "the fact side must still be registered"
    );
    assert!(
        !store_registered(&ctx, "dim-bucket"),
        "an empty dimension file list must register no dimension store"
    );
}

/// Scenario: a same-bucket join registers one routing store holding an inner store per side.
#[test]
fn a_shared_bucket_join_registers_one_routing_store_over_both_sides() {
    let spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");

    let described = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the shared bucket's store must be registered")
        .to_string();

    assert!(
        described.starts_with("PrefixRoutingObjectStore("),
        "the shared bucket's one store must be the router: {described}"
    );
    for side in ["fact=", "dimension="] {
        assert!(
            described.contains(side),
            "the router must hold an inner store for {side}: {described}"
        );
    }
}

/// Scenario: each side of a shared-bucket join answers HEADs from its own size index.
#[tokio::test]
async fn shared_bucket_join_answers_each_sides_head_from_that_sides_index() {
    use ::object_store::ObjectStoreExt;
    const DIM_SIZE: u64 = 4242;
    const FACT_SIZE: u64 = 1024;

    let spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", DIM_SIZE)],
    );
    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");
    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the shared-bucket store must be registered");

    for (path, expected) in [
        ("data/part-0.parquet", FACT_SIZE),
        ("db/dim/data/dim-0.parquet", DIM_SIZE),
    ] {
        // Bounded so a fall-through to the unreachable endpoint fails fast instead of retrying.
        let meta = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.head(&ObjectStorePath::from(path)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the HEAD of {path} must be answered from the spec, not over the network")
        })
        .unwrap_or_else(|e| panic!("head of the indexed file {path} must succeed: {e}"));

        assert_eq!(
            meta.size, expected,
            "the routed store must answer {path}'s size from its own side's index"
        );
    }
}

/// Scenario: each side's size index holds exactly its own files.
#[test]
fn each_side_size_index_holds_only_its_own_files() {
    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![
            FileEntry::new("data/dim-0.parquet", 11),
            FileEntry::new("data/dim-1.parquet", 22),
        ],
    );
    spec.common.table_root = "s3://test-bucket/db/fact".into();
    spec.files = vec![
        FileEntry::new("data/fact-0.parquet", 33),
        FileEntry::new("data/fact-1.parquet", 44),
    ];

    let resolved = inline_resolved(&spec);
    let sides = present_sides(&spec, &resolved);
    assert_eq!(sides.len(), 2, "a join spec must present both sides");

    for (side, expected) in sides.iter().zip([
        HashMap::from([
            (ObjectStorePath::from("db/fact/data/fact-0.parquet"), 33_u64),
            (ObjectStorePath::from("db/fact/data/fact-1.parquet"), 44_u64),
        ]),
        HashMap::from([
            (ObjectStorePath::from("db/dim/data/dim-0.parquet"), 11_u64),
            (ObjectStorePath::from("db/dim/data/dim-1.parquet"), 22_u64),
        ]),
    ]) {
        let index = side_size_index(side.files, side.table_root)
            .unwrap_or_else(|e| panic!("the {} side's index must build: {e}", side.label));
        assert_eq!(
            index, expected,
            "the {} side's index must hold exactly its own files, and no other side's",
            side.label
        );
    }
}

/// Scenario: a spec without a join registers the plain spec-sized store answering HEADs without I/O.
#[tokio::test]
async fn a_spec_without_a_join_registers_one_sized_store_over_its_own_files() {
    use ::object_store::ObjectStoreExt;

    let mut spec = minimal_spec();
    spec.files = vec![
        FileEntry::new("s3://test-bucket/data/part-0.parquet", 1024),
        FileEntry::new("s3://test-bucket/data/part-1.parquet", 2048),
    ];

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");
    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the one side's store must be registered");

    let described = store.to_string();
    assert!(
        described.starts_with("SpecSizedObjectStore(AmazonS3("),
        "a spec with no join block must register its sized store directly, unrouted: \
         {described}"
    );

    for (path, expected) in [("data/part-0.parquet", 1024), ("data/part-1.parquet", 2048)] {
        // Bounded so a fall-through to the unreachable endpoint fails fast instead of retrying.
        let meta = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.head(&ObjectStorePath::from(path)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the HEAD of {path} must be answered from the spec, not over the network")
        })
        .unwrap_or_else(|e| panic!("head of the indexed file {path} must succeed: {e}"));

        assert_eq!(
            meta.size, expected,
            "the sized store must answer {path}'s size from its own index"
        );
    }
}

/// Scenario: a path neither side owns is refused naming the path and both sides, without credentials.
#[tokio::test]
async fn an_unroutable_path_is_refused_naming_no_credential() {
    use ::object_store::ObjectStoreExt;

    const FACT_SECRET: &str = "FACTSIDESECRETVALUE";
    const DIM_SECRET: &str = "DIMSIDESECRETVALUE";
    const UNROUTABLE: &str = "elsewhere/x.parquet";

    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    spec.common.storage = ScanStorage::Inline(s3_backend("http://localhost:9000", FACT_SECRET));
    spec.common
        .join
        .as_mut()
        .expect("the spec carries a join block")
        .storage = ScanStorage::Inline(s3_backend("http://localhost:9000", DIM_SECRET));

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");
    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the shared bucket's store must be registered");

    let error = store
        .get(&ObjectStorePath::from(UNROUTABLE))
        .await
        .expect_err("a path neither side owns must be refused, never routed to a side");

    let message = error.to_string();
    for named in [UNROUTABLE, "fact", "dimension", "db/dim"] {
        assert!(
            message.contains(named),
            "the refusal must name '{named}': {message}"
        );
    }
    for secret in [FACT_SECRET, DIM_SECRET] {
        assert!(
            !message.contains(secret),
            "the refusal must not leak either side's credential: {message}"
        );
    }
}

/// Scenario: a delete file under a different store root is rejected naming "delete file".
#[test]
fn a_delete_file_under_a_different_root_is_rejected() {
    let files = vec![FileEntry::with_deletes(
        "s3://bucket-a/data/f1.parquet",
        1,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: "s3://bucket-b/deletes/f1-deletes.parquet".to_string(),
            size: 1,
        }],
    )];

    let error = validate_uniform_object_store_files(&files, "", "s3://bucket-a/data/f0.parquet")
        .expect_err("a delete file under a different root must be rejected");

    assert!(
        matches!(error, UdfError::User(ref m) if m.contains("delete file")),
        "the refusal must name 'delete file': {error:?}"
    );
}

/// Scenario: a deletion vector is never checked against the side's store root.
#[test]
fn a_deletion_vector_is_not_checked_against_the_object_store_root() {
    let files = vec![FileEntry::with_deletes(
        "s3://bucket-a/data/f1.parquet",
        1,
        vec![DeleteMechanism::DeltaDeletionVector {
            storage: DeltaDeletionVectorStorage::UuidRelative,
            path_or_inline_dv: "not-a-uri-token".to_string(),
            offset: None,
            size_in_bytes: 1,
            cardinality: 1,
        }],
    )];

    validate_uniform_object_store_files(&files, "", "s3://bucket-a/data/f0.parquet")
        .expect("a deletion vector must not be checked against the object-store root");
}

/// Scenario: each side's inner store is built from that side's own backend (observed via endpoint reached).
#[tokio::test]
async fn each_side_inner_store_is_built_from_its_own_backend() {
    use ::object_store::ObjectStoreExt;

    let fact_endpoint = RecordingEndpoint::bind().await;
    let dimension_endpoint = RecordingEndpoint::bind().await;

    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    spec.common.storage =
        ScanStorage::Inline(s3_backend(&fact_endpoint.url, "FACTSIDESECRETVALUE"));
    spec.common
        .join
        .as_mut()
        .expect("the spec carries a join block")
        .storage = ScanStorage::Inline(s3_backend(&dimension_endpoint.url, "DIMSIDESECRETVALUE"));

    let ctx = build_session_context(&spec, &inline_resolved(&spec), 0).expect("build must succeed");
    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the shared bucket's one store must be registered");

    for (label, path, own, other) in [
        (
            "fact",
            "data/part-0.parquet",
            &fact_endpoint,
            &dimension_endpoint,
        ),
        (
            "dimension",
            "db/dim/data/dim-0.parquet",
            &dimension_endpoint,
            &fact_endpoint,
        ),
    ] {
        let other_before = other.requests().len();

        // Bounded: a store with neither endpoint would retry a refused connection for minutes.
        let _refused = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            store.get(&ObjectStorePath::from(path)),
        )
        .await
        .unwrap_or_else(|_| panic!("the {label} side's read must reach an endpoint, not hang"));

        assert!(
            own.requests().iter().any(|request| request.contains(path)),
            "the {label} side's read must reach the {label} side's OWN endpoint; \
             it received {:?}",
            own.requests()
        );
        assert_eq!(
            other.requests().len(),
            other_before,
            "the {label} side's read must NOT reach the other side's endpoint; \
             it received {:?}",
            other.requests()
        );
    }
}

/// Scenario: the size index is keyed by the `ListingTableUrl` prefix for relative and absolute entries.
#[test]
fn size_index_keys_by_listing_url_prefix() {
    let mut spec = minimal_spec();
    spec.common.table_root = "s3://bucket/db/table".into();
    spec.files = vec![
        FileEntry::new("data/rel.parquet", 111),
        FileEntry::new("s3://bucket/db/table/data/abs.parquet", 222),
    ];
    let index = side_size_index(&spec.files, &spec.common.table_root).expect("index must build");

    let rel_key = ObjectStorePath::from("db/table/data/rel.parquet");
    let abs_key = ObjectStorePath::from("db/table/data/abs.parquet");
    assert_eq!(index.get(&rel_key), Some(&111));
    assert_eq!(index.get(&abs_key), Some(&222));

    // The prefix DataFusion 54 hands to head().
    let rel_url = ListingTableUrl::parse("s3://bucket/db/table/data/rel.parquet").unwrap();
    assert_eq!(rel_url.prefix(), &rel_key);
}

/// Scenario: an Adls size-index key is relative to the container-scoped store root.
#[test]
fn spec_size_index_keys_an_abfss_file_without_its_container() {
    let mut spec = adls_spec(
        "abfss://container@account.dfs.core.windows.net/path/to",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    spec.files = vec![FileEntry::new("file.parquet", 999)];
    let index = side_size_index(&spec.files, &spec.common.table_root).expect("index must build");

    let key = ObjectStorePath::from("path/to/file.parquet");
    assert_eq!(
        index.get(&key),
        Some(&999),
        "index must key the file relative to the store root, excluding the container"
    );

    let url = ListingTableUrl::parse(
        "abfss://container@account.dfs.core.windows.net/path/to/file.parquet",
    )
    .unwrap();
    assert_eq!(url.prefix(), &key);
}

/// Scenario: the store URL derives from the first file's reconstructed absolute URI.
#[test]
fn side_store_url_returns_the_same_url_for_s3_as_the_deleted_bucket_derivation() {
    let rel = vec![FileEntry::new("data/part-0.parquet", 1)];
    assert_eq!(
        side_store_url(&rel, "s3://warehouse/db/table").unwrap(),
        bucket_url("warehouse")
    );

    let abs = vec![FileEntry::new("s3://legacy-bucket/data/part-0.parquet", 1)];
    assert_eq!(
        side_store_url(&abs, "").unwrap(),
        bucket_url("legacy-bucket")
    );
}

/// Scenario: the store URL keeps the file's own scheme (e.g. `s3a`), matching DataFusion's lookup.
#[test]
fn side_store_url_preserves_the_s3a_scheme_so_the_key_matches_the_lookup() {
    let files = vec![FileEntry::new("data/part-0.parquet", 1)];
    let derived = side_store_url(&files, "s3a://warehouse/db/table")
        .expect("an s3a file list must yield a store URL");
    assert_eq!(derived.as_str(), "s3a://warehouse");

    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&derived, Arc::new(::object_store::memory::InMemory::new()));
    let lookup = ListingTableUrl::parse("s3a://warehouse/db/table/data/part-0.parquet")
        .expect("the file URI must parse")
        .object_store();
    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(lookup.as_ref())
            .is_ok(),
        "the store must be resolvable under the key the scan looks up"
    );
}

/// Scenario: two `abfss://` sides in different containers of one account are rejected; same container or different accounts pass.
#[test]
fn validate_sides_share_one_store_rejects_two_containers_in_one_account() {
    let colliding = abfss_spec(
        "abfss://facts@acct.dfs.core.windows.net/db/fact",
        "abfss://dims@acct.dfs.core.windows.net/db/dim",
    );
    let err = validate_sides_share_one_store(&colliding)
        .expect_err("two containers of one storage account must be rejected");
    assert!(
        matches!(err, UdfError::User(_)),
        "a colliding spec is caller input, not an internal fault; got {err:?}"
    );

    validate_sides_share_one_store(&abfss_spec(
        "abfss://facts@acct.dfs.core.windows.net/db/fact",
        "abfss://facts@acct.dfs.core.windows.net/db/dim",
    ))
    .expect("two sides in one container need one store and must be accepted");

    validate_sides_share_one_store(&abfss_spec(
        "abfss://facts@acct.dfs.core.windows.net/db/fact",
        "abfss://dims@other.dfs.core.windows.net/db/dim",
    ))
    .expect("sides in different storage accounts must be accepted");
}

/// Scenario: the container-collision precondition never fires on S3.
#[test]
fn validate_sides_share_one_store_accepts_every_s3_spec_shape() {
    let dim_files = vec![FileEntry::new("data/dim-0.parquet", 64)];
    for (shape, spec) in [
        ("no join", minimal_spec()),
        (
            "join in the fact bucket",
            spec_with_join("s3://test-bucket/db/dim", dim_files.clone()),
        ),
        (
            "join in another bucket",
            spec_with_join("s3://dim-bucket/db/dim", dim_files),
        ),
        (
            "join with an empty file list",
            spec_with_join("s3://dim-bucket/db/dim", Vec::new()),
        ),
    ] {
        validate_sides_share_one_store(&spec)
            .unwrap_or_else(|e| panic!("the '{shape}' S3 shape must be accepted, got {e:?}"));
    }
}

/// Scenario: the wrapper answers HEAD from the size index and delegates unknown paths and data reads.
#[tokio::test]
async fn sized_store_serves_head_from_index_and_delegates_otherwise() {
    use ::object_store::ObjectStoreExt;
    use ::object_store::memory::InMemory;

    // Empty inner store: a successful head can only come from the size index.
    let inner = Arc::new(InMemory::new());
    let known = ObjectStorePath::from("db/table/data/f.parquet");
    let mut sizes = HashMap::new();
    sizes.insert(known.clone(), 4096u64);
    let store = SpecSizedObjectStore::new(inner, sizes);

    let meta = store
        .head(&known)
        .await
        .expect("head of a known path must be served from the index");
    assert_eq!(meta.size, 4096);
    assert_eq!(meta.location, known);
    assert!(meta.e_tag.is_none());
    assert!(meta.version.is_none());

    let unknown = ObjectStorePath::from("db/table/data/missing.parquet");
    assert!(
        matches!(
            store.head(&unknown).await,
            Err(::object_store::Error::NotFound { .. })
        ),
        "an unindexed path must delegate to the inner store"
    );

    // Synthetic metadata must never satisfy an actual byte read.
    assert!(
        matches!(
            store.get(&known).await,
            Err(::object_store::Error::NotFound { .. })
        ),
        "a data read must delegate to the inner store, not the size index"
    );
}

/// Scenario: a direct-storage store is wrapped in a LimitStore, not SpecSizedObjectStore.
#[test]
fn build_admission_limited_store_wraps_the_s3_backend_in_a_limit_store() {
    let backend = s3_backend("http://s3.example.com", "secret");
    let store_url = Url::parse("s3://test-bucket").expect("URL must parse");

    let store = build_admission_limited_store(&backend, &store_url, &[])
        .expect("an S3 backend must build an admission-limited store");

    assert!(
        store
            .to_string()
            .starts_with(&format!("LimitStore({DIRECT_STORAGE_ADMISSION_LIMIT}, ")),
        "expected a LimitStore capped at {DIRECT_STORAGE_ADMISSION_LIMIT}, got: {store}"
    );
    assert!(
        !store.to_string().starts_with("SpecSizedObjectStore("),
        "a direct-storage store must not be wrapped in SpecSizedObjectStore: {store}"
    );
}

#[test]
fn build_admission_limited_store_wraps_the_adls_backend_in_a_limit_store() {
    let backend = adls_backend(AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()));
    let store_url =
        Url::parse("abfss://container@acct.dfs.core.windows.net").expect("URL must parse");

    let store = build_admission_limited_store(&backend, &store_url, &[])
        .expect("an Adls backend must build an admission-limited store");

    assert!(
        store
            .to_string()
            .starts_with(&format!("LimitStore({DIRECT_STORAGE_ADMISSION_LIMIT}, ")),
        "expected a LimitStore capped at {DIRECT_STORAGE_ADMISSION_LIMIT}, got: {store}"
    );
}
