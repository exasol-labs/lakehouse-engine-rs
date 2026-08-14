use super::*;
use crate::scan::runtime::{DEFAULT_BUDGET_BYTES, MIN_POOL_FLOOR_BYTES};
use crate::scan::spec::{
    DeleteMechanism, DeltaDeletionVectorStorage, JoinSpec, JoinType, StorageProps,
};
use crate::scan::test_support::minimal_spec;
use ::object_store::ClientConfigKey;
use datafusion::execution::memory_pool::MemoryLimit;

/// The store key an S3 bucket is registered under.
fn bucket_url(bucket: &str) -> Url {
    Url::parse(&format!("s3://{bucket}")).expect("bucket URL must parse")
}

/// Whether the session's registry holds a store under `bucket`'s key.
fn store_registered(ctx: &SessionContext, bucket: &str) -> bool {
    ctx.runtime_env()
        .object_store_registry
        .get_store(&bucket_url(bucket))
        .is_ok()
}

/// Build the FACT side's store through the seam under test, with the same
/// whole-spec connection budget and union redaction set `build_session_context`
/// passes to every side.
fn build_fact_side(spec: &ScanSpec) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let sides = present_sides(spec);
    build_side_store(
        &sides[0],
        spec.common.s3_max_connections,
        &spec.common.all_secret_values(),
    )
}

/// A two-sided spec rooted at the given `abfss://` locations, one relative
/// file per side — the shape the container-collision precondition rules on.
fn abfss_spec(fact_root: &str, dim_root: &str) -> ScanSpec {
    let mut spec = spec_with_join(dim_root, vec![FileEntry::new("data/dim-0.parquet", 64)]);
    spec.common.table_root = fact_root.into();
    spec.files = vec![FileEntry::new("data/fact-0.parquet", 128)];
    spec
}

/// `minimal_spec` (fact side in `test-bucket`) plus a broadcast-join dimension
/// side rooted at `dim_root` — the shape driving the second registration.
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
        storage: spec.common.storage.clone(),
    });
    spec
}

/// An S3 backend reaching `endpoint`, carrying `secret` as its secret key — the
/// two externally observable fields a test tells one side's store from the
/// other's by. Path-style (the `StorageProps` default) is what makes `endpoint`
/// reachable at all: the S3 arm applies it only for path-style stores.
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

/// A loopback endpoint recording the requests it receives, so WHICH endpoint a
/// read reached is observable. Every request is answered with a refusal: the
/// read is expected to fail — only its destination is under test.
struct RecordingEndpoint {
    url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
}

impl RecordingEndpoint {
    async fn bind() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A 4xx is not retried by object_store, so each read reaches the
        // endpoint exactly once and fails fast.
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

/// A positive memory limit causes the DataFusion pool to be sized at fraction × (limit − overhead).
/// Uses minimal_spec defaults: fraction=0.6, overhead=200 MiB.
#[test]
fn session_context_sizes_pool_from_ctx_limit() {
    let limit: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
    let spec = minimal_spec();
    let overhead_bytes = spec.common.instance_overhead_mb * 1024 * 1024;
    let net = limit - overhead_bytes;
    let expected_budget = (net as f64 * spec.common.memory_pool_fraction) as usize;
    let ctx = build_session_context(&spec, limit).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, expected_budget,
            "pool budget must be fraction × (limit − overhead)"
        ),
        _ => panic!("expected Finite pool limit"),
    }
}

/// A zero memory limit causes the DataFusion pool to use the conservative default budget.
#[test]
fn session_context_uses_default_budget_on_zero_limit() {
    let ctx = build_session_context(&minimal_spec(), 0).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, DEFAULT_BUDGET_BYTES as usize,
            "pool budget must equal the 1 GiB default when limit is unknown (0)"
        ),
        _ => panic!("expected Finite pool limit"),
    }
}

/// Task 5.2: explicit non-default fraction/overhead in spec flow through to pool sizing.
///
/// Builds a spec with fraction=0.5 and overhead=256 MiB, calls build_session_context
/// with a known limit (4 GiB), and asserts the pool equals 0.5 × (4 GiB − 256 MiB).
/// This proves the values are read from the spec, not from hardcoded constants.
#[test]
fn memory_budget_round_trips_into_scan_spec() {
    let mut spec = minimal_spec();
    spec.common.memory_pool_fraction = 0.5;
    spec.common.instance_overhead_mb = 256;
    let limit: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB
    let overhead_bytes = 256_u64 * 1024 * 1024;
    let net = limit - overhead_bytes;
    let expected = (net as f64 * 0.5_f64) as usize;
    let ctx = build_session_context(&spec, limit).expect("build must succeed");
    match ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(actual) => assert_eq!(
            actual, expected,
            "pool budget must be 0.5 × (4 GiB − 256 MiB); got {actual}, expected {expected}"
        ),
        _ => panic!("expected Finite pool limit"),
    }
    // Verify this is NOT the MIN_POOL_FLOOR_BYTES (it should be much larger).
    assert!(
        expected > MIN_POOL_FLOOR_BYTES as usize,
        "expected budget must exceed the floor"
    );
}

/// The resolved connection budget is carried onto the object store's HTTP
/// client options as the warm-connection-pool ceiling per host.
#[test]
fn client_options_carry_connection_budget() {
    let opts = client_options_for(32);
    assert_eq!(
        opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
        Some("32".to_string()),
        "client options must carry the resolved connection budget as pool_max_idle_per_host"
    );
}

/// A zero budget clamps to at least 1 so the pool ceiling is never zero/negative.
#[test]
fn client_options_clamp_budget_to_at_least_one() {
    let opts = client_options_for(0);
    assert_eq!(
        opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
        Some("1".to_string()),
        "a zero budget must clamp to at least 1"
    );
}

/// EVERY side's store is built with the WHOLE-spec connection budget, never an
/// `N / side_count` share of it.
///
/// A built `AmazonS3` does not expose its pool configuration back out, so what
/// is pinned is the budget each side's call RECEIVES — read from the spec, never
/// a literal — together with `client_options_carry_connection_budget`'s mapping
/// of that value onto `PoolMaxIdlePerHost`. Exercised against the private
/// `build_side_store` seam directly so that function need not be `pub`.
#[test]
fn each_side_store_gets_the_full_connection_budget() {
    // Non-default: the shared fixture convention is 8.
    const BUDGET: usize = 16;

    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    spec.common.s3_max_connections = BUDGET;

    let sides = present_sides(&spec);
    assert_eq!(sides.len(), 2, "a join spec must present both sides");
    let all_secrets = spec.common.all_secret_values();

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

/// [`build_table_root_store`] answers the raw backend store from a table root ALONE,
/// not wrapped in [`SpecSizedObjectStore`] — the seam Delta planning needs, since
/// `_delta_log` file sizes are unknown until the log itself is read and the reader
/// holds no file list to derive a store root from. [`build_side_store`] must still
/// wrap that same store, unchanged, in [`SpecSizedObjectStore`].
#[test]
fn the_table_root_store_is_the_unwrapped_store_a_scan_side_wraps() {
    let spec = minimal_spec();
    let sides = present_sides(&spec);
    let all_secrets = spec.common.all_secret_values();
    let budget = spec.common.s3_max_connections;
    // Same bucket as the fixture's one file, so both builders resolve one store.
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

/// Two sides resolving to distinct buckets each get their OWN registered store:
/// the two buckets yield two DataFusion registry keys, so neither registration
/// overwrites the other and each side keeps its own credential.
#[test]
fn join_sides_in_two_buckets_register_two_stores() {
    let spec = spec_with_join(
        "s3://dim-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

    for bucket in ["test-bucket", "dim-bucket"] {
        assert!(
            store_registered(&ctx, bucket),
            "a store must be resolvable for {bucket}"
        );
    }
}

/// The S3 arm never inspects the file URI's scheme — it reads only the URL host
/// as the bucket name — so an `s3a://` side registers a store exactly like an
/// `s3://` one, keyed under its own `s3a://` registry URL.
#[test]
fn an_s3a_scheme_side_registers_a_store_under_its_own_key() {
    let mut spec = minimal_spec();
    spec.files = vec![FileEntry::new(
        "s3a://test-bucket/data/part-0.parquet",
        1024,
    )];
    let expected = Url::parse("s3a://test-bucket").expect("URL must parse");

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&expected)
            .is_ok(),
        "the s3a:// side's store must be resolvable under its own registered key"
    );
}

/// A syntactically valid (base64) account key: `MicrosoftAzureBuilder::build`
/// decodes the access key with `AzureAccessKey::try_new`, which rejects any
/// non-base64 fixture before the store ever gets far enough to register.
const VALID_ACCOUNT_KEY: &str = "c3RhdGljLWFjY291bnQta2V5";

/// An Azure backend with the given credential, for a fixed test account.
fn adls_backend(cred: AdlsCred) -> StorageBackend {
    StorageBackend::Adls {
        account_name: "acct".into(),
        cred,
    }
}

/// A one-sided Adls spec rooted at `table_root`, under the given credential.
fn adls_spec(table_root: &str, cred: AdlsCred) -> ScanSpec {
    let mut spec = minimal_spec();
    spec.common.storage = adls_backend(cred);
    spec.common.table_root = table_root.into();
    spec.files = vec![FileEntry::new("data/part-0.parquet", 1)];
    spec
}

/// A two-sided Adls spec: fact side at `fact_root`, dimension side at
/// `dim_root`, both read under the same credential.
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
        storage: spec.common.storage.clone(),
    });
    spec
}

/// [`side_store_url`]'s own return value carries the container (userinfo),
/// but DataFusion's registry key does not: `get_url_key`
/// (`datafusion-execution-54.1.0/src/object_store.rs:268-274`) keys only on
/// scheme, host and port, dropping userinfo. So `get_store` succeeds for ANY
/// container of the same account host, not just the one registered — this
/// asymmetry is exactly the collision `validate_sides_share_one_store` exists
/// to reject.
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

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

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

/// Two sides rooted in the SAME container of the same account share one registry
/// key, so ONE store is registered for both — the router holding an inner store
/// per side, exactly the S3 shared-bucket contract.
#[test]
fn azure_sides_in_one_container_share_one_routing_store() {
    let spec = adls_spec_with_join(
        "abfss://container@acct.dfs.core.windows.net/db/fact",
        "abfss://container@acct.dfs.core.windows.net/db/dim",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    let ctx = build_session_context(&spec, 0).expect("build must succeed");

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

/// Two sides in different storage ACCOUNTS differ at the registry-key level
/// (the host), so each registers its own store under its own URL.
#[test]
fn azure_sides_in_different_accounts_register_two_stores() {
    let spec = adls_spec_with_join(
        "abfss://facts@acct1.dfs.core.windows.net/db/fact",
        "abfss://dims@acct2.dfs.core.windows.net/db/dim",
        AdlsCred::Sas("sv=2021&sig=static-sas-signature".into()),
    );

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

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

/// A join across two BACKENDS — an `s3://` fact side and an `abfss://` dimension
/// side — registers one store per side. The scheme alone makes the two registry
/// keys differ, and [`build_side_store`] dispatches on each side's OWN backend, so
/// the Azure side is never addressed through an `AmazonS3Builder`.
///
/// This is the evidence that a cross-backend join is SERVEABLE, and therefore why
/// no plan-time guard refuses one: a join's sides are compared nowhere, because
/// each is read through its own store.
#[test]
fn sides_on_different_backends_each_register_their_own_store() {
    const DIM_ROOT: &str = "abfss://dims@acct.dfs.core.windows.net/db/dim";
    let mut spec = spec_with_join(DIM_ROOT, vec![FileEntry::new("data/dim-0.parquet", 64)]);
    spec.common
        .join
        .as_mut()
        .expect("spec_with_join sets a join block")
        .storage = adls_backend(AdlsCred::Sas("sv=2021&sig=static-sas-signature".into()));

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

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

/// `MicrosoftAzureBuilder::with_url` recognises `abfs` as a host-suffix-matched
/// scheme exactly like `abfss` (`object_store`'s `azure::builder::parse_url` matches
/// `"az" | "abfs" | "abfss"` identically), so an `abfs://` side must register a
/// store the same way.
#[test]
fn an_abfs_scheme_side_registers_a_store_under_its_own_key() {
    let spec = adls_spec(
        "abfs://container@acct.dfs.core.windows.net/db/table",
        AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
    );
    let expected =
        Url::parse("abfs://container@acct.dfs.core.windows.net").expect("URL must parse");

    let ctx = build_session_context(&spec, 0).expect("build must succeed");

    assert!(
        ctx.runtime_env()
            .object_store_registry
            .get_store(&expected)
            .is_ok(),
        "the abfs:// side's store must be resolvable under its own registered key"
    );
}

/// `MicrosoftAzureBuilder` accepts only four host suffixes
/// (`dfs`/`blob`.`core.windows.net`/`fabric.microsoft.com`). A host outside
/// that set must fail loud at `build()` with `UrlNotRecognised` — not collapse
/// silently to some other account — and the surfaced error must carry no
/// credential value, redacted by the same value-then-label pass as the S3 arm.
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

/// The dimension-side guard: an empty dimension file list registers only the
/// fact side. Without the guard, deriving a store key from no files fails the
/// whole session build — even though such a spec has no dimension store to
/// register, whatever root the join block names.
#[test]
fn join_with_empty_dimension_file_list_registers_only_the_fact_side() {
    let spec = spec_with_join("s3://dim-bucket/db/dim", Vec::new());

    let ctx = build_session_context(&spec, 0)
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

/// A join spec whose two sides resolve to ONE bucket registers ONE store under
/// that bucket's key, and that store is the router holding an inner store per
/// side. Routing is the only shape that can serve two credentials through a
/// registry key DataFusion derives from scheme, host and port alone.
#[test]
fn a_shared_bucket_join_registers_one_routing_store_over_both_sides() {
    let spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    let ctx = build_session_context(&spec, 0).expect("build must succeed");

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

/// Each side of a shared-bucket join is sized from its OWN index, reached
/// through the one registered routing store: a HEAD for either side's file is
/// answered from the spec, by that side's store. Were a side's index to hold
/// only the other side's files, its HEAD would fall through to the (unreachable)
/// endpoint instead of being answered locally.
#[tokio::test]
async fn shared_bucket_join_answers_each_sides_head_from_that_sides_index() {
    use ::object_store::ObjectStoreExt;
    const DIM_SIZE: u64 = 4242;
    const FACT_SIZE: u64 = 1024;

    let spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", DIM_SIZE)],
    );
    let ctx = build_session_context(&spec, 0).expect("build must succeed");
    let store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&bucket_url("test-bucket"))
        .expect("the shared-bucket store must be registered");

    for (path, expected) in [
        ("data/part-0.parquet", FACT_SIZE),
        ("db/dim/data/dim-0.parquet", DIM_SIZE),
    ] {
        // Bounded so a fall-through to the (unreachable) endpoint fails fast and
        // legibly instead of exhausting the object-store retry budget; a HEAD
        // served from the index does no I/O and never waits.
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

/// Each side's size index holds EXACTLY its own files. A whole-spec index — the
/// pre-fix shape — would let one side's credentialed store answer a HEAD for a
/// path only the other side is authorized to read.
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

    let sides = present_sides(&spec);
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

/// A spec with NO join block registers the plain spec-sized store — one
/// credential to serve, so nothing to route — over an index of its own files,
/// each of whose HEADs is answered from the spec without I/O.
#[tokio::test]
async fn a_spec_without_a_join_registers_one_sized_store_over_its_own_files() {
    use ::object_store::ObjectStoreExt;

    let mut spec = minimal_spec();
    spec.files = vec![
        FileEntry::new("s3://test-bucket/data/part-0.parquet", 1024),
        FileEntry::new("s3://test-bucket/data/part-1.parquet", 2048),
    ];

    let ctx = build_session_context(&spec, 0).expect("build must succeed");
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
        // Bounded so a fall-through to the (unreachable) endpoint fails fast
        // instead of exhausting the object-store retry budget.
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

/// A path NEITHER side owns is refused, naming the path and both sides while
/// carrying no credential value from either. The refusal is the router's
/// `object_store::Error` — the only error type an `ObjectStore` read can return
/// — and reaches the caller wrapped by DataFusion.
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
    spec.common.storage = s3_backend("http://localhost:9000", FACT_SECRET);
    spec.common
        .join
        .as_mut()
        .expect("the spec carries a join block")
        .storage = s3_backend("http://localhost:9000", DIM_SECRET);

    let ctx = build_session_context(&spec, 0).expect("build must succeed");
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

/// A delete file resolving to a different object-store root than the first file
/// is rejected, naming "delete file" so the message distinguishes it from a
/// mismatched data file.
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

/// A deletion vector names no object-store path of its own, so it is never
/// checked against the side's object-store root — even when `path_or_inline_dv`
/// is not a URI at all.
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

/// Each side's inner store is built from THAT side's OWN storage backend — the
/// falsifiable provenance gate: the defect built the dimension side's store from
/// `common.storage`, the FACT side's credential.
///
/// Two same-bucket sides pointing at DIFFERENT endpoints share one DataFusion
/// registry key, so the endpoint a read actually REACHES is the only observable
/// provenance — `AmazonS3`'s `Display` prints its bucket alone, which both sides
/// share. A data read is what reaches an endpoint at all: a HEAD is answered
/// from the size index without I/O.
#[tokio::test]
async fn each_side_inner_store_is_built_from_its_own_backend() {
    use ::object_store::ObjectStoreExt;

    let fact_endpoint = RecordingEndpoint::bind().await;
    let dimension_endpoint = RecordingEndpoint::bind().await;

    let mut spec = spec_with_join(
        "s3://test-bucket/db/dim",
        vec![FileEntry::new("data/dim-0.parquet", 64)],
    );
    spec.common.storage = s3_backend(&fact_endpoint.url, "FACTSIDESECRETVALUE");
    spec.common
        .join
        .as_mut()
        .expect("the spec carries a join block")
        .storage = s3_backend(&dimension_endpoint.url, "DIMSIDESECRETVALUE");

    let ctx = build_session_context(&spec, 0).expect("build must succeed");
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

        // Bounded: a store built with neither endpoint would retry a refused
        // connection for minutes rather than fail legibly.
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

/// 4.2: the size index is keyed by the object-store `Path` DataFusion passes
/// to `head` for an exact-file URL — i.e. the `ListingTableUrl` prefix. A
/// relative entry keys under the reconstructed path; an absolute entry keys
/// under its own path.
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

    // The keys equal what an exact-file ListingTableUrl reports as its prefix
    // (the value DataFusion 54 hands to head()).
    let rel_url = ListingTableUrl::parse("s3://bucket/db/table/data/rel.parquet").unwrap();
    assert_eq!(rel_url.prefix(), &rel_key);
}

/// 4.2: an Adls-backed spec's size-index key excludes the container.
/// `object_store::path::Path` is relative to the store root `side_store_url`
/// derives (an Azure side registers scoped to one container), so the
/// size-index key for a file inside that store must key as
/// `path/to/file.parquet` — never re-including the container/account
/// authority — exactly mirroring `size_index_keys_by_listing_url_prefix`
/// above, just with an `abfss://` root instead of `s3://`.
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

/// The store URL is derived from the reconstructed absolute URI of the first
/// file — for a relative first entry via the table root, for an absolute-only
/// spec (empty root) from the entry itself — and for every `s3://` input it is
/// the very URL the deleted bucket derivation was formatted back into, so the
/// registered key is unchanged.
#[test]
fn side_store_url_returns_the_same_url_for_s3_as_the_deleted_bucket_derivation() {
    // Relative first entry: bucket comes from the table root.
    let rel = vec![FileEntry::new("data/part-0.parquet", 1)];
    assert_eq!(
        side_store_url(&rel, "s3://warehouse/db/table").unwrap(),
        bucket_url("warehouse")
    );

    // Absolute first entry, empty root (legacy): unchanged behavior.
    let abs = vec![FileEntry::new("s3://legacy-bucket/data/part-0.parquet", 1)];
    assert_eq!(
        side_store_url(&abs, "").unwrap(),
        bucket_url("legacy-bucket")
    );
}

/// The derivation keeps the file list's own scheme instead of rewriting it to
/// `s3://`, so a store registered under it is found by the lookup DataFusion
/// actually performs — `ListingTableUrl::object_store()` on the file URI, which
/// preserves `s3a`. The deleted derivation registered `s3://<bucket>`, a key
/// that lookup never asks for.
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

/// The container-collision precondition. DataFusion keys the object-store
/// registry by scheme, host and port only, so two `abfss://` sides in
/// different containers of ONE storage account share a key while needing two
/// different stores — the dimension side would be read out of the fact side's
/// container with no error. The spec is rejected instead.
///
/// Two accepting controls keep the rule from degenerating into "any spec with
/// two sides is rejected": the rule keys on the store URL, so two sides in ONE
/// container need one store and are accepted, and two different accounts
/// differ in the registry key too, so they get their own stores and cannot
/// collide.
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

/// The precondition can never fire on S3: an `s3://` URI carries no userinfo,
/// so a side's store URL and its registry key hold the same authority. Every
/// S3 spec shape the scan builds passes it unchanged.
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

/// 4.2: the wrapper answers a HEAD (`get_opts` with `head`) from the size
/// index with no I/O, and falls through to the inner store for an unknown
/// path and for data reads.
#[tokio::test]
async fn sized_store_serves_head_from_index_and_delegates_otherwise() {
    use ::object_store::ObjectStoreExt;
    use ::object_store::memory::InMemory;

    // An empty in-memory store: any real head/get is a NotFound, so a
    // successful head can only have come from the size index.
    let inner = Arc::new(InMemory::new());
    let known = ObjectStorePath::from("db/table/data/f.parquet");
    let mut sizes = HashMap::new();
    sizes.insert(known.clone(), 4096u64);
    let store = SpecSizedObjectStore::new(inner, sizes);

    // Known path: metadata is synthesized from the spec size.
    let meta = store
        .head(&known)
        .await
        .expect("head of a known path must be served from the index");
    assert_eq!(meta.size, 4096);
    assert_eq!(meta.location, known);
    assert!(meta.e_tag.is_none());
    assert!(meta.version.is_none());

    // Unknown path: head falls through to the inner store (NotFound).
    let unknown = ObjectStorePath::from("db/table/data/missing.parquet");
    assert!(
        matches!(
            store.head(&unknown).await,
            Err(::object_store::Error::NotFound { .. })
        ),
        "an unindexed path must delegate to the inner store"
    );

    // Data read (head == false) of the known path also delegates — the
    // synthetic metadata must never satisfy an actual byte read.
    assert!(
        matches!(
            store.get(&known).await,
            Err(::object_store::Error::NotFound { .. })
        ),
        "a data read must delegate to the inner store, not the size index"
    );
}
