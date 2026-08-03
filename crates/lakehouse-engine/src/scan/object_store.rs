//! Object-store construction and DataFusion session-context wiring: registers the
//! object store each scan side reads its files through — dispatching on the scan
//! spec's `StorageBackend` and wrapping each store in the spec-sized HEAD
//! decorator — and constructs the memory-pool-sized `SessionContext`.

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakehouse_catalog::redact_error_text;
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use url::{Position, Url};

use super::session_config_for_spec;
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{AdlsCred, FileEntry, ScanSpec, StorageBackend};

/// Build a DataFusion `SessionContext` with an object store registered per scan side.
///
/// Sizes the DataFusion memory pool from `memory_limit_bytes` (UDF per-instance
/// limit in bytes; `0` = unknown sentinel → conservative 1024 MB default) and
/// probes `/tmp` for disk-spill eligibility.
pub(super) fn build_session_context(
    spec: &ScanSpec,
    memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
    validate_sides_share_one_store(spec)?;

    let config = session_config_for_spec(spec);

    // Memory pool + spill config.
    let spill = probe_tmp_spill();
    let runtime_env = build_runtime_env(
        memory_limit_bytes,
        spec.common.memory_pool_fraction,
        spec.common.instance_overhead_mb * 1024 * 1024,
        spill,
    )
    .map_err(|e| UdfError::User(format!("failed to build DataFusion runtime env: {e}")))?;

    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    // Register the object store for the sharded fact side, wrapped so that
    // per-file HEAD requests are answered from the caller-supplied sizes in the
    // spec instead of issuing an object-store HEAD over the network.
    //
    // ONE whole-spec size index covering `spec.files` AND `join.files` is built
    // here and handed to BOTH registrations below, because a store registered for
    // one side answers the HEADs of every side that resolves to it. Narrowing it
    // to the side being registered would leave the dimension files unindexed in
    // the shared-bucket case, where only the fact store is ever registered.
    let sizes = build_spec_size_index(spec)?;
    let registration = StoreRegistration {
        backend: &spec.common.storage,
        connection_budget: spec.common.s3_max_connections,
        sizes: &sizes,
    };
    register_side_store(
        &ctx,
        &registration,
        ScanSide {
            files: &spec.files,
            table_root: &spec.common.table_root,
        },
    )?;

    // A broadcast join's dimension side may live in a different bucket than the
    // sharded fact side. Register a store for it too (same credentials, same size
    // index) so DataFusion can resolve its object store; when it coincides with
    // the fact side — the common same-warehouse case — the registry already holds
    // that store key, so the registration is skipped and the fact store serves
    // both sides.
    if let Some(join) = &spec.common.join
        && !join.files.is_empty()
    {
        register_side_store(
            &ctx,
            &registration,
            ScanSide {
                files: &join.files,
                table_root: &join.table_root,
            },
        )?;
    }

    Ok(ctx)
}

/// One side of a scan (the fact side or a join's dimension side): its own file
/// list and table root.
struct ScanSide<'a> {
    files: &'a [FileEntry],
    table_root: &'a str,
}

/// Whole-spec values shared identically by every side's registration call.
struct StoreRegistration<'a> {
    backend: &'a StorageBackend,
    connection_budget: usize,
    sizes: &'a HashMap<ObjectStorePath, u64>,
}

/// Register the object store one side of a scan reads its files through, keyed by
/// the store URL that side's own file list resolves to, and answer whether that
/// registration was new: `Some(url)` = registered under `url`, `None` = the
/// registry already held that key and nothing was (re-)registered.
///
/// Dispatches on the storage backend because CONSTRUCTING the store is a
/// backend-specific decision. The store URL is not: [`side_store_url`] derives it
/// once for every backend, and each arm only reads out of it the part its builder
/// needs — the host as an S3 bucket name, the whole URL for Azure.
///
/// The `None` case is what makes a broadcast join's two sides safe to register
/// unconditionally: sides sharing one bucket collapse onto one store as an
/// artifact of the registry key, with no bucket comparison at the call site.
/// `StoreRegistration::sizes` is therefore the WHOLE spec's size index and not
/// the side's — the one store that survives that collapse must answer the sized
/// HEADs of both sides — which is why it sits with the whole-spec values rather
/// than beside the per-side file list in [`ScanSide`].
fn register_side_store(
    ctx: &SessionContext,
    registration: &StoreRegistration<'_>,
    side: ScanSide<'_>,
) -> Result<Option<Url>, UdfError> {
    match registration.backend {
        StorageBackend::S3(storage) => {
            let store_url = side_store_url(side.files, side.table_root)?;
            let bucket = store_url
                .host_str()
                .ok_or_else(|| UdfError::User(format!("file URI has no bucket/host: {store_url}")))?
                .to_string();
            if ctx
                .runtime_env()
                .object_store_registry
                .get_store(&store_url)
                .is_ok()
            {
                return Ok(None);
            }

            // `with_client_options` REPLACES the builder's whole `ClientOptions` (it does
            // not merge), so it must run before `with_allow_http`, which layers onto
            // whatever `ClientOptions` is already set. Reversing this order silently
            // drops `allow_http`, breaking plain-HTTP endpoints like MinIO.
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_region(&storage.region)
                .with_access_key_id(&storage.access_key)
                .with_secret_access_key(&storage.secret_key)
                .with_client_options(client_options_for(registration.connection_budget))
                .with_allow_http(storage.allow_http);

            // Path-style stores (MinIO and other S3-compatibles) need the explicit endpoint
            // and path-style addressing. For real AWS S3 (virtual-hosted) we must NOT set an
            // endpoint: object_store derives https://<bucket>.s3.<region>.amazonaws.com from
            // the region. Setting a regional endpoint without the bucket sends requests to
            // the account root -> S3 returns 403 (s3:ListAllMyBuckets).
            if storage.path_style {
                builder = builder
                    .with_endpoint(&storage.endpoint)
                    .with_virtual_hosted_style_request(false);
            }

            if let Some(token) = &storage.session_token {
                builder = builder.with_token(token);
            }

            let secrets = storage.secret_values();
            let s3 = builder.build().map_err(|e| {
                // Do not echo the error directly — it might contain credential fragments.
                UdfError::User(format!(
                    "failed to configure S3 object store: {}",
                    redact_error_text(&e.to_string(), &secrets)
                ))
            })?;

            let sized_store = SpecSizedObjectStore::new(Arc::new(s3), registration.sizes.clone());
            ctx.runtime_env()
                .register_object_store(&store_url, Arc::new(sized_store));
            Ok(Some(store_url))
        }
        StorageBackend::Adls { cred, .. } => {
            let store_url = side_store_url(side.files, side.table_root)?;
            if ctx
                .runtime_env()
                .object_store_registry
                .get_store(&store_url)
                .is_ok()
            {
                return Ok(None);
            }

            let builder = MicrosoftAzureBuilder::new()
                .with_url(store_url.as_str())
                .with_client_options(client_options_for(registration.connection_budget));
            let builder = match cred {
                AdlsCred::AccountKey(key) => builder.with_access_key(key),
                AdlsCred::Sas(sas) => builder.with_config(AzureConfigKey::SasKey, sas),
            };

            let secrets = registration.backend.secret_values();
            let azure = builder.build().map_err(|e| {
                UdfError::User(format!(
                    "failed to configure Azure object store: {}",
                    redact_error_text(&e.to_string(), &secrets)
                ))
            })?;

            let sized_store =
                SpecSizedObjectStore::new(Arc::new(azure), registration.sizes.clone());
            ctx.runtime_env()
                .register_object_store(&store_url, Arc::new(sized_store));
            Ok(Some(store_url))
        }
    }
}

/// HTTP client options that bound the object store's warm connection pool to the
/// resolved connection-concurrency budget.
///
/// `object_store` 0.13.2 exposes no hard "max concurrent requests" ceiling — the
/// reqwest/hyper backend never caps in-flight connections. `pool_max_idle_per_host`
/// is the closest available knob: it bounds how many established connections the
/// pool keeps warm (idle, reusable) per host, whose reqwest default is unbounded.
/// This is the axis that maps to "how many concurrent fetches from S3 the instance
/// keeps warm", independent of the DataFusion CPU thread/partition budget. Clamped
/// to at least 1 so the ceiling is never zero.
fn client_options_for(budget: usize) -> ClientOptions {
    ClientOptions::new().with_pool_max_idle_per_host(budget.max(1))
}

/// Reconstruct the absolute file URI for a per-shard `(path, _)` entry.
///
/// An entry that already contains a scheme (`"://"`) is absolute and returned
/// unchanged. Otherwise it is relative to `table_root` and joined onto it with
/// exactly one `/` separator (a trailing `/` on the root and a leading `/` on the
/// entry are both trimmed first, so the separator is neither doubled nor dropped).
pub(crate) fn reconstruct_abs_uri(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

/// Build the map of caller-known file sizes keyed by the object-store [`Path`]
/// the store observes in `head` — i.e. the `ListingTableUrl` prefix DataFusion
/// passes for an exact-file (non-collection) URL. Keying by that prefix is what
/// lets [`SpecSizedObjectStore`] satisfy each per-file metadata lookup from the
/// spec without a network round-trip.
///
/// [`Path`]: object_store::path::Path
fn build_spec_size_index(spec: &ScanSpec) -> Result<HashMap<ObjectStorePath, u64>, UdfError> {
    let mut sizes = HashMap::with_capacity(spec.files.len());
    index_file_sizes(&mut sizes, &spec.files, &spec.common.table_root)?;
    // A broadcast join carries the dimension side's full file list; its per-file
    // sizes are indexed too so DataFusion answers the dimension HEADs from the spec
    // (no network round-trip), exactly as it does for the sharded fact side.
    if let Some(join) = &spec.common.join {
        index_file_sizes(&mut sizes, &join.files, &join.table_root)?;
    }
    Ok(sizes)
}

/// Insert each [`FileEntry`] into `sizes`, keyed by the object-store [`Path`]
/// DataFusion passes to `head` for that exact-file URL (the `ListingTableUrl`
/// prefix), reconstructing relative paths against `table_root`.
///
/// [`Path`]: object_store::path::Path
fn index_file_sizes(
    sizes: &mut HashMap<ObjectStorePath, u64>,
    files: &[FileEntry],
    table_root: &str,
) -> Result<(), UdfError> {
    for entry in files {
        let abs = reconstruct_abs_uri(&entry.path, table_root);
        let url = ListingTableUrl::parse(&abs)
            .map_err(|e| UdfError::User(format!("invalid listing URL '{abs}': {e}")))?;
        sizes.insert(url.prefix().clone(), entry.size);
    }
    Ok(())
}

/// An [`ObjectStore`] decorator that answers per-file metadata (`head`) from a
/// caller-supplied size index instead of the network, delegating every other
/// operation to the wrapped store.
///
/// DataFusion resolves an exact-file `ListingTableUrl` by calling `head` on the
/// store, which (object_store 0.13.2) dispatches through the `ObjectStoreExt`
/// blanket to `get_opts(location, GetOptions { head: true, .. })`. So the HEAD is
/// intercepted here in `get_opts`: when `head` is set and the location is present
/// in the index, a synthetic [`ObjectMeta`] built from the spec size is returned
/// with no I/O. Data reads (`head == false`) and all non-`get_opts` operations
/// fall through to the inner store unchanged.
#[derive(Debug)]
struct SpecSizedObjectStore {
    inner: Arc<dyn ObjectStore>,
    sizes: HashMap<ObjectStorePath, u64>,
}

impl SpecSizedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, sizes: HashMap<ObjectStorePath, u64>) -> Self {
        Self { inner, sizes }
    }
}

impl std::fmt::Display for SpecSizedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpecSizedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SpecSizedObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head
            && let Some(&size) = self.sizes.get(location)
        {
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: object_store::Attributes::default(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The object-store URL one scan side reads its files through: the
/// `scheme://userinfo@host:port` slice of the side's first reconstructed file
/// URI, with a relative first entry resolved against `table_root`.
///
/// The single derivation every backend and [`validate_sides_share_one_store`]
/// read, so the key a store is registered under and the key DataFusion looks it
/// up with agree by construction rather than by inspection. The slice is exactly
/// the one `ListingTableUrl::object_store()` takes, and it deliberately KEEPS the
/// userinfo — which is where an `abfss://` URI carries its container — unlike
/// DataFusion's coarser registry key, which drops it.
fn side_store_url(files: &[FileEntry], table_root: &str) -> Result<Url, UdfError> {
    let first = files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    let abs = reconstruct_abs_uri(&first.path, table_root);
    let url = Url::parse(&abs).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    let store = &url[Position::BeforeScheme..Position::BeforePath];
    Url::parse(store)
        .map_err(|e| UdfError::User(format!("invalid object-store root '{store}': {e}")))
}

/// Reject a scan spec whose sides would collapse onto ONE registered object store
/// while needing DIFFERENT ones.
///
/// DataFusion keys its object-store registry by scheme, host and port only
/// (`get_url_key`, `datafusion-execution-54.1.0/src/object_store.rs:268-274`),
/// dropping the userinfo [`side_store_url`] keeps. On `abfss://` that userinfo IS
/// the container, and the container is the scope of the store actually built, so
/// two sides in different containers of one storage account share a registry key
/// but need two stores: whichever registered first would serve both, silently
/// reading one side's files out of the other side's container.
///
/// The key formula is DataFusion's and cannot be changed here, so the only safe
/// reading of such a spec is to refuse it. Stated over the two derived URLs and
/// not over any backend, so it also holds for a future backend whose store scope
/// is finer than its registry key — and it can never fire for S3, whose URIs
/// carry no userinfo.
///
/// Only an empty DIMENSION side is ignored: `build_session_context` skips it
/// before registration (`!join.files.is_empty()`), so it can neither collide nor
/// be derived from. An empty FACT side is NOT ignored — it still reaches
/// [`side_store_url`] and fails there with "scan spec has no files", exactly as it
/// would without this check.
fn validate_sides_share_one_store(spec: &ScanSpec) -> Result<(), UdfError> {
    let fact = (spec.files.as_slice(), spec.common.table_root.as_str());
    let dimension = spec
        .common
        .join
        .as_ref()
        .map(|join| (join.files.as_slice(), join.table_root.as_str()));

    let sides = std::iter::once(fact)
        .chain(dimension)
        .filter(|(files, _)| !files.is_empty());

    let mut by_registry_key: HashMap<String, Url> = HashMap::new();
    for (files, table_root) in sides {
        let store_url = side_store_url(files, table_root)?;
        let registry_key = format!(
            "{}://{}",
            store_url.scheme(),
            &store_url[Position::BeforeHost..Position::AfterPort]
        );
        if let Some(other) = by_registry_key.insert(registry_key, store_url.clone())
            && other != store_url
        {
            return Err(UdfError::User(format!(
                "scan spec sides need different object stores ('{other}' and '{store_url}') but \
                 DataFusion registers a store by scheme, host and port only, so both sides would \
                 be read through whichever of the two registered first"
            )));
        }
    }
    Ok(())
}

/// Verify every data file and associated delete file in `files` resolves to the
/// same object-store root (scheme + host) as `first_abs`.
///
/// The scan registers a single object store per side, keyed by that root (see
/// [`register_file_list`] / [`build_session_context`]); a file under a different
/// root would be read through the wrong store. This fails loud on a mixed-root
/// file list rather than misreading or failing confusingly downstream. Called
/// once per registered table, so a join's fact and dimension sides are each
/// checked against their own first file (they may legitimately live in different
/// buckets, each with its own registered store).
pub(super) fn validate_uniform_object_store_files(
    files: &[FileEntry],
    table_root: &str,
    first_abs: &str,
) -> Result<(), UdfError> {
    // Compare the exact `ObjectStoreUrl` (scheme + authority) each file resolves
    // to — the very key the store is registered/looked up under — so the check
    // matches the runtime invariant precisely (and accepts every URI form the
    // scan itself accepts, e.g. bare local paths).
    let store_key = |abs: &str| -> Result<String, UdfError> {
        Ok(ListingTableUrl::parse(abs)
            .map_err(|e| UdfError::User(format!("invalid file URI '{abs}': {e}")))?
            .object_store()
            .as_str()
            .to_string())
    };
    let expected = store_key(first_abs)?;
    let check = |abs: &str, kind: &str| -> Result<(), UdfError> {
        let got = store_key(abs)?;
        if got != expected {
            return Err(UdfError::User(format!(
                "scan spec mixes object-store roots: {kind} '{abs}' resolves to store '{got}' but \
                 the first file resolves to '{expected}'; the scan registers a single object store"
            )));
        }
        Ok(())
    };
    for entry in files {
        check(&reconstruct_abs_uri(&entry.path, table_root), "data file")?;
        for delete in &entry.deletes {
            check(
                &reconstruct_abs_uri(&delete.path, table_root),
                "delete file",
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::runtime::{DEFAULT_BUDGET_BYTES, MIN_POOL_FLOOR_BYTES};
    use crate::scan::spec::{JoinSpec, JoinType};
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

    /// Register one side through the seam under test, always with the WHOLE-spec
    /// size index — the same thing `build_session_context` passes to every call.
    fn register_side(
        ctx: &SessionContext,
        spec: &ScanSpec,
        files: &[FileEntry],
        table_root: &str,
    ) -> Result<Option<Url>, UdfError> {
        let sizes = build_spec_size_index(spec).expect("size index must build");
        register_side_store(
            ctx,
            &StoreRegistration {
                backend: &spec.common.storage,
                connection_budget: spec.common.s3_max_connections,
                sizes: &sizes,
            },
            ScanSide { files, table_root },
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
        });
        spec
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

    /// The store built for a side inherits the spec's connection budget, and the
    /// build succeeds without leaking any credential value into an error.
    /// Exercised as a unit test against the private `register_side_store` seam
    /// directly (rather than an integration test) so that function does not need
    /// to be `pub` — it is otherwise only ever called from `build_session_context`.
    #[test]
    fn build_s3_store_applies_spec_connection_budget() {
        let mut spec = minimal_spec();
        spec.common.s3_max_connections = 16;
        let ctx = SessionContext::new();

        assert_eq!(
            register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
                .expect("store must build with a connection budget"),
            Some(bucket_url("test-bucket")),
            "a non-default connection budget must still register the side's store"
        );
    }

    /// Two sides resolving to distinct buckets each get their own store, reported
    /// by the URL each was registered under and both resolvable from the registry.
    #[test]
    fn register_side_store_registers_one_store_per_distinct_side() {
        let dim_files = vec![FileEntry::new("data/dim-0.parquet", 64)];
        let spec = spec_with_join("s3://dim-bucket/db/dim", dim_files.clone());
        let ctx = SessionContext::new();

        assert_eq!(
            register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
                .expect("fact side must register"),
            Some(bucket_url("test-bucket"))
        );
        assert_eq!(
            register_side(&ctx, &spec, &dim_files, "s3://dim-bucket/db/dim")
                .expect("dimension side must register"),
            Some(bucket_url("dim-bucket"))
        );

        for bucket in ["test-bucket", "dim-bucket"] {
            assert!(
                store_registered(&ctx, bucket),
                "a store must be resolvable for {bucket}"
            );
        }
    }

    /// A dimension side resolving to the fact side's bucket registers no second
    /// store: the registry already holds that key, so the call reports `None` and
    /// the already-registered fact store serves both sides. The skip is an
    /// artifact of the key, never a bucket comparison at the call site.
    #[test]
    fn join_dimension_side_sharing_the_fact_bucket_is_not_registered_twice() {
        let dim_files = vec![FileEntry::new("data/dim-0.parquet", 64)];
        let spec = spec_with_join("s3://test-bucket/db/dim", dim_files.clone());
        let ctx = SessionContext::new();
        register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
            .expect("fact side must register");

        assert_eq!(
            register_side(&ctx, &spec, &dim_files, "s3://test-bucket/db/dim")
                .expect("a shared-bucket dimension side must not fail"),
            None,
            "a dimension side in the fact bucket must not be registered twice"
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
    fn register_side_store_returns_the_container_qualified_url_but_the_registry_key_drops_the_container()
     {
        let spec = adls_spec(
            "abfss://container@acct.dfs.core.windows.net/db/table",
            AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
        );
        let ctx = SessionContext::new();
        let expected =
            Url::parse("abfss://container@acct.dfs.core.windows.net").expect("URL must parse");

        assert_eq!(
            register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
                .expect("an Azure side must register"),
            Some(expected.clone())
        );
        assert!(
            ctx.runtime_env()
                .object_store_registry
                .get_store(&expected)
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

    /// A second side rooted in the SAME container of the same account shares the
    /// first side's registry key: the registration is skipped and reports `None`,
    /// exactly the S3 shared-bucket contract.
    #[test]
    fn register_side_store_skips_a_second_side_in_the_same_container() {
        let spec = adls_spec_with_join(
            "abfss://container@acct.dfs.core.windows.net/db/fact",
            "abfss://container@acct.dfs.core.windows.net/db/dim",
            AdlsCred::AccountKey(VALID_ACCOUNT_KEY.into()),
        );
        let ctx = SessionContext::new();
        register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
            .expect("fact side must register");

        let join = spec.common.join.as_ref().expect("spec carries a join");
        assert_eq!(
            register_side(&ctx, &spec, &join.files, &join.table_root)
                .expect("a same-container dimension side must not fail"),
            None,
            "a dimension side in the same container must not be registered twice"
        );
    }

    /// Two sides in different storage ACCOUNTS differ at the registry-key level
    /// (the host), so both register their own store under their own URL.
    #[test]
    fn register_side_store_registers_both_sides_in_different_accounts() {
        let spec = adls_spec_with_join(
            "abfss://facts@acct1.dfs.core.windows.net/db/fact",
            "abfss://dims@acct2.dfs.core.windows.net/db/dim",
            AdlsCred::Sas("sv=2021&sig=static-sas-signature".into()),
        );
        let ctx = SessionContext::new();

        assert_eq!(
            register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
                .expect("fact side must register"),
            Some(Url::parse("abfss://facts@acct1.dfs.core.windows.net").expect("URL must parse"))
        );
        let join = spec.common.join.as_ref().expect("spec carries a join");
        assert_eq!(
            register_side(&ctx, &spec, &join.files, &join.table_root)
                .expect("dimension side in a different account must register"),
            Some(Url::parse("abfss://dims@acct2.dfs.core.windows.net").expect("URL must parse"))
        );
    }

    /// `MicrosoftAzureBuilder` accepts only four host suffixes
    /// (`dfs`/`blob`.`core.windows.net`/`fabric.microsoft.com`). A host outside
    /// that set must fail loud at `build()` with `UrlNotRecognised` — not collapse
    /// silently to some other account — and the surfaced error must carry no
    /// credential value, redacted by the same value-then-label pass as the S3 arm.
    #[test]
    fn register_side_store_surfaces_an_unrecognised_azure_host_redacted() {
        let secret = "static-account-key";
        let spec = adls_spec(
            "abfss://container@sovereign.example.com/db/table",
            AdlsCred::AccountKey(secret.into()),
        );
        let ctx = SessionContext::new();

        let err = register_side(&ctx, &spec, &spec.files, &spec.common.table_root)
            .expect_err("an unrecognised Azure host suffix must be rejected");
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

    /// The size index handed to every registration is the WHOLE spec's. In the
    /// shared-bucket case only the fact store is registered, and that ONE store
    /// must answer a DIMENSION file's HEAD from the spec — a per-side size map
    /// would silently push those HEADs onto the network.
    #[tokio::test]
    async fn shared_bucket_join_store_answers_both_sides_sizes_from_the_spec() {
        use ::object_store::ObjectStoreExt;
        const DIM_SIZE: u64 = 4242;

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

        // Bounded so a fall-through to the (unreachable) endpoint fails fast and
        // legibly instead of exhausting the object-store retry budget; a HEAD
        // served from the index does no I/O and never waits.
        let meta = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.head(&ObjectStorePath::from("db/dim/data/dim-0.parquet")),
        )
        .await
        .expect("the dimension HEAD must be answered from the spec, not over the network")
        .expect("head of an indexed dimension file must succeed");

        assert_eq!(
            meta.size, DIM_SIZE,
            "the fact-side store must answer the dimension file's size from the whole-spec index"
        );
    }

    /// 4.1: a `://`-bearing entry is absolute and passes through unchanged.
    #[test]
    fn reconstruct_absolute_entry_passes_through() {
        assert_eq!(
            reconstruct_abs_uri(
                "s3://bucket/db/table/data/f.parquet",
                "s3://bucket/db/table"
            ),
            "s3://bucket/db/table/data/f.parquet"
        );
        // Passthrough holds even against an empty root.
        assert_eq!(
            reconstruct_abs_uri("s3://other/x.parquet", ""),
            "s3://other/x.parquet"
        );
    }

    /// 4.1: a relative entry joins onto the root with exactly one separator,
    /// regardless of a trailing `/` on the root or a leading `/` on the entry.
    #[test]
    fn reconstruct_relative_entry_normalizes_single_separator() {
        let expected = "s3://bucket/db/table/data/f.parquet";
        // Neither side carries the separator.
        assert_eq!(
            reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table"),
            expected
        );
        // Trailing slash on the root only.
        assert_eq!(
            reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table/"),
            expected
        );
        // Leading slash on the entry only.
        assert_eq!(
            reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table"),
            expected
        );
        // Both sides carry the separator — still not doubled.
        assert_eq!(
            reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table/"),
            expected
        );
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
        let index = build_spec_size_index(&spec).expect("index must build");

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
        let index = build_spec_size_index(&spec).expect("index must build");

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
}
