//! Object-store construction and DataFusion session-context wiring: builds the
//! S3/MinIO object store (size-indexed HEAD wrapper), registers it on the
//! session runtime, and constructs the memory-pool-sized `SessionContext`.

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use super::session_config_for_spec;
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{FileEntry, ScanSpec};

/// Build a DataFusion SessionContext with the MinIO object store registered.
///
/// Sizes the DataFusion memory pool from `memory_limit_bytes` (UDF per-instance
/// limit in bytes; `0` = unknown sentinel → conservative 1024 MB default) and
/// probes `/tmp` for disk-spill eligibility.
pub(super) fn build_session_context(
    spec: &ScanSpec,
    memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
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

    // Register the MinIO object store for the S3 URL scheme, wrapped so that
    // per-file HEAD requests are answered from the caller-supplied sizes in the
    // spec instead of issuing an object-store HEAD over the network.
    let sizes = build_spec_size_index(spec)?;
    let bucket = extract_bucket(spec)?;
    register_bucket_store(
        &ctx,
        &spec.common.storage,
        &bucket,
        spec.common.s3_max_connections,
        &sizes,
    )?;

    // A broadcast join's dimension side may live in a different bucket than the
    // sharded fact side. Register a store for it too (same credentials, same size
    // index) so DataFusion can resolve its object store; skip when it coincides
    // with the fact bucket — the common same-warehouse case, where the fact store
    // already serves both sides.
    if let Some(join) = &spec.common.join
        && !join.files.is_empty()
    {
        let dim_bucket = extract_bucket_from_files(&join.files, &join.table_root)?;
        if dim_bucket != bucket {
            register_bucket_store(
                &ctx,
                &spec.common.storage,
                &dim_bucket,
                spec.common.s3_max_connections,
                &sizes,
            )?;
        }
    }

    Ok(ctx)
}

/// Build an S3 store for `bucket` (sized-HEAD wrapped) and register it on `ctx`
/// under the `s3://{bucket}` URL. Shared by the single-table scan and the two
/// sides of a broadcast join (which may span two buckets under one credential).
fn register_bucket_store(
    ctx: &SessionContext,
    storage: &crate::scan::spec::StorageProps,
    bucket: &str,
    s3_max_connections: usize,
    sizes: &HashMap<ObjectStorePath, u64>,
) -> Result<(), UdfError> {
    let s3 = build_s3_store(storage, bucket, s3_max_connections)?;
    let sized_store = SpecSizedObjectStore::new(Arc::new(s3), sizes.clone());
    let store_url = Url::parse(&format!("s3://{bucket}"))
        .map_err(|e| UdfError::User(format!("invalid bucket URL: {e}")))?;
    ctx.runtime_env()
        .register_object_store(&store_url, Arc::new(sized_store));
    Ok(())
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

/// Build an AmazonS3 (MinIO-compatible) object store from StorageProps, sizing the
/// HTTP connection pool to the resolved `s3_max_connections` budget.
fn build_s3_store(
    storage: &crate::scan::spec::StorageProps,
    bucket: &str,
    s3_max_connections: usize,
) -> Result<impl ObjectStore, UdfError> {
    // `with_client_options` REPLACES the builder's whole `ClientOptions` (it does
    // not merge), so it must run before `with_allow_http`, which layers onto
    // whatever `ClientOptions` is already set. Reversing this order silently
    // drops `allow_http`, breaking plain-HTTP endpoints like MinIO.
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_client_options(client_options_for(s3_max_connections))
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
    builder.build().map_err(|e| {
        // Do not echo the error directly — it might contain credential fragments.
        let stripped = crate::scan::emit::redact_secret_values(&e.to_string(), &secrets);
        UdfError::User(format!(
            "failed to configure S3 object store: {}",
            crate::scan::emit::redact_credentials(&stripped)
        ))
    })
}

/// Extract the S3 bucket name from the first file in the spec.
///
/// The first entry may now be relative to `table_root`, so it is reconstructed
/// into its absolute URI first (a `://`-bearing entry passes through unchanged);
/// the bucket is then the host of that absolute URI. For the all-absolute case
/// (empty `table_root`) reconstruction is a no-op, so behavior is unchanged.
fn extract_bucket(spec: &ScanSpec) -> Result<String, UdfError> {
    extract_bucket_from_files(&spec.files, &spec.common.table_root)
}

/// Extract the S3 bucket (host) from the first entry of an explicit file list,
/// reconstructing a relative first entry against `table_root`. Shared by the fact
/// side (`extract_bucket`) and a join's dimension side.
fn extract_bucket_from_files(files: &[FileEntry], table_root: &str) -> Result<String, UdfError> {
    let first = files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    let abs = reconstruct_abs_uri(&first.path, table_root);
    let url = Url::parse(&abs).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    url.host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| UdfError::User(format!("file URI has no bucket/host: {abs}")))
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
    use crate::scan::test_support::minimal_spec;
    use ::object_store::ClientConfigKey;
    use datafusion::execution::memory_pool::MemoryLimit;

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

    /// The store built from a spec inherits the spec's connection budget, and the
    /// build succeeds without leaking any credential value into an error. Exercised
    /// as a unit test against the private `build_s3_store` seam directly (rather
    /// than an integration test) so this function does not need to be `pub` — it
    /// is otherwise only ever called internally from `build_session_context`.
    #[test]
    fn build_s3_store_applies_spec_connection_budget() {
        let mut spec = minimal_spec();
        spec.common.s3_max_connections = 16;
        let bucket = extract_bucket(&spec).expect("bucket must parse");
        build_s3_store(
            &spec.common.storage,
            &bucket,
            spec.common.s3_max_connections,
        )
        .expect("store must build with a connection budget");
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

    /// 4.3: the bucket is derived from the reconstructed absolute URI of the
    /// first file — for a relative first entry it comes via the table root, for
    /// an absolute-only spec (empty root) behavior is unchanged.
    #[test]
    fn extract_bucket_handles_relative_and_absolute_first_entry() {
        // Relative first entry: bucket comes from the table root.
        let mut rel = minimal_spec();
        rel.common.table_root = "s3://warehouse/db/table".into();
        rel.files = vec![FileEntry::new("data/part-0.parquet", 1)];
        assert_eq!(extract_bucket(&rel).unwrap(), "warehouse");

        // Absolute first entry, empty root (legacy): unchanged behavior.
        let mut abs = minimal_spec();
        abs.common.table_root = String::new();
        abs.files = vec![FileEntry::new("s3://legacy-bucket/data/part-0.parquet", 1)];
        assert_eq!(extract_bucket(&abs).unwrap(), "legacy-bucket");
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
