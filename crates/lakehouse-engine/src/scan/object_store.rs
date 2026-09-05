//! Object-store construction and DataFusion session-context wiring: builds the
//! object store each scan side reads its files through — dispatching on THAT
//! side's `StorageBackend` and wrapping each store in the spec-sized HEAD
//! decorator over THAT side's files — registers one store per DataFusion registry
//! key, and constructs the memory-pool-sized `SessionContext`.

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

use super::raw_scan::register_nested_json_render_udf;
use super::session_config_for_spec;
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{AdlsCred, FileEntry, ScanSpec, StorageBackend, reconstruct_abs_uri};
use crate::scan::storage_ref::ResolvedScanStorage;
use crate::scan::store_router::{PrefixRoutingObjectStore, RoutedSide, ScanSide};

/// Build a DataFusion `SessionContext` with an object store registered per scan side.
///
/// Sizes the DataFusion memory pool from `memory_limit_bytes` (UDF per-instance
/// limit in bytes; `0` = unknown sentinel → conservative 1024 MB default) and
/// probes `/tmp` for disk-spill eligibility.
///
/// Every store is built from `storage` — the backends `resolve_scan_storage`
/// produced for this invocation — never from the spec, which carries only a
/// reference to the CONNECTION that supplies them.
pub(super) fn build_session_context(
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
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
    register_nested_json_render_udf(&ctx);

    let sides = present_sides(spec, storage);

    // The redaction set is EVERY side's secrets, not those of the side whose store
    // is being built: each store ends up behind a router that can raise an error
    // while either side's credential is in scope. `build_side_store` sees one side
    // and structurally cannot assemble the union, so it is read from its single
    // owner — the RESOLVED pair — and passed down.
    let all_secrets = storage.all_secret_values();

    // Each side gets its OWN inner store: built from its OWN backend, and sized
    // from its OWN files, so neither one side's credential nor its size index can
    // serve the other side's paths. When both sides resolve to one DataFusion
    // registry key — the same-warehouse Databricks norm — the router is what lets
    // that single key still serve two credentials.
    //
    // A join spec routes EVERY group, including a group holding one side (the two
    // sides in different buckets): one code path, no credential or bucket
    // comparison that could be wrong. A spec with no dimension side registers its
    // one store directly, as it always has — it has one credential, so there is
    // nothing to route and no reason to give a raw scan a new way to fail.
    let has_dimension_side = sides.len() > 1;
    for (store_url, group) in group_sides_by_store_url(&sides)? {
        let store: Arc<dyn ObjectStore> = if has_dimension_side {
            let mut routed = Vec::with_capacity(group.len());
            for side in group {
                let inner = build_side_store(side, spec.common.s3_max_connections, &all_secrets)?;
                routed.push(RoutedSide::new(side, inner)?);
            }
            Arc::new(PrefixRoutingObjectStore::new(routed))
        } else {
            build_side_store(group[0], spec.common.s3_max_connections, &all_secrets)?
        };
        ctx.runtime_env().register_object_store(&store_url, store);
    }

    Ok(ctx)
}

/// The sides this spec registers a store for, FACT SIDE FIRST — the order
/// [`PrefixRoutingObjectStore`] reads as its tie-break when one path is eligible
/// for both sides, so the ordering here is a contract and not a formatting
/// choice.
///
/// A join block with an EMPTY file list contributes no side: it names no path to
/// route, no file to size, and no URI to derive a store key from.
///
/// Each side's backend comes from `storage`, paired with the spec block that
/// names its files. A join block present with files but no resolved dimension
/// backend is unreachable through `resolve_scan_storage`, which derives the pair
/// from this same spec; such a pair can only come from
/// [`ResolvedScanStorage::from_backends`], and it registers no dimension store —
/// so no dimension credential is in scope for the union either.
fn present_sides<'a>(spec: &'a ScanSpec, storage: &'a ResolvedScanStorage) -> Vec<ScanSide<'a>> {
    let mut sides = vec![ScanSide {
        label: "fact",
        files: &spec.files,
        table_root: &spec.common.table_root,
        backend: storage.primary(),
    }];
    if let Some(join) = &spec.common.join
        && !join.files.is_empty()
        && let Some(backend) = storage.join()
    {
        sides.push(ScanSide {
            label: "dimension",
            files: &join.files,
            table_root: &join.table_root,
            backend,
        });
    }
    sides
}

/// Group `sides` by the object-store URL each resolves to — the sides that must
/// share ONE registered store, because DataFusion serves one store per registry
/// key. Order is preserved both across groups and within a group, so the fact
/// side stays first in whichever group holds it.
///
/// Grouping on [`side_store_url`] is FINER than DataFusion's registry key, which
/// drops the userinfo an `abfss://` URI carries its container in. Two sides that
/// differ only there would group apart yet register under one key, the second
/// silently replacing the first — which is sound here only because
/// [`validate_sides_share_one_store`] has already refused such a spec.
fn group_sides_by_store_url<'s, 'f>(
    sides: &'s [ScanSide<'f>],
) -> Result<Vec<(Url, Vec<&'s ScanSide<'f>>)>, UdfError> {
    let mut groups: Vec<(Url, Vec<&ScanSide<'_>>)> = Vec::new();
    for side in sides {
        let store_url = side_store_url(side.files, side.table_root)?;
        match groups.iter_mut().find(|(url, _)| *url == store_url) {
            Some((_, group)) => group.push(side),
            None => groups.push((store_url, vec![side])),
        }
    }
    Ok(groups)
}

/// Build the object store ONE side of a scan reads its files through: its own
/// backend's credential, wrapped in the spec-sized HEAD decorator over its OWN
/// files, so neither that credential nor that size index can serve another side's
/// paths.
///
/// Registering the result is the CALLER's, because sides resolving to one
/// DataFusion registry key must share one registered store and only the caller
/// knows which sides those are. `all_secrets` arrives from the caller for the same
/// reason: it is EVERY present side's secret values, and a function holding one
/// side's backend cannot redact an error against a side it never sees.
fn build_side_store(
    side: &ScanSide<'_>,
    connection_budget: usize,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let sizes = side_size_index(side.files, side.table_root)?;
    let store_url = side_store_url(side.files, side.table_root)?;
    let store = build_undecorated_store(side.backend, &store_url, connection_budget, all_secrets)?;
    Ok(Arc::new(SpecSizedObjectStore::new(store, sizes)))
}

/// Build the undecorated `Arc<dyn ObjectStore>` the table rooted at `table_root`
/// is read through — no spec-sized HEAD wrapper.
///
/// Delta planning needs exactly this seam, and needs it keyed on the table root
/// rather than on a scan side: its `_delta_log` file sizes are unknown until the
/// log itself is read, so it cannot go through [`SpecSizedObjectStore`], which
/// requires sizes up front, and at plan time it holds no file list to derive a
/// store root from.
pub(crate) fn build_table_root_store(
    backend: &StorageBackend,
    table_root: &str,
    connection_budget: usize,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let store_url = store_root_url(table_root)?;
    build_undecorated_store(backend, &store_url, connection_budget, all_secrets)
}

/// Build the undecorated store `backend`'s credential covers, scoped to
/// `store_url`.
///
/// Dispatches on the storage backend because CONSTRUCTING the store is a
/// backend-specific decision. The store root is not: it arrives already derived,
/// and each arm only reads out of it the part its builder needs — the host as an S3
/// bucket name, the whole URL for Azure. The caller derives it because the two
/// callers derive it from different things: a scan side from its first file, a
/// Delta table from its root alone.
fn build_undecorated_store(
    backend: &StorageBackend,
    store_url: &Url,
    connection_budget: usize,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    match backend {
        StorageBackend::S3(storage) => {
            let bucket = store_url.host_str().ok_or_else(|| {
                UdfError::User(format!("file URI has no bucket/host: {store_url}"))
            })?;

            // `with_client_options` REPLACES the builder's whole `ClientOptions` (it does
            // not merge), so it must run before `with_allow_http`, which layers onto
            // whatever `ClientOptions` is already set. Reversing this order silently
            // drops `allow_http`, breaking plain-HTTP endpoints like MinIO.
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(&storage.region)
                .with_access_key_id(&storage.access_key)
                .with_secret_access_key(&storage.secret_key)
                .with_client_options(client_options_for(connection_budget))
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

            let s3 = builder.build().map_err(|e| {
                // Do not echo the error directly — it might contain credential fragments.
                UdfError::User(format!(
                    "failed to configure S3 object store: {}",
                    redact_error_text(&e.to_string(), all_secrets)
                ))
            })?;

            Ok(Arc::new(s3))
        }
        StorageBackend::Adls { cred, .. } => {
            let builder = MicrosoftAzureBuilder::new()
                .with_url(store_url.as_str())
                .with_client_options(client_options_for(connection_budget));
            let builder = match cred {
                AdlsCred::AccountKey(key) => builder.with_access_key(key),
                AdlsCred::Sas(sas) => builder.with_config(AzureConfigKey::SasKey, sas),
            };

            let azure = builder.build().map_err(|e| {
                UdfError::User(format!(
                    "failed to configure Azure object store: {}",
                    redact_error_text(&e.to_string(), all_secrets)
                ))
            })?;

            Ok(Arc::new(azure))
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

/// Build ONE side's map of caller-known file sizes, keyed by the object-store
/// [`Path`] the store observes in `head` — i.e. the `ListingTableUrl` prefix
/// DataFusion passes for an exact-file (non-collection) URL. Keying by that prefix
/// is what lets [`SpecSizedObjectStore`] satisfy each per-file metadata lookup from
/// the spec without a network round-trip.
///
/// Scoped to one side and never to the whole spec: each side's store answers only
/// its own side's metadata lookups, so an index carrying another side's files
/// would let one side's credentialed store answer a `head` it must never see.
///
/// [`Path`]: object_store::path::Path
fn side_size_index(
    files: &[FileEntry],
    table_root: &str,
) -> Result<HashMap<ObjectStorePath, u64>, UdfError> {
    let mut sizes = HashMap::with_capacity(files.len());
    index_file_sizes(&mut sizes, files, table_root)?;
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
/// up with agree by construction rather than by inspection.
fn side_store_url(files: &[FileEntry], table_root: &str) -> Result<Url, UdfError> {
    let first = files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    store_root_url(&reconstruct_abs_uri(&first.path, table_root))
}

/// The object-store root `uri` sits under: its `scheme://userinfo@host:port` slice,
/// with the path dropped.
///
/// ONE home for that derivation, so a scan side and a Delta table root cannot
/// disagree on what "the store this credential covers" means. The slice is exactly
/// the one `ListingTableUrl::object_store()` takes, and it deliberately KEEPS the
/// userinfo — which is where an `abfss://` URI carries its container — unlike
/// DataFusion's coarser registry key, which drops it.
fn store_root_url(uri: &str) -> Result<Url, UdfError> {
    let url = Url::parse(uri).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    let store = &url[Position::BeforeScheme..Position::BeforePath];
    Url::parse(store)
        .map_err(|e| UdfError::User(format!("invalid object-store root '{store}': {e}")))
}

/// Reject a scan spec whose sides would collapse onto ONE registered object store
/// while needing DIFFERENT ones.
///
/// DataFusion keys its object-store registry by scheme, host and port only
/// (`get_url_key`, `datafusion-execution-54.1.0/src/object_store.rs:268-274`,
/// whose own test asserts `s3://username:password@host:123` keys as
/// `s3://host:123`, `:330-332`), dropping the userinfo [`side_store_url`] keeps.
/// On `abfss://` that userinfo IS the container, and the container is the scope of
/// the store actually built, so two sides in different containers of one storage
/// account share a registry key but need two stores: whichever registered first
/// would serve both, silently reading one side's files out of the other side's
/// container.
///
/// Prefix routing does NOT subsume this guard. [`PrefixRoutingObjectStore`] tells
/// two sides apart by the `object_store::Path` its trait methods receive, but that
/// path is container-RELATIVE while the store it routes to is container-SCOPED —
/// so two tables sitting at the same relative path in two containers of one
/// account yield IDENTICAL paths, which no path-based router can distinguish.
/// Routing is what lets ONE registry key serve TWO credentials; it is not what
/// tells two containers apart.
///
/// What WOULD subsume this guard is a userinfo-retaining registry key, and
/// `object_store` 0.13.2 ships exactly that (`registry.rs:220-223` keys from
/// position 0 over a path-segment prefix tree) — DataFusion 54.1.0 uses it
/// nowhere. Since the key formula is DataFusion's and cannot be changed here, the
/// only safe reading of such a spec is to refuse it. Stated over the two derived
/// URLs and not over any backend, so it also holds for a future backend whose
/// store scope is finer than its registry key — and it can never fire for S3,
/// whose URIs carry no userinfo.
///
/// Only an empty DIMENSION side is ignored: [`present_sides`] drops it before any
/// store is built (`!join.files.is_empty()`), so it can neither collide nor be
/// derived from. An empty FACT side is NOT ignored — it still reaches
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
/// same object-store root (scheme + host) as `first_abs`. A delete mechanism that
/// names no object-store path has no root to check.
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
            if let Some(path) = delete.object_store_path() {
                check(&reconstruct_abs_uri(path, table_root), "delete file")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "object_store_tests.rs"]
mod tests;
