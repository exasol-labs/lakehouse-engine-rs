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
use object_store::limit::LimitStore;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use url::{Position, Url};

use super::checked_div::register_checked_float_div_udf;
use super::raw_scan::register_nested_json_render_udf;
use super::session_config_for_spec;
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{AdlsCred, FileEntry, ScanSpec, StorageBackend, reconstruct_abs_uri};
use crate::scan::storage_ref::ResolvedScanStorage;
use crate::scan::store_router::{PrefixRoutingObjectStore, RoutedSide, ScanSide};

/// `memory_limit_bytes == 0` means unknown and falls back to the default pool budget.
pub(super) fn build_session_context(
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
    memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
    validate_sides_share_one_store(spec)?;

    let config = session_config_for_spec(spec);

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
    register_checked_float_div_udf(&ctx);

    let sides = present_sides(spec, storage);

    // Every side's secrets: each store sits behind a router that can raise an error while either
    // side's credential is in scope, and `build_side_store` sees only one side.
    let all_secrets = storage.all_secret_values();

    // Each side gets its own inner store and size index. A join spec routes every group, even a
    // single-side one, so there is one code path and no credential comparison to get wrong; a
    // spec without a dimension side registers its one store directly.
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

/// FACT SIDE FIRST: [`PrefixRoutingObjectStore`] uses this order as its tie-break. A join block
/// with an empty file list contributes no side.
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

/// DataFusion serves one store per registry key. Grouping on [`side_store_url`] is finer than
/// that key (it keeps `abfss://` userinfo), which is sound only because
/// [`validate_sides_share_one_store`] already refused sides differing only there.
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

/// Wrapped over this side's own files only, so its credential and size index never serve
/// another side's paths. `all_secrets` covers every side, which this function cannot see.
fn build_side_store(
    side: &ScanSide<'_>,
    connection_budget: usize,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let sizes = side_size_index(side.files, side.table_root)?;
    let store_url = side_store_url(side.files, side.table_root)?;
    let store = build_undecorated_store(
        side.backend,
        &store_url,
        StoreBounds {
            connection_budget,
            admission_limit: None,
        },
        all_secrets,
    )?;
    Ok(Arc::new(SpecSizedObjectStore::new(store, sizes)))
}

/// No spec-sized HEAD wrapper: Delta's `_delta_log` sizes are unknown until the log is read, and
/// at plan time there is no file list to derive a store root from.
pub(crate) fn build_table_root_store(
    backend: &StorageBackend,
    table_root: &str,
    connection_budget: usize,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let store_url = store_root_url(table_root)?;
    build_undecorated_store(
        backend,
        &store_url,
        StoreBounds {
            connection_budget,
            admission_limit: None,
        },
        all_secrets,
    )
}

struct StoreBounds {
    connection_budget: usize,
    /// `None` leaves the store uncapped; the caller bounds concurrency itself.
    admission_limit: Option<usize>,
}

fn build_undecorated_store(
    backend: &StorageBackend,
    store_url: &Url,
    bounds: StoreBounds,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    let StoreBounds {
        connection_budget,
        admission_limit,
    } = bounds;
    match backend {
        StorageBackend::S3(storage) => {
            let bucket = store_url.host_str().ok_or_else(|| {
                UdfError::User(format!("file URI has no bucket/host: {store_url}"))
            })?;

            // `with_client_options` REPLACES the whole `ClientOptions`, so it must precede
            // `with_allow_http`; reversed, `allow_http` is silently dropped (breaking MinIO).
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(&storage.region)
                .with_access_key_id(&storage.access_key)
                .with_secret_access_key(&storage.secret_key)
                .with_client_options(client_options_for(connection_budget))
                .with_allow_http(storage.allow_http);

            // Real AWS S3 must NOT get an endpoint: object_store derives the virtual-hosted URL
            // from the region, and a regional endpoint without the bucket yields 403.
            if storage.path_style {
                builder = builder
                    .with_endpoint(&storage.endpoint)
                    .with_virtual_hosted_style_request(false);
            }

            if let Some(token) = &storage.session_token {
                builder = builder.with_token(token);
            }

            let s3 = builder.build().map_err(|e| {
                // The raw error might contain credential fragments.
                UdfError::User(format!(
                    "failed to configure S3 object store: {}",
                    redact_error_text(&e.to_string(), all_secrets)
                ))
            })?;

            Ok(apply_admission_limit(s3, admission_limit))
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

            Ok(apply_admission_limit(azure, admission_limit))
        }
    }
}

/// Wraps before erasing: `LimitStore<T>` needs `T: ObjectStore`, which `Arc<dyn ObjectStore>` is not.
fn apply_admission_limit<T: ObjectStore>(
    store: T,
    admission_limit: Option<usize>,
) -> Arc<dyn ObjectStore> {
    match admission_limit {
        Some(limit) => Arc::new(LimitStore::new(store, limit)),
        None => Arc::new(store),
    }
}

/// Unmeasured, deliberately conservative default.
pub(crate) const DIRECT_STORAGE_ADMISSION_LIMIT: usize = 16;

/// Derives the idle-connection budget from the admission cap so the two knobs never drift apart.
pub(crate) fn build_admission_limited_store(
    backend: &StorageBackend,
    store_url: &Url,
    all_secrets: &[&str],
) -> Result<Arc<dyn ObjectStore>, UdfError> {
    build_undecorated_store(
        backend,
        store_url,
        StoreBounds {
            connection_budget: DIRECT_STORAGE_ADMISSION_LIMIT,
            admission_limit: Some(DIRECT_STORAGE_ADMISSION_LIMIT),
        },
        all_secrets,
    )
}

/// `object_store` 0.13.2 has no in-flight connection cap; `pool_max_idle_per_host` (unbounded by
/// default in reqwest) is the closest knob, bounding warm connections per host.
fn client_options_for(budget: usize) -> ClientOptions {
    ClientOptions::new().with_pool_max_idle_per_host(budget.max(1))
}

/// Keyed by the `ListingTableUrl` prefix DataFusion passes to `head`, so lookups need no network
/// round-trip. Scoped to one side so a credentialed store never answers another side's `head`.
fn side_size_index(
    files: &[FileEntry],
    table_root: &str,
) -> Result<HashMap<ObjectStorePath, u64>, UdfError> {
    let mut sizes = HashMap::with_capacity(files.len());
    index_file_sizes(&mut sizes, files, table_root)?;
    Ok(sizes)
}

/// Keyed by the `ListingTableUrl` prefix DataFusion passes to `head`.
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

/// DataFusion resolves an exact-file URL via `head`, which object_store 0.13.2 dispatches to
/// `get_opts(.., GetOptions { head: true, .. })`, so HEADs for indexed paths are answered here
/// with no I/O; everything else delegates.
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

/// The `scheme://userinfo@host:port` slice of the side's first file URI. The single derivation
/// used for registration and validation, so they agree by construction.
fn side_store_url(files: &[FileEntry], table_root: &str) -> Result<Url, UdfError> {
    let first = files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    store_root_url(&reconstruct_abs_uri(&first.path, table_root))
}

/// The `scheme://userinfo@host:port` slice, as `ListingTableUrl::object_store()` takes it. Keeps
/// the userinfo (where `abfss://` carries its container), unlike DataFusion's registry key.
pub(crate) fn store_root_url(uri: &str) -> Result<Url, UdfError> {
    let url = Url::parse(uri).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    let store = &url[Position::BeforeScheme..Position::BeforePath];
    Url::parse(store)
        .map_err(|e| UdfError::User(format!("invalid object-store root '{store}': {e}")))
}

/// DataFusion keys its registry by scheme, host and port only (`get_url_key`,
/// `datafusion-execution-54.1.0/src/object_store.rs:268-274`), dropping userinfo. On `abfss://`
/// that is the container, so two sides in different containers of one account would share one
/// store and silently read one side's files from the other's container.
///
/// Prefix routing cannot catch this: its paths are container-RELATIVE, so identical relative
/// paths in two containers are indistinguishable. The key formula is DataFusion's, so the only
/// safe reading is refusal. Never fires for S3 (no userinfo). An empty dimension side is ignored,
/// as [`present_sides`] drops it.
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

/// The scan registers one store per side keyed by that root, so a mixed-root file list would be
/// read through the wrong store; this fails loud instead. Delete mechanisms without a path are
/// skipped.
pub(super) fn validate_uniform_object_store_files(
    files: &[FileEntry],
    table_root: &str,
    first_abs: &str,
) -> Result<(), UdfError> {
    // The exact registry key, so the check matches the runtime invariant and accepts every URI
    // form the scan accepts (e.g. bare local paths).
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
