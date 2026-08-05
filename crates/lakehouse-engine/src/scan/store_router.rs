//! Per-side object-store routing for a broadcast join whose two sides share one
//! bucket.
//!
//! DataFusion keys its object-store registry on `scheme://host[:port]` alone
//! (`get_url_key`, `datafusion-execution-54.1.0/src/object_store.rs:266-274`;
//! `ObjectStoreUrl::parse` rejects any URL carrying a path, `:58-72`), so one
//! bucket is served by exactly ONE registered store and no registry-level change
//! can attach a second credential to it. Yet a join's two sides each own a
//! credential — a vended credential is scoped to the table it was resolved for —
//! while routinely sharing a bucket (the Databricks norm: one metastore bucket
//! for two tables of one catalog). [`PrefixRoutingObjectStore`] reconciles the
//! two: registered once per bucket over one already-credentialed inner store per
//! side, it serves every operation carrying a path from the store of the side
//! that owns that path, so one side's credential is never used on the other's
//! file. The trait methods see the full `object_store::Path` the registry cannot.
//!
//! Routing matches a side's OWN enumerated file paths first and its table root
//! only as a fallback, because the Iceberg table spec permits a table's files to
//! sit outside its `location` (Appendix E, Version 4: "Absolute paths must be
//! used for files that do not share a common prefix with the table location") —
//! a root-only rule would misroute a spec-legal table. Exact membership is also
//! complete, not merely preferred: the scan discovers no files, so each side's
//! spec already names every path that side will request. The root fallback is
//! therefore unreachable on a well-formed spec and a path matching neither is a
//! planning defect, reported as an error rather than guessed at — guessing would
//! issue the request with a credential of unknown scope for that path.

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use crate::scan::spec::{FileEntry, StorageBackend, reconstruct_abs_uri};

/// Name this store reports itself under, in a routing error and in `Display`.
const STORE_NAME: &str = "PrefixRoutingObjectStore";

/// One side of a scan (the fact side or a join's dimension side): the file list,
/// table root, and storage backend that side is read through, plus the label a
/// routing error names it by.
///
/// EVERY field is per-side, `backend` included: a vended credential is scoped to
/// the table it was resolved for, so a join's dimension side is read through
/// `JoinSpec::storage` and never through the fact side's `common.storage`.
pub(super) struct ScanSide<'a> {
    pub(super) label: &'static str,
    pub(super) files: &'a [FileEntry],
    pub(super) table_root: &'a str,
    pub(super) backend: &'a StorageBackend,
}

/// One routable side of a join scan: the object-store paths it owns, the root its
/// files were resolved against, and the store holding ITS credential.
#[derive(Debug)]
pub struct RoutedSide {
    label: &'static str,
    owned: HashSet<ObjectStorePath>,
    root: Option<ObjectStorePath>,
    store: Arc<dyn ObjectStore>,
}

impl RoutedSide {
    /// Derive one side's routing coordinates from the file list and table root its
    /// scan spec carries, over `store` — the already-credentialed store built from
    /// THAT side's storage backend. `side.label` names the side in routing errors
    /// (`"fact"`, `"dimension"`).
    ///
    /// The owned set holds every data file AND every positional-delete file of
    /// every entry, since the scan requests both. Both the owned paths and the
    /// root are derived through `ListingTableUrl::parse(..).prefix()` — the
    /// derivation the spec-sized HEAD index already uses — so file paths and roots
    /// share one coordinate system by construction rather than by inspection.
    ///
    /// An empty `side.table_root` yields NO root: such a spec carries only absolute
    /// file paths, and a rootless side must not claim a path it never enumerated
    /// (an empty root prefix-matches every path).
    pub(super) fn new(side: &ScanSide<'_>, store: Arc<dyn ObjectStore>) -> Result<Self, UdfError> {
        let mut owned = HashSet::with_capacity(side.files.len());
        for file in side.files {
            owned.insert(store_path(&file.path, side.table_root)?);
            for delete in &file.deletes {
                owned.insert(store_path(&delete.path, side.table_root)?);
            }
        }
        let root = match side.table_root {
            "" => None,
            root => Some(listing_prefix(root)?),
        };
        Ok(Self {
            label: side.label,
            owned,
            root,
            store,
        })
    }
}

fn store_path(entry_path: &str, table_root: &str) -> Result<ObjectStorePath, UdfError> {
    listing_prefix(&reconstruct_abs_uri(entry_path, table_root))
}

fn listing_prefix(uri: &str) -> Result<ObjectStorePath, UdfError> {
    Ok(ListingTableUrl::parse(uri)
        .map_err(|e| UdfError::User(format!("invalid listing URL '{uri}': {e}")))?
        .prefix()
        .clone())
}

/// An [`ObjectStore`] that serves each requested path through the inner store of
/// the join side owning it: one store per bucket, one credential per side.
#[derive(Debug)]
pub struct PrefixRoutingObjectStore {
    // Shared behind an `Arc` so the one routing rule is reachable from the
    // `'static` stream `delete_stream` returns as well as from `&self` methods.
    sides: Arc<[RoutedSide]>,
}

impl PrefixRoutingObjectStore {
    /// Route between `sides`, which MUST be ordered FACT side first: routing scans
    /// them in order and keeps the first match, so their order IS the tie-break
    /// that stops one spec from routing one path two ways across invocations.
    pub fn new(sides: Vec<RoutedSide>) -> Self {
        Self {
            sides: sides.into(),
        }
    }

    fn route(&self, path: &ObjectStorePath) -> object_store::Result<&Arc<dyn ObjectStore>> {
        Ok(&self.sides[owning_side(&self.sides, path)?].store)
    }

    fn route_listing(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<&Arc<dyn ObjectStore>> {
        let prefix = prefix.ok_or_else(|| unprefixed_listing_error(&self.sides))?;
        self.route(prefix)
    }

    /// Route a two-path operation, which ONE side must own entirely: both paths are
    /// touched under a single credential, so a pair spanning two sides has no
    /// credential covering it.
    fn route_pair(
        &self,
        operation: &str,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> object_store::Result<&Arc<dyn ObjectStore>> {
        let source = owning_side(&self.sides, from)?;
        let destination = owning_side(&self.sides, to)?;
        if source != destination {
            return Err(object_store::Error::Generic {
                store: STORE_NAME,
                source: format!(
                    "cannot {operation} '{from}' to '{to}': the two paths are owned by different \
                     join sides ('{}' and '{}'), whose storage credentials differ",
                    self.sides[source].label, self.sides[destination].label
                )
                .into(),
            });
        }
        Ok(&self.sides[source].store)
    }
}

impl std::fmt::Display for PrefixRoutingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE_NAME}(")?;
        for (position, side) in self.sides.iter().enumerate() {
            if position > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}={}", side.label, side.store)?;
        }
        write!(f, ")")
    }
}

/// The index of the side owning `path`: the FIRST side whose spec enumerates it
/// exactly, else the side whose table root is its LONGEST prefix, else a routing
/// error. Both steps resolve a tie in favour of the earlier — fact — side.
fn owning_side(sides: &[RoutedSide], path: &ObjectStorePath) -> object_store::Result<usize> {
    if let Some(index) = sides.iter().position(|side| side.owned.contains(path)) {
        return Ok(index);
    }

    let mut longest: Option<(usize, usize)> = None;
    for (index, side) in sides.iter().enumerate() {
        let Some(root) = &side.root else { continue };
        if !path.prefix_matches(root) {
            continue;
        }
        // Measured in raw length rather than segment count: `Path::parts_count`
        // reports 1 for the store root, and two roots that both prefix-match one
        // path are nested, so raw length orders them exactly as segments would.
        let matched = root.as_ref().len();
        if longest.is_none_or(|(_, best)| matched > best) {
            longest = Some((index, matched));
        }
    }

    longest
        .map(|(index, _)| index)
        .ok_or_else(|| unowned_path_error(sides, path))
}

fn unowned_path_error(sides: &[RoutedSide], path: &ObjectStorePath) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: format!(
            "no join side owns object-store path '{path}' (tried {}); every path a scan requests \
             is one its own spec enumerates or one under its own table root, so this is a defect \
             of the plan that produced the spec — routing it to a side anyway would issue the \
             request with a credential of unknown scope for that path",
            describe_sides(sides)
        )
        .into(),
    }
}

fn unprefixed_listing_error(sides: &[RoutedSide]) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: format!(
            "a listing carrying no path prefix is bucket-wide and cannot be attributed to one \
             join side (tried {})",
            describe_sides(sides)
        )
        .into(),
    }
}

fn describe_sides(sides: &[RoutedSide]) -> String {
    sides
        .iter()
        .map(|side| match &side.root {
            Some(root) => format!("{} (table root '{root}')", side.label),
            None => format!("{} (no table root)", side.label),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn failed_stream<T: Send + 'static>(
    error: object_store::Error,
) -> BoxStream<'static, object_store::Result<T>> {
    futures::stream::once(async move { Err(error) }).boxed()
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for PrefixRoutingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.route(location)?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.route(location)?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.route(location)?.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectStorePath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.route(location)?.get_ranges(location, ranges).await
    }

    /// Routes each streamed path on its own, so one stream spanning both sides
    /// deletes every path through its owning side's credential.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        let sides = Arc::clone(&self.sides);
        locations
            .then(move |location| {
                let sides = Arc::clone(&sides);
                async move {
                    let location = location?;
                    let store = Arc::clone(&sides[owning_side(&sides, &location)?].store);
                    store.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    /// A PREFIXED listing routes by the same two-step rule as any other path — the
    /// scan's schema-inference branch lists either a data file's own exact path or a
    /// directory under one side's table root, and both are covered by that rule. A
    /// prefix-LESS listing is bucket-wide and belongs to no side.
    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.route_listing(prefix) {
            Ok(store) => store.list(prefix),
            Err(error) => failed_stream(error),
        }
    }

    /// Routes on `prefix`; `offset` is a resume cursor within that one listing, not
    /// a second target.
    fn list_with_offset(
        &self,
        prefix: Option<&ObjectStorePath>,
        offset: &ObjectStorePath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.route_listing(prefix) {
            Ok(store) => store.list_with_offset(prefix, offset),
            Err(error) => failed_stream(error),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.route_listing(prefix)?
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.route_pair("copy", from, to)?
            .copy_opts(from, to, options)
            .await
    }

    async fn rename_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.route_pair("rename", from, to)?
            .rename_opts(from, to, options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::spec::{DeleteFileContentType, DeleteFileRef, StorageProps};
    use object_store::memory::InMemory;
    use std::sync::LazyLock;

    const FACT_LABEL: &str = "fact";
    const DIM_LABEL: &str = "dimension";
    const FACT_ROOT: &str = "s3://bucket/wh/fact";
    const DIM_ROOT: &str = "s3://bucket/wh/dim";
    const FACT_FILE: &str = "wh/fact/data/f1.parquet";
    const DIM_FILE: &str = "wh/dim/data/d1.parquet";
    const SHARED_URI: &str = "s3://bucket/shared/x.parquet";
    const SHARED: &str = "shared/x.parquet";

    fn entry(path: &str) -> FileEntry {
        FileEntry {
            path: path.to_string(),
            size: 1,
            deletes: Vec::new(),
        }
    }

    /// One side's routing coordinates. `RoutedSide::new` reads `label`, `files`,
    /// and `table_root` only — never the backend — so every side here shares one.
    fn scan_side<'a>(
        label: &'static str,
        files: &'a [FileEntry],
        table_root: &'a str,
    ) -> ScanSide<'a> {
        static UNREAD_BACKEND: LazyLock<StorageBackend> =
            LazyLock::new(|| StorageBackend::S3(StorageProps::default()));
        ScanSide {
            label,
            files,
            table_root,
            backend: &UNREAD_BACKEND,
        }
    }

    fn entry_with_delete(path: &str, delete: &str) -> FileEntry {
        FileEntry {
            path: path.to_string(),
            size: 1,
            deletes: vec![DeleteFileRef {
                path: delete.to_string(),
                size: 1,
                content_type: DeleteFileContentType::PositionDeletes,
            }],
        }
    }

    /// An in-memory store holding `label` as the payload of every path in `paths`,
    /// so a value read back through the router names the side that served it.
    async fn store_labelled(label: &str, paths: &[&str]) -> Arc<dyn ObjectStore> {
        let store = InMemory::new();
        for path in paths {
            store
                .put(
                    &ObjectStorePath::from(*path),
                    PutPayload::from(label.to_string()),
                )
                .await
                .expect("seed in-memory object");
        }
        Arc::new(store)
    }

    fn fact_side(store: Arc<dyn ObjectStore>) -> RoutedSide {
        RoutedSide::new(
            &scan_side(FACT_LABEL, &[entry("data/f1.parquet")], FACT_ROOT),
            store,
        )
        .expect("fact side")
    }

    fn dimension_side(store: Arc<dyn ObjectStore>) -> RoutedSide {
        RoutedSide::new(
            &scan_side(DIM_LABEL, &[entry("data/d1.parquet")], DIM_ROOT),
            store,
        )
        .expect("dimension side")
    }

    /// A fact-first router whose two sides BOTH hold `paths`, so a misroute returns
    /// the other side's payload instead of failing to find anything.
    async fn router_over_both_sides(paths: &[&str]) -> PrefixRoutingObjectStore {
        PrefixRoutingObjectStore::new(vec![
            fact_side(store_labelled(FACT_LABEL, paths).await),
            dimension_side(store_labelled(DIM_LABEL, paths).await),
        ])
    }

    /// A fact-first router whose two sides each hold one marker object BELOW
    /// `prefix`, named after that side, so a routed listing's own result names the
    /// side that served it.
    async fn router_with_listing_markers_below(prefix: &str) -> PrefixRoutingObjectStore {
        let fact_marker = format!("{prefix}/{FACT_LABEL}");
        let dimension_marker = format!("{prefix}/{DIM_LABEL}");
        PrefixRoutingObjectStore::new(vec![
            fact_side(store_labelled(FACT_LABEL, &[fact_marker.as_str()]).await),
            dimension_side(store_labelled(DIM_LABEL, &[dimension_marker.as_str()]).await),
        ])
    }

    async fn side_enumerating_shared(
        label: &'static str,
        root: &str,
        shared: &FileEntry,
    ) -> RoutedSide {
        RoutedSide::new(
            &scan_side(label, std::slice::from_ref(shared), root),
            store_labelled(label, &[SHARED]).await,
        )
        .expect("side enumerating the shared path")
    }

    async fn text_at(store: &dyn ObjectStore, path: &str) -> String {
        let payload = store
            .get(&ObjectStorePath::from(path))
            .await
            .expect("get through the router")
            .bytes()
            .await
            .expect("read payload");
        String::from_utf8(payload.to_vec()).expect("utf-8 payload")
    }

    async fn listed_locations(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
        store
            .list(Some(&ObjectStorePath::from(prefix)))
            .map(|meta| meta.expect("listed object").location.to_string())
            .collect()
            .await
    }

    async fn single_delete_stream_result(
        store: &dyn ObjectStore,
        path: &str,
    ) -> object_store::Result<ObjectStorePath> {
        let path = ObjectStorePath::from(path);
        let mut results: Vec<_> = store
            .delete_stream(futures::stream::once(async move { Ok(path) }).boxed())
            .collect()
            .await;
        assert_eq!(results.len(), 1, "one streamed path yields one result");
        results.remove(0)
    }

    #[test]
    fn side_construction_rejects_a_file_path_that_is_no_valid_object_store_path() {
        let err = RoutedSide::new(
            &scan_side("fact", &[entry("data//f1.parquet")], "s3://bucket/wh/fact"),
            Arc::new(InMemory::new()),
        )
        .expect_err("an empty path segment cannot be resolved to an object-store path");

        assert!(
            err.to_string().contains("data//f1.parquet"),
            "error must name the offending file path, got: {err}"
        );
    }

    #[test]
    fn side_construction_rejects_a_table_root_that_is_no_valid_object_store_path() {
        let err = RoutedSide::new(
            &scan_side("fact", &[], "s3://bucket/wh//fact"),
            Arc::new(InMemory::new()),
        )
        .expect_err("an empty path segment cannot be resolved to an object-store path");

        assert!(
            err.to_string().contains("s3://bucket/wh//fact"),
            "error must name the offending table root, got: {err}"
        );
    }

    #[tokio::test]
    async fn each_sides_own_enumerated_file_path_routes_to_that_side() {
        let router = router_over_both_sides(&[FACT_FILE, DIM_FILE]).await;

        assert_eq!(text_at(&router, FACT_FILE).await, FACT_LABEL);
        assert_eq!(text_at(&router, DIM_FILE).await, DIM_LABEL);
    }

    #[tokio::test]
    async fn an_out_of_tree_positional_delete_file_routes_to_its_data_files_side() {
        const OUT_OF_TREE_DELETE: &str = "deletes/f1-deletes.parquet";
        let fact = RoutedSide::new(
            &scan_side(
                FACT_LABEL,
                &[entry_with_delete(
                    "data/f1.parquet",
                    "s3://bucket/deletes/f1-deletes.parquet",
                )],
                FACT_ROOT,
            ),
            store_labelled(FACT_LABEL, &[OUT_OF_TREE_DELETE]).await,
        )
        .expect("fact side");
        let router = PrefixRoutingObjectStore::new(vec![
            fact,
            dimension_side(store_labelled(DIM_LABEL, &[OUT_OF_TREE_DELETE]).await),
        ]);

        assert_eq!(text_at(&router, OUT_OF_TREE_DELETE).await, FACT_LABEL);
    }

    #[tokio::test]
    async fn a_path_under_one_sides_root_that_neither_side_enumerates_routes_by_root() {
        const UNDER_DIM_ROOT: &str = "wh/dim/data/unlisted.parquet";
        let router = router_over_both_sides(&[UNDER_DIM_ROOT]).await;

        assert_eq!(text_at(&router, UNDER_DIM_ROOT).await, DIM_LABEL);
    }

    #[tokio::test]
    async fn path_outside_every_side_errors_naming_path_and_roots() {
        const FOREIGN: &str = "elsewhere/x.parquet";
        let router = router_over_both_sides(&[FOREIGN]).await;

        let error = router
            .get(&ObjectStorePath::from(FOREIGN))
            .await
            .expect_err("no side owns the path or a root covering it");

        let message = error.to_string();
        for named in [FOREIGN, FACT_LABEL, "wh/fact", DIM_LABEL, "wh/dim"] {
            assert!(
                message.contains(named),
                "error must name '{named}', got: {message}"
            );
        }
    }

    #[tokio::test]
    async fn nested_side_roots_route_by_the_longest_matching_root() {
        const UNDER_BOTH_ROOTS: &str = "wh/dim/data/unlisted.parquet";
        const UNDER_THE_OUTER_ROOT_ONLY: &str = "wh/other/unlisted.parquet";
        let seeded = [UNDER_BOTH_ROOTS, UNDER_THE_OUTER_ROOT_ONLY];
        // The fact side comes FIRST and holds the SHORTER root, so a
        // first-match-wins root rule would claim both paths for it.
        let router = PrefixRoutingObjectStore::new(vec![
            RoutedSide::new(
                &scan_side(
                    FACT_LABEL,
                    &[entry("fact/data/f1.parquet")],
                    "s3://bucket/wh",
                ),
                store_labelled(FACT_LABEL, &seeded).await,
            )
            .expect("fact side"),
            dimension_side(store_labelled(DIM_LABEL, &seeded).await),
        ]);

        assert_eq!(text_at(&router, UNDER_BOTH_ROOTS).await, DIM_LABEL);
        assert_eq!(
            text_at(&router, UNDER_THE_OUTER_ROOT_ONLY).await,
            FACT_LABEL
        );
    }

    #[tokio::test]
    async fn a_path_both_sides_enumerate_routes_to_the_earlier_fact_side() {
        let shared = entry(SHARED_URI);
        let fact_first = PrefixRoutingObjectStore::new(vec![
            side_enumerating_shared(FACT_LABEL, FACT_ROOT, &shared).await,
            side_enumerating_shared(DIM_LABEL, DIM_ROOT, &shared).await,
        ]);
        let dimension_first = PrefixRoutingObjectStore::new(vec![
            side_enumerating_shared(DIM_LABEL, DIM_ROOT, &shared).await,
            side_enumerating_shared(FACT_LABEL, FACT_ROOT, &shared).await,
        ]);

        assert_eq!(text_at(&fact_first, SHARED).await, FACT_LABEL);
        // Reversed, the dimension side wins — so both sides really do own the path
        // and the tie is broken by the sides' order alone.
        assert_eq!(text_at(&dimension_first, SHARED).await, DIM_LABEL);
    }

    #[tokio::test]
    async fn a_side_without_a_table_root_routes_only_the_paths_it_enumerates() {
        const UNDER_NO_ROOT: &str = "wh/fact/data/unlisted.parquet";
        let seeded = [FACT_FILE, UNDER_NO_ROOT];
        let router = PrefixRoutingObjectStore::new(vec![
            RoutedSide::new(
                &scan_side(
                    FACT_LABEL,
                    &[entry("s3://bucket/wh/fact/data/f1.parquet")],
                    "",
                ),
                store_labelled(FACT_LABEL, &seeded).await,
            )
            .expect("fact side"),
            dimension_side(store_labelled(DIM_LABEL, &seeded).await),
        ]);

        assert_eq!(text_at(&router, FACT_FILE).await, FACT_LABEL);
        let error = router
            .get(&ObjectStorePath::from(UNDER_NO_ROOT))
            .await
            .expect_err("a rootless side must not claim a path it never enumerated");
        assert!(
            error.to_string().contains(UNDER_NO_ROOT),
            "error must name the unroutable path, got: {error}"
        );
    }

    #[tokio::test]
    async fn listing_an_exact_file_prefix_routes_to_that_files_side() {
        let router = router_with_listing_markers_below(DIM_FILE).await;

        assert_eq!(
            listed_locations(&router, DIM_FILE).await,
            vec![format!("{DIM_FILE}/{DIM_LABEL}")]
        );
    }

    #[tokio::test]
    async fn listing_a_table_root_prefix_routes_to_that_roots_side() {
        const DIM_ROOT_PREFIX: &str = "wh/dim";
        let router = router_with_listing_markers_below(DIM_ROOT_PREFIX).await;

        assert_eq!(
            listed_locations(&router, DIM_ROOT_PREFIX).await,
            vec![format!("{DIM_ROOT_PREFIX}/{DIM_LABEL}")]
        );
    }

    #[tokio::test]
    async fn a_listing_carrying_no_prefix_is_unroutable() {
        let router = router_over_both_sides(&[FACT_FILE]).await;

        let listed = router
            .list(None)
            .next()
            .await
            .expect("one stream item")
            .expect_err("a bucket-wide listing cannot be attributed to one side");
        let delimited = router
            .list_with_delimiter(None)
            .await
            .expect_err("a bucket-wide listing cannot be attributed to one side");

        for error in [listed, delimited] {
            let message = error.to_string();
            assert!(
                message.contains("wh/fact") && message.contains("wh/dim"),
                "error must name the roots that were tried, got: {message}"
            );
        }
    }

    #[tokio::test]
    async fn a_one_side_router_errors_on_a_path_that_side_does_not_own() {
        let router = PrefixRoutingObjectStore::new(vec![fact_side(
            store_labelled(FACT_LABEL, &[DIM_FILE]).await,
        )]);

        let error = router
            .get(&ObjectStorePath::from(DIM_FILE))
            .await
            .expect_err("the only side owns neither that path nor a root covering it");

        let message = error.to_string();
        assert!(
            message.contains(DIM_FILE) && message.contains("wh/fact"),
            "error must name the path and the one side's root, got: {message}"
        );
    }

    #[tokio::test]
    async fn copying_within_one_side_delegates_to_that_sides_store() {
        const FACT_COPY: &str = "wh/fact/data/f1-copy.parquet";
        let router = router_over_both_sides(&[FACT_FILE]).await;

        router
            .copy(
                &ObjectStorePath::from(FACT_FILE),
                &ObjectStorePath::from(FACT_COPY),
            )
            .await
            .expect("copy within one side");

        assert_eq!(text_at(&router, FACT_COPY).await, FACT_LABEL);
    }

    #[tokio::test]
    async fn copying_across_two_sides_is_refused() {
        let router = router_over_both_sides(&[FACT_FILE, DIM_FILE]).await;

        let error = router
            .copy(
                &ObjectStorePath::from(FACT_FILE),
                &ObjectStorePath::from(DIM_FILE),
            )
            .await
            .expect_err("one operation cannot span two credential scopes");

        let message = error.to_string();
        assert!(
            message.contains(FACT_LABEL) && message.contains(DIM_LABEL),
            "error must name both sides, got: {message}"
        );
    }

    #[tokio::test]
    async fn renaming_across_two_sides_is_refused() {
        let router = router_over_both_sides(&[FACT_FILE, DIM_FILE]).await;

        let error = router
            .rename(
                &ObjectStorePath::from(FACT_FILE),
                &ObjectStorePath::from(DIM_FILE),
            )
            .await
            .expect_err("one operation cannot span two credential scopes");

        let message = error.to_string();
        assert!(
            message.contains(FACT_LABEL) && message.contains(DIM_LABEL),
            "error must name both sides, got: {message}"
        );
    }

    #[tokio::test]
    async fn a_streamed_delete_routes_each_path_to_its_own_side() {
        let router = router_over_both_sides(&[FACT_FILE, DIM_FILE]).await;

        single_delete_stream_result(&router, DIM_FILE)
            .await
            .expect("the dimension side's delete");

        let error = router
            .get(&ObjectStorePath::from(DIM_FILE))
            .await
            .expect_err("the dimension side's object was deleted");
        assert!(
            matches!(error, object_store::Error::NotFound { .. }),
            "the delete must have hit the dimension side's store, got: {error}"
        );
        assert_eq!(text_at(&router, FACT_FILE).await, FACT_LABEL);
    }

    #[tokio::test]
    async fn a_streamed_delete_of_a_path_no_side_owns_errors() {
        const FOREIGN: &str = "elsewhere/x.parquet";
        let router = router_over_both_sides(&[FOREIGN]).await;

        let error = single_delete_stream_result(&router, FOREIGN)
            .await
            .expect_err("no side owns the path");

        assert!(
            error.to_string().contains(FOREIGN),
            "error must name the unroutable path, got: {error}"
        );
    }
}
