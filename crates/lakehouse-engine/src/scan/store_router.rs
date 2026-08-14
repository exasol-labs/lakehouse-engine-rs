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
    /// The owned set holds every data file AND every delete FILE an entry's
    /// mechanisms name, since the scan requests both; a mechanism carrying no
    /// object-store path of its own claims nothing, having no path to route. Both
    /// the owned paths and the
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
                if let Some(path) = delete.object_store_path() {
                    owned.insert(store_path(path, side.table_root)?);
                }
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
    /// touched through a single inner store, and routing is per side, so a pair
    /// spanning two sides has no store covering it. The refusal is about cross-side
    /// path OWNERSHIP, not credential inequality — this router is installed for every
    /// join, including the common same-warehouse case where both sides' backends are
    /// byte-identical, and it must not claim a difference it never compared.
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
                     join sides ('{}' and '{}'), and each side is served by its OWN store, so no \
                     one store covers both paths",
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
#[path = "store_router_tests.rs"]
mod tests;
