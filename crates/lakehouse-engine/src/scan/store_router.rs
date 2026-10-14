//! DataFusion keys its object-store registry on `scheme://host[:port]` alone, so one bucket gets
//! exactly one store, yet each join side owns a table-scoped vended credential while routinely
//! sharing a bucket (the Databricks norm). [`PrefixRoutingObjectStore`] serves each path from the
//! store of the side owning it, so one side's credential never reads the other's file.
//!
//! Exact enumerated paths match first and the table root only as a fallback: the Iceberg spec
//! permits files outside `location` (Appendix E, Version 4: "Absolute paths must be used for files
//! that do not share a common prefix with the table location"). A path matching neither is a
//! planning defect and errors, since guessing would use a credential of unknown scope.

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

const STORE_NAME: &str = "PrefixRoutingObjectStore";

/// Every field is per-side, `backend` included: a vended credential is scoped to its own table,
/// so the dimension side reads through `JoinSpec::storage`, never `common.storage`.
pub(super) struct ScanSide<'a> {
    pub(super) label: &'static str,
    pub(super) files: &'a [FileEntry],
    pub(super) table_root: &'a str,
    pub(super) backend: &'a StorageBackend,
}

#[derive(Debug)]
pub struct RoutedSide {
    label: &'static str,
    owned: HashSet<ObjectStorePath>,
    root: Option<ObjectStorePath>,
    store: Arc<dyn ObjectStore>,
}

impl RoutedSide {
    /// The owned set holds every data file and delete FILE the scan will request. Paths and root
    /// both derive through `ListingTableUrl::parse(..).prefix()`, sharing one coordinate system.
    /// An empty `side.table_root` yields no root, since an empty prefix would match every path.
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

#[derive(Debug)]
pub struct PrefixRoutingObjectStore {
    // `Arc` so the routing rule is reachable from the `'static` stream `delete_stream` returns.
    sides: Arc<[RoutedSide]>,
}

impl PrefixRoutingObjectStore {
    /// `sides` MUST be ordered fact side first: the first match wins, so order is the tie-break
    /// that keeps one spec from routing one path two ways across invocations.
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

    /// One side must own both paths, since each side has its own store. The refusal is about path
    /// OWNERSHIP, not credential inequality: same-warehouse joins have byte-identical backends.
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

/// First side enumerating `path` exactly, else the side with the LONGEST matching root; ties
/// favour the earlier (fact) side.
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
        // Raw length, not `parts_count` (which reports 1 for the store root); matching roots are
        // nested, so length orders them as segments would.
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

    /// A prefix-less listing is bucket-wide and belongs to no side.
    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.route_listing(prefix) {
            Ok(store) => store.list(prefix),
            Err(error) => failed_stream(error),
        }
    }

    /// `offset` is a resume cursor within the listing, not a second routing target.
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
