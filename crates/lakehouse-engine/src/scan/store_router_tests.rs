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
fn scan_side<'a>(label: &'static str, files: &'a [FileEntry], table_root: &'a str) -> ScanSide<'a> {
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
