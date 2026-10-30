use super::super::test_support::*;
use super::*;
use crate::scan::spec::{DeleteMechanism, DeltaDeletionVectorStorage};

#[test]
fn delete_file_paths_use_relative_absolute_encoding() {
    let root = "s3://warehouse/db/table";
    let entry = FileEntry::with_deletes(
        format!("{root}/data/part-0.parquet"),
        1000,
        vec![
            pos_delete(&format!("{root}/data/deletes/del-0.parquet"), 50),
            pos_delete("s3://other-bucket/del-x.parquet", 60),
        ],
    );
    let shards = relativize_shards_to_root(vec![vec![entry]], root);
    let e = &shards[0][0];
    assert_eq!(e.path, "data/part-0.parquet", "data path must relativize");
    assert_eq!(
        e.deletes[0],
        DeleteMechanism::IcebergPositionalDelete {
            path: "data/deletes/del-0.parquet".into(),
            size: 50,
        },
        "under-root delete path must relativize EXACTLY like the data path, with size \
         and mechanism preserved"
    );
    assert_eq!(
        e.deletes[1],
        DeleteMechanism::IcebergPositionalDelete {
            path: "s3://other-bucket/del-x.parquet".into(),
            size: 60,
        },
        "a delete path not under the root must stay absolute"
    );
}

#[test]
fn relativization_leaves_a_deletion_vectors_path_or_inline_dv_untouched() {
    let root = "s3://warehouse/db/table";
    let under_root_vector = DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::AbsolutePath,
        path_or_inline_dv: format!("{root}/deletion_vector.bin"),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    };
    let uuid_vector = DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
        offset: None,
        size_in_bytes: 36,
        cardinality: 2,
    };

    let shards = relativize_shards_to_root(
        vec![vec![
            FileEntry::with_deletes(
                format!("{root}/data/part-0.parquet"),
                1000,
                vec![under_root_vector.clone()],
            ),
            FileEntry::with_deletes(
                format!("{root}/data/part-1.parquet"),
                1000,
                vec![uuid_vector.clone()],
            ),
        ]],
        root,
    );

    assert_eq!(
        shards[0][0].path, "data/part-0.parquet",
        "the data file path must still relativize"
    );
    assert_eq!(
        shards[0][0].deletes[0], under_root_vector,
        "an under-root absolute path_or_inline_dv must survive untouched"
    );
    assert_eq!(
        shards[0][1].deletes[0], uuid_vector,
        "a UUID path_or_inline_dv must survive untouched"
    );
}

fn relativize(path: &str, table_root: &str) -> String {
    relativize_path_to_root(path, ObjectLocation::parse_wire(table_root).ok().as_ref())
}

/// Mirror of the scan UDF's `reconstruct_abs_uri` join rule, avoiding a cross-crate dependency.
fn reconstruct_abs_uri_mirror(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

#[test]
fn sibling_prefix_paths_are_not_relativized() {
    let root = "s3://w/db/events";

    let under = format!("{root}/data/f.parquet");
    assert_eq!(
        relativize(&under, root),
        "data/f.parquet",
        "under-root path must be relativized"
    );

    let archive = format!("{root}-archive/f.parquet");
    assert_eq!(
        relativize(&archive, root),
        archive,
        "sibling '-archive' path must stay absolute"
    );
    let sibling2 = format!("{root}2/data/f.parquet");
    assert_eq!(
        relativize(&sibling2, root),
        sibling2,
        "sibling '2' path must stay absolute"
    );

    assert_eq!(
        relativize(root, root),
        root,
        "path equal to the root must stay absolute, not become an empty entry"
    );

    for original in [&under, &archive, &sibling2, &root.to_string()] {
        let emitted = relativize(original, root);
        assert_eq!(
            reconstruct_abs_uri_mirror(&emitted, root),
            *original,
            "round-trip must be identity for {original}"
        );
    }
}

#[test]
fn abfss_paths_relativize_and_reconstruct_losslessly() {
    let root = "abfss://container@account.dfs.core.windows.net/db/table";
    let original = format!("{root}/data/part-0.parquet");

    let relative = relativize(&original, root);
    assert_eq!(
        relative, "data/part-0.parquet",
        "abfss path under the root must relativize just like s3"
    );

    let reconstructed = reconstruct_abs_uri_mirror(&relative, root);
    assert_eq!(
        reconstructed, original,
        "reconstructed abfss URI must equal the original byte-for-byte"
    );
}

#[test]
fn a_root_holding_escaped_characters_still_relativizes_its_files() {
    let raw_root = "s3://bucket/db/odd%3D#?€/t";
    let root = ObjectLocation::from_raw(raw_root).unwrap().to_wire();
    let wire = |raw: &str| ObjectLocation::from_raw(raw).unwrap().to_wire();
    let entry = FileEntry::with_deletes(
        wire(&format!("{raw_root}/data/f%3D.parquet")),
        1000,
        vec![pos_delete(&wire(&format!("{raw_root}/data/d.parquet")), 50)],
    );

    let shards = relativize_shards_to_root(vec![vec![entry]], &root);

    assert_eq!(root, "s3://bucket/db/odd%253D%23%3F%E2%82%AC/t");
    assert_eq!(shards[0][0].path, "data/f%253D.parquet");
    assert_eq!(
        shards[0][0].deletes[0].object_store_path(),
        Some("data/d.parquet")
    );
}
