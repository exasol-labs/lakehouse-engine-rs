use super::super::test_support::*;
use super::*;
use crate::scan::spec::{DeleteMechanism, DeltaDeletionVectorStorage};

/// A data file's associated positional-delete file paths are relativized by
/// the SAME rule as the data-file path: an under-root path is stripped to a
/// root-relative path, a path not under the root stays absolute. Delete size
/// and mechanism are preserved.
#[test]
fn delete_file_paths_use_relative_absolute_encoding() {
    let root = "s3://warehouse/db/table";
    let entry = FileEntry::with_deletes(
        format!("{root}/data/part-0.parquet"),
        1000,
        vec![
            // under the table root — must relativize exactly like the data path
            pos_delete(&format!("{root}/data/deletes/del-0.parquet"), 50),
            // not under the root — must stay absolute
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

/// A Delta deletion vector rides through relativization byte for byte, while the DATA
/// file path it accompanies relativizes as always.
///
/// The `p` (absolute-path) storage kind is the discriminating case: its
/// `path_or_inline_dv` looks exactly like an under-root object-store path, so a
/// path-blind relativization would strip the root from a value the scan resolves at
/// file registration — turning a resolvable descriptor into an unreadable one.
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

/// Mirror of the scan UDF's `reconstruct_abs_uri` join rule, so the round-trip
/// invariant can be asserted here without a cross-crate dependency: an entry that
/// already carries a scheme (`"://"`) is absolute and returned unchanged; any
/// other entry is joined onto the root with exactly one `/`.
fn reconstruct_abs_uri_mirror(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

/// A path that shares the table root only as a bare STRING prefix (no `/`
/// segment boundary) must NOT be relativized: stripping it and rejoining with a
/// single `/` corrupts the URI (finding R.1). Only true under-root paths are
/// stripped; everything else stays absolute and round-trips to itself.
#[test]
fn sibling_prefix_paths_are_not_relativized() {
    let root = "s3://w/db/events";

    // A genuine under-root path IS relativized (existing behavior preserved).
    let under = format!("{root}/data/f.parquet");
    assert_eq!(
        relativize_path_to_root(&under, root),
        "data/f.parquet",
        "under-root path must be relativized"
    );

    // Sibling directories that share the root as a bare prefix but break at no
    // `/` boundary stay ABSOLUTE (not stripped).
    let archive = format!("{root}-archive/f.parquet");
    assert_eq!(
        relativize_path_to_root(&archive, root),
        archive,
        "sibling '-archive' path must stay absolute"
    );
    let sibling2 = format!("{root}2/data/f.parquet");
    assert_eq!(
        relativize_path_to_root(&sibling2, root),
        sibling2,
        "sibling '2' path must stay absolute"
    );

    // A path exactly equal to the root stays absolute (no empty entry).
    assert_eq!(
        relativize_path_to_root(root, root),
        root,
        "path equal to the root must stay absolute, not become an empty entry"
    );

    // Every case round-trips back to the original absolute path through the
    // scan UDF's reconstruct rule.
    for original in [&under, &archive, &sibling2, &root.to_string()] {
        let emitted = relativize_path_to_root(original, root);
        assert_eq!(
            reconstruct_abs_uri_mirror(&emitted, root),
            *original,
            "round-trip must be identity for {original}"
        );
    }
}

/// The `abfss://` scheme carries userinfo (the container name) in its
/// authority (`abfss://<container>@<account>.dfs.core.windows.net/...`),
/// unlike `s3://`'s bare-bucket authority. The relativize/reconstruct round
/// trip must still be lossless: relativizing an under-root `abfss://` file
/// path against its table root and reconstructing via the scan UDF's join
/// rule must reproduce the original URI byte-for-byte, exactly like the
/// `s3://` case above.
#[test]
fn abfss_paths_relativize_and_reconstruct_losslessly() {
    let root = "abfss://container@account.dfs.core.windows.net/db/table";
    let original = format!("{root}/data/part-0.parquet");

    let relative = relativize_path_to_root(&original, root);
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
