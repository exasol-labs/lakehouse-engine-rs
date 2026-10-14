use crate::scan::spec::FileEntry;

/// Strips only at a real path-segment boundary, so `<root>-archive/...` or a path
/// equal to the root stays absolute; this keeps the round-trip with the scan UDF's
/// single-`/` join lossless and never emits an empty relative entry.
fn relativize_path_to_root(path: &str, table_root: &str) -> String {
    let at_segment_boundary = !table_root.is_empty()
        && path.starts_with(table_root)
        && (table_root.ends_with('/') || path[table_root.len()..].starts_with('/'));
    if at_segment_boundary {
        let rest = &path[table_root.len()..];
        rest.strip_prefix('/').unwrap_or(rest).to_string()
    } else {
        path.to_string()
    }
}

/// A delete mechanism with no object-store path (a Delta deletion vector's
/// `path_or_inline_dv`) is left untouched: the scan resolves it later, and
/// relativizing it would corrupt it.
pub(super) fn relativize_shards_to_root(
    shards: Vec<Vec<FileEntry>>,
    table_root: &str,
) -> Vec<Vec<FileEntry>> {
    shards
        .into_iter()
        .map(|shard| {
            shard
                .into_iter()
                .map(|mut entry| {
                    entry.path = relativize_path_to_root(&entry.path, table_root);
                    for delete in &mut entry.deletes {
                        if let Some(path) = delete.object_store_path_mut() {
                            *path = relativize_path_to_root(path, table_root);
                        }
                    }
                    entry
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
#[path = "shard_paths_tests.rs"]
mod tests;
