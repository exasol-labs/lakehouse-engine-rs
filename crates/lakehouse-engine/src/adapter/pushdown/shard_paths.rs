use crate::scan::ObjectLocation;
use crate::scan::spec::FileEntry;

/// Compared by raw key parts, so a root and a path escaped apart still match, and only at a real
/// segment boundary: `<root>-archive/...` or a path equal to the root stays absolute.
fn relativize_path_to_root(path: &str, table_root: Option<&ObjectLocation>) -> String {
    let relative = table_root
        .filter(|_| path.contains("://"))
        .and_then(|root| ObjectLocation::parse_wire(path).ok()?.relative_to(root));
    relative.unwrap_or_else(|| path.to_string())
}

/// A delete mechanism with no object-store path (a Delta deletion vector's
/// `path_or_inline_dv`) is left untouched: the scan resolves it later, and
/// relativizing it would corrupt it.
pub(super) fn relativize_shards_to_root(
    shards: Vec<Vec<FileEntry>>,
    table_root: &str,
) -> Vec<Vec<FileEntry>> {
    let root = ObjectLocation::parse_wire(table_root).ok();
    shards
        .into_iter()
        .map(|shard| {
            shard
                .into_iter()
                .map(|mut entry| {
                    entry.path = relativize_path_to_root(&entry.path, root.as_ref());
                    for delete in &mut entry.deletes {
                        if let Some(path) = delete.object_store_path_mut() {
                            *path = relativize_path_to_root(path, root.as_ref());
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
