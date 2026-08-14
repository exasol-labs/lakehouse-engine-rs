use crate::scan::spec::FileEntry;

/// Emit a file path relative to `table_root` when the file lives under it,
/// otherwise pass the absolute path through unchanged.
///
/// Stripping happens ONLY at a real path-segment boundary: the root must be a
/// prefix AND either end with `/` or be followed by a `/` in the path. A path that
/// merely shares the root as a bare string prefix (e.g. `<root>-archive/...`,
/// `<root>2/...`) or one exactly equal to the root does NOT match, so it stays
/// absolute — this keeps the round-trip with the scan UDF's single-`/` join lossless
/// and avoids emitting an empty relative entry. After a boundary match the root
/// prefix and then a single leading `/` are stripped, so the relative path has no
/// leading slash. An empty `table_root` (legacy / no resolved root) always yields an
/// absolute path.
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

/// Strip `table_root` from every under-root file path in each shard (see
/// [`relativize_path_to_root`]) while preserving byte sizes and shard membership.
/// Paths not under the root stay absolute.
///
/// A delete mechanism naming a delete FILE
/// ([`DeleteMechanism::object_store_path`]) has that path relativized by the SAME
/// [`relativize_path_to_root`] rule as the data-file path, so the scan UDF rejoins
/// them onto `table_root` identically (delete files written by the same engine live
/// under the same table root). A mechanism naming no path — a Delta deletion vector, whose `path_or_inline_dv`
/// is resolved into a path at file registration and never addressed from the delete
/// list itself — is left entirely untouched: relativizing it would corrupt a value
/// the scan resolves later, regardless of whether it looks like a UUID token, an
/// inline payload, or an absolute path. Every other member of every mechanism is
/// preserved unchanged.
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
