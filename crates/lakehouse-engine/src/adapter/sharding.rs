/// Partitions a list of file paths into balanced, disjoint shards for
/// multi-node scan distribution.
///
/// Produces exactly `min(n.max(1), files.len())` shards. File counts across
/// shards differ by at most one. Returns an empty `Vec` when `files` is empty.
/// When `n` is zero it is treated as one, so a non-empty file list always
/// yields at least one shard.
pub fn partition_files(files: Vec<String>, n: usize) -> Vec<Vec<String>> {
    if files.is_empty() {
        return vec![];
    }
    let len = files.len();
    let shard_count = n.max(1).min(len);
    let base = len / shard_count;
    let remainder = len % shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut start = 0;
    for i in 0..shard_count {
        let extra = usize::from(i < remainder);
        let end = start + base + extra;
        shards.push(files[start..end].to_vec());
        start = end;
    }
    shards
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn file_list(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("file-{i}.parquet")).collect()
    }

    #[test]
    fn partition_balanced_disjoint_full_coverage() {
        let files = file_list(10);
        let n = 3;
        let shards = partition_files(files.clone(), n);

        let expected_shard_count = n.min(files.len());
        assert_eq!(shards.len(), expected_shard_count, "wrong shard count");

        let all_files: Vec<String> = shards.iter().flatten().cloned().collect();
        let unique_files: HashSet<&String> = all_files.iter().collect();
        assert_eq!(
            unique_files.len(),
            all_files.len(),
            "files appear in more than one shard"
        );
        assert_eq!(
            unique_files,
            files.iter().collect::<HashSet<_>>(),
            "not all files covered"
        );

        let sizes: Vec<usize> = shards.iter().map(|s| s.len()).collect();
        let max_size = *sizes.iter().max().unwrap();
        let min_size = *sizes.iter().min().unwrap();
        assert!(
            max_size - min_size <= 1,
            "shards are not balanced: max={max_size}, min={min_size}"
        );
    }

    #[test]
    fn partition_caps_shards_at_file_count() {
        let files = file_list(3);
        let shards = partition_files(files.clone(), 10);

        assert_eq!(
            shards.len(),
            files.len(),
            "shard count must equal file count"
        );
        for shard in &shards {
            assert_eq!(shard.len(), 1, "each shard must contain exactly one file");
        }
    }

    #[test]
    fn partition_single_node_returns_one_shard_with_all_files_in_order() {
        let files = file_list(5);
        let shards = partition_files(files.clone(), 1);

        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], files);
    }

    #[test]
    fn partition_empty_input_returns_empty_vec() {
        let shards = partition_files(vec![], 4);
        assert!(shards.is_empty());
    }

    #[test]
    fn partition_uneven_split_sizes_differ_by_one() {
        let files = file_list(7);
        let shards = partition_files(files.clone(), 3);

        assert_eq!(shards.len(), 3);
        let sizes: Vec<usize> = shards.iter().map(|s| s.len()).collect();
        assert_eq!(
            sizes,
            vec![3, 2, 2],
            "expected sizes 3,2,2 for 7 files into 3 shards"
        );

        let all_files: Vec<String> = shards.into_iter().flatten().collect();
        assert_eq!(all_files, files, "file order must be preserved");
    }
}
