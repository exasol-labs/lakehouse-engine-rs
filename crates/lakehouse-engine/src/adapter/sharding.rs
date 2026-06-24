/// Partitions a list of `(file_path, size_bytes)` pairs into `n` byte-balanced,
/// disjoint shards using the Longest-Processing-Time-first (LPT) greedy heuristic.
///
/// Algorithm:
/// 1. Clamp `n` to `[1, files.len()]` so every shard is non-empty.
/// 2. Sort files by size descending (treat 0-byte files as 1 byte so they are
///    never skipped and land in the currently lightest shard).
/// 3. Greedily assign each file to the shard with the smallest cumulative byte total.
///
/// Returns `Vec<Vec<String>>` (paths only, no sizes). Empty input → empty `Vec`.
pub fn partition_files_by_bytes(files: Vec<(String, u64)>, n: usize) -> Vec<Vec<String>> {
    if files.is_empty() {
        return vec![];
    }

    let shard_count = n.max(1).min(files.len());

    // Sort descending by effective size (0 treated as 1 for ordering).
    let mut sorted = files;
    sorted.sort_by(|(_, a), (_, b)| {
        let ea = if *a == 0 { 1 } else { *a };
        let eb = if *b == 0 { 1 } else { *b };
        eb.cmp(&ea)
    });

    // Each entry: (file_paths, cumulative_bytes).
    let mut shards: Vec<(Vec<String>, u64)> = (0..shard_count).map(|_| (vec![], 0u64)).collect();

    for (path, size) in sorted {
        let effective_size = if size == 0 { 1 } else { size };
        // Find the shard with the smallest cumulative byte total.
        let (lightest_idx, _) = shards
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, bytes))| *bytes)
            .expect("shards is non-empty");
        shards[lightest_idx].0.push(path);
        shards[lightest_idx].1 += effective_size;
    }

    shards.into_iter().map(|(paths, _)| paths).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ---------------------------------------------------------------------------
    // partition_files_by_bytes tests
    // ---------------------------------------------------------------------------

    /// Scenario: G shards are byte-balanced — the maximum cumulative shard size
    /// minus the minimum is less than the largest single file size.
    #[test]
    fn partition_by_bytes_balances_cumulative_size() {
        // 6 files with sizes: 100, 200, 300, 400, 500, 600 → total 2100
        let files: Vec<(String, u64)> = vec![
            ("a.parquet".into(), 100),
            ("b.parquet".into(), 200),
            ("c.parquet".into(), 300),
            ("d.parquet".into(), 400),
            ("e.parquet".into(), 500),
            ("f.parquet".into(), 600),
        ];
        let shards = partition_files_by_bytes(files, 3);
        assert_eq!(shards.len(), 3, "expected 3 shards");

        // Compute cumulative byte size per shard.
        let sizes_map: std::collections::HashMap<String, u64> = vec![
            ("a.parquet".to_string(), 100),
            ("b.parquet".to_string(), 200),
            ("c.parquet".to_string(), 300),
            ("d.parquet".to_string(), 400),
            ("e.parquet".to_string(), 500),
            ("f.parquet".to_string(), 600),
        ]
        .into_iter()
        .collect();

        let shard_bytes: Vec<u64> = shards
            .iter()
            .map(|s| s.iter().map(|f: &String| sizes_map[f.as_str()]).sum())
            .collect();

        let max_bytes = *shard_bytes.iter().max().unwrap();
        let min_bytes = *shard_bytes.iter().min().unwrap();
        // 100+200+300+400+500+600 = 2100 over 3 shards; LPT lands exactly 700 each.
        assert_eq!(
            max_bytes, min_bytes,
            "shards not perfectly balanced: max={max_bytes}, min={min_bytes}"
        );
    }

    /// Scenario: All files appear exactly once across the shards (disjoint + full coverage).
    #[test]
    fn partition_by_bytes_disjoint_full_coverage() {
        let files: Vec<(String, u64)> = (0..10)
            .map(|i| (format!("file-{i}.parquet"), (i as u64 + 1) * 100))
            .collect();
        let shards = partition_files_by_bytes(files.clone(), 4);

        let all_files: Vec<String> = shards.iter().flatten().cloned().collect();
        let unique: HashSet<&String> = all_files.iter().collect();

        assert_eq!(unique.len(), all_files.len(), "duplicate files in shards");
        assert_eq!(
            unique,
            files.iter().map(|(p, _)| p).collect::<HashSet<_>>(),
            "not all files covered"
        );
    }

    /// Scenario: Files with size 0 are treated as size 1 and are never skipped.
    #[test]
    fn partition_by_bytes_zero_size_treated_as_one_never_skipped() {
        let files: Vec<(String, u64)> = vec![
            ("big.parquet".into(), 1000),
            ("zero1.parquet".into(), 0),
            ("zero2.parquet".into(), 0),
            ("tiny.parquet".into(), 1),
        ];
        let shards = partition_files_by_bytes(files.clone(), 2);

        let all_files: Vec<String> = shards.iter().flatten().cloned().collect();
        // All 4 files must appear.
        assert_eq!(all_files.len(), 4, "a zero-size file was dropped");

        let unique: HashSet<&String> = all_files.iter().collect();
        assert_eq!(unique.len(), 4, "duplicate files");
        assert!(
            unique.contains(&"zero1.parquet".to_string()),
            "zero1 must be present"
        );
        assert!(
            unique.contains(&"zero2.parquet".to_string()),
            "zero2 must be present"
        );
    }

    /// Scenario: When G >= file_count, each file gets its own shard.
    #[test]
    fn partition_by_bytes_one_file_per_shard_when_g_exceeds_count() {
        let files: Vec<(String, u64)> = vec![
            ("a.parquet".into(), 100),
            ("b.parquet".into(), 200),
            ("c.parquet".into(), 300),
        ];
        // Request more shards than files.
        let shards = partition_files_by_bytes(files.clone(), 10);

        assert_eq!(shards.len(), 3_usize, "shard count must equal file count");
        for shard in &shards {
            assert_eq!(
                shard.len(),
                1_usize,
                "each shard must contain exactly one file"
            );
        }
    }
}
