/// A shardable file whose byte size drives the byte-balancing heuristic.
///
/// Implemented both for the legacy `(path, size)` tuple (still used by unit
/// tests and any pre-`FileEntry` caller) and for [`FileEntry`](crate::scan::spec::FileEntry)
/// (the production shard element, which additionally carries its associated
/// positional-delete refs). The delete refs ride along with their data file
/// through sharding untouched — only `shard_bytes` participates in balancing.
pub trait ShardWeight {
    /// The byte size used to balance this file across shards.
    fn shard_bytes(&self) -> u64;
}

impl ShardWeight for (String, u64) {
    fn shard_bytes(&self) -> u64 {
        self.1
    }
}

impl ShardWeight for crate::scan::spec::FileEntry {
    fn shard_bytes(&self) -> u64 {
        self.size
    }
}

/// Partitions a list of files into `n` byte-balanced, disjoint shards using the
/// Longest-Processing-Time-first (LPT) greedy heuristic.
///
/// Generic over any [`ShardWeight`] element, so both the legacy `(path, size)`
/// tuple and [`FileEntry`](crate::scan::spec::FileEntry) shard identically; a
/// `FileEntry`'s positional-delete refs travel with their data file (each entry
/// is moved whole into its shard), never split off.
///
/// Algorithm:
/// 1. Clamp `n` to `[1, files.len()]` so every shard is non-empty.
/// 2. Sort files by size descending (treat 0-byte files as 1 byte so they are
///    never skipped and land in the currently lightest shard).
/// 3. Greedily assign each file to the shard with the smallest cumulative byte total.
///
/// Each shard carries its entries verbatim (the original input size, with 0-byte
/// files reported as `0`; the 0→1 treatment applies only to balancing, not to
/// the emitted size). Empty input → empty `Vec`.
pub fn partition_files_by_bytes<T: ShardWeight>(files: Vec<T>, n: usize) -> Vec<Vec<T>> {
    if files.is_empty() {
        return vec![];
    }

    let shard_count = n.max(1).min(files.len());

    // Sort descending by effective size (0 treated as 1 for ordering).
    let mut sorted = files;
    sorted.sort_by(|a, b| {
        let ea = a.shard_bytes().max(1);
        let eb = b.shard_bytes().max(1);
        eb.cmp(&ea)
    });

    // Each entry: (file_entries, cumulative_bytes).
    let mut shards: Vec<(Vec<T>, u64)> = (0..shard_count).map(|_| (vec![], 0u64)).collect();

    for file in sorted {
        let effective_size = file.shard_bytes().max(1);
        // Find the shard with the smallest cumulative byte total.
        let (lightest_idx, _) = shards
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, bytes))| *bytes)
            .expect("shards is non-empty");
        shards[lightest_idx].0.push(file);
        shards[lightest_idx].1 += effective_size;
    }

    shards.into_iter().map(|(entries, _)| entries).collect()
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
            .map(|s| {
                s.iter()
                    .map(|(p, _): &(String, u64)| sizes_map[p.as_str()])
                    .sum()
            })
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

        let all_files: Vec<String> = shards.iter().flatten().map(|(p, _)| p.clone()).collect();
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

        let all_files: Vec<String> = shards.iter().flatten().map(|(p, _)| p.clone()).collect();
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

    /// Scenario: Each shard entry carries `(path, size)` and the size matches the
    /// original input size for that path — including a 0-byte file, whose reported
    /// size stays `0` (the 0→1 rule affects only balancing, not the emitted size).
    #[test]
    fn partition_by_bytes_propagates_size_into_shards() {
        let files: Vec<(String, u64)> = vec![
            ("a.parquet".into(), 100),
            ("b.parquet".into(), 200),
            ("c.parquet".into(), 300),
            ("zero.parquet".into(), 0),
        ];
        let expected: std::collections::HashMap<String, u64> = files.iter().cloned().collect();

        let shards = partition_files_by_bytes(files, 2);

        let mut seen: HashSet<String> = HashSet::new();
        for shard in &shards {
            for (path, size) in shard {
                assert_eq!(
                    *size, expected[path],
                    "size for {path} does not match the input size"
                );
                assert!(
                    seen.insert(path.clone()),
                    "duplicate path {path} across shards"
                );
            }
        }
        assert_eq!(seen.len(), expected.len(), "not all files covered");
    }
}
