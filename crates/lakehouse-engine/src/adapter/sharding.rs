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
#[path = "sharding_tests.rs"]
mod tests;
