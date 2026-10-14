/// Positional-delete refs ride along with their data file; only `shard_bytes` drives balancing.
pub trait ShardWeight {
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

/// Longest-Processing-Time-first greedy split into `n` (clamped to `[1, files.len()]`) disjoint
/// shards. A 0-byte file weighs 1 for balancing only; entries are emitted verbatim.
pub fn partition_files_by_bytes<T: ShardWeight>(files: Vec<T>, n: usize) -> Vec<Vec<T>> {
    if files.is_empty() {
        return vec![];
    }

    let shard_count = n.max(1).min(files.len());

    let mut sorted = files;
    sorted.sort_by(|a, b| {
        let ea = a.shard_bytes().max(1);
        let eb = b.shard_bytes().max(1);
        eb.cmp(&ea)
    });

    let mut shards: Vec<(Vec<T>, u64)> = (0..shard_count).map(|_| (vec![], 0u64)).collect();

    for file in sorted {
        let effective_size = file.shard_bytes().max(1);
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
