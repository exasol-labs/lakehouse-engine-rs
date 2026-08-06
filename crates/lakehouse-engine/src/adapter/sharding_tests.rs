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
