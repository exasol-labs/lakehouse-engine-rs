use super::ScanUdfs;
use crate::adapter::pushdown::test_support::test_context;
use crate::scan::spec::{FileEntry, ScanStorage};

#[test]
fn the_udfs_are_qualified_by_a_nonempty_schema() {
    let qualified = ScanUdfs::in_schema(Some("schema"));
    assert_eq!(qualified.scan, "\"schema\".LAKEHOUSE_SCAN");
    assert_eq!(
        qualified.distribute,
        "\"schema\".LAKEHOUSE_DISTRIBUTE_FILES"
    );
    for unqualified in [ScanUdfs::in_schema(Some("")), ScanUdfs::in_schema(None)] {
        assert_eq!(unqualified.scan, "LAKEHOUSE_SCAN");
        assert_eq!(unqualified.distribute, "LAKEHOUSE_DISTRIBUTE_FILES");
    }
}

/// Scenario: The parallelism factor drives work-unit sharding
#[test]
fn the_shard_count_is_the_parallelism_factor_times_the_node_count() {
    let files = |n: usize| (0..n).map(|i| FileEntry::new(format!("f{i}"), 1)).collect();
    for (nodes, factor, file_count, expected) in [(2, 3, 10, 6), (2, 3, 4, 4), (100, 4, 1000, 300)]
    {
        let mut context = test_context();
        context.cluster_nodes = nodes;
        context.tuning.parallelism_factor = factor;
        let shards = context.shards(files(file_count), "");
        assert_eq!(
            shards.files.len(),
            expected,
            "{nodes} nodes, factor {factor}, {file_count} files"
        );
    }
}

/// Scenario: The memory budget controls reach the scan spec
#[test]
fn the_common_spec_carries_the_resolved_tuning() {
    let mut context = test_context();
    context.tuning.memory_pool_fraction = 0.4;
    context.tuning.instance_overhead_mb = 123;
    context.tuning.s3_max_connections = 9;
    let spec = context.common_spec(
        String::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ScanStorage::Connection {
            name: "C".to_string(),
            allow_http: false,
        },
    );
    assert_eq!(spec.memory_pool_fraction, 0.4);
    assert_eq!(spec.instance_overhead_mb, 123);
    assert_eq!(spec.s3_max_connections, 9);
}
