use super::test_support::minimal_spec;
use super::*;

/// Scenario: session_config_for_spec applies df_batch_size and clamps sub-1 values to 1.
#[test]
fn session_config_applies_batch_size_and_clamps_floor() {
    let mut spec = minimal_spec();
    spec.common.df_batch_size = 4096;
    let config = session_config_for_spec(&spec);
    assert_eq!(
        config.batch_size(),
        4096,
        "SessionConfig must use df_batch_size from spec"
    );

    spec.common.df_batch_size = 0;
    let config_clamped = session_config_for_spec(&spec);
    assert_eq!(
        config_clamped.batch_size(),
        1,
        "df_batch_size of 0 must be clamped to 1"
    );
}

/// Scenario: scan enables Parquet row-group and page pruning.
#[test]
fn session_config_enables_parquet_pruning_flags() {
    let config = session_config_for_spec(&minimal_spec());
    let parquet = &config.options().execution.parquet;
    assert!(
        parquet.pruning,
        "row-group statistics pruning must be enabled"
    );
    assert!(
        parquet.enable_page_index,
        "page-index pruning must be enabled"
    );
    assert!(
        parquet.pushdown_filters,
        "predicate pushdown into the Parquet decode must be enabled (DataFusion defaults it off)"
    );
}

/// Scenario: SessionConfig applies target_partitions from the spec.
#[test]
fn session_config_uses_spec_target_partitions() {
    let mut spec = minimal_spec();
    spec.common.df_target_partitions = 4;
    let config = session_config_for_spec(&spec);
    assert_eq!(
        config.target_partitions(),
        4,
        "SessionConfig must use df_target_partitions from spec"
    );
}

/// Scenario: df_threads_per_udf == 1 selects the current-thread runtime.
#[test]
fn runtime_is_current_thread_when_threads_is_one() {
    let rt = build_scan_runtime(1).expect("runtime must build");
    assert_eq!(
        rt.handle().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread,
        "df_threads_per_udf == 1 must yield a current-thread runtime"
    );
}

/// Scenario: df_threads_per_udf > 1 selects the multi-thread runtime.
#[test]
fn runtime_is_multi_thread_when_threads_exceeds_one() {
    let rt = build_scan_runtime(4).expect("runtime must build");
    assert_eq!(
        rt.handle().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "df_threads_per_udf > 1 must yield a multi-thread runtime"
    );
}

/// Scenario: a multi-thread runtime with live detached tasks is torn down deterministically.
#[test]
fn run_on_runtime_tears_down_multi_thread_runtime_with_live_background_task() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let rt = build_scan_runtime(2).expect("multi-thread runtime must build");
    let parked_started = Arc::new(AtomicBool::new(false));
    let started_in_future = parked_started.clone();

    let started_in_outer = parked_started.clone();
    let before = std::time::Instant::now();
    let result = run_on_runtime(rt, async move {
        // Host analog of the hyper pool/reaper tasks object_store keeps alive past the last poll.
        tokio::spawn(async move {
            started_in_future.store(true, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        });
        // A single `yield_now()` is flaky under a loaded scheduler; wait until the task is live.
        while !started_in_outer.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        42_u32
    });
    let elapsed = before.elapsed();

    assert_eq!(result, 42, "future result must be returned before teardown");
    assert!(
        elapsed < RUNTIME_SHUTDOWN_GRACE + std::time::Duration::from_secs(2),
        "run_on_runtime must return within the bounded grace window, took {elapsed:?}"
    );
    // Otherwise the test proves nothing.
    assert!(
        parked_started.load(Ordering::SeqCst),
        "the detached background task must have started before teardown"
    );
    // Reaching here without a process abort is the core assertion.
}
