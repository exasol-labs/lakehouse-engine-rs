use super::test_support::minimal_spec;
use super::*;

/// Task 4.3: session_config_for_spec applies df_batch_size and clamps sub-1 values to 1.
///
/// Verifies that:
/// 1. An explicit batch size flows through to SessionConfig::batch_size().
/// 2. A zero batch size is clamped to 1 (sub-1 values must not reach DataFusion as-is).
#[test]
fn session_config_applies_batch_size_and_clamps_floor() {
    // 1. Explicit batch size is applied.
    let mut spec = minimal_spec();
    spec.common.df_batch_size = 4096;
    let config = session_config_for_spec(&spec);
    assert_eq!(
        config.batch_size(),
        4096,
        "SessionConfig must use df_batch_size from spec"
    );

    // 2. Zero (sub-1) batch size is clamped to 1.
    spec.common.df_batch_size = 0;
    let config_clamped = session_config_for_spec(&spec);
    assert_eq!(
        config_clamped.batch_size(),
        1,
        "df_batch_size of 0 must be clamped to 1"
    );
}

/// Parquet row-group statistics pruning, page-index pruning, and predicate
/// pushdown are all enabled on the session config — not left to the
/// DataFusion defaults (`pushdown_filters` defaults to `false`).
///
/// Scenario: Scan enables Parquet row-group and page pruning.
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

/// SessionConfig applies target_partitions from the spec.
///
/// Scenario: session_config_uses_spec_target_partitions
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

/// A spec with df_threads_per_udf == 1 selects the current-thread runtime.
///
/// Scenario: runtime_is_current_thread_when_threads_is_one
#[test]
fn runtime_is_current_thread_when_threads_is_one() {
    let rt = build_scan_runtime(1).expect("runtime must build");
    assert_eq!(
        rt.handle().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread,
        "df_threads_per_udf == 1 must yield a current-thread runtime"
    );
}

/// A spec with df_threads_per_udf > 1 selects the multi-thread runtime.
///
/// Scenario: runtime_is_multi_thread_when_threads_exceeds_one
#[test]
fn runtime_is_multi_thread_when_threads_exceeds_one() {
    let rt = build_scan_runtime(4).expect("runtime must build");
    assert_eq!(
        rt.handle().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "df_threads_per_udf > 1 must yield a multi-thread runtime"
    );
}

/// Teardown regression: a multi-thread runtime that still has detached
/// background tasks live when `block_on` returns must be torn down
/// deterministically, not by an implicit `Drop` that races those tasks.
///
/// This reproduces the *mechanism* of the live `err_zombie` VM abort: the scan
/// future completes and returns its result while object_store's hyper client
/// (modeled here by a spawned task parked past the future's return) is still
/// alive. The fix is `run_on_runtime`, which drives the runtime down via
/// `shutdown_timeout` from the synchronous context. The invariants:
///   1. the future's result is returned intact (work completed before teardown);
///   2. `run_on_runtime` returns within the grace window (it drains/cancels the
///      detached task rather than blocking forever or aborting);
///   3. control reaches the assertions — a raced teardown abort would have
///      killed the test process before this point.
///
/// A real VM-process abort cannot be reproduced on the host (it needs Exasol's
/// VM teardown), so this asserts the deterministic-shutdown seam the abort-free
/// fix depends on; the live bench is the end-to-end arbiter.
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
        // Spawn a detached task that outlives the future's return — the
        // host analog of hyper's connection-pool/reaper tasks that object_store
        // keeps alive past the last poll of the scan stream.
        tokio::spawn(async move {
            started_in_future.store(true, Ordering::SeqCst);
            // Park far longer than the grace window; shutdown must cancel it.
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        });
        // Yield until the detached task has actually reached its body (set the
        // flag) so the runtime genuinely has live background work at teardown
        // time. A single `yield_now()` is not enough under a loaded scheduler:
        // the detached task may not be polled before this future returns,
        // which made the "task started" assertion flaky. The bounded yield
        // loop is deterministic without changing what is tested — the task is
        // still parked on its 3600s sleep when teardown runs.
        while !started_in_outer.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        42_u32
    });
    let elapsed = before.elapsed();

    // 1. The future's result survives the explicit teardown.
    assert_eq!(result, 42, "future result must be returned before teardown");
    // 2. Teardown is bounded — it returns near-immediately (the detached task
    //    is cancelled), never blocking on the 3600s park.
    assert!(
        elapsed < RUNTIME_SHUTDOWN_GRACE + std::time::Duration::from_secs(2),
        "run_on_runtime must return within the bounded grace window, took {elapsed:?}"
    );
    // 3. The detached task was genuinely live (otherwise the test proves nothing).
    assert!(
        parked_started.load(Ordering::SeqCst),
        "the detached background task must have started before teardown"
    );
    // Reaching here without a process abort is the core assertion: deterministic
    // shutdown replaced the racy implicit drop.
}
