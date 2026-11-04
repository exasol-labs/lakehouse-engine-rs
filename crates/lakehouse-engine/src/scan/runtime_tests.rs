use super::*;
use datafusion::execution::memory_pool::MemoryConsumer;

/// Scenario: DataFusion memory pool is sized from the per-instance memory limit
#[test]
fn build_runtime_env_sizes_pool_from_net_budget() {
    let limit: u64 = 4096 * 1024 * 1024;
    let overhead: u64 = 200 * 1024 * 1024;
    let fraction = 0.6_f64;
    let env = build_runtime_env(limit, fraction, overhead, SpillMode::NoDisk).unwrap();
    let net = limit - overhead;
    let expected = (net as f64 * fraction) as usize;
    let reservation = MemoryConsumer::new("test").register(&env.memory_pool);
    assert!(
        reservation.try_grow(expected).is_ok(),
        "growing to the budget must succeed"
    );
    let reservation2 = MemoryConsumer::new("test2").register(&env.memory_pool);
    assert!(
        reservation2.try_grow(1).is_err(),
        "growing 1 byte beyond the budget must fail for GreedyPool"
    );
}

/// Scenario: Scan clamps the memory pool to a minimum floor when overhead exceeds the limit
#[test]
fn build_runtime_env_clamps_to_floor_when_overhead_exceeds_limit() {
    let limit: u64 = 100 * 1024 * 1024;
    let overhead: u64 = 200 * 1024 * 1024;
    let fraction = 0.6_f64;
    let env = build_runtime_env(limit, fraction, overhead, SpillMode::NoDisk).unwrap();
    let expected = MIN_POOL_FLOOR_BYTES as usize;
    let reservation = MemoryConsumer::new("test").register(&env.memory_pool);
    assert!(
        reservation.try_grow(expected).is_ok(),
        "floor budget must be usable when overhead exceeds limit"
    );
    let reservation2 = MemoryConsumer::new("test2").register(&env.memory_pool);
    assert!(
        reservation2.try_grow(1).is_err(),
        "one byte beyond the floor must fail"
    );
}

/// Scenario: Default memory budget is used when the limit is unknown
#[test]
fn build_runtime_env_uses_default_budget_on_zero_limit() {
    let env = build_runtime_env(0, 0.6, 0, SpillMode::NoDisk).unwrap();
    let default_budget = DEFAULT_BUDGET_BYTES as usize;
    let reservation = MemoryConsumer::new("test").register(&env.memory_pool);
    assert!(
        reservation.try_grow(default_budget).is_ok(),
        "default budget must accommodate 1 GiB"
    );
    let reservation2 = MemoryConsumer::new("test2").register(&env.memory_pool);
    assert!(
        reservation2.try_grow(1).is_err(),
        "one byte beyond default budget must fail"
    );
}

/// Scenario: Spill to disk is enabled when /tmp is real disk with free space
#[test]
fn build_runtime_env_uses_fair_spill_pool_when_disk() {
    let tmp = std::env::temp_dir();
    let env = build_runtime_env(0, 0.6, 0, SpillMode::Disk(tmp)).unwrap();
    assert_eq!(
        env.memory_pool.name(),
        "fair",
        "disk spill path must use FairSpillPool"
    );
}

/// Scenario: An over-budget scan without spill disk fails with a clean resources-exhausted error
#[test]
fn build_runtime_env_uses_greedy_pool_when_no_disk() {
    let env = build_runtime_env(0, 0.6, 0, SpillMode::NoDisk).unwrap();
    assert_eq!(
        env.memory_pool.name(),
        "greedy",
        "no-disk path must use GreedyMemoryPool"
    );
}
