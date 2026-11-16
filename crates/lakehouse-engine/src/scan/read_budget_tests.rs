use super::*;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use futures::FutureExt;

fn budget(capacity: usize) -> (Arc<dyn MemoryPool>, Arc<ReadBudget>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let budget = Arc::new(ReadBudget::with_capacity(&pool, capacity));
    (pool, budget)
}

/// Scenario: Row-group reads wait on the read budget
#[tokio::test]
async fn a_read_waits_until_the_held_bytes_fit_the_budget() {
    let (pool, budget) = budget(100);
    let first = budget.admit(60).await;
    let second = budget.admit(40).await;
    assert_eq!(pool.reserved(), 100);

    let mut third = Box::pin(budget.admit(30));
    assert!((&mut third).now_or_never().is_none());
    assert_eq!(pool.reserved(), 100);

    drop(first);
    let third = third.await;
    assert_eq!(pool.reserved(), 70);
    drop((second, third));
    assert_eq!(pool.reserved(), 0);
}

/// Scenario: A row group larger than the read budget still reads
#[tokio::test]
async fn a_read_larger_than_the_budget_is_admitted_alone() {
    let (pool, budget) = budget(100);
    let small = budget.admit(10).await;
    let mut large = Box::pin(budget.admit(1_000));
    assert!((&mut large).now_or_never().is_none());

    drop(small);
    let large = large.await;
    assert_eq!(pool.reserved(), 1_000);

    let mut next = Box::pin(budget.admit(1));
    assert!((&mut next).now_or_never().is_none());
    drop(large);
    next.await;
}

#[tokio::test]
async fn an_extended_lease_counts_without_waiting() {
    let (pool, budget) = budget(100);
    let mut lease = budget.admit(100).await;
    lease.extend(50);
    assert_eq!(pool.reserved(), 150);

    let mut next = Box::pin(budget.admit(1));
    assert!((&mut next).now_or_never().is_none());
    drop(lease);
    next.await;
}

#[tokio::test]
async fn a_cancelled_waiter_does_not_block_the_next_read() {
    let (_pool, budget) = budget(100);
    let held = budget.admit(100).await;
    let mut cancelled = Box::pin(budget.admit(50));
    assert!((&mut cancelled).now_or_never().is_none());
    drop(cancelled);

    let mut next = Box::pin(budget.admit(50));
    assert!((&mut next).now_or_never().is_none());
    drop(held);
    next.await;
}

#[test]
fn the_budget_is_a_quarter_of_a_bounded_pool() {
    let pool: Arc<dyn MemoryPool> = Arc::new(
        datafusion::execution::memory_pool::GreedyMemoryPool::new(4_000),
    );
    assert_eq!(ReadBudget::new(&pool).capacity, 1_000);
}
