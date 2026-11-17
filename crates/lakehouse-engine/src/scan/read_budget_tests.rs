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

/// Scenario: Every fetch of a row group waits on the read budget
#[tokio::test]
async fn a_lease_waits_to_extend_until_the_held_bytes_fit() {
    let (pool, budget) = budget(100);
    let mut first = budget.admit(40).await;
    let second = budget.admit(40).await;

    let mut extended = Box::pin(first.extend(50));
    assert!((&mut extended).now_or_never().is_none());
    assert_eq!(pool.reserved(), 80);

    drop(second);
    extended.await;
    assert_eq!(pool.reserved(), 90);
}

/// Scenario: Every fetch of a row group waits on the read budget
#[tokio::test]
async fn a_lease_extends_beyond_the_budget_when_no_other_lease_is_running() {
    let (pool, budget) = budget(100);
    let mut first = budget.admit(50).await;
    let mut second = budget.admit(50).await;

    let mut head = Box::pin(first.extend(30));
    assert!((&mut head).now_or_never().is_none());
    let mut behind = Box::pin(second.extend(30));
    assert!((&mut behind).now_or_never().is_none());

    // Both leases wait, so the one at the head of the queue goes beyond the budget.
    head.await;
    assert_eq!(pool.reserved(), 130);
    assert!((&mut behind).now_or_never().is_none());

    drop(first);
    behind.await;
    assert_eq!(pool.reserved(), 80);
}

/// Scenario: Only one stream reads beyond the read budget
#[tokio::test]
async fn a_lease_beyond_the_budget_extends_again_and_lets_no_other_lease_pass() {
    let (pool, budget) = budget(100);
    let mut first = budget.admit(50).await;
    let mut second = budget.admit(50).await;

    let mut head = Box::pin(first.extend(30));
    assert!((&mut head).now_or_never().is_none());
    let mut behind = Box::pin(second.extend(30));
    assert!((&mut behind).now_or_never().is_none());
    head.await;

    // A third fetch of the row group beyond the budget does not wait behind `second`.
    first.extend(30).now_or_never().expect("extends at once");
    assert_eq!(pool.reserved(), 160);
    assert!((&mut behind).now_or_never().is_none());

    drop(first);
    behind.await;
    assert_eq!(pool.reserved(), 80);
}

/// Scenario: Only one stream reads beyond the read budget
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_bytes_stay_within_the_budget_and_one_row_group_over_three_fetches() {
    const ROW_GROUP: usize = 110;
    let (pool, budget) = budget(100);
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let (pool, budget, peak) = (Arc::clone(&pool), Arc::clone(&budget), Arc::clone(&peak));
        readers.spawn(async move {
            for _ in 0..20 {
                let mut lease = budget.admit(10).await;
                for _ in 0..2 {
                    tokio::task::yield_now().await;
                    lease.extend(50).await;
                    peak.fetch_max(pool.reserved(), std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
    }
    while let Some(reader) = readers.join_next().await {
        reader.unwrap();
    }
    let peak = peak.load(std::sync::atomic::Ordering::Relaxed);
    assert!(peak <= 100 + ROW_GROUP, "{peak} bytes held");
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn a_waiting_read_is_not_admitted_when_the_only_lease_extends() {
    let (pool, budget) = budget(100);
    let mut lease = budget.admit(60).await;
    let mut next = Box::pin(budget.admit(60));
    assert!((&mut next).now_or_never().is_none());

    lease.extend(30).await;
    assert!((&mut next).now_or_never().is_none());
    assert_eq!(pool.reserved(), 90);

    drop(lease);
    let _next = next.await;
    assert_eq!(pool.reserved(), 60);
}

#[tokio::test]
async fn a_lease_dropped_while_it_waits_to_extend_frees_its_bytes() {
    let (pool, budget) = budget(100);
    let mut first = budget.admit(60).await;
    let second = budget.admit(40).await;
    {
        let mut cancelled = Box::pin(first.extend(50));
        assert!((&mut cancelled).now_or_never().is_none());
    }
    drop(first);
    assert_eq!(pool.reserved(), 40);

    // `second` is still counted as running, so a read that does not fit waits for it.
    let mut next = Box::pin(budget.admit(70));
    assert!((&mut next).now_or_never().is_none());
    drop(second);
    let _next = next.await;
    assert_eq!(pool.reserved(), 70);
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
