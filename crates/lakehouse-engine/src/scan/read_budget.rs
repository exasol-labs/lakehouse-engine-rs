//! The byte budget of one invocation's row-group reads. A stream holds the fetched bytes of its
//! current row group, so without a budget scan memory grows with the stream count.

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

/// The budget is the memory pool divided by this.
const POOL_SHARE: usize = 4;

#[derive(Debug)]
pub(crate) struct ReadBudget {
    capacity: usize,
    held: Mutex<usize>,
    /// Admits waiters one at a time in arrival order, so a large read is not starved.
    turn: tokio::sync::Mutex<()>,
    freed: Notify,
    reservation: MemoryReservation,
}

impl ReadBudget {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        let capacity = match pool.memory_limit() {
            MemoryLimit::Finite(limit) => limit / POOL_SHARE,
            _ => usize::MAX,
        };
        Self::with_capacity(pool, capacity)
    }

    pub(super) fn with_capacity(pool: &Arc<dyn MemoryPool>, capacity: usize) -> Self {
        Self {
            capacity,
            held: Mutex::default(),
            turn: tokio::sync::Mutex::default(),
            freed: Notify::new(),
            reservation: MemoryConsumer::new("scan row-group reads").register(pool),
        }
    }

    fn held(&self) -> MutexGuard<'_, usize> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Waits until `bytes` fit. A read is admitted when nothing else is held, whatever its size,
    /// so a row group larger than the budget still reads.
    pub(super) async fn admit(self: &Arc<Self>, bytes: usize) -> ReadLease {
        let _turn = self.turn.lock().await;
        loop {
            {
                let mut held = self.held();
                if *held == 0 || held.saturating_add(bytes) <= self.capacity {
                    *held += bytes;
                    break;
                }
            }
            self.freed.notified().await;
        }
        self.reservation.grow(bytes);
        ReadLease {
            budget: Arc::clone(self),
            bytes,
        }
    }
}

/// Bytes held against the budget and reserved in the pool until dropped.
#[derive(Debug)]
pub(super) struct ReadLease {
    budget: Arc<ReadBudget>,
    bytes: usize,
}

impl ReadLease {
    /// Never waits: a reader that waited while it holds bytes could deadlock the budget.
    pub(super) fn extend(&mut self, bytes: usize) {
        *self.budget.held() += bytes;
        self.budget.reservation.grow(bytes);
        self.bytes += bytes;
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        *self.budget.held() -= self.bytes;
        self.budget.reservation.shrink(self.bytes);
        self.budget.freed.notify_one();
    }
}

#[cfg(test)]
#[path = "read_budget_tests.rs"]
mod tests;
