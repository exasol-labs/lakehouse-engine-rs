//! The byte budget of one invocation's row-group reads. A stream holds the fetched bytes of its
//! current row group, so without a budget scan memory grows with the stream count.

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

/// The budget is the memory pool divided by this.
const POOL_SHARE: usize = 4;

#[derive(Debug, Default)]
struct Held {
    bytes: usize,
    /// Leases whose reader is not waiting to fetch more.
    running: usize,
}

/// Waiters pass one at a time in arrival order, so a large read is not starved.
#[derive(Debug, Default)]
struct Queue {
    turn: tokio::sync::Mutex<()>,
    freed: Notify,
}

#[derive(Debug)]
pub(crate) struct ReadBudget {
    capacity: usize,
    held: Mutex<Held>,
    admits: Queue,
    /// Apart from `admits`, so a lease finishes its row group before a new one is admitted.
    extends: Queue,
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
            admits: Queue::default(),
            extends: Queue::default(),
            reservation: MemoryConsumer::new("scan row-group reads").register(pool),
        }
    }

    fn held(&self) -> MutexGuard<'_, Held> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wake(&self) {
        self.admits.freed.notify_one();
        self.extends.freed.notify_one();
    }

    /// Waits until `bytes` fit, or until `unblocked` says that waiting longer frees nothing.
    async fn take(&self, bytes: usize, queue: &Queue, unblocked: fn(&Held) -> bool) {
        let _turn = queue.turn.lock().await;
        loop {
            {
                let mut held = self.held();
                if unblocked(&held) || held.bytes.saturating_add(bytes) <= self.capacity {
                    held.bytes += bytes;
                    held.running += 1;
                    break;
                }
            }
            queue.freed.notified().await;
        }
        self.reservation.grow(bytes);
    }

    /// A read is admitted when nothing is held, whatever its size, so a row group larger than
    /// the budget still reads.
    pub(super) async fn admit(self: &Arc<Self>, bytes: usize) -> ReadLease {
        self.take(bytes, &self.admits, |held| held.bytes == 0).await;
        ReadLease {
            budget: Arc::clone(self),
            bytes,
            waiting: false,
        }
    }
}

/// Bytes held against the budget and reserved in the pool until dropped.
#[derive(Debug)]
pub(super) struct ReadLease {
    budget: Arc<ReadBudget>,
    bytes: usize,
    /// Set while `extend` waits, and left set when that wait is cancelled.
    waiting: bool,
}

impl ReadLease {
    /// Keeps the held bytes while it waits. When every lease waits here, none frees bytes, so
    /// the first one goes beyond the budget.
    pub(super) async fn extend(&mut self, bytes: usize) {
        let budget = &self.budget;
        if !self.waiting {
            self.waiting = true;
            budget.held().running -= 1;
            budget.wake();
        }
        budget
            .take(bytes, &budget.extends, |held| held.running == 0)
            .await;
        self.waiting = false;
        self.bytes += bytes;
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        {
            let mut held = self.budget.held();
            held.bytes -= self.bytes;
            held.running -= usize::from(!self.waiting);
        }
        self.budget.reservation.shrink(self.bytes);
        self.budget.wake();
    }
}

#[cfg(test)]
#[path = "read_budget_tests.rs"]
mod tests;
