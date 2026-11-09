//! The one holder of a credential that expires, such as an OAuth2 token or vended storage
//! credentials: it renews the value before it expires and remembers every value it held.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Reads wall-clock time as Unix epoch milliseconds, the unit of every expiry a holder keeps and
/// of a vended `s3.session-token-expires-at-ms`. Injected, so a test can move time.
pub type EpochClock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// The wall clock of every holder outside tests.
pub fn system_clock() -> EpochClock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            })
    })
}

/// A credential value and the epoch millisecond it expires at; `None` never expires.
#[derive(Clone)]
pub struct Lease<T> {
    pub value: T,
    pub expires_at_ms: Option<u64>,
}

/// A held lease that reached its renewal point, as [`ExpiringCredential::get_or_keep`] shows it to
/// its decision: when the lease expires, when this holder last renewed (`None` before its first
/// renewal), and the time the holder read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenewalPoint {
    pub expires_at_ms: u64,
    pub renewed_at_ms: Option<u64>,
    pub now_ms: u64,
}

/// What a decision does with a lease at its renewal point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtRenewalPoint {
    Renew,
    Keep,
}

/// A credential that is renewed once it comes within the renewal skew of its expiry, so a request
/// never carries a value that expires in flight. Callers wait on one lock for the renewal, so a
/// value that reaches its renewal point is renewed once however many callers arrive. It keeps
/// every value it held, so error text can be stripped of each one.
pub struct ExpiringCredential<T> {
    current: tokio::sync::Mutex<Holding<T>>,
    held: Mutex<Vec<T>>,
    skew_ms: u64,
    clock: EpochClock,
}

struct Holding<T> {
    lease: Option<Lease<T>>,
    renewed_at_ms: Option<u64>,
}

impl<T: Clone> ExpiringCredential<T> {
    /// Holds nothing, so the first [`Self::get`] renews.
    pub fn new(skew: Duration, clock: EpochClock) -> Self {
        Self::holding(None, skew, clock)
    }

    /// Holds `lease` from the start, such as the token of a session's first grant.
    pub fn seeded(lease: Lease<T>, skew: Duration, clock: EpochClock) -> Self {
        Self::holding(Some(lease), skew, clock)
    }

    fn holding(lease: Option<Lease<T>>, skew: Duration, clock: EpochClock) -> Self {
        Self {
            held: Mutex::new(lease.iter().map(|lease| lease.value.clone()).collect()),
            current: tokio::sync::Mutex::new(Holding {
                lease,
                renewed_at_ms: None,
            }),
            skew_ms: u64::try_from(skew.as_millis()).unwrap_or(u64::MAX),
            clock,
        }
    }

    /// The held value, renewed first through `renew` when nothing is held or the value is within
    /// the skew of its expiry. `renew` gets the time the holder read, so a lifetime it learns
    /// becomes an expiry on this holder's clock. A failed renewal keeps the held lease.
    pub async fn get<E, Fut>(&self, renew: impl FnOnce(u64) -> Fut) -> Result<T, E>
    where
        Fut: Future<Output = Result<Lease<T>, E>>,
    {
        self.get_or_keep(|_| Ok(AtRenewalPoint::Renew), renew).await
    }

    /// [`Self::get`] for a credential whose renewal can be declined, such as one the source
    /// cannot renew yet or at all: a held lease at its renewal point goes to `decide` first, under
    /// the same lock, which keeps it, has it renewed through `renew`, or fails. With nothing held,
    /// `renew` runs without a decision.
    pub async fn get_or_keep<E, Fut>(
        &self,
        decide: impl FnOnce(RenewalPoint) -> Result<AtRenewalPoint, E>,
        renew: impl FnOnce(u64) -> Fut,
    ) -> Result<T, E>
    where
        Fut: Future<Output = Result<Lease<T>, E>>,
    {
        let mut current = self.current.lock().await;
        let now_ms = (self.clock)();
        if let Some(lease) = &current.lease {
            let Some(expires_at_ms) = lease.expires_at_ms.filter(|_| self.is_due(lease, now_ms))
            else {
                return Ok(lease.value.clone());
            };
            let point = RenewalPoint {
                expires_at_ms,
                renewed_at_ms: current.renewed_at_ms,
                now_ms,
            };
            if decide(point)? == AtRenewalPoint::Keep {
                return Ok(lease.value.clone());
            }
        }
        let renewed = renew(now_ms).await?;
        self.remember(&renewed.value);
        let value = renewed.value.clone();
        *current = Holding {
            lease: Some(renewed),
            renewed_at_ms: Some(now_ms),
        };
        Ok(value)
    }

    /// Every value this holder has held, the current one included, oldest first.
    pub fn held_values(&self) -> Vec<T> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn is_due(&self, lease: &Lease<T>, now_ms: u64) -> bool {
        lease
            .expires_at_ms
            .is_some_and(|expires_at_ms| now_ms.saturating_add(self.skew_ms) >= expires_at_ms)
    }

    fn remember(&self, value: &T) {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value.clone());
    }
}

#[cfg(test)]
#[path = "expiring_tests.rs"]
mod tests;
