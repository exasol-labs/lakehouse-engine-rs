//! The one retry policy of catalog writes: Iceberg's commit defaults, a bounded count of retries
//! with a jittered exponential wait between them.

use std::collections::HashMap;
use std::time::Duration;

/// Iceberg's `commit.retry.num-retries` default, also the bound of namespace and table creation.
pub(crate) const DEFAULT_RETRIES: u32 = 4;
const NUM_RETRIES_PROPERTY: &str = "commit.retry.num-retries";
/// Iceberg's `commit.retry.min-wait-ms` and `commit.retry.max-wait-ms` defaults.
const MIN_WAIT: Duration = Duration::from_millis(100);
const MAX_WAIT: Duration = Duration::from_secs(60);
/// The share of a wait added at random, so writers that failed together retry apart.
const JITTER_SHARE: f64 = 0.1;

/// The retries a commit to a table with `properties` may make: its `commit.retry.num-retries`,
/// else Iceberg's default.
pub(crate) fn commit_retries(properties: &HashMap<String, String>) -> u32 {
    properties
        .get(NUM_RETRIES_PROPERTY)
        .and_then(|retries| retries.parse().ok())
        .unwrap_or(DEFAULT_RETRIES)
}

/// The wait before retry number `retry` (1 for the first): 100 ms doubled for each later retry,
/// at most 60 s, plus up to a tenth more for `jitter` in `[0, 1)`, never above 60 s.
pub(crate) fn backoff_wait(retry: u32, jitter: f64) -> Duration {
    let factor = 2_u32.saturating_pow(retry.saturating_sub(1));
    let wait = MIN_WAIT.saturating_mul(factor).min(MAX_WAIT);
    wait.mul_f64(1.0 + JITTER_SHARE * jitter.clamp(0.0, 1.0))
        .min(MAX_WAIT)
}

/// Counts the retries of one operation against its bound.
pub(crate) struct Retries {
    made: u32,
    limit: u32,
}

impl Retries {
    pub(crate) fn new(limit: u32) -> Self {
        Self { made: 0, limit }
    }

    /// Waits before the next retry and returns true, or returns false at once when the bound is
    /// reached.
    pub(crate) async fn wait_for_next(&mut self) -> bool {
        if self.made >= self.limit {
            return false;
        }
        self.made += 1;
        tokio::time::sleep(backoff_wait(self.made, rand::random())).await;
        true
    }

    /// The attempts made so far, the first one included.
    pub(crate) fn attempts(&self) -> u32 {
        self.made + 1
    }
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
