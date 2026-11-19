//! The one retry policy of catalog writes: Iceberg's commit properties and defaults, a bounded
//! count of retries with a jittered exponential wait between them.

use std::collections::HashMap;
use std::time::Duration;

/// Iceberg's `commit.retry.num-retries` default, also the bound of namespace and table creation.
pub(crate) const DEFAULT_RETRIES: u32 = 4;
pub(crate) const NUM_RETRIES_PROPERTY: &str = "commit.retry.num-retries";
/// The commit retries of an export's table: all of its writers commit to that one table, so
/// they collide far more often than Iceberg's default of 4 allows for.
pub(crate) const EXPORT_RETRIES: u32 = 20;
pub(crate) const MIN_WAIT_PROPERTY: &str = "commit.retry.min-wait-ms";
pub(crate) const MAX_WAIT_PROPERTY: &str = "commit.retry.max-wait-ms";
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

/// The retries of an append to a table with `properties`: at least [`EXPORT_RETRIES`], because
/// a table another engine created rarely sets the property for the export's writer fan-out.
pub(crate) fn append_retries(properties: &HashMap<String, String>) -> u32 {
    commit_retries(properties).max(EXPORT_RETRIES)
}

/// The bounds of the waits between retries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Waits {
    min: Duration,
    max: Duration,
}

impl Waits {
    /// Iceberg's `commit.retry.min-wait-ms` and `commit.retry.max-wait-ms` defaults.
    pub(crate) const DEFAULT: Self = Self {
        min: Duration::from_millis(100),
        max: Duration::from_secs(60),
    };

    /// The waits of a commit to a table with `properties`: its `commit.retry.min-wait-ms` and
    /// `commit.retry.max-wait-ms`, each else Iceberg's default.
    pub(crate) fn of_table(properties: &HashMap<String, String>) -> Self {
        let millis = |property: &str| {
            properties
                .get(property)
                .and_then(|millis| millis.parse().ok())
                .map(Duration::from_millis)
        };
        Self {
            min: millis(MIN_WAIT_PROPERTY).unwrap_or(Self::DEFAULT.min),
            max: millis(MAX_WAIT_PROPERTY).unwrap_or(Self::DEFAULT.max),
        }
    }

    /// The wait before retry number `retry` (1 for the first): the minimum doubled for each later
    /// retry, at most the maximum, plus up to a tenth more for `jitter` in `[0, 1)`, never above
    /// the maximum.
    pub(crate) fn backoff(self, retry: u32, jitter: f64) -> Duration {
        let factor = 2_u32.saturating_pow(retry.saturating_sub(1));
        let wait = self.min.saturating_mul(factor).min(self.max);
        wait.mul_f64(1.0 + JITTER_SHARE * jitter.clamp(0.0, 1.0))
            .min(self.max)
    }
}

/// Counts the retries of one operation against its bound.
pub(crate) struct Retries {
    made: u32,
    limit: u32,
    waits: Waits,
}

impl Retries {
    /// Retries with Iceberg's default waits, for a request on no table.
    pub(crate) fn new(limit: u32) -> Self {
        Self::with_waits(limit, Waits::DEFAULT)
    }

    pub(crate) fn with_waits(limit: u32, waits: Waits) -> Self {
        Self {
            made: 0,
            limit,
            waits,
        }
    }

    /// Waits before the next retry and returns true, or returns false at once when the bound is
    /// reached.
    pub(crate) async fn wait_for_next(&mut self) -> bool {
        if self.made >= self.limit {
            return false;
        }
        self.made += 1;
        tokio::time::sleep(self.waits.backoff(self.made, rand::random())).await;
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
