use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use object_store::path::Path as ObjectStorePath;
use tokio::sync::watch;

/// Long enough for a read past the budget to arrive while the gated reads are still held.
const HOLD: Duration = Duration::from_millis(50);

/// Bounds the wait when the expected overlap never forms, so the peak assertion fails instead
/// of the test hanging.
const GATE_TIMEOUT: Duration = Duration::from_secs(5);

/// Peak-concurrency counter over reads whose path contains a needle. Probed reads wait at a gate
/// until `gate_at` are in flight, so reaching the expected peak never depends on scheduling.
#[derive(Debug)]
pub struct ConcurrencyProbe {
    needles: Vec<String>,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    gate_at: usize,
    gate: watch::Sender<bool>,
}

/// Decrements on drop so a cancelled read (fired timeout) never leaks an in-flight count.
pub struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ConcurrencyProbe {
    /// Returns the probe and its peak counter. `gate_at` is the peak the test expects.
    pub fn new(needles: Vec<String>, gate_at: usize) -> (Self, Arc<AtomicUsize>) {
        let peak = Arc::new(AtomicUsize::new(0));
        let probe = Self {
            needles,
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
            gate_at,
            gate: watch::Sender::new(false),
        };
        (probe, peak)
    }

    pub fn is_probed_read(&self, location: &ObjectStorePath) -> bool {
        let path = location.as_ref();
        self.needles.iter().any(|n| path.contains(n.as_str()))
    }

    /// Counts the read as in flight until the returned guard drops. Once the gate opens, later
    /// reads pass it at once, so a read waiting for a freed permit never waits again.
    pub async fn enter(&self) -> InFlightGuard {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let guard = InFlightGuard {
            in_flight: Arc::clone(&self.in_flight),
        };
        if now >= self.gate_at {
            self.gate.send_replace(true);
        }
        let mut opened = self.gate.subscribe();
        let _ = tokio::time::timeout(GATE_TIMEOUT, opened.wait_for(|open| *open)).await;
        tokio::time::sleep(HOLD).await;
        guard
    }
}
