//! Escape hatches into the QUIC backend. Nothing here is covered by semver.

use std::time::Duration;

use crate::common::runtime::QuicRuntime;

/// Counters for one network thread.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DriverMetrics {
    /// Times the thread was woken to handle commands. At most one per frame.
    pub wakes: u64,
    /// Time spent handling work, not counting time waiting for it.
    pub busy: Duration,
}

/// The counters for each of the runtime's network threads, since it started.
#[doc(hidden)]
pub fn driver_metrics(runtime: &QuicRuntime) -> Vec<DriverMetrics> {
    runtime
        .metrics()
        .map(|(wakes, busy)| DriverMetrics { wakes, busy })
        .collect()
}
