//! `TimeService` (`commons/time/TimeService.java`) as the data container and the expiration logic read it:
//! wall-clock milliseconds. Tests replace a node's service with a [`ControlledTimeService`]
//! (`org.infinispan.util.ControlledTimeService`), as `AbstractClusterListenerUtilTest.injectTimeServices` does.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The clock the data container and expiration read.
pub trait TimeService: Send + Sync {
    /// `wallClockTime()`
    fn wall_clock_time(&self) -> i64;
}

/// `EmbeddedTimeService`: the system clock.
pub struct SystemTimeService;

impl TimeService for SystemTimeService {
    fn wall_clock_time(&self) -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
    }
}

/// `ControlledTimeService`: a clock that only moves when told to, starting at the instant it was made.
pub struct ControlledTimeService {
    now: AtomicI64,
}

impl ControlledTimeService {
    /// A clock stopped at the current time.
    pub fn new() -> Self {
        ControlledTimeService { now: AtomicI64::new(SystemTimeService.wall_clock_time()) }
    }

    /// `advance(delta)`, in milliseconds.
    pub fn advance(&self, millis: i64) {
        self.now.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Default for ControlledTimeService {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeService for ControlledTimeService {
    fn wall_clock_time(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}
