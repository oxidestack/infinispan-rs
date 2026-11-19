//! The slice of `StateTransferLockImpl` (`statetransfer/StateTransferLockImpl.java`) and
//! `DistributionManager` the non-transactional REPL path needs.
//!
//! * [`DistributionManager`] is the topology the data path routes with. Upstream the consumer sets it
//!   (`distributionManager.setCacheTopology`) *after* it started tracking keys for state transfer, so no
//!   write can be committed under the new topology before the commit manager watches for it.
//! * [`StateTransferLock`] keeps the installed topology id and lets a request that was sent under a
//!   newer topology wait until this node installed it (`topologyFuture`,
//!   `StateTransferLockImpl.java:117-135`). The shared/exclusive topology lock itself is not needed:
//!   installing the topology here is one atomic replacement of an `Arc`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use crate::topology::cache_topology::CacheTopology;

/// `DistributionManager`: holds the cache's current topology.
#[derive(Default)]
pub struct DistributionManager {
    topology: Mutex<Option<CacheTopology>>,
}

impl DistributionManager {
    /// A manager with no topology yet.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The current topology.
    pub fn cache_topology(&self) -> Option<CacheTopology> {
        self.topology.lock().unwrap().clone()
    }

    /// Installs `t`.
    pub fn set_cache_topology(&self, t: CacheTopology) {
        *self.topology.lock().unwrap() = Some(t);
    }
}

/// `StateTransferLock`: lets commands wait until the node has installed the topology they were sent in.
pub struct StateTransferLock {
    topology_id: watch::Sender<i32>,
}

impl Default for StateTransferLock {
    fn default() -> Self {
        Self { topology_id: watch::channel(-1).0 }
    }
}

impl StateTransferLock {
    /// A lock with no topology installed.
    pub fn new() -> Self {
        Self::default()
    }

    /// The id of the installed topology; `-1` before the first.
    pub fn topology_id(&self) -> i32 {
        *self.topology_id.borrow()
    }

    /// `notifyTopologyInstalled`.
    pub fn notify_topology_installed(&self, topology_id: i32) {
        self.topology_id.send_modify(|v| {
            if topology_id > *v {
                *v = topology_id
            }
        });
    }

    /// `topologyFuture(expectedTopologyId)`: resolves once a topology with at least that id is installed;
    /// `Err` when it does not within `timeout` (upstream: `failedWaitingForTopology`).
    pub async fn topology_future(&self, expected: i32, timeout: Duration) -> Result<(), String> {
        let mut rx = self.topology_id.subscribe();
        match tokio::time::timeout(timeout, rx.wait_for(|v| *v >= expected)).await {
            Ok(Ok(_)) => Ok(()),
            _ => Err(format!("ISPN000: Timed out waiting for topology {expected}")),
        }
    }
}
