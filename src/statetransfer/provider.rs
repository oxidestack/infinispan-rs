//! `StateProviderImpl` (`statetransfer/StateProviderImpl.java`): answers `StateTransferStartCommand`
//! by starting an [`OutboundTransferTask`], and `StateTransferCancelCommand` by cancelling segments of
//! one. Transfers to a node that left the write hash are cancelled on every topology update. (Transaction
//! transfer, `getTransactionsForSegments`, and cluster listener transfer are not ported.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::env::StateTransferEnv;
use super::int_set::IntSet;
use super::outbound_task::OutboundTransferTask;
use crate::ch::address::Address;
use crate::topology::cache_topology::CacheTopology;

/// `StateTransferConfiguration.chunkSize` default.
pub const DEFAULT_CHUNK_SIZE: usize = 512;

/// `StateProviderImpl`: answers state requests by sending the requested segments.
pub struct StateProvider {
    env: Arc<dyn StateTransferEnv>,
    cache_name: String,
    chunk_size: usize,
    timeout: Duration,
    by_destination: Mutex<HashMap<Address, Vec<Arc<OutboundTransferTask>>>>,
}

impl StateProvider {
    /// The provider of `cache_name`, sending `chunk_size` entries per chunk.
    pub fn new(env: Arc<dyn StateTransferEnv>, cache_name: &str, chunk_size: usize, timeout: Duration) -> Arc<Self> {
        Arc::new(Self { env, cache_name: cache_name.to_string(), chunk_size, timeout, by_destination: Mutex::new(HashMap::new()) })
    }

    /// Whether any outbound transfer is running.
    pub fn is_state_transfer_in_progress(&self) -> bool {
        !self.by_destination.lock().unwrap().is_empty()
    }

    /// `onTopologyUpdate`: cancel outbound transfers for destinations that are no longer members.
    pub fn on_topology_update(&self, topology: &CacheTopology) {
        let members: Vec<Address> = topology.write_consistent_hash().map(|c| c.members().to_vec()).unwrap_or_default();
        let mut m = self.by_destination.lock().unwrap();
        let gone: Vec<Address> = m.keys().filter(|a| !members.contains(a)).cloned().collect();
        for a in gone {
            for t in m.remove(&a).unwrap_or_default() {
                t.cancel();
            }
        }
    }

    /// `startOutboundTransfer`: the destination already has an `InboundTransferTask` waiting for these segments.
    pub fn start_outbound_transfer(self: &Arc<Self>, destination: Address, request_topology_id: i32, segments: IntSet, apply_state: bool) {
        let Ok(task) = OutboundTransferTask::new(destination.clone(), segments, self.chunk_size, request_topology_id, self.timeout, &self.cache_name, apply_state) else {
            return;
        };
        let task = Arc::new(task);
        self.by_destination.lock().unwrap().entry(destination).or_default().push(task.clone());
        let this = self.clone();
        tokio::spawn(async move {
            task.execute(this.env.as_ref()).await;
            this.remove_transfer(&task);
        });
    }

    /// `cancelOutboundTransfer`.
    pub fn cancel_outbound_transfer(&self, destination: &Address, topology_id: i32, segments: &IntSet) {
        let tasks: Vec<Arc<OutboundTransferTask>> = self.by_destination.lock().unwrap().get(destination).cloned().unwrap_or_default();
        for t in tasks {
            if t.topology_id() == topology_id {
                t.cancel_segments(segments);
                if t.is_cancelled() {
                    self.remove_transfer(&t);
                }
            }
        }
    }

    fn remove_transfer(&self, task: &Arc<OutboundTransferTask>) {
        let mut m = self.by_destination.lock().unwrap();
        if let Some(list) = m.get_mut(task.destination()) {
            list.retain(|t| !Arc::ptr_eq(t, task));
            if list.is_empty() {
                m.remove(task.destination());
            }
        }
    }

    /// `stop`: cancel all outbound transfers.
    pub fn stop(&self) {
        for (_, tasks) in self.by_destination.lock().unwrap().drain() {
            for t in tasks {
                t.cancel();
            }
        }
    }
}
