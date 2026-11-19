//! `StateTransferManagerImpl` (`statetransfer/StateTransferManagerImpl.java`): what a cache does with an
//! installed topology (`doTopologyUpdate` / `updateProviderAndConsumer`), and the dispatch of the three
//! state transfer commands to the provider and the consumer.

use std::sync::Arc;
use std::time::Duration;

use super::commit_manager::CommitManager;
use super::consumer::StateConsumer;
use super::env::StateTransferEnv;
use super::lock::{DistributionManager, StateTransferLock};
use super::provider::{DEFAULT_CHUNK_SIZE, StateProvider};
use crate::ch::address::Address;
use crate::commands::rpc::CacheRpcPayload;
use crate::topology::cache_topology::CacheTopology;
use crate::topology::messages::Response;
use crate::topology::support::BoxFuture;

/// `StateTransferConfiguration.timeout` default: 4 minutes.
pub const DEFAULT_STATE_TRANSFER_TIMEOUT: Duration = Duration::from_secs(240);

/// `StateTransferManagerImpl`: a cache's state transfer, its consumer and provider.
pub struct StateTransferManager {
    /// Requests and applies the segments the node gains.
    pub consumer: Arc<StateConsumer>,
    /// Sends the segments other nodes request.
    pub provider: Arc<StateProvider>,
    /// The topology commands wait for.
    pub lock: Arc<StateTransferLock>,
    /// The cache's current topology.
    pub distribution: Arc<DistributionManager>,
    timeout: Duration,
    /// `PerCacheInboundInvocationHandler.setFirstTopologyAsMember`
    first_topology_as_member: std::sync::atomic::AtomicI32,
}

impl StateTransferManager {
    /// The state transfer of `cache_name`, with `timeout` per transfer and `chunk_size` entries per chunk.
    pub fn new(
        env: Arc<dyn StateTransferEnv>,
        cache_name: &str,
        num_segments: i32,
        commit_manager: Arc<CommitManager>,
        distribution: Arc<DistributionManager>,
        timeout: Duration,
        chunk_size: usize,
    ) -> Arc<Self> {
        let lock = Arc::new(StateTransferLock::new());
        let consumer = StateConsumer::new(env.clone(), cache_name, num_segments, timeout, commit_manager, distribution.clone(), lock.clone());
        let provider = StateProvider::new(env, cache_name, chunk_size, timeout);
        Arc::new(Self { consumer, provider, lock, distribution, timeout, first_topology_as_member: std::sync::atomic::AtomicI32::new(-1) })
    }

    /// [`Self::new`] with the default timeout and chunk size.
    pub fn with_defaults(env: Arc<dyn StateTransferEnv>, cache_name: &str, num_segments: i32, commit_manager: Arc<CommitManager>, distribution: Arc<DistributionManager>) -> Arc<Self> {
        Self::new(env, cache_name, num_segments, commit_manager, distribution, DEFAULT_STATE_TRANSFER_TIMEOUT, DEFAULT_CHUNK_SIZE)
    }

    /// `updateProviderAndConsumer`: the consumer installs the topology and starts its transfers; the
    /// returned future completes when the phase may be confirmed to the coordinator.
    pub fn on_topology(&self, topology: CacheTopology, is_rebalance: bool) -> BoxFuture<'static, ()> {
        // doTopologyUpdate:169-176: the first topology in which the local node is a member
        let me = self.consumer.local_address();
        if topology.members().contains(&me) && !self.distribution.cache_topology().is_some_and(|t| t.members().contains(&me)) {
            self.first_topology_as_member.store(topology.topology_id, std::sync::atomic::Ordering::SeqCst);
        }
        let transfer = self.consumer.on_topology_update(topology.clone(), is_rebalance);
        self.provider.on_topology_update(&topology);
        transfer
    }

    /// The answer to a state transfer command that arrived from `origin` (`None` is the null answer).
    pub async fn handle(self: &Arc<Self>, origin: &Address, payload: CacheRpcPayload) -> Option<Response> {
        match payload {
            CacheRpcPayload::StateStart(c) => {
                // the request was sent under a topology this node may not have installed yet
                if self.lock.topology_future(c.topology_id, self.timeout).await.is_err() {
                    return Some(Response::Exception {
                        kind: crate::topology::messages::ExceptionKind::CacheException,
                        message: Some(format!("Timed out waiting for topology {}", c.topology_id)),
                    });
                }
                self.provider.start_outbound_transfer(origin.clone(), c.topology_id, c.segments, true);
                Some(Response::Successful(None))
            }
            CacheRpcPayload::StateCancel(c) => {
                self.provider.cancel_outbound_transfer(origin, c.topology_id, &c.segments);
                None
            }
            CacheRpcPayload::StateResponse(r) => {
                self.consumer.apply_state(origin, r.topology_id, r.push_transfer, r.chunks).await;
                None
            }
            _ => None,
        }
    }

    /// `isCommandSentBeforeFirstTopology`.
    pub fn is_command_sent_before_first_topology(&self, command_topology_id: i32) -> bool {
        0 <= command_topology_id && command_topology_id < self.first_topology_as_member.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Cancels every transfer.
    pub fn stop(&self) {
        self.consumer.stop();
        self.provider.stop();
    }
}
