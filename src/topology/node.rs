//! One node's topology stack: the [`LocalTopologyManager`] and the [`ClusterTopologyManager`] over one
//! transport, plus the `GlobalRpcCommand.invokeAsync(gcr)` dispatch every topology command goes
//! through (`commands/topology/*.java`).

use std::sync::Arc;
use std::time::Duration;

use super::cache_topology::{CacheTopology, Phase};
use super::cluster_cache_status::{PersistentUuidManager, Result, TopologyError};
use super::cluster_topology_manager::{ClusterTopologyManager, TopologyConfig};
use super::commands::TopologyCommand;
use super::local_topology_manager::LocalTopologyManager;
use super::messages::{ManagerStatusResponse, ResponseValue};
use super::support::BoxFuture;
use super::transport::{LocalInvoker, TopologyTransport};
use crate::ch::address::Address;

/// One node's topology managers: the local one every node runs and the cluster one the coordinator uses.
pub struct TopologyNode {
    /// The local topology manager.
    pub ltm: LocalTopologyManager,
    /// The cluster topology manager.
    pub ctm: ClusterTopologyManager,
    /// The address to persistent UUID mapping both managers share.
    pub persistent_uuids: Arc<PersistentUuidManager>,
    transport: Arc<dyn TopologyTransport>,
}

struct NodeInvoker {
    ltm: LocalTopologyManager,
    ctm: ClusterTopologyManager,
}

impl TopologyNode {
    /// The managers of a node on `transport`, wired to invoke each other's commands locally.
    pub fn new(transport: Arc<dyn TopologyTransport>, config: TopologyConfig) -> Arc<Self> {
        let persistent_uuids = PersistentUuidManager::new();
        let ltm = LocalTopologyManager::new(transport.clone(), persistent_uuids.clone(), config.global_timeout);
        let ctm = ClusterTopologyManager::new(transport.clone(), persistent_uuids.clone(), config);
        let invoker = Arc::new(NodeInvoker { ltm: ltm.clone(), ctm: ctm.clone() });
        ltm.set_invoker(invoker.clone());
        ctm.set_invoker(invoker);
        let ctm2 = ctm.clone();
        ltm.set_rebalancing_enabled_source(Arc::new(move || ctm2.is_rebalancing_enabled()));
        Arc::new(Self { ltm, ctm, persistent_uuids, transport })
    }

    /// The node's address.
    pub fn address(&self) -> Address {
        self.transport.address()
    }

    /// Start the managers: the local one first, so it can answer requests, then the cluster one,
    /// which handles the initial view and learns the rebalancing status.
    pub async fn start(&self) {
        self.ltm.start();
        self.ctm.start().await;
    }

    /// `ViewChanged`/`Merged` listener entry point.
    pub fn on_view_change(&self, merge_view: bool, view_id: i32) {
        self.ctm.handle_cluster_view(merge_view, view_id);
    }

    /// Stops the managers.
    pub fn stop(&self) {
        self.ctm.stop();
        self.ltm.stop();
    }

    /// Run a command on this node as if it had arrived from `origin`.
    pub fn invoke(&self, origin: Address, command: TopologyCommand) -> BoxFuture<'static, Result<Option<ResponseValue>>> {
        NodeInvoker { ltm: self.ltm.clone(), ctm: self.ctm.clone() }.invoke(origin, command)
    }
}

impl LocalInvoker for NodeInvoker {
    fn invoke(&self, origin: Address, command: TopologyCommand) -> BoxFuture<'static, Result<Option<ResponseValue>>> {
        let ltm = self.ltm.clone();
        let ctm = self.ctm.clone();
        Box::pin(async move {
            match command {
                TopologyCommand::CacheJoin { cache_name, join_info, view_id } => {
                    let r = ctm.handle_join(&cache_name, &origin, &join_info, view_id).await?;
                    Ok(r.map(ResponseValue::CacheStatus))
                }
                TopologyCommand::CacheLeave { cache_name, view_id } => {
                    ctm.handle_leave(&cache_name, &origin, view_id).await?;
                    Ok(None)
                }
                TopologyCommand::RebalancePhaseConfirm { cache_name, topology_id, view_id } => {
                    ctm.handle_rebalance_phase_confirm(&cache_name, &origin, topology_id, view_id).await?;
                    Ok(None)
                }
                TopologyCommand::RebalanceStatusRequest { cache_name } => {
                    let status = ctm.rebalancing_status(cache_name.as_deref());
                    Ok(Some(ResponseValue::RebalancingStatus(status)))
                }
                TopologyCommand::RebalanceStart { cache_name, fields } => {
                    let topology = fields.to_cache_topology()?;
                    ltm.handle_rebalance(&cache_name, topology, fields.view_id, &origin).await?;
                    Ok(None)
                }
                TopologyCommand::TopologyUpdate { cache_name, fields, availability_mode } => {
                    if !ltm.is_running() {
                        log::debug!("Discard topology update because topology manager not running");
                        return Ok(None);
                    }
                    let topology = fields.to_cache_topology()?;
                    ltm.handle_topology_update(&cache_name, topology, availability_mode, fields.view_id, &origin).await?;
                    Ok(None)
                }
                TopologyCommand::CacheStatusRequest { view_id } => {
                    if !ltm.is_running() {
                        log::debug!("Reply with empty status request because topology manager not running");
                        return Ok(Some(ResponseValue::ManagerStatus(ManagerStatusResponse { caches: vec![], rebalancing_enabled: true })));
                    }
                    Ok(Some(ResponseValue::ManagerStatus(ltm.handle_status_request(view_id).await?)))
                }
                TopologyCommand::TopologyUpdateStable { cache_name, fields, topology_restored } => {
                    if !ltm.is_running() {
                        log::debug!("Discard stable update because topology manager not running");
                        return Ok(None);
                    }
                    let topology = CacheTopology::new(
                        fields.topology_id,
                        fields.rebalance_id,
                        topology_restored,
                        fields.current_ch,
                        fields.pending_ch,
                        None,
                        Phase::NoRebalance,
                        fields.actual_members,
                        fields.persistent_uuids,
                    )
                    .map_err(TopologyError)?;
                    ltm.handle_stable_topology_update(&cache_name, topology, &origin, fields.view_id).await?;
                    Ok(None)
                }
                TopologyCommand::HeartBeat => Ok(None),
            }
        })
    }
}

/// `distributedSyncTimeout` for tests that want failures quickly.
pub fn short_timeouts() -> TopologyConfig {
    TopologyConfig { global_timeout: Duration::from_secs(20) }
}
