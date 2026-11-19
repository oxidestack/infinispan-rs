//! A cluster node over a real JGroups channel: [`JGroupsTopologyTransport`] carries the
//! topology commands, and this module adds what `ReplCluster` does in memory for the data path: a
//! [`CacheEngine`] per started cache, wired to the same channel (`CacheTransport` over
//! `RpcManager.invokeCommand`), with inbound `CacheRpcCommand` frames routed to the engine named in
//! them, state transfer included. A JVM running Infinispan 14 can be a member of the same cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::encoder_cache::Cache;
use super::write_pipeline::{CacheConfig, CacheEngine, CacheTransport, EngineEnv, RpcResult, TopologySource};
use crate::ch::address::Address;
use crate::commands::rpc::CacheRpcCommand;
use crate::statetransfer::lock::DistributionManager;
use crate::statetransfer::manager::StateTransferManager;
use crate::topology::cluster_cache_status::TopologyError;
use crate::topology::cluster_topology_manager::TopologyConfig;
use crate::topology::jgroups_transport::{CacheRpcSink, JGroupsTopologyTransport};
use crate::topology::messages::{ExceptionKind, Response};
use crate::topology::node::TopologyNode;
use crate::topology::support::BoxFuture;
use crate::topology::testing::{PhaseConfirmingHandler, TransferHook, repl_join_info_with_timeout};
use crate::topology::local_topology_manager::{AllowReadWritesPartitionHandling, PartitionHandling};
use crate::topology::wire::ObjectOutput;
use crate::remoting::transport::TransportError;

type Engines = Arc<Mutex<HashMap<String, Arc<CacheEngine>>>>;

struct JGroupsCacheTransport {
    transport: Arc<JGroupsTopologyTransport>,
}

impl CacheTransport for JGroupsCacheTransport {
    fn address(&self) -> Address {
        use crate::topology::transport::TopologyTransport;
        self.transport.address()
    }

    fn view_id(&self) -> i32 {
        use crate::topology::transport::TopologyTransport;
        self.transport.view_id()
    }

    fn members(&self) -> Vec<Address> {
        use crate::topology::transport::TopologyTransport;
        self.transport.members()
    }

    fn invoke(&self, target: &Address, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, RpcResult> {
        self.transport.request_cache(target, request, timeout)
    }

    fn invoke_on_all(&self, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Vec<(Address, RpcResult)>> {
        let me = self.address();
        let calls: Vec<_> = self
            .members()
            .into_iter()
            .filter(|a| *a != me)
            .map(|t| {
                let fut = self.transport.request_cache(&t, request.clone(), timeout);
                (t, tokio::spawn(fut))
            })
            .collect();
        Box::pin(async move {
            let mut out = Vec::new();
            for (t, h) in calls {
                out.push((t, h.await.unwrap_or_else(|e| Err(crate::topology::transport::RemoteError::Other(e.to_string())))));
            }
            out
        })
    }
}

struct EngineRouter(Engines);

fn response_bytes(r: &Response) -> Vec<u8> {
    let mut o = ObjectOutput::new();
    o.write_response(r);
    o.into_bytes()
}

impl CacheRpcSink for EngineRouter {
    fn handle(&self, origin: Address, request: Vec<u8>) -> BoxFuture<'static, Option<Vec<u8>>> {
        let engines = self.0.clone();
        Box::pin(async move {
            let engine = CacheRpcCommand::peek_cache_name(&request).ok().and_then(|n| engines.lock().unwrap().get(&n).cloned());
            let Some(engine) = engine else { return Some(response_bytes(&Response::CacheNotFound)) };
            match engine.handle_rpc_from(Some(&origin), &request).await {
                Ok(answer) => answer,
                Err(e) => Some(response_bytes(&Response::Exception { kind: ExceptionKind::CacheException, message: Some(e.to_string()) })),
            }
        })
    }
}

struct ManagerHook(Arc<StateTransferManager>);

impl TransferHook for ManagerHook {
    fn on_topology(&self, topology: crate::topology::cache_topology::CacheTopology, is_rebalance: bool) -> BoxFuture<'static, BoxFuture<'static, ()>> {
        let transfer = self.0.on_topology(topology, is_rebalance);
        #[allow(clippy::async_yields_async)]
        Box::pin(async move { transfer })
    }
}

/// One node: a JGroups channel, the topology managers and the caches started on it.
pub struct ClusterNode {
    /// The JGroups transport the node's topology and cache commands travel over.
    pub transport: Arc<JGroupsTopologyTransport>,
    /// The node's topology managers.
    pub node: Arc<TopologyNode>,
    engines: Engines,
    caches: Mutex<HashMap<String, Cache>>,
}

impl ClusterNode {
    /// Build the channel from `stack_xml`, join `cluster_name` and start the topology managers.
    pub async fn start(stack_xml: &str, cluster_name: &str, global_timeout: Duration) -> Result<Arc<Self>, TransportError> {
        let transport = JGroupsTopologyTransport::connect(stack_xml, cluster_name).await?;
        let node = TopologyNode::new(transport.clone(), TopologyConfig { global_timeout });
        let engines: Engines = Arc::new(Mutex::new(HashMap::new()));
        transport.set_cache_sink(Arc::new(EngineRouter(engines.clone())));
        node.start().await;
        transport.attach(node.clone());
        Ok(Arc::new(Self { transport, node, engines, caches: Mutex::new(HashMap::new()) }))
    }

    /// `getCache(name)` of a REPL_SYNC cache: the engine is reachable before the join, so the joiner can
    /// receive state while its initial transfer is awaited (as `ReplCluster::start_cache_with`).
    pub async fn start_cache(&self, cfg: CacheConfig) -> Result<(Cache, Arc<PhaseConfirmingHandler>), TopologyError> {
        let name = cfg.name.clone();
        let transport = Arc::new(JGroupsCacheTransport { transport: self.transport.clone() });
        let distribution = DistributionManager::new();
        let dist = distribution.clone();
        let topology: TopologySource = Arc::new(move || dist.cache_topology());
        let (num_segments, st_timeout, chunk, await_initial) = (cfg.num_segments, cfg.state_transfer_timeout, cfg.state_chunk_size, cfg.await_initial_transfer);
        let engine = CacheEngine::new(cfg, transport, topology);
        let env = Arc::new(EngineEnv(Arc::downgrade(&engine)));
        let manager = StateTransferManager::new(env, &name, num_segments, engine.commit_manager.clone(), distribution, st_timeout, chunk);
        engine.set_state_transfer(manager.clone());
        self.engines.lock().unwrap().insert(name.clone(), engine.clone());

        let handler = PhaseConfirmingHandler::new(&name, &self.node.ltm, self.node.address());
        handler.set_transfer_hook(Arc::new(ManagerHook(manager)));
        let phm: Arc<dyn PartitionHandling> = AllowReadWritesPartitionHandling::new();
        if let Err(e) = self.node.ltm.join(&name, repl_join_info_with_timeout(self.node.ltm.persistent_uuid(), st_timeout), handler.clone(), phm).await {
            self.engines.lock().unwrap().remove(&name);
            return Err(e);
        }
        if await_initial && !handler.wait_for_initial_transfer(st_timeout).await {
            self.engines.lock().unwrap().remove(&name);
            return Err(TopologyError(format!("TimeoutException: ISPN000658: Initial state transfer timed out for cache {name} on {:?}", self.node.address())));
        }
        let cache = Cache::new(engine);
        self.caches.lock().unwrap().insert(name, cache.clone());
        Ok((cache, handler))
    }

    /// The cache `name`, if it was started on this node.
    pub fn cache(&self, name: &str) -> Option<Cache> {
        self.caches.lock().unwrap().get(name).cloned()
    }

    /// Stops the caches' state transfer, the topology managers and the channel.
    pub async fn stop(&self) {
        for (_, e) in self.engines.lock().unwrap().drain() {
            if let Some(m) = e.state_transfer() {
                m.stop();
            }
        }
        self.node.stop();
        self.transport.disconnect().await;
    }
}
