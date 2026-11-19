//! An in-memory cluster for exercising the topology managers: nodes with their own views (so a
//! network partition is just two nodes installing disjoint views), a transport that **marshals every
//! command and answer** through the real wire codecs (`TopologyCommand::to_bytes`,
//! `ObjectOutput::write_response`) before delivering it, and a state-transfer stand-in
//! ([`PhaseConfirmingHandler`]) that confirms each rebalance phase the way
//! `StateTransferManagerImpl.updateProviderAndConsumer` does once its transfer completes.
//!
//! It plays the role of upstream's `MultipleCacheManagersTest`/`TestingUtil` view helpers and of the
//! `MockTransport`/`MockLocalTopologyManager` pair `ClusterTopologyManagerImplTest` uses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use super::cache_topology::{CacheTopology, Phase};
use super::cluster_cache_status::{RebalancingStatus, TopologyError};
use super::cluster_topology_manager::TopologyConfig;
use super::commands::TopologyCommand;
use super::local_topology_manager::{
    AllowReadWritesPartitionHandling, CacheTopologyHandler, LocalTopologyManager, PartitionHandling, WeakLocalTopologyManager,
};
use super::messages::*;
use super::node::TopologyNode;
use super::support::{BoxFuture, eager_spawn};
use super::transport::{RemoteError, TopologyTransport};
use super::wire::{ObjectInput, ObjectOutput};
use crate::ch::address::Address;

/// `new CacheJoinInfo(SyncReplicatedConsistentHashFactory, 256 segments, ...)` as a REPL_SYNC cache
/// with `numOwners` irrelevant to the replicated CH.
pub fn repl_join_info(persistent_uuid: Address) -> CacheJoinInfo {
    repl_join_info_with_timeout(persistent_uuid, Duration::from_millis(30_000))
}

/// [`repl_join_info`] with `stateTransfer().timeout()` as the join timeout, in whole milliseconds as
/// `CacheJoinInfo.timeout` carries it (`StateTransferManagerImpl.start`: `configuration.clustering()
/// .stateTransfer().timeout()`), so a sub-millisecond timeout is 0.
pub fn repl_join_info_with_timeout(persistent_uuid: Address, timeout: Duration) -> CacheJoinInfo {
    CacheJoinInfo {
        consistent_hash_factory: ConsistentHashFactoryKind::SyncReplicated,
        num_segments: 256,
        num_owners: 2,
        timeout: timeout.as_millis() as i64,
        cache_mode: CacheMode::ReplSync,
        capacity_factor: 1.0,
        persistent_uuid: Some(persistent_uuid),
        persistent_state_checksum: None,
    }
}

#[derive(Clone)]
struct View {
    id: i32,
    members: Vec<Address>,
}

/// Hooks around a command's execution on its target, for tests that hold a command at a given point
/// (upstream's `Mocks.blockInboundGlobalCommandExecution`).
pub trait DeliveryInterceptor: Send + Sync {
    /// Awaited before the command runs on its target.
    fn before_invoke(&self, _command: &TopologyCommand) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// Awaited after the command ran, before its answer is sent back.
    fn after_invoke(&self, _command: &TopologyCommand) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

struct NetNode {
    interceptor: Mutex<Option<Arc<dyn DeliveryInterceptor>>>,
    address: Address,
    view: Mutex<View>,
    view_tx: watch::Sender<i32>,
    node: Mutex<Option<Arc<TopologyNode>>>,
    alive: AtomicBool,
}

#[derive(Default)]
struct Network {
    nodes: Mutex<HashMap<Address, Arc<NetNode>>>,
}

impl Network {
    fn get(&self, a: &Address) -> Option<Arc<NetNode>> {
        self.nodes.lock().unwrap().get(a).cloned()
    }

    /// Both ends must list each other in their current views (a partition discards the rest).
    fn reachable(&self, from: &Address, to: &Address) -> Option<Arc<NetNode>> {
        let f = self.get(from)?;
        let t = self.get(to)?;
        if !f.alive.load(Ordering::SeqCst) || !t.alive.load(Ordering::SeqCst) {
            return None;
        }
        let f_sees_t = f.view.lock().unwrap().members.contains(to);
        let t_sees_f = t.view.lock().unwrap().members.contains(from);
        if f_sees_t && t_sees_f { Some(t) } else { None }
    }
}

/// The transport of one in-memory node.
pub struct InMemoryTransport {
    net: Arc<Network>,
    me: Address,
}

fn deliver(target: Arc<NetNode>, origin: Address, bytes: Vec<u8>) -> BoxFuture<'static, Result<Response, RemoteError>> {
    Box::pin(async move {
        let node = target.node.lock().unwrap().clone();
        let Some(node) = node else { return Err(RemoteError::Suspect("node not started".into())) };
        let command = TopologyCommand::from_bytes(&bytes).map_err(|e| RemoteError::Other(e.to_string()))?;
        let interceptor = target.interceptor.lock().unwrap().clone();
        if let Some(i) = &interceptor {
            i.before_invoke(&command).await;
        }
        let result = eager_spawn(node.invoke(origin, command.clone())).await.map_err(|e| RemoteError::Other(e.to_string()))?;
        if let Some(i) = &interceptor {
            i.after_invoke(&command).await;
        }
        match result {
            Ok(v) => {
                let response = Response::Successful(v.map(Box::new));
                // marshal the answer exactly as the wire would carry it
                let mut o = ObjectOutput::new();
                o.write_response(&response);
                let bytes = o.into_bytes();
                ObjectInput::new(&bytes).read_response().map_err(|e| RemoteError::Other(e.to_string()))
            }
            Err(e) => Err(RemoteError::Other(e.0)),
        }
    })
}

impl TopologyTransport for InMemoryTransport {
    fn address(&self) -> Address {
        self.me.clone()
    }

    fn view_id(&self) -> i32 {
        self.net.get(&self.me).unwrap().view.lock().unwrap().id
    }

    fn members(&self) -> Vec<Address> {
        self.net.get(&self.me).unwrap().view.lock().unwrap().members.clone()
    }

    fn with_view(&self, view_id: i32) -> BoxFuture<'static, ()> {
        let mut rx = self.net.get(&self.me).unwrap().view_tx.subscribe();
        Box::pin(async move {
            let _ = rx.wait_for(|v| *v >= view_id).await;
        })
    }

    fn invoke_command(&self, target: &Address, command: TopologyCommand, timeout: Duration) -> BoxFuture<'static, Result<Response, RemoteError>> {
        let Some(t) = self.net.reachable(&self.me, target) else {
            let target = target.clone();
            return Box::pin(async move { Err(RemoteError::Suspect(format!("{target:?} is not reachable"))) });
        };
        let fut = deliver(t, self.me.clone(), command.to_bytes());
        Box::pin(async move { tokio::time::timeout(timeout, fut).await.unwrap_or(Err(RemoteError::Timeout)) })
    }

    fn invoke_command_on_all(
        &self,
        targets: Option<Vec<Address>>,
        command: TopologyCommand,
        timeout: Duration,
    ) -> BoxFuture<'static, Vec<(Address, Result<Response, RemoteError>)>> {
        let members = self.members();
        let targets: Vec<Address> = match targets {
            Some(t) => t.into_iter().filter(|a| *a != self.me).collect(),
            None => members.iter().filter(|a| **a != self.me).cloned().collect(),
        };
        let bytes = command.to_bytes();
        let calls: Vec<(Address, BoxFuture<'static, Result<Response, RemoteError>>)> = targets
            .into_iter()
            .map(|t| {
                let fut: BoxFuture<'static, Result<Response, RemoteError>> = match self.net.reachable(&self.me, &t) {
                    Some(n) if members.contains(&t) => deliver(n, self.me.clone(), bytes.clone()),
                    _ => {
                        let t2 = t.clone();
                        Box::pin(async move { Err(RemoteError::Suspect(format!("{t2:?} is not reachable"))) })
                    }
                };
                (t, fut)
            })
            .collect();
        Box::pin(async move {
            let mut handles = Vec::new();
            for (t, f) in calls {
                handles.push((t, tokio::spawn(async move { tokio::time::timeout(timeout, f).await.unwrap_or(Err(RemoteError::Timeout)) })));
            }
            let mut out = Vec::new();
            for (t, h) in handles {
                out.push((t, h.await.unwrap_or(Err(RemoteError::Other("task failed".into())))));
            }
            out
        })
    }

    fn send_to_all(&self, command: TopologyCommand) {
        let bytes = command.to_bytes();
        for m in self.members() {
            if m != self.me {
                self.send_bytes(&m, bytes.clone());
            }
        }
    }

    fn send_to(&self, target: &Address, command: TopologyCommand) {
        self.send_bytes(target, command.to_bytes());
    }
}

impl InMemoryTransport {
    fn send_bytes(&self, target: &Address, bytes: Vec<u8>) {
        if let Some(t) = self.net.reachable(&self.me, target) {
            let fut = deliver(t, self.me.clone(), bytes);
            eager_spawn(async move {
                let _ = fut.await;
            });
        }
    }
}

/// What `StateTransferManagerImpl.updateProviderAndConsumer` does with a topology: install it and start
/// the state transfer. The outer future resolves when the topology is installed (the local topology
/// manager blocks further updates on it); the inner one when the state of the topology has been received
/// and the phase may be confirmed.
pub trait TransferHook: Send + Sync {
    /// Installs `topology`; the outer future resolves once installed, the inner one once its state has been received.
    fn on_topology(&self, topology: CacheTopology, is_rebalance: bool) -> BoxFuture<'static, BoxFuture<'static, ()>>;
}

/// `StateTransferManagerImpl`'s part in the protocol: after a topology with a rebalance phase is
/// installed, confirm it to the coordinator (once "state transfer" is done).
pub struct PhaseConfirmingHandler {
    cache_name: String,
    ltm: WeakLocalTopologyManager,
    /// While set, confirmations are withheld (a state transfer that has not finished).
    hold: Arc<AtomicBool>,
    /// Every topology the handler received, in order.
    pub history: Mutex<Vec<CacheTopology>>,
    initial_transfer_complete: watch::Sender<bool>,
    local: Address,
    /// the cache's state transfer, when it has one (otherwise "state transfer" is instantaneous)
    hook: Mutex<Option<Arc<dyn TransferHook>>>,
}

impl PhaseConfirmingHandler {
    /// A handler for `cache_name` on the node of `ltm`.
    pub fn new(cache_name: &str, ltm: &LocalTopologyManager, local: Address) -> Arc<Self> {
        Arc::new(Self {
            cache_name: cache_name.to_string(),
            ltm: ltm.downgrade(),
            hold: Arc::new(AtomicBool::new(false)),
            history: Mutex::new(Vec::new()),
            initial_transfer_complete: watch::channel(false).0,
            local,
            hook: Mutex::new(None),
        })
    }

    /// Sets the cache's state transfer stand-in.
    pub fn set_transfer_hook(&self, hook: Arc<dyn TransferHook>) {
        *self.hook.lock().unwrap() = Some(hook);
    }

    /// While `hold` is set, phase confirmations are withheld.
    pub fn hold_confirmations(&self, hold: bool) {
        self.hold.store(hold, Ordering::SeqCst);
    }

    /// `initialStateTransferComplete`: the node has seen a stable topology that lists it as a read owner.
    pub async fn wait_for_initial_transfer(&self, timeout: Duration) -> bool {
        let mut rx = self.initial_transfer_complete.subscribe();
        tokio::time::timeout(timeout, rx.wait_for(|v| *v)).await.is_ok()
    }

    /// Every topology the handler received, in order.
    pub fn topologies(&self) -> Vec<CacheTopology> {
        self.history.lock().unwrap().clone()
    }

    fn on_topology(&self, topology: CacheTopology, is_rebalance: bool) -> BoxFuture<'static, ()> {
        let phase = topology.phase;
        self.history.lock().unwrap().push(topology.clone());
        let hook = self.hook.lock().unwrap().clone();
        let ltm = self.ltm.clone();
        let name = self.cache_name.clone();
        let (id, rid) = (topology.topology_id, topology.rebalance_id);
        let hold = self.hold.clone();
        let initial = self.initial_transfer_complete.clone();
        let local = self.local.clone();
        let read_has_me = topology.read_consistent_hash().is_some_and(|ch| ch.members().contains(&local));
        Box::pin(async move {
            let transfer = match hook {
                Some(h) => Some(h.on_topology(topology, is_rebalance).await),
                None => None,
            };
            if matches!(phase, Phase::Transitory | Phase::ReadOldWriteAll | Phase::ReadAllWriteAll | Phase::ReadNewWriteAll) {
                tokio::spawn(async move {
                    if let Some(t) = transfer {
                        t.await;
                    }
                    while hold.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    if let Some(ltm) = ltm.upgrade() {
                        ltm.confirm_rebalance_phase(&name, id, rid);
                    }
                });
            }
            // completeInitialTransferIfNeeded
            if phase == Phase::NoRebalance && read_has_me {
                let _ = initial.send_replace(true);
            }
        })
    }
}

impl CacheTopologyHandler for PhaseConfirmingHandler {
    fn update_consistent_hash(&self, topology: CacheTopology) -> BoxFuture<'static, ()> {
        self.on_topology(topology, false)
    }

    fn rebalance(&self, topology: CacheTopology) -> BoxFuture<'static, ()> {
        self.on_topology(topology, true)
    }
}

struct TestNode {
    net_node: Arc<NetNode>,
    node: Arc<TopologyNode>,
    ltm: LocalTopologyManager,
    handlers: Mutex<HashMap<String, Arc<PhaseConfirmingHandler>>>,
}

/// A cluster of in-memory nodes.
pub struct TestCluster {
    net: Arc<Network>,
    nodes: Mutex<Vec<Arc<TestNode>>>,
    config: TopologyConfig,
    next_id: Mutex<i64>,
}

impl TestCluster {
    /// An empty cluster.
    pub fn new(config: TopologyConfig) -> Arc<Self> {
        Arc::new(Self { net: Arc::new(Network::default()), nodes: Mutex::new(Vec::new()), config, next_id: Mutex::new(0) })
    }

    fn node(&self, idx: usize) -> Arc<TestNode> {
        self.nodes.lock().unwrap()[idx].clone()
    }

    /// The address of node `idx`.
    pub fn address(&self, idx: usize) -> Address {
        self.node(idx).net_node.address.clone()
    }

    /// The topology managers of node `idx`.
    pub fn topology_node(&self, idx: usize) -> Arc<TopologyNode> {
        self.node(idx).node.clone()
    }

    /// The number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    /// Whether the cluster has no node.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a node, not yet in any view with others and not started.
    pub fn add_node(&self) -> usize {
        let mut id = self.next_id.lock().unwrap();
        *id += 1;
        let n = *id;
        // a distinct logical address per node
        let address = Address::uuid(0x5000 + n * 0x111, 0x7000 + n * 0x37);
        let (tx, _) = watch::channel(0);
        let net_node = Arc::new(NetNode {
            interceptor: Mutex::new(None),
            address: address.clone(),
            view: Mutex::new(View { id: 0, members: vec![address.clone()] }),
            view_tx: tx,
            node: Mutex::new(None),
            alive: AtomicBool::new(true),
        });
        self.net.nodes.lock().unwrap().insert(address.clone(), net_node.clone());
        let transport = Arc::new(InMemoryTransport { net: self.net.clone(), me: address });
        let node = TopologyNode::new(transport, self.config.clone());
        *net_node.node.lock().unwrap() = Some(node.clone());
        let ltm = node.ltm.clone();
        let tn = Arc::new(TestNode { net_node, node, ltm, handlers: Mutex::new(HashMap::new()) });
        let mut nodes = self.nodes.lock().unwrap();
        nodes.push(tn);
        nodes.len() - 1
    }

    /// Install a view with `members` (in this order) on each of them: `TestingUtil.installNewView`.
    /// The view id is one more than the highest id any member currently has.
    pub fn install_view(&self, members: &[usize], merge: bool) -> i32 {
        let addrs: Vec<Address> = members.iter().map(|i| self.address(*i)).collect();
        let id = members.iter().map(|i| self.node(*i).net_node.view.lock().unwrap().id).max().unwrap_or(0) + 1;
        for i in members {
            let n = self.node(*i);
            *n.net_node.view.lock().unwrap() = View { id, members: addrs.clone() };
            let _ = n.net_node.view_tx.send_replace(id);
        }
        for i in members {
            let n = self.node(*i);
            if n.node.ltm.is_running() || n.node.ctm.status() != super::cluster_topology_manager::ClusterManagerStatus::Initializing {
                n.node.on_view_change(merge, id);
            }
        }
        id
    }

    /// Start the managers of node `idx` (its first view is already installed).
    pub async fn start_node(&self, idx: usize) {
        self.node(idx).node.start().await;
    }

    /// Hold commands arriving at node `idx` (see [`DeliveryInterceptor`]).
    pub fn set_delivery_interceptor(&self, idx: usize, interceptor: Option<Arc<dyn DeliveryInterceptor>>) {
        *self.node(idx).net_node.interceptor.lock().unwrap() = interceptor;
    }

    /// Spy on node `idx`'s local topology manager.
    pub fn set_local_interceptor(&self, idx: usize, i: Option<Arc<dyn super::local_topology_manager::LocalTopologyInterceptor>>) {
        self.node(idx).node.ltm.set_interceptor(i);
    }

    /// Whether `from` and `to` are alive and list each other in their views (a partition cuts this).
    pub fn reachable(&self, from: &Address, to: &Address) -> bool {
        self.net.reachable(from, to).is_some()
    }

    /// The current view of node `idx`: `(id, members)`.
    pub fn view_of(&self, idx: usize) -> (i32, Vec<Address>) {
        let v = self.node(idx).net_node.view.lock().unwrap().clone();
        (v.id, v.members)
    }

    /// Stop node `idx` abruptly: it stays out of every view and answers nothing.
    pub fn kill_node(&self, idx: usize) {
        let n = self.node(idx);
        n.net_node.alive.store(false, Ordering::SeqCst);
        n.node.stop();
    }

    /// Start `cache_name` on node `idx` (`cache.start()` / `getCache`): joins and waits for the
    /// initial state transfer.
    pub async fn start_cache(&self, idx: usize, cache_name: &str) -> Result<Arc<PhaseConfirmingHandler>, TopologyError> {
        self.start_cache_with_hook(idx, cache_name, None).await
    }

    /// `start_cache` with the cache's state transfer attached before it joins.
    pub async fn start_cache_with_hook(&self, idx: usize, cache_name: &str, hook: Option<Arc<dyn TransferHook>>) -> Result<Arc<PhaseConfirmingHandler>, TopologyError> {
        self.start_cache_hooked(idx, cache_name, hook, true).await
    }

    /// As [`TestCluster::start_cache_with_hook`]; `await_initial_transfer = false` returns as soon as the
    /// join was answered (`stateTransfer().awaitInitialTransfer(false)`).
    pub async fn start_cache_hooked(&self, idx: usize, cache_name: &str, hook: Option<Arc<dyn TransferHook>>, await_initial_transfer: bool) -> Result<Arc<PhaseConfirmingHandler>, TopologyError> {
        self.start_cache_hooked_with_timeout(idx, cache_name, hook, await_initial_transfer, Duration::from_millis(30_000)).await
    }

    /// As [`TestCluster::start_cache_hooked`] with the cache's `stateTransfer().timeout()`: the timeout of its
    /// join request and of the wait for the initial transfer.
    pub async fn start_cache_hooked_with_timeout(
        &self,
        idx: usize,
        cache_name: &str,
        hook: Option<Arc<dyn TransferHook>>,
        await_initial_transfer: bool,
        state_transfer_timeout: Duration,
    ) -> Result<Arc<PhaseConfirmingHandler>, TopologyError> {
        let n = self.node(idx);
        let handler = PhaseConfirmingHandler::new(cache_name, &n.ltm, n.net_node.address.clone());
        if let Some(h) = hook {
            handler.set_transfer_hook(h);
        }
        n.handlers.lock().unwrap().insert(cache_name.to_string(), handler.clone());
        let join_info = repl_join_info_with_timeout(n.node.ltm.persistent_uuid(), state_transfer_timeout);
        n.ltm.join(cache_name, join_info, handler.clone(), AllowReadWritesPartitionHandling::new() as Arc<dyn PartitionHandling>).await?;
        if await_initial_transfer && !handler.wait_for_initial_transfer(state_transfer_timeout).await {
            return Err(TopologyError(format!("TimeoutException: ISPN000658: Initial state transfer timed out for cache {cache_name} on {:?}", n.net_node.address)));
        }
        Ok(handler)
    }

    /// `cache.stop()`: the node leaves the cache (`StateTransferManagerImpl.stop`).
    pub async fn stop_cache(&self, idx: usize, cache_name: &str) {
        let n = self.node(idx);
        n.ltm.leave(cache_name, Duration::from_secs(30)).await;
        n.handlers.lock().unwrap().remove(cache_name);
    }

    /// The phase-confirming handler of `cache_name` on node `idx`.
    pub fn handler(&self, idx: usize, cache_name: &str) -> Arc<PhaseConfirmingHandler> {
        self.node(idx).handlers.lock().unwrap().get(cache_name).cloned().expect("cache started")
    }

    /// A node's local view of a cache (`LocalTopologyManager.getCacheTopology`).
    pub fn cache_topology(&self, idx: usize, cache_name: &str) -> Option<CacheTopology> {
        self.node(idx).ltm.cache_topology(cache_name)
    }

    /// Node `idx`'s stable topology of `cache_name` (`LocalTopologyManager.getStableCacheTopology`).
    pub fn stable_topology(&self, idx: usize, cache_name: &str) -> Option<CacheTopology> {
        self.node(idx).ltm.stable_cache_topology(cache_name)
    }

    /// `TestingUtil.waitForNoRebalance`: every listed node sees a `NO_REBALANCE` topology without a
    /// pending CH whose current CH has exactly `nodes.len()` members, all with the same topology id, and
    /// the coordinator reports `COMPLETE`.
    /// The coordinator announces the end of a rebalance as two messages, the topology and then the
    /// stable topology; upstream tests read the stable topology right after this returns and rely on
    /// the JVM's timing, so this also waits until every node has handled the commands it received.
    pub async fn wait_for_no_rebalance(&self, nodes: &[usize], cache_name: &str, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let topologies: Vec<Option<CacheTopology>> = nodes.iter().map(|i| self.cache_topology(*i, cache_name)).collect();
            // `chContainsAllMembers` (`TestingUtil.java:426`): the topology a node sees from before a view
            // change has the wrong number of members, so waiting for a partition does not return early.
            let settled = topologies
                .iter()
                .all(|t| t.as_ref().is_some_and(|t| t.phase == Phase::NoRebalance && t.pending_ch.is_none() && t.current_ch.as_ref().is_some_and(|ch| ch.members().len() == nodes.len())))
                && topologies.windows(2).all(|w| w[0].as_ref().unwrap().topology_id == w[1].as_ref().unwrap().topology_id)
                && nodes.iter().all(|i| {
                    let n = self.node(*i);
                    n.node.ctm.status() != super::cluster_topology_manager::ClusterManagerStatus::Coordinator
                        || n.node.ctm.rebalancing_status(Some(cache_name)) == RebalancingStatus::Complete
                })
                // After the coordinator's status: it reports `COMPLETE` under the lock that also covers
                // sending the end-of-rebalance messages, so once it does they are queued on the nodes.
                && nodes.iter().all(|i| self.node(*i).ltm.is_idle());
            if settled {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("no rebalance settled for {cache_name}: {:?}", topologies.iter().map(|t| t.as_ref().map(|t| (t.topology_id, t.phase, t.members().len()))).collect::<Vec<_>>()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until `f` holds, polling.
    pub async fn eventually(&self, timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if f() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// One thing a [`MockTransport`] was asked to send.
#[derive(Debug, Clone, PartialEq)]
pub struct Sent {
    /// `None` for a broadcast.
    pub target: Option<Address>,
    /// The command.
    pub command: TopologyCommand,
}

type Responder = Box<dyn Fn(&Address, &TopologyCommand) -> Result<Response, RemoteError> + Send + Sync>;

struct MockState {
    view: View,
    sent: Vec<Sent>,
}

/// `MockTransport` of `ClusterTopologyManagerImplTest`: a transport that records what the manager
/// sends and answers requests from a script. Other members do not really exist.
pub struct MockTransport {
    me: Address,
    state: Mutex<MockState>,
    view_tx: watch::Sender<i32>,
    responder: Mutex<Responder>,
}

impl MockTransport {
    /// A transport for the node `me`, alone in its view, answering every request with an empty success.
    pub fn new(me: Address) -> Arc<Self> {
        let default: Responder = Box::new(|_, cmd| {
            Ok(match cmd {
                TopologyCommand::CacheStatusRequest { .. } => Response::Successful(Some(Box::new(ResponseValue::ManagerStatus(ManagerStatusResponse {
                    caches: vec![],
                    rebalancing_enabled: true,
                })))),
                _ => Response::Successful(None),
            })
        });
        Arc::new(Self {
            state: Mutex::new(MockState { view: View { id: 0, members: vec![me.clone()] }, sent: Vec::new() }),
            me,
            view_tx: watch::channel(0).0,
            responder: Mutex::new(default),
        })
    }

    /// `transport.init(viewId, members)` / `updateView`.
    pub fn update_view(&self, id: i32, members: Vec<Address>) {
        self.state.lock().unwrap().view = View { id, members };
        let _ = self.view_tx.send_replace(id);
    }

    /// Replaces the script that answers requests.
    pub fn set_responder(&self, f: impl Fn(&Address, &TopologyCommand) -> Result<Response, RemoteError> + Send + Sync + 'static) {
        *self.responder.lock().unwrap() = Box::new(f);
    }

    /// Everything sent so far, in order.
    pub fn sent(&self) -> Vec<Sent> {
        self.state.lock().unwrap().sent.clone()
    }

    /// `expectCommand(type, check)`: wait for the next recorded command satisfying `pred`, removing it.
    pub async fn expect_command(&self, what: &str, pred: impl Fn(&TopologyCommand) -> bool) -> TopologyCommand {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            {
                let mut st = self.state.lock().unwrap();
                if let Some(pos) = st.sent.iter().position(|s| pred(&s.command)) {
                    return st.sent.remove(pos).command;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {what}; sent so far: {:?}", self.sent());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn record(&self, target: Option<Address>, command: &TopologyCommand) {
        // the command must survive the wire
        let bytes = command.to_bytes();
        let back = TopologyCommand::from_bytes(&bytes).expect("a sent command decodes");
        assert_eq!(&back, command, "a sent command round-trips through the marshaller");
        self.state.lock().unwrap().sent.push(Sent { target, command: command.clone() });
    }
}

impl TopologyTransport for MockTransport {
    fn address(&self) -> Address {
        self.me.clone()
    }

    fn view_id(&self) -> i32 {
        self.state.lock().unwrap().view.id
    }

    fn members(&self) -> Vec<Address> {
        self.state.lock().unwrap().view.members.clone()
    }

    fn with_view(&self, view_id: i32) -> BoxFuture<'static, ()> {
        let mut rx = self.view_tx.subscribe();
        Box::pin(async move {
            let _ = rx.wait_for(|v| *v >= view_id).await;
        })
    }

    fn invoke_command(&self, target: &Address, command: TopologyCommand, _timeout: Duration) -> BoxFuture<'static, Result<Response, RemoteError>> {
        self.record(Some(target.clone()), &command);
        let r = (self.responder.lock().unwrap())(target, &command);
        Box::pin(async move { r })
    }

    fn invoke_command_on_all(
        &self,
        targets: Option<Vec<Address>>,
        command: TopologyCommand,
        _timeout: Duration,
    ) -> BoxFuture<'static, Vec<(Address, Result<Response, RemoteError>)>> {
        self.record(None, &command);
        let members = self.members();
        let targets: Vec<Address> = match targets {
            Some(t) => t.into_iter().filter(|a| *a != self.me).collect(),
            None => members.into_iter().filter(|a| *a != self.me).collect(),
        };
        let results: Vec<_> = targets.iter().map(|t| (t.clone(), (self.responder.lock().unwrap())(t, &command))).collect();
        Box::pin(async move { results })
    }

    fn send_to_all(&self, command: TopologyCommand) {
        self.record(None, &command);
    }

    fn send_to(&self, target: &Address, command: TopologyCommand) {
        self.record(Some(target.clone()), &command);
    }
}
