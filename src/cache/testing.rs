//! An in-memory REPL_SYNC cluster for the data path: nodes of [`crate::topology::testing::TestCluster`]
//! (views, partitions and the topology managers), each running a [`CacheEngine`] whose transport
//! **marshals every request and answer** through the real codecs before delivering it, so the
//! tests exercise the same bytes a JGroups channel would carry.
//!
//! A node that joins after data was written receives it by state transfer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::encoder_cache::Cache;
use super::write_pipeline::{CacheConfig, CacheEngine, CacheTransport, EngineEnv, TopologySource};
use crate::statetransfer::lock::DistributionManager;
use crate::statetransfer::manager::StateTransferManager;
use crate::topology::testing::TransferHook;
use crate::ch::address::Address;
use crate::commands::rpc::CacheRpcCommand;
use crate::topology::cluster_cache_status::TopologyError;
use crate::topology::cluster_topology_manager::TopologyConfig;
use crate::topology::messages::Response;
use crate::topology::support::BoxFuture;
use crate::topology::testing::TestCluster;
use crate::topology::transport::RemoteError;
use crate::topology::wire::ObjectInput;

/// What a test does with a request in flight (`Mocks`/`ControlledRpcManager` in upstream tests).
pub enum Intercepted {
    /// Deliver it normally.
    Deliver,
    /// Do not deliver; fail the call.
    Fail(RemoteError),
    /// Do not deliver; answer with this.
    Answer(Option<Response>),
}

/// A hook every request between nodes of a [`ReplCluster`] passes through.
pub trait RpcInterceptor: Send + Sync {
    /// Awaited before the request reaches `to`; may delay it.
    fn intercept(&self, from: &Address, to: &Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted>;
    /// Awaited after `to` produced its answer, before it travels back.
    fn after(&self, _from: &Address, _to: &Address, _command: &CacheRpcCommand) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

struct DataNet {
    cluster: Arc<TestCluster>,
    engines: Mutex<HashMap<(String, Address), Arc<CacheEngine>>>,
    interceptor: Mutex<Option<Arc<dyn RpcInterceptor>>>,
}

struct MemTransport {
    net: Arc<DataNet>,
    idx: usize,
    me: Address,
    cache: String,
}

fn deliver(
    net: Arc<DataNet>,
    cache: String,
    from: Address,
    to: Address,
    request: Vec<u8>,
    timeout: Duration,
) -> BoxFuture<'static, Result<Option<Response>, RemoteError>> {
    Box::pin(async move {
        // the whole round trip (interceptors included) is bounded by the request timeout
        tokio::time::timeout(timeout, deliver_inner(net, cache, from, to, request, timeout)).await.unwrap_or(Err(RemoteError::Timeout))
    })
}

fn deliver_inner(
    net: Arc<DataNet>,
    cache: String,
    from: Address,
    to: Address,
    request: Vec<u8>,
    timeout: Duration,
) -> BoxFuture<'static, Result<Option<Response>, RemoteError>> {
    Box::pin(async move {
        if !net.cluster.reachable(&from, &to) {
            return Err(RemoteError::Suspect(format!("{to:?} is not reachable")));
        }
        let command = CacheRpcCommand::from_bytes(&request).map_err(|e| RemoteError::Other(e.to_string()))?;
        let interceptor = net.interceptor.lock().unwrap().clone();
        if let Some(i) = &interceptor {
            match i.intercept(&from, &to, &command).await {
                Intercepted::Deliver => {}
                Intercepted::Fail(e) => return Err(e),
                Intercepted::Answer(a) => return Ok(a),
            }
        }
        let engine = net.engines.lock().unwrap().get(&(cache, to.clone())).cloned();
        let Some(engine) = engine else { return Ok(Some(Response::CacheNotFound)) };
        let origin = from.clone();
        let mut work = tokio::spawn(async move { engine.handle_rpc_from(Some(&origin), &request).await });
        // A request waiting for a member that leaves the view fails with a `SuspectException`
        // (`RequestCorrelator.receiveView` / `Request.viewChange`), it does not wait for its timeout.
        let deadline = tokio::time::Instant::now() + timeout;
        let answer = loop {
            tokio::select! {
                r = &mut work => match r {
                    Err(e) => return Err(RemoteError::Other(e.to_string())),
                    Ok(Err(e)) => return Err(RemoteError::Other(e.to_string())),
                    // a member that died while it worked never gets its answer back
                    Ok(Ok(_)) if !net.cluster.reachable(&from, &to) => return Err(RemoteError::Suspect(format!("{to:?} left the view"))),
                    Ok(Ok(a)) => break a,
                },
                _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    if !net.cluster.reachable(&from, &to) {
                        return Err(RemoteError::Suspect(format!("{to:?} left the view")));
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(RemoteError::Timeout);
                    }
                }
            }
        };
        if let Some(i) = &interceptor {
            i.after(&from, &to, &command).await;
        }
        match answer {
            None => Ok(None),
            Some(bytes) => ObjectInput::new(&bytes).read_response().map(Some).map_err(|e| RemoteError::Other(e.to_string())),
        }
    })
}

impl CacheTransport for MemTransport {
    fn address(&self) -> Address {
        self.me.clone()
    }

    fn view_id(&self) -> i32 {
        self.net.cluster.view_of(self.idx).0
    }

    fn members(&self) -> Vec<Address> {
        self.net.cluster.view_of(self.idx).1
    }

    fn invoke(&self, target: &Address, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Result<Option<Response>, RemoteError>> {
        deliver(self.net.clone(), self.cache.clone(), self.me.clone(), target.clone(), request, timeout)
    }

    fn invoke_on_all(&self, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Vec<(Address, Result<Option<Response>, RemoteError>)>> {
        let targets: Vec<Address> = self.members().into_iter().filter(|a| *a != self.me).collect();
        let net = self.net.clone();
        let (cache, me) = (self.cache.clone(), self.me.clone());
        Box::pin(async move {
            let handles: Vec<_> = targets
                .into_iter()
                .map(|t| {
                    let fut = deliver(net.clone(), cache.clone(), me.clone(), t.clone(), request.clone(), timeout);
                    (t, tokio::spawn(fut))
                })
                .collect();
            let mut out = Vec::new();
            for (t, h) in handles {
                out.push((t, h.await.unwrap_or_else(|e| Err(RemoteError::Other(e.to_string())))));
            }
            out
        })
    }
}

struct ManagerHook(Arc<StateTransferManager>);

impl TransferHook for ManagerHook {
    fn on_topology(&self, topology: crate::topology::cache_topology::CacheTopology, is_rebalance: bool) -> BoxFuture<'static, BoxFuture<'static, ()>> {
        let transfer = self.0.on_topology(topology, is_rebalance);
        // the topology is installed (synchronously); the transfer future is what the confirmation waits for
        #[allow(clippy::async_yields_async)]
        Box::pin(async move { transfer })
    }
}

/// A cluster of in-memory nodes running REPL_SYNC caches.
pub struct ReplCluster {
    /// The nodes and their topology managers.
    pub nodes: Arc<TestCluster>,
    net: Arc<DataNet>,
    caches: Mutex<HashMap<(usize, String), Cache>>,
}

impl ReplCluster {
    /// An empty cluster.
    pub fn new() -> Self {
        let nodes = TestCluster::new(TopologyConfig { global_timeout: Duration::from_secs(20) });
        let net = Arc::new(DataNet { cluster: nodes.clone(), engines: Mutex::new(HashMap::new()), interceptor: Mutex::new(None) });
        Self { nodes, net, caches: Mutex::new(HashMap::new()) }
    }

    /// Install (or clear) the hook every request passes through.
    pub fn set_interceptor(&self, i: Option<Arc<dyn RpcInterceptor>>) {
        *self.net.interceptor.lock().unwrap() = i;
    }

    /// Start `n` nodes one after another, each joining `cache_names` (a new view per node), the way
    /// `createClusteredCaches` grows a cluster. Returns once the topology is stable.
    pub async fn start(n: usize, cache_names: &[&str]) -> Self {
        Self::start_with(n, cache_names, |c| c).await
    }

    /// [`Self::start`], each cache's configuration passed through `configure`.
    pub async fn start_with(n: usize, cache_names: &[&str], configure: impl Fn(CacheConfig) -> CacheConfig) -> Self {
        let c = Self::new();
        for i in 0..n {
            let idx = c.nodes.add_node();
            let members: Vec<usize> = (0..=idx).collect();
            c.nodes.install_view(&members, false);
            c.nodes.start_node(idx).await;
            for name in cache_names {
                c.start_cache_with(idx, name, configure(CacheConfig::new(name))).await.expect("cache starts");
            }
            for name in cache_names {
                c.nodes.wait_for_no_rebalance(&members, name, Duration::from_secs(20)).await.expect("topology settles");
            }
            let _ = i;
        }
        c.wait_for_engines(Duration::from_secs(20)).await;
        c
    }

    /// Wait until every running engine commands under the topology its node's `LocalTopologyManager` reports
    /// (the engine learns it from the state transfer hook, a moment after the manager installs it). A command
    /// stamped with an older id than the node it reaches is, rightly, retried, which a test that has just
    /// waited for `NO_REBALANCE` does not expect.
    pub async fn wait_for_engines(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let lagging = self.net.engines.lock().unwrap().iter().any(|((name, addr), engine)| {
                let idx = (0..self.nodes.len()).find(|i| self.nodes.address(*i) == *addr);
                let want = idx.and_then(|i| self.nodes.cache_topology(i, name)).map(|t| t.topology_id);
                engine.topology_id() != want
            });
            if !lagging {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "the engines did not catch up with their topology managers");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Start a cache whose listener is registered before it joins, so it would see every notification
    /// state transfer fires (there must be none).
    pub async fn start_cache_with_listener(&self, idx: usize, name: &str, listener: Arc<dyn super::write_pipeline::EntryListener>) -> Result<Cache, TopologyError> {
        self.start_cache_full(idx, name, CacheConfig::new(name), Some(listener), None).await
    }

    /// Start a cache with an [`ExecuteHook`](super::write_pipeline::ExecuteHook) installed before it joins.
    pub async fn start_cache_with_engine_hook(&self, idx: usize, name: &str, cfg: CacheConfig, hook: Arc<dyn super::write_pipeline::ExecuteHook>) -> Result<Cache, TopologyError> {
        self.start_cache_full(idx, name, cfg, None, Some(hook)).await
    }

    /// Starts the cache `name` with `cfg` on node `idx`.
    pub async fn start_cache_with(&self, idx: usize, name: &str, cfg: CacheConfig) -> Result<Cache, TopologyError> {
        self.start_cache_full(idx, name, cfg, None, None).await
    }

    async fn start_cache_full(&self, idx: usize, name: &str, cfg: CacheConfig, listener: Option<Arc<dyn super::write_pipeline::EntryListener>>, execute_hook: Option<Arc<dyn super::write_pipeline::ExecuteHook>>) -> Result<Cache, TopologyError> {
        // The engine exists (and is reachable) before the cache joins, as upstream's components are wired
        // before `StateTransferManager.start` sends the join request: the joiner must be able to receive
        // state while its `getCache` is still waiting for the initial transfer.
        let transport = Arc::new(MemTransport { net: self.net.clone(), idx, me: self.nodes.address(idx), cache: name.to_string() });
        let distribution = DistributionManager::new();
        let dist = distribution.clone();
        let topology: TopologySource = Arc::new(move || dist.cache_topology());
        let (num_segments, st_timeout, chunk, await_initial) = (cfg.num_segments, cfg.state_transfer_timeout, cfg.state_chunk_size, cfg.await_initial_transfer);
        let engine = CacheEngine::new(cfg, transport, topology);
        if let Some(l) = listener {
            engine.add_listener(l);
        }
        engine.set_execute_hook(execute_hook);
        let env = Arc::new(EngineEnv(Arc::downgrade(&engine)));
        let manager = StateTransferManager::new(env, name, num_segments, engine.commit_manager.clone(), distribution, st_timeout, chunk);
        engine.set_state_transfer(manager.clone());
        self.net.engines.lock().unwrap().insert((name.to_string(), self.nodes.address(idx)), engine.clone());
        let hook: Arc<dyn TransferHook> = Arc::new(ManagerHook(manager));
        if let Err(e) = self.nodes.start_cache_hooked_with_timeout(idx, name, Some(hook), await_initial, st_timeout).await {
            self.net.engines.lock().unwrap().remove(&(name.to_string(), self.nodes.address(idx)));
            return Err(e);
        }
        let cache = Cache::new(engine);
        self.caches.lock().unwrap().insert((idx, name.to_string()), cache.clone());
        Ok(cache)
    }

    /// `cache.stop()`: stop its state transfer, leave the cluster's view of the cache and forget the data.
    pub async fn stop_cache(&self, idx: usize, name: &str) {
        let key = (name.to_string(), self.nodes.address(idx));
        let engine = self.net.engines.lock().unwrap().remove(&key);
        if let Some(m) = engine.and_then(|e| e.state_transfer()) {
            m.stop();
        }
        self.caches.lock().unwrap().remove(&(idx, name.to_string()));
        self.nodes.stop_cache(idx, name).await;
    }

    /// Add a node to the cluster (a new view with every earlier node), start its managers and have it
    /// join `cache_names`. Returns its index once the joins returned (the topology may still be rebalancing).
    pub async fn add_node_and_join(&self, cache_names: &[&str], configure: impl Fn(CacheConfig) -> CacheConfig) -> usize {
        let idx = self.add_started_node();
        self.nodes.start_node(idx).await;
        for name in cache_names {
            self.start_cache_with(idx, name, configure(CacheConfig::new(name))).await.expect("cache starts");
        }
        idx
    }

    /// A new node in a new view with all the existing ones; not started.
    pub fn add_started_node(&self) -> usize {
        let idx = self.nodes.add_node();
        let members: Vec<usize> = (0..=idx).collect();
        self.nodes.install_view(&members, false);
        idx
    }

    /// `TestingUtil.killCacheManagers(manager)` followed by `blockUntilViewsReceived`: node `idx` stops
    /// abruptly and the view without it is installed on the nodes that were its view-mates.
    pub fn kill_node(&self, idx: usize) {
        self.stop_node_abruptly(idx);
        self.install_view_without(idx);
    }

    /// The first half of [`ReplCluster::kill_node`]: the node answers nothing from now on (requests in
    /// flight to it fail as suspect) but the survivors have not been given a new view yet.
    pub fn stop_node_abruptly(&self, idx: usize) {
        self.nodes.kill_node(idx);
    }

    /// The second half: the survivors' view loses `idx`.
    pub fn install_view_without(&self, idx: usize) {
        let (_, members) = self.nodes.view_of(idx);
        let survivors: Vec<usize> = members.iter().map(|a| self.index_of(a)).filter(|i| *i != idx).collect();
        self.nodes.install_view(&survivors, false);
    }

    /// `TestingUtil.replaceComponent(manager(idx), TimeService.class, ts, true)`: every cache of the node reads
    /// its time from `ts` from now on.
    pub fn set_time_service(&self, idx: usize, ts: Arc<dyn super::time_service::TimeService>) {
        let me = self.nodes.address(idx);
        for ((_, addr), engine) in self.net.engines.lock().unwrap().iter() {
            if *addr == me {
                engine.set_time_service(ts.clone());
            }
        }
    }

    /// The cache `name` of node `idx`.
    pub fn cache(&self, idx: usize, name: &str) -> Cache {
        self.caches.lock().unwrap().get(&(idx, name.to_string())).cloned().expect("cache started on node")
    }

    /// The address of node `idx`.
    pub fn address(&self, idx: usize) -> Address {
        self.nodes.address(idx)
    }

    /// Node index of an address.
    pub fn index_of(&self, a: &Address) -> usize {
        (0..self.nodes.len()).find(|i| self.nodes.address(*i) == *a).expect("known node")
    }
}

impl Default for ReplCluster {
    fn default() -> Self {
        Self::new()
    }
}
