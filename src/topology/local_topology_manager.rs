//! Port of `LocalTopologyManagerImpl` (core 14.0.35.Final, `topology/LocalTopologyManagerImpl.java`),
//! `LocalCacheStatus`, and the `CacheTopologyHandler`/`PartitionHandlingManager` seams it drives: the
//! per-node side of the topology protocol. A cache joins through [`LocalTopologyManager::join`],
//! receives topology updates and rebalance starts from the coordinator, confirms every rebalance
//! phase, and answers the new coordinator's status request.
//!
//! Out of scope: persisted global state (`GlobalStateManager`, `restoreCacheTopology`, the
//! `stable` completion future and `waitingRecovery`: all only matter when a cache has persisted
//! state), JMX attributes and cache shutdown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::cache_topology::{CacheTopology, ConsistentHash, Phase};
use super::cluster_cache_status::*;
use super::commands::TopologyCommand;
use super::messages::{AvailabilityMode, CacheJoinInfo, CacheStatusResponse, ManagerStatusResponse, ResponseValue};
use super::support::{ActionSequencer, BoxFuture, eager_spawn};
use super::transport::{LocalInvoker, TopologyTransport};
use crate::ch::address::Address;

/// `CacheTopologyHandler`: what a cache's state transfer does with an installed topology.
pub trait CacheTopologyHandler: Send + Sync {
    /// A topology without a rebalance (`updateConsistentHash`).
    fn update_consistent_hash(&self, topology: CacheTopology) -> BoxFuture<'static, ()>;
    /// A topology that starts or advances a rebalance (`rebalance`).
    fn rebalance(&self, topology: CacheTopology) -> BoxFuture<'static, ()>;
}

/// The slice of `PartitionHandlingManager` the local topology manager uses.
pub trait PartitionHandling: Send + Sync {
    /// The cache's availability mode.
    fn availability_mode(&self) -> AvailabilityMode;
    /// Sets the cache's availability mode.
    fn set_availability_mode(&self, mode: AvailabilityMode) -> BoxFuture<'static, ()>;
    /// Called with every topology the cache installs.
    fn on_topology_update(&self, current: Option<&CacheTopology>);
}

/// `PartitionHandlingManager` for caches with `whenSplit=ALLOW_READ_WRITES` (the default): there is
/// no `PartitionHandlingInterceptor`, so the mode is only recorded.
#[derive(Default)]
pub struct AllowReadWritesPartitionHandling {
    mode: Mutex<Option<AvailabilityMode>>,
}

impl AllowReadWritesPartitionHandling {
    /// A handler with no mode set, so the cache is available.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl PartitionHandling for AllowReadWritesPartitionHandling {
    fn availability_mode(&self) -> AvailabilityMode {
        self.mode.lock().unwrap().unwrap_or(AvailabilityMode::Available)
    }

    fn set_availability_mode(&self, mode: AvailabilityMode) -> BoxFuture<'static, ()> {
        *self.mode.lock().unwrap() = Some(mode);
        Box::pin(async {})
    }

    fn on_topology_update(&self, _current: Option<&CacheTopology>) {}
}

/// Test seam equivalent to the Mockito spies upstream's tests put on `LocalTopologyManager`: awaited
/// at the top of the corresponding `handle*` method.
pub trait LocalTopologyInterceptor: Send + Sync {
    /// Awaited before a status request is handled.
    fn before_handle_status_request(&self, _view_id: i32) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// Awaited before a topology update is handled.
    fn before_handle_topology_update(&self, _cache_name: &str, _topology: &CacheTopology, _view_id: i32) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// Awaited before a rebalance is handled.
    fn before_handle_rebalance(&self, _cache_name: &str, _topology: &CacheTopology, _view_id: i32) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// `LocalCacheStatus`.
struct LocalCacheStatus {
    join_info: CacheJoinInfo,
    handler: Arc<dyn CacheTopologyHandler>,
    phm: Arc<dyn PartitionHandling>,
    known_members: Vec<Address>,
    current_topology: Option<CacheTopology>,
    stable_topology: Option<CacheTopology>,
}

#[derive(Default)]
struct RunningCaches {
    caches: HashMap<String, Arc<Mutex<LocalCacheStatus>>>,
    latest_status_response_view_id: i32,
}

pub(crate) struct Inner {
    transport: Arc<dyn TopologyTransport>,
    invoker: OnceLock<Arc<dyn LocalInvoker>>,
    persistent_uuids: Arc<PersistentUuidManager>,
    persistent_uuid: Address,
    global_timeout: Duration,
    sequencer: ActionSequencer,
    running: AtomicBool,
    running_caches: Mutex<RunningCaches>,
    interceptor: Mutex<Option<Arc<dyn LocalTopologyInterceptor>>>,
    rebalancing_enabled_source: OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>,
}

/// The per-node topology manager.
#[derive(Clone)]
pub struct LocalTopologyManager {
    pub(crate) inner: Arc<Inner>,
}

/// A non-owning handle, for objects the manager itself keeps (cache topology handlers).
#[derive(Clone)]
pub struct WeakLocalTopologyManager(std::sync::Weak<Inner>);

impl WeakLocalTopologyManager {
    /// The manager, if it still exists.
    pub fn upgrade(&self) -> Option<LocalTopologyManager> {
        self.0.upgrade().map(|inner| LocalTopologyManager { inner })
    }
}

impl LocalTopologyManager {
    /// A handle that does not keep the manager alive.
    pub fn downgrade(&self) -> WeakLocalTopologyManager {
        WeakLocalTopologyManager(Arc::downgrade(&self.inner))
    }
}

/// `PersistentUUID.randomUUID()`: two unpredictable longs (the OS-seeded `RandomState`, as
/// `jgroups-rs` does for `UUID.randomUUID()`).
fn random_persistent_uuid() -> Address {
    use std::hash::{BuildHasher, Hash, Hasher};
    fn rand_i64(salt: u8) -> i64 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        std::time::SystemTime::now().hash(&mut h);
        salt.hash(&mut h);
        h.finish() as i64
    }
    Address::Persistent { msb: rand_i64(1), lsb: rand_i64(2) }
}

impl LocalTopologyManager {
    /// A manager sending commands over `transport`, with a new persistent UUID.
    pub fn new(transport: Arc<dyn TopologyTransport>, persistent_uuids: Arc<PersistentUuidManager>, global_timeout: Duration) -> Self {
        let persistent_uuid = random_persistent_uuid();
        Self {
            inner: Arc::new(Inner {
                transport,
                invoker: OnceLock::new(),
                persistent_uuids,
                persistent_uuid,
                global_timeout,
                sequencer: ActionSequencer::new(),
                running: AtomicBool::new(false),
                running_caches: Mutex::new(RunningCaches::default()),
                interceptor: Mutex::new(None),
                rebalancing_enabled_source: OnceLock::new(),
            }),
        }
    }

    pub(crate) fn set_invoker(&self, invoker: Arc<dyn LocalInvoker>) {
        let _ = self.inner.invoker.set(invoker);
    }

    pub(crate) fn set_rebalancing_enabled_source(&self, f: Arc<dyn Fn() -> bool + Send + Sync>) {
        let _ = self.inner.rebalancing_enabled_source.set(f);
    }

    /// `start()` (`:51-70`).
    pub fn start(&self) {
        let i = &self.inner;
        i.persistent_uuids.add_persistent_address_mapping(&i.transport.address(), &i.persistent_uuid);
        i.running_caches.lock().unwrap().latest_status_response_view_id = i.transport.view_id();
        i.running.store(true, Ordering::SeqCst);
    }

    /// `stop()` (`:72-79`).
    pub fn stop(&self) {
        self.inner.running.store(false, Ordering::SeqCst);
    }

    /// Whether the manager is running.
    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::SeqCst)
    }

    /// The node's persistent UUID.
    pub fn persistent_uuid(&self) -> Address {
        self.inner.persistent_uuid.clone()
    }

    /// Sets the test interceptor, or removes it with `None`.
    pub fn set_interceptor(&self, interceptor: Option<Arc<dyn LocalTopologyInterceptor>>) {
        *self.inner.interceptor.lock().unwrap() = interceptor;
    }

    fn interceptor(&self) -> Option<Arc<dyn LocalTopologyInterceptor>> {
        self.inner.interceptor.lock().unwrap().clone()
    }

    /// Test seam: put a cache in the state `join` would have left it in. This is what upstream's
    /// `MockLocalTopologyManager.init(joinInfo, topology, stableTopology, availabilityMode)` does for the
    /// tests that start a manager "in the middle" of a cache's life.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn preload_cache_status(
        &self,
        cache_name: &str,
        join_info: CacheJoinInfo,
        handler: Arc<dyn CacheTopologyHandler>,
        phm: Arc<dyn PartitionHandling>,
        current_topology: Option<CacheTopology>,
        stable_topology: Option<CacheTopology>,
        known_members: Vec<Address>,
    ) {
        let status = LocalCacheStatus { join_info, handler, phm, known_members, current_topology, stable_topology };
        self.inner.running_caches.lock().unwrap().caches.insert(cache_name.to_string(), Arc::new(Mutex::new(status)));
    }

    /// `getCacheTopology` (`:578-582`).
    pub fn cache_topology(&self, cache_name: &str) -> Option<CacheTopology> {
        let lcs = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned()?;
        lcs.lock().unwrap().current_topology.clone()
    }

    /// Every topology command received so far has been handled (test support: the commands of one
    /// rebalance end, the topology update and the stable topology update, are separate messages).
    pub fn is_idle(&self) -> bool {
        self.inner.sequencer.is_idle()
    }

    /// `getStableCacheTopology` (`:583-587`).
    pub fn stable_cache_topology(&self, cache_name: &str) -> Option<CacheTopology> {
        let lcs = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned()?;
        lcs.lock().unwrap().stable_topology.clone()
    }

    /// `getCacheAvailability` (`:669-673`).
    pub fn cache_availability(&self, cache_name: &str) -> Option<AvailabilityMode> {
        let lcs = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned()?;
        let m = lcs.lock().unwrap().phm.availability_mode();
        Some(m)
    }

    /// The names of the caches running on the node.
    pub fn running_cache_names(&self) -> Vec<String> {
        self.inner.running_caches.lock().unwrap().caches.keys().cloned().collect()
    }

    async fn with_view(&self, view_id: i32, timeout: Duration) -> Result<()> {
        let fut = self.inner.transport.with_view(view_id);
        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            TopologyError(format!(
                "ISPN000333: Timed out waiting for view {view_id}, current view is {}",
                self.inner.transport.view_id()
            ))
        })
    }

    fn local(&self) -> Arc<dyn LocalInvoker> {
        self.inner.invoker.get().expect("LocalTopologyManager is not wired to a node").clone()
    }

    /// `TopologyManagementHelper.executeOnCoordinator`.
    async fn execute_on_coordinator(&self, command: TopologyCommand, timeout: Duration) -> Result<Option<ResponseValue>> {
        let i = &self.inner;
        let coordinator = i.transport.coordinator();
        if i.transport.address() == coordinator {
            return self.local().invoke(i.transport.address(), command).await;
        }
        match i.transport.invoke_command(&coordinator, command, timeout).await? {
            super::messages::Response::Successful(v) => Ok(v.map(|b| *b)),
            other => Err(TopologyError(format!("ISPN000204: Received unexpected response {other:?} from {coordinator:?}"))),
        }
    }

    /// `TopologyManagementHelper.executeOnCoordinatorAsync`.
    fn execute_on_coordinator_async(&self, command: TopologyCommand) {
        let i = &self.inner;
        if i.transport.is_coordinator() {
            let fut = self.local().invoke(i.transport.address(), command);
            eager_spawn(async move {
                let _ = fut.await;
            });
        } else {
            i.transport.send_to(&i.transport.coordinator(), command);
        }
    }

    // ------------------------------------------------------------------ join

    /// `join` (`:81-142`), without the persisted-state recovery branch: the topology the new cache
    /// starts from.
    pub async fn join(
        &self,
        cache_name: &str,
        join_info: CacheJoinInfo,
        handler: Arc<dyn CacheTopologyHandler>,
        phm: Arc<dyn PartitionHandling>,
    ) -> Result<Option<CacheTopology>> {
        // Use the action sequencer for the initial join request. This ensures that all topology updates
        // from the coordinator will be delayed until the join is done.
        let this = self.clone();
        let name = cache_name.to_string();
        self.inner
            .sequencer
            .order_on_key(cache_name, async move {
                log::debug!("Node {:?} joining cache {name}", this.inner.transport.address());
                let cache_status = Arc::new(Mutex::new(LocalCacheStatus {
                    join_info: join_info.clone(),
                    handler,
                    phm,
                    known_members: Vec::new(),
                    current_topology: None,
                    stable_topology: None,
                }));
                {
                    let mut rc = this.inner.running_caches.lock().unwrap();
                    if rc.caches.contains_key(&name) {
                        return Err(TopologyError("java.lang.IllegalStateException: A cache can only join once".into()));
                    }
                    rc.caches.insert(name.clone(), cache_status.clone());
                }
                let timeout = Duration::from_millis(join_info.timeout.max(0) as u64);
                let join_response = this.send_join_request(&name, &join_info, timeout).await?;
                this.handle_join_response(&name, &cache_status, join_response).await
            })
            .await
    }

    /// `sendJoinRequest` (`:143-181`).
    async fn send_join_request(&self, cache_name: &str, join_info: &CacheJoinInfo, timeout: Duration) -> Result<CacheStatusResponse> {
        let i = &self.inner;
        loop {
            let view_id = i.transport.view_id();
            let command = TopologyCommand::CacheJoin { cache_name: cache_name.to_string(), join_info: join_info.clone(), view_id };
            let result = self.execute_on_coordinator(command, timeout).await;
            let current_view_id = i.transport.view_id();
            if view_id != current_view_id {
                log::trace!("Received new view {current_view_id} before join response for cache {cache_name}, retrying");
                continue;
            }
            match result {
                Ok(Some(ResponseValue::CacheStatus(r))) => return Ok(r),
                Ok(None) => {
                    log::debug!("Coordinator sent a null join response, retrying in view {}", view_id + 1);
                    self.with_view(view_id + 1, timeout).await?;
                    continue;
                }
                Ok(Some(other)) => return Err(TopologyError(format!("unexpected join response {other:?}"))),
                Err(e) if e.0.starts_with("SuspectException") => {
                    // Either the coordinator is shutting down
                    // Or the JGroups stack includes FORK and the coordinator hasn't connected its ForkChannel yet.
                    log::debug!("Join request received CacheNotFoundResponse for cache {cache_name}, retrying");
                }
                Err(e) => {
                    log::debug!("Join request failed for cache {cache_name}: {e}");
                    return Err(e);
                }
            }
            // Can't use a value based on the state transfer timeout because cache org.infinispan.CONFIG
            // uses the default timeout, which is too long for tests (4 minutes)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `handleJoinResponse` (`:187-208`).
    async fn handle_join_response(
        &self,
        cache_name: &str,
        cache_status: &Arc<Mutex<LocalCacheStatus>>,
        initial_status: CacheStatusResponse,
    ) -> Result<Option<CacheTopology>> {
        log::debug!("Cache {cache_name} received join response {initial_status:?}");
        if initial_status.is_empty() {
            return Err(TopologyError("empty join response (persisted-state recovery) is not supported".into()));
        }
        let ct = self.get_join_topology(&initial_status);
        let view_id = self.inner.transport.view_id();
        let applied = self
            .do_handle_topology_update(cache_name, ct.clone(), initial_status.availability_mode, view_id, &self.inner.transport.coordinator(), cache_status)
            .await?;
        if !applied {
            return Err(TopologyError(
                "java.lang.IllegalStateException: We already had a newer topology by the time we received the join response".into(),
            ));
        }
        cache_status.lock().unwrap().known_members = initial_status.current.clone().unwrap_or_default();
        self.do_handle_stable_topology_update(
            cache_name,
            initial_status.stable_topology.clone(),
            view_id,
            &self.inner.transport.coordinator(),
            cache_status,
        );
        Ok(ct)
    }

    /// `getJoinTopology` (`:209-222`).
    fn get_join_topology(&self, response: &CacheStatusResponse) -> Option<CacheTopology> {
        // First, try to utilize the coordinator topology *before* the join.
        let ct = response.cache_topology.clone()?;
        // If the join receives a topology already in rebalance AND it is counted towards the current
        // members, utilize the stable topology (the node is joining, it has no state to donate).
        if ct.phase != Phase::NoRebalance && ct.actual_members.contains(&self.inner.transport.address()) {
            return response.stable_topology.clone();
        }
        Some(ct)
    }

    /// `leave` (`:230-240`).
    pub async fn leave(&self, cache_name: &str, timeout: Duration) {
        let i = &self.inner;
        log::debug!("Node {:?} leaving cache {cache_name}", i.transport.address());
        i.running_caches.lock().unwrap().caches.remove(cache_name);
        let command = TopologyCommand::CacheLeave {
            cache_name: cache_name.to_string(),
            view_id: i.transport.view_id(),
        };
        if let Err(e) = self.execute_on_coordinator(command, timeout).await {
            log::debug!("Error sending the leave request for cache {cache_name} to coordinator: {e}");
        }
    }

    /// `confirmRebalancePhase` (`:241-252`).
    pub fn confirm_rebalance_phase(&self, cache_name: &str, topology_id: i32, _rebalance_id: i32) {
        // Note that if the coordinator changes again after we sent the command, we will get another
        // query for the status of our running caches. So we don't need to retry if the command failed.
        let i = &self.inner;
        self.execute_on_coordinator_async(TopologyCommand::RebalancePhaseConfirm {
            cache_name: cache_name.to_string(),
            topology_id,
            view_id: i.transport.view_id(),
        });
    }

    // ----------------------------------------------------- coordinator requests

    /// `handleStatusRequest` (`:254-293`): called by the coordinator.
    pub async fn handle_status_request(&self, view_id: i32) -> Result<ManagerStatusResponse> {
        if let Some(i) = self.interceptor() {
            i.before_handle_status_request(view_id).await;
        }
        // As long as we have an older view, we can still process topologies from the old coordinator
        self.with_view(view_id, self.inner.global_timeout).await?;
        let mut caches: Vec<(String, CacheStatusResponse)> = Vec::new();
        {
            let mut rc = self.inner.running_caches.lock().unwrap();
            rc.latest_status_response_view_id = view_id;
            for (name, status) in rc.caches.iter() {
                let s = status.lock().unwrap();
                // Ignore caches that haven't finished joining yet.
                let Some(current) = &s.current_topology else { continue };
                caches.push((
                    name.clone(),
                    CacheStatusResponse {
                        cache_join_info: Some(s.join_info.clone()),
                        cache_topology: Some(current.clone()),
                        stable_topology: s.stable_topology.clone(),
                        availability_mode: Some(s.phm.availability_mode()),
                        current: Some(s.known_members.clone()),
                    },
                ));
            }
        }
        log::debug!("Sending cluster status response for view {view_id}");
        let rebalancing_enabled = self.inner.rebalancing_enabled_source.get().map(|f| f()).unwrap_or(true);
        Ok(ManagerStatusResponse::with_hash_map_order(caches, rebalancing_enabled))
    }

    /// `handleTopologyUpdate` (`:294-328`).
    pub async fn handle_topology_update(
        &self,
        cache_name: &str,
        cache_topology: CacheTopology,
        availability_mode: Option<AvailabilityMode>,
        view_id: i32,
        sender: &Address,
    ) -> Result<()> {
        if !self.is_running() {
            log::trace!("Ignoring consistent hash update {} for cache {cache_name}, the local cache manager is not running", cache_topology.topology_id);
            return Ok(());
        }
        let Some(cache_status) = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned() else {
            log::trace!("Ignoring consistent hash update {} for cache {cache_name} that doesn't exist locally", cache_topology.topology_id);
            return Ok(());
        };
        if let Some(i) = self.interceptor() {
            i.before_handle_topology_update(cache_name, &cache_topology, view_id).await;
        }
        let timeout = Duration::from_millis(cache_status.lock().unwrap().join_info.timeout.max(0) as u64);
        let result: Result<()> = async {
            self.with_view(view_id, timeout).await?;
            let this = self.clone();
            let name = cache_name.to_string();
            let sender = sender.clone();
            let cs = cache_status.clone();
            self.inner
                .sequencer
                .order_on_key(cache_name, async move {
                    this.do_handle_topology_update(&name, Some(cache_topology), availability_mode, view_id, &sender, &cs).await.map(|_| ())
                })
                .await
        }
        .await;
        if let Err(e) = result {
            log::error!("Failed to handle a topology update for cache {cache_name}: {e}");
        }
        Ok(())
    }

    /// `doHandleTopologyUpdate` (`:334-407`): `true` if the topology was applied.
    async fn do_handle_topology_update(
        &self,
        cache_name: &str,
        cache_topology: Option<CacheTopology>,
        availability_mode: Option<AvailabilityMode>,
        view_id: i32,
        sender: &Address,
        cache_status: &Arc<Mutex<LocalCacheStatus>>,
    ) -> Result<bool> {
        let existing_topology;
        let Some(cache_topology) = cache_topology else {
            // No topology yet: happens when a cache is being restarted from state.
            // Still, return true because we don't want to re-send the join request.
            return Ok(true);
        };
        {
            // Register all persistent UUIDs locally
            self.register_persistent_uuid(&cache_topology)?;
            let s = cache_status.lock().unwrap();
            existing_topology = s.current_topology.clone();
            if let Some(e) = &existing_topology
                && cache_topology.topology_id <= e.topology_id
            {
                log::debug!(
                    "Ignoring late consistent hash update for cache {cache_name}, current topology is {} received {}",
                    e.topology_id, cache_topology.topology_id
                );
                return Ok(false);
            }
            drop(s);
            if !self.update_cache_topology(cache_name, &cache_topology, view_id, sender, cache_status) {
                return Ok(false);
            }
        }
        let (handler, kind, phm) = {
            let s = cache_status.lock().unwrap();
            (s.handler.clone(), s.join_info.consistent_hash_factory, s.phm.clone())
        };
        let current_ch = cache_topology.current_ch.clone();
        let pending_ch = cache_topology.pending_ch.clone();
        let union_ch: Option<ConsistentHash> = match (&pending_ch, &current_ch) {
            (Some(p), Some(c)) => Some(match cache_topology.phase {
                // When removing members from topology, we have to make sure that the unionCH has
                // owners from pendingCH (which is used as the readCH in this phase) before
                // owners from currentCH, as primary owners must match in readCH and writeCH.
                Phase::ReadNewWriteAll => kind.union(p, c)?,
                _ => kind.union(c, p)?,
            }),
            _ => None,
        };
        let persistent_uuids = self.inner.persistent_uuids.map_addresses(&cache_topology.actual_members)?;
        let union_topology = CacheTopology::new(
            cache_topology.topology_id,
            cache_topology.rebalance_id,
            cache_topology.restored_from_state,
            current_ch,
            pending_ch,
            union_ch.clone(),
            cache_topology.phase,
            cache_topology.actual_members.clone(),
            Some(persistent_uuids),
        )?;
        let update_availability_mode_first = availability_mode != Some(AvailabilityMode::Available);
        self.reset_local_topology_before_rebalance(cache_name, &cache_topology, existing_topology.as_ref(), &handler).await?;
        if update_availability_mode_first && let Some(m) = availability_mode {
            phm.set_availability_mode(m).await;
        }
        let start_conflict_resolution = cache_topology.phase == Phase::ConflictResolution;
        if !start_conflict_resolution
            && union_ch.is_some()
            && existing_topology.as_ref().is_none_or(|e| e.rebalance_id != cache_topology.rebalance_id)
        {
            // This CH_UPDATE command was sent after a REBALANCE_START command, but arrived first.
            // We will start the rebalance now and ignore the REBALANCE_START command when it arrives.
            log::trace!("This topology update has a pending CH, starting the rebalance now");
            handler.rebalance(union_topology).await;
        } else {
            handler.update_consistent_hash(union_topology).await;
        }
        if !update_availability_mode_first && let Some(m) = availability_mode {
            phm.set_availability_mode(m).await;
        }
        Ok(true)
    }

    /// `registerPersistentUUID` (`:408-416`).
    fn register_persistent_uuid(&self, t: &CacheTopology) -> Result<()> {
        let uuids = t.persistent_uuids.as_ref().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
        for (a, u) in t.actual_members.iter().zip(uuids) {
            self.inner.persistent_uuids.add_persistent_address_mapping(a, u);
        }
        Ok(())
    }

    /// `updateCacheTopology` (`:417-426`) and `validateCommandViewId` (`:431-445`).
    fn update_cache_topology(&self, cache_name: &str, t: &CacheTopology, view_id: i32, sender: &Address, cs: &Arc<Mutex<LocalCacheStatus>>) -> bool {
        let rc = self.inner.running_caches.lock().unwrap();
        if !self.validate_command_view_id(&rc, t, view_id, sender, cache_name) {
            return false;
        }
        log::debug!("Updating local topology for cache {cache_name}: {}", t.topology_id);
        cs.lock().unwrap().current_topology = Some(t.clone());
        true
    }

    fn validate_command_view_id(&self, rc: &RunningCaches, t: &CacheTopology, view_id: i32, sender: &Address, cache_name: &str) -> bool {
        if *sender != self.inner.transport.coordinator() {
            log::debug!("Ignoring topology {} for cache {cache_name} from old coordinator {sender:?}", t.topology_id);
            return false;
        }
        if view_id < rc.latest_status_response_view_id {
            log::debug!(
                "Ignoring topology {} for cache {cache_name} from view {view_id} received after status request from view {}",
                t.topology_id, rc.latest_status_response_view_id
            );
            return false;
        }
        true
    }

    /// `resetLocalTopologyBeforeRebalance` (`:446-479`).
    async fn reset_local_topology_before_rebalance(
        &self,
        cache_name: &str,
        new_topology: &CacheTopology,
        old_topology: Option<&CacheTopology>,
        handler: &Arc<dyn CacheTopologyHandler>,
    ) -> Result<()> {
        // Cannot rely on the pending CH, because it is also used for conflict resolution
        let new_rebalance = new_topology.phase != Phase::NoRebalance && new_topology.phase != Phase::ConflictResolution;
        if !new_rebalance {
            return Ok(());
        }
        // The initial topology doesn't need a reset because we are guaranteed not to be a member
        let Some(old) = old_topology else { return Ok(()) };
        // We only need a reset if we missed a topology update
        if new_topology.topology_id <= old.topology_id + 1 {
            return Ok(());
        }
        // We have missed a topology update, and that topology might have removed some of our segments.
        // If this rebalance adds those same segments, we need to remove the old data/inbound transfers first.
        if new_topology.rebalance_id != old.rebalance_id {
            // The currentCH changed, we need to install a "reset" topology with the new currentCH first
            self.register_persistent_uuid(new_topology)?;
            let reset = CacheTopology::new(
                new_topology.topology_id - 1,
                new_topology.rebalance_id - 1,
                new_topology.restored_from_state,
                new_topology.current_ch.clone(),
                None,
                None,
                Phase::NoRebalance,
                new_topology.actual_members.clone(),
                Some(self.inner.persistent_uuids.map_addresses(&new_topology.actual_members)?),
            )?;
            log::debug!("Installing fake cache topology {} for cache {cache_name}", reset.topology_id);
            handler.update_consistent_hash(reset).await;
        }
        Ok(())
    }

    /// `handleStableTopologyUpdate` (`:480-489`).
    pub async fn handle_stable_topology_update(
        &self,
        cache_name: &str,
        new_stable_topology: CacheTopology,
        sender: &Address,
        view_id: i32,
    ) -> Result<()> {
        let Some(cs) = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned() else {
            return Ok(());
        };
        let this = self.clone();
        let name = cache_name.to_string();
        let sender = sender.clone();
        self.inner
            .sequencer
            .order_on_key(cache_name, async move {
                this.do_handle_stable_topology_update(&name, Some(new_stable_topology), view_id, &sender, &cs);
            })
            .await;
        Ok(())
    }

    /// `doHandleStableTopologyUpdate` (`:490-507`).
    fn do_handle_stable_topology_update(
        &self,
        cache_name: &str,
        new_stable_topology: Option<CacheTopology>,
        view_id: i32,
        sender: &Address,
        cs: &Arc<Mutex<LocalCacheStatus>>,
    ) {
        let rc = self.inner.running_caches.lock().unwrap();
        // validateCommandViewId needs a topology only for its log line
        let log_topology = new_stable_topology.clone().unwrap_or_else(|| {
            CacheTopology::new(0, 0, false, None, None, None, Phase::NoRebalance, vec![], None).unwrap()
        });
        if !self.validate_command_view_id(&rc, &log_topology, view_id, sender, cache_name) {
            return;
        }
        let mut s = cs.lock().unwrap();
        let replace = match (&s.stable_topology, &new_stable_topology) {
            (None, _) => true,
            (Some(old), Some(new)) => old.topology_id < new.topology_id,
            (Some(_), None) => false,
        };
        if replace {
            log::trace!("Updating stable topology for cache {cache_name}: {new_stable_topology:?}");
            s.stable_topology = new_stable_topology;
            let current = s.current_topology.clone();
            s.phm.on_topology_update(current.as_ref());
        }
    }

    /// `handleRebalance` (`:508-577`).
    pub async fn handle_rebalance(&self, cache_name: &str, cache_topology: CacheTopology, view_id: i32, sender: &Address) -> Result<()> {
        if !self.is_running() {
            log::debug!("Ignoring rebalance request {} for cache {cache_name}, the local cache manager is not running", cache_topology.topology_id);
            return Ok(());
        }
        let Some(cache_status) = self.inner.running_caches.lock().unwrap().caches.get(cache_name).cloned() else {
            log::trace!("Ignoring rebalance {} for cache {cache_name} that doesn't exist locally", cache_topology.topology_id);
            return Ok(());
        };
        if let Some(i) = self.interceptor() {
            i.before_handle_rebalance(cache_name, &cache_topology, view_id).await;
        }
        let timeout = Duration::from_millis(cache_status.lock().unwrap().join_info.timeout.max(0) as u64);
        let result: Result<()> = async {
            self.with_view(view_id, timeout).await?;
            let this = self.clone();
            let name = cache_name.to_string();
            let sender = sender.clone();
            let cs = cache_status.clone();
            let t = cache_topology.clone();
            self.inner
                .sequencer
                .order_on_key(cache_name, async move { this.do_handle_rebalance(view_id, &cs, t, &name, &sender).await })
                .await
        }
        .await;
        if let Err(e) = result {
            log::error!("Failed to handle a rebalance for cache {cache_name}: {e}");
        }
        Ok(())
    }

    /// `doHandleRebalance` (`:546-577`).
    async fn do_handle_rebalance(
        &self,
        view_id: i32,
        cache_status: &Arc<Mutex<LocalCacheStatus>>,
        cache_topology: CacheTopology,
        cache_name: &str,
        sender: &Address,
    ) -> Result<()> {
        let existing_topology = {
            let existing = cache_status.lock().unwrap().current_topology.clone();
            if let Some(e) = &existing
                && cache_topology.topology_id <= e.topology_id
            {
                // Start rebalance commands are sent asynchronously to the entire cluster
                // So it's possible to receive an old one on a joiner after the joiner has already become a member.
                log::debug!("Ignoring old rebalance for cache {cache_name}, current topology is {}", e.topology_id);
                return Ok(());
            }
            existing
        };
        if !self.update_cache_topology(cache_name, &cache_topology, view_id, sender, cache_status) {
            return Ok(());
        }
        let (handler, kind) = {
            let s = cache_status.lock().unwrap();
            (s.handler.clone(), s.join_info.consistent_hash_factory)
        };
        let (Some(c), Some(p)) = (&cache_topology.current_ch, &cache_topology.pending_ch) else {
            return Err(TopologyError("java.lang.NullPointerException".into()));
        };
        let union_ch = kind.union(c, p)?;
        let new_topology = CacheTopology::new(
            cache_topology.topology_id,
            cache_topology.rebalance_id,
            cache_topology.restored_from_state,
            cache_topology.current_ch.clone(),
            cache_topology.pending_ch.clone(),
            Some(union_ch),
            cache_topology.phase,
            cache_topology.actual_members.clone(),
            cache_topology.persistent_uuids.clone(),
        )?;
        self.reset_local_topology_before_rebalance(cache_name, &cache_topology, existing_topology.as_ref(), &handler).await?;
        log::debug!("Starting local rebalance for cache {cache_name}, topology = {}", cache_topology.topology_id);
        handler.rebalance(new_topology).await;
        Ok(())
    }

    // ------------------------------------------------------- rebalancing control

    /// `isCacheRebalancingEnabled` / `getRebalancingStatus` (`:609-656`): ask the coordinator.
    pub async fn rebalancing_status(&self, cache_name: Option<&str>) -> Result<RebalancingStatus> {
        let i = &self.inner;
        let deadline = tokio::time::Instant::now() + i.global_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let cmd = TopologyCommand::RebalanceStatusRequest { cache_name: cache_name.map(String::from) };
            match self.execute_on_coordinator(cmd, remaining).await {
                Ok(Some(ResponseValue::RebalancingStatus(s))) => return Ok(s),
                Ok(other) => return Err(TopologyError(format!("unexpected rebalancing status answer {other:?}"))),
                Err(e) if e.0.starts_with("SuspectException") => {
                    // Coordinator left the cluster while querying rebalancing status, retrying
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
