//! Port of `ClusterTopologyManagerImpl` (core 14.0.35.Final, `topology/ClusterTopologyManagerImpl.java`)
//! and `TopologyManagementHelper`: the coordinator-side manager. It tracks the cluster view, keeps one
//! [`ClusterCacheStatus`] per cache, answers joins/leaves/phase confirmations, recovers the cluster
//! status when this node becomes coordinator, and broadcasts topology commands.
//!
//! Out of scope (see `cluster_cache_status.rs`): global state persistence, `PreferConsistencyStrategy`,
//! cache shutdown (`CacheShutdownCommand`) and the JMX/event-log surface.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::cache_topology::CacheTopology;
use super::cluster_cache_status::*;
use super::commands::{TopologyCommand, TopologyFields};
use super::messages::{AvailabilityMode, CacheJoinInfo, CacheMode, CacheStatusResponse, Response, ResponseValue};
use super::support::{ActionSequencer, ConditionFuture, eager_spawn};
use super::transport::{LocalInvoker, RemoteError, TopologyTransport};
use crate::ch::address::Address;
use crate::ch::java_util::{HashMapInit, hash_map_iteration_order};

/// How many times the coordinator asks the members for their status when it starts.
pub const INITIAL_CONNECTION_ATTEMPTS: i32 = 10;
/// How many times cluster recovery is attempted after a view change.
pub const CLUSTER_RECOVERY_ATTEMPTS: i32 = 10;

/// `ClusterTopologyManager.ClusterManagerStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterManagerStatus {
    /// Starting.
    Initializing,
    /// A member that is not the coordinator.
    RegularMember,
    /// The coordinator.
    Coordinator,
    /// The coordinator, recovering the cluster's caches after a view change.
    RecoveringCluster,
    /// Stopping.
    Stopping,
}

impl ClusterManagerStatus {
    /// Whether the manager is not stopping.
    pub fn is_running(self) -> bool {
        self != ClusterManagerStatus::Stopping
    }

    /// Whether the node acts as the coordinator.
    pub fn is_coordinator(self) -> bool {
        matches!(self, ClusterManagerStatus::Coordinator | ClusterManagerStatus::RecoveringCluster)
    }
}

/// Settings the manager reads from `GlobalConfiguration`.
#[derive(Debug, Clone)]
pub struct TopologyConfig {
    /// `transport().distributedSyncTimeout()` (default 4 minutes).
    pub global_timeout: Duration,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self { global_timeout: Duration::from_secs(240) }
    }
}

struct CacheStatusEntry {
    status: Mutex<ClusterCacheStatus>,
    /// `ClusterCacheStatus.hasInitialTopologyFuture`
    has_initial_topology: ConditionFuture,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobalRebalanceStatus {
    NotRecovered,
    Enabled,
    Disabled,
}

struct ViewState {
    view_id: i32,
    status: ClusterManagerStatus,
}

pub(crate) struct Inner {
    transport: Arc<dyn TopologyTransport>,
    invoker: OnceLock<Arc<dyn LocalInvoker>>,
    config: TopologyConfig,
    persistent_uuids: Arc<PersistentUuidManager>,
    view: Mutex<ViewState>,
    cache_status_map: Mutex<HashMap<String, Arc<CacheStatusEntry>>>,
    global_rebalancing: Mutex<GlobalRebalanceStatus>,
    sequencer: ActionSequencer,
    join_view_future: ConditionFuture,
    recovery_attempts: AtomicI32,
    stopped: AtomicBool,
}

/// What `ClusterCacheStatus` sees of the transport (`getMembers`, `getViewId`).
struct TransportView(Arc<dyn TopologyTransport>);

impl ViewSource for TransportView {
    fn members(&self) -> Vec<Address> {
        self.0.members()
    }
    fn view_id(&self) -> i32 {
        self.0.view_id()
    }
}

/// The broadcast side of `ClusterTopologyManagerImpl`, handed to every `ClusterCacheStatus`.
struct Hooks(std::sync::Weak<Inner>);

impl ClusterTopologyHooks for Hooks {
    fn broadcast_rebalance_start(&self, cache_name: &str, topology: &CacheTopology) {
        if let Some(i) = self.0.upgrade() {
            let view_id = i.view_id();
            let cmd = TopologyCommand::RebalanceStart {
                cache_name: cache_name.to_string(),
                fields: TopologyFields::of(topology, view_id),
            };
            i.execute_on_cluster_async(cmd);
        }
    }

    fn broadcast_topology_update(&self, cache_name: &str, topology: &CacheTopology, availability_mode: Option<AvailabilityMode>) {
        if let Some(i) = self.0.upgrade() {
            let view_id = i.view_id();
            let cmd = TopologyCommand::TopologyUpdate {
                cache_name: cache_name.to_string(),
                fields: TopologyFields::of(topology, view_id),
                availability_mode,
            };
            i.execute_on_cluster_async(cmd);
        }
    }

    fn broadcast_stable_topology_update(&self, cache_name: &str, topology: &CacheTopology) {
        if let Some(i) = self.0.upgrade() {
            let view_id = i.view_id();
            let cmd = TopologyCommand::TopologyUpdateStable {
                cache_name: cache_name.to_string(),
                fields: TopologyFields::of(topology, view_id),
                topology_restored: topology.restored_from_state,
            };
            i.execute_on_cluster_async(cmd);
        }
    }

    fn remove_cache_status(&self, cache_name: &str) {
        if let Some(i) = self.0.upgrade() {
            i.cache_status_map.lock().unwrap().remove(cache_name);
        }
    }

    fn is_rebalancing_enabled(&self) -> bool {
        self.0.upgrade().is_none_or(|i| i.is_rebalancing_enabled())
    }
}

impl Inner {
    fn view_id(&self) -> i32 {
        self.view.lock().unwrap().view_id
    }

    fn status(&self) -> ClusterManagerStatus {
        self.view.lock().unwrap().status
    }

    fn is_rebalancing_enabled(&self) -> bool {
        *self.global_rebalancing.lock().unwrap() != GlobalRebalanceStatus::Disabled
    }

    fn local(&self) -> Arc<dyn LocalInvoker> {
        self.invoker.get().expect("ClusterTopologyManager is not wired to a node").clone()
    }

    /// `TopologyManagementHelper.executeOnClusterAsync`: send to the others, invoke on self.
    fn execute_on_cluster_async(&self, command: TopologyCommand) {
        self.transport.send_to_all(command.clone());
        // The command already logs any exception in invoke()
        let fut = self.local().invoke(self.transport.address(), command);
        eager_spawn(async move {
            let _ = fut.await;
        });
    }

    /// `executeOnClusterSync`: remote answers first, then the local node's.
    async fn execute_on_cluster_sync(
        &self,
        command: TopologyCommand,
        timeout: Duration,
    ) -> Vec<(Address, std::result::Result<Response, RemoteError>)> {
        let remote = self.transport.invoke_command_on_all(None, command.clone(), timeout);
        let local = self.local().invoke(self.transport.address(), command);
        let mut results = remote.await;
        let local_result = match local.await {
            Ok(v) => Ok(Response::Successful(v.map(Box::new))),
            Err(e) => Err(RemoteError::Other(e.0)),
        };
        results.push((self.transport.address(), local_result));
        results
    }

    /// `executeOnCoordinator`: the answer's value (a non-successful answer is an error).
    async fn execute_on_coordinator(&self, command: TopologyCommand, timeout: Duration) -> Result<Option<ResponseValue>> {
        let coordinator = self.transport.coordinator();
        let response = if self.transport.address() == coordinator {
            match self.local().invoke(self.transport.address(), command).await {
                Ok(v) => Response::Successful(v.map(Box::new)),
                Err(e) => return Err(e),
            }
        } else {
            self.transport.invoke_command(&coordinator, command, timeout).await?
        };
        match response {
            Response::Successful(v) => Ok(v.map(|b| *b)),
            other => Err(TopologyError(format!("ISPN000204: Received unexpected response {other:?} from {coordinator:?}"))),
        }
    }

    fn init_cache_status_if_absent(self: &Arc<Self>, cache_name: &str, cache_mode: CacheMode) -> Arc<CacheStatusEntry> {
        let mut map = self.cache_status_map.lock().unwrap();
        map.entry(cache_name.to_string())
            .or_insert_with(|| {
                // `whenSplit=ALLOW_READ_WRITES`, the default: PreferAvailabilityStrategy.
                let status = ClusterCacheStatus::new(
                    cache_name,
                    RebalanceType::from_cache_mode(cache_mode),
                    Arc::new(Hooks(Arc::downgrade(self))),
                    Arc::new(TransportView(self.transport.clone())),
                    self.persistent_uuids.clone(),
                );
                Arc::new(CacheStatusEntry { status: Mutex::new(status), has_initial_topology: ConditionFuture::new() })
            })
            .clone()
    }

    fn can_handle_join(&self, joiner_view_id: i32) -> bool {
        let v = self.view.lock().unwrap();
        joiner_view_id <= v.view_id
            && v.status != ClusterManagerStatus::RecoveringCluster
            && v.status != ClusterManagerStatus::Initializing
    }

    /// `updateClusterState`: `false` when the view needs no further handling.
    fn update_cluster_state(&self, merge_view: bool, new_view_id: i32) -> bool {
        let mut v = self.view.lock().unwrap();
        if new_view_id < self.transport.view_id() {
            log::trace!("Ignoring old cluster view notification: {new_view_id}");
            return false;
        }
        let is_coordinator = self.transport.is_coordinator();
        let became_coordinator = is_coordinator && !v.status.is_coordinator();
        log::trace!("Received new cluster view: {new_view_id}, isCoordinator = {is_coordinator}, old status = {:?}", v.status);
        if !is_coordinator {
            v.status = ClusterManagerStatus::RegularMember;
            return false;
        }
        if became_coordinator || merge_view {
            v.status = ClusterManagerStatus::RecoveringCluster;
        }
        // notify threads that might be waiting to join
        v.view_id = new_view_id;
        true
    }

    /// `handleClusterView`'s body, run on the manager's sequencer.
    async fn handle_cluster_view_action(self: Arc<Self>, merge_view: bool, new_view_id: i32) {
        if !self.update_cluster_state(merge_view, new_view_id) {
            return;
        }
        match self.status() {
            ClusterManagerStatus::RecoveringCluster => {
                if let Err(Some(msg)) = self.recover_cluster_status(new_view_id).await {
                    log::error!("Error handling view {new_view_id}: {msg}");
                }
            }
            ClusterManagerStatus::Coordinator => {
                // Unblock any joiners waiting for the view
                self.join_view_future.update();
                // If we have recovered the cluster status, we rebalance the caches to include minor partitions
                // If we processed a regular view, we prune members that left.
                self.update_cache_members(new_view_id).await;
            }
            _ => {}
        }
    }

    /// `recoverClusterStatus` (`:318-366`). `Err(None)` is upstream's silent `SKIP_RECOVERY_FUTURE`.
    async fn recover_cluster_status(self: &Arc<Self>, new_view_id: i32) -> std::result::Result<(), Option<String>> {
        // Clean up leftover cache status information from the last time we were coordinator.
        self.cache_status_map.lock().unwrap().clear();
        self.recovery_attempts.store(0, Ordering::SeqCst);
        let collected = self.fetch_cluster_status(new_view_id).await?;
        log::debug!("Cluster recovery found {} caches, members are {:?}", collected.responses_by_cache.len(), self.transport.members());
        for (cache_name, responses) in &collected.responses_by_cache {
            let Some(join_info) = responses.iter().find_map(|(_, r)| r.cache_join_info.clone()) else { continue };
            let entry = self.init_cache_status_if_absent(cache_name, join_info.cache_mode);
            let mut status = entry.status.lock().unwrap();
            if let Err(e) = status.do_merge_partitions(responses) {
                log::error!("Failed to recover cache {cache_name} state: {e}");
            }
        }
        {
            let mut v = self.view.lock().unwrap();
            if v.view_id != new_view_id {
                log::debug!("View updated while we were recovering the cluster for view {new_view_id}");
                return Ok(());
            }
            v.status = ClusterManagerStatus::Coordinator;
            *self.global_rebalancing.lock().unwrap() =
                if collected.rebalancing_enabled { GlobalRebalanceStatus::Enabled } else { GlobalRebalanceStatus::Disabled };
        }
        let entries: Vec<(String, Arc<CacheStatusEntry>)> =
            self.cache_status_map.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (name, entry) in entries {
            let inner = self.clone();
            // orderOnCache(cacheName, ...): not awaited, like upstream
            let fut = self.sequencer.order_on_key(&name, async move {
                if let Err(e) = entry.status.lock().unwrap().do_handle_cluster_view(new_view_id)
                    && inner.status().is_running()
                {
                    log::error!("Error updating members list for view {new_view_id}: {e}");
                }
            });
            tokio::spawn(fut);
        }
        // Unblock any joiners waiting for the view
        self.join_view_future.update();
        Ok(())
    }

    /// `fetchClusterStatus` (`:433-472`).
    async fn fetch_cluster_status(self: &Arc<Self>, new_view_id: i32) -> std::result::Result<CacheStatusCollector, Option<String>> {
        loop {
            let attempt = self.recovery_attempts.fetch_add(1, Ordering::SeqCst);
            log::debug!("Recovering cluster status for view {new_view_id}, attempt {attempt}");
            let timeout = self.config.global_timeout / CLUSTER_RECOVERY_ATTEMPTS as u32;
            let results = self.execute_on_cluster_sync(TopologyCommand::CacheStatusRequest { view_id: new_view_id }, timeout).await;
            if new_view_id < self.transport.view_id() {
                log::trace!("Ignoring cluster state responses for view {new_view_id}, we already have view {}", self.transport.view_id());
                return Err(None);
            }
            let mut collector = CacheStatusCollector::default();
            let mut failure: Option<RemoteError> = None;
            for (sender, result) in results {
                match result {
                    Ok(r) => collector.add_response(&sender, r),
                    Err(RemoteError::Suspect(_)) => collector.suspected_members.push(sender),
                    Err(e) => failure = Some(failure.unwrap_or(e)),
                }
            }
            match failure {
                None => {
                    if !collector.suspected_members.is_empty() {
                        log::debug!("Missing cache status responses from nodes {:?}", collector.suspected_members);
                    }
                    collector.finish();
                    return Ok(collector);
                }
                Some(RemoteError::Timeout) if attempt < CLUSTER_RECOVERY_ATTEMPTS => {
                    log::warn!("Failed to recover cluster state: timeout; retrying");
                }
                Some(e) => return Err(Some(e.to_string())),
            }
        }
    }

    /// `updateCacheMembers` (`:473-497`).
    async fn update_cache_members(self: &Arc<Self>, view_id: i32) {
        // Confirm that view's members are all available first, so in a network split scenario
        // we can enter degraded mode without starting a rebalance first
        if let Err(e) = self.confirm_members_available().await {
            if self.status().is_running() {
                log::error!("Error updating members list for view {view_id}: {e}");
            }
            return;
        }
        let new_view_id = self.transport.view_id();
        if new_view_id != view_id {
            log::debug!("Skipping cache members update for view {view_id}, newer view received: {new_view_id}");
            return;
        }
        let entries: Vec<Arc<CacheStatusEntry>> = self.cache_status_map.lock().unwrap().values().cloned().collect();
        for entry in entries {
            if let Err(e) = entry.status.lock().unwrap().do_handle_cluster_view(view_id)
                && self.status().is_running()
            {
                log::error!("Error updating members list for view {view_id}: {e}");
            }
        }
    }

    /// `confirmMembersAvailable` (`:498-512`): heartbeat every expected member.
    async fn confirm_members_available(&self) -> Result<()> {
        let mut expected: Vec<Address> = Vec::new();
        let entries: Vec<Arc<CacheStatusEntry>> = self.cache_status_map.lock().unwrap().values().cloned().collect();
        for entry in entries {
            for m in entry.status.lock().unwrap().expected_members() {
                if !expected.contains(m) {
                    expected.push(m.clone());
                }
            }
        }
        let members = self.transport.members();
        expected.retain(|a| members.contains(a));
        let timeout = self.config.global_timeout / CLUSTER_RECOVERY_ATTEMPTS as u32;
        let results = self.transport.invoke_command_on_all(Some(expected), TopologyCommand::HeartBeat, timeout).await;
        for (sender, r) in results {
            // VoidResponseCollector.validOnly(): a leaver, an exception or a timeout fails the whole stage
            if let Err(e) = r {
                return Err(TopologyError(format!("{sender:?}: {e}")));
            }
        }
        Ok(())
    }

    fn prepare_join(self: &Arc<Self>, cache_name: &str, joiner: &Address, join_info: &CacheJoinInfo, joiner_view_id: i32) -> Result<Option<Arc<CacheStatusEntry>>> {
        let v = self.view.lock().unwrap();
        if !v.status.is_running() {
            log::debug!("Ignoring join request from {joiner:?} for cache {cache_name}, the local cache manager is shutting down");
            return Err(TopologyError("org.infinispan.manager.IllegalLifecycleStateException".into()));
        }
        if joiner_view_id < v.view_id {
            log::debug!("Ignoring join request from {joiner:?} for cache {cache_name}, joiner's view id is too old: {joiner_view_id}");
            return Ok(None);
        }
        drop(v);
        Ok(Some(self.init_cache_status_if_absent(cache_name, join_info.cache_mode)))
    }
}

/// `CacheStatusResponseCollector` (`:233-288`).
#[derive(Default)]
struct CacheStatusCollector {
    responses_by_cache: Vec<(String, Vec<(Address, CacheStatusResponse)>)>,
    suspected_members: Vec<Address>,
    rebalancing_enabled: bool,
    initialised: bool,
}

impl CacheStatusCollector {
    fn add_response(&mut self, sender: &Address, response: Response) {
        if !self.initialised {
            self.rebalancing_enabled = true;
            self.initialised = true;
        }
        if let Response::Successful(Some(v)) = response
            && let ResponseValue::ManagerStatus(node_status) = *v
        {
            self.rebalancing_enabled &= node_status.rebalancing_enabled;
            for (cache_name, csr) in node_status.caches {
                let pos = match self.responses_by_cache.iter().position(|(n, _)| *n == cache_name) {
                    Some(p) => p,
                    None => {
                        self.responses_by_cache.push((cache_name.clone(), Vec::new()));
                        self.responses_by_cache.len() - 1
                    }
                };
                self.responses_by_cache[pos].1.push((sender.clone(), csr));
            }
        }
    }

    /// Put each cache's responses in the iteration order of the `HashMap<Address, CacheStatusResponse>`
    /// upstream collects them into (the order the availability strategy sees them in), and the
    /// caches in `HashMap<String, ...>` order.
    fn finish(&mut self) {
        if !self.initialised {
            self.rebalancing_enabled = true;
        }
        for (_, responses) in &mut self.responses_by_cache {
            let hashes: Vec<i32> = responses.iter().map(|(a, _)| a.hash_code()).collect();
            let order = hash_map_iteration_order(&hashes, HashMapInit::Default);
            let reordered: Vec<_> = order.into_iter().map(|i| responses[i].clone()).collect();
            *responses = reordered;
        }
        let hashes: Vec<i32> = self.responses_by_cache.iter().map(|(n, _)| super::messages::java_string_hash(n)).collect();
        let order = hash_map_iteration_order(&hashes, HashMapInit::Default);
        let reordered: Vec<_> = order.into_iter().map(|i| self.responses_by_cache[i].clone()).collect();
        self.responses_by_cache = reordered;
    }
}

/// The coordinator-side topology manager of one node.
#[derive(Clone)]
pub struct ClusterTopologyManager {
    pub(crate) inner: Arc<Inner>,
}

impl ClusterTopologyManager {
    /// A manager sending commands over `transport`.
    pub fn new(transport: Arc<dyn TopologyTransport>, persistent_uuids: Arc<PersistentUuidManager>, config: TopologyConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                invoker: OnceLock::new(),
                config,
                persistent_uuids,
                view: Mutex::new(ViewState { view_id: -1, status: ClusterManagerStatus::Initializing }),
                cache_status_map: Mutex::new(HashMap::new()),
                global_rebalancing: Mutex::new(GlobalRebalanceStatus::NotRecovered),
                sequencer: ActionSequencer::new(),
                join_view_future: ConditionFuture::new(),
                recovery_attempts: AtomicI32::new(0),
                stopped: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn set_invoker(&self, invoker: Arc<dyn LocalInvoker>) {
        let _ = self.inner.invoker.set(invoker);
    }

    /// The manager's status.
    pub fn status(&self) -> ClusterManagerStatus {
        self.inner.status()
    }

    /// `start()` (`:66-77`): handle the initial view, then learn the global rebalancing status.
    pub async fn start(&self) {
        // The listener already missed the initial view
        self.handle_cluster_view(false, self.inner.transport.view_id());
        let enabled = self.fetch_rebalancing_status_from_coordinator(INITIAL_CONNECTION_ATTEMPTS).await;
        *self.inner.global_rebalancing.lock().unwrap() = if enabled { GlobalRebalanceStatus::Enabled } else { GlobalRebalanceStatus::Disabled };
    }

    /// `fetchRebalancingStatusFromCoordinator` (`:78-97`).
    async fn fetch_rebalancing_status_from_coordinator(&self, attempts: i32) -> bool {
        let i = &self.inner;
        if i.transport.is_coordinator() {
            return i.is_rebalancing_enabled();
        }
        let mut attempts = attempts;
        loop {
            // `new RebalanceStatusRequestCommand()`: a null cacheName asks for the global status
            let cmd = TopologyCommand::RebalanceStatusRequest { cache_name: None };
            let timeout = i.config.global_timeout / INITIAL_CONNECTION_ATTEMPTS as u32;
            match i.execute_on_coordinator(cmd, timeout).await {
                Ok(Some(ResponseValue::RebalancingStatus(s))) => return s != RebalancingStatus::Suspended,
                Ok(_) => return true,
                Err(e) => {
                    // Assume any timeout is because the coordinator doesn't have a dispatcher yet and retry.
                    if attempts == 1 || !e.0.contains("TimeoutException") {
                        log::error!("Error reading rebalancing status from {:?}: {e}", i.transport.coordinator());
                        return true;
                    }
                    log::debug!("Timed out waiting for rebalancing status from coordinator, trying again");
                    attempts -= 1;
                }
            }
        }
    }

    /// `stop()` (`:98-109`).
    pub fn stop(&self) {
        self.inner.view.lock().unwrap().status = ClusterManagerStatus::Stopping;
        self.inner.stopped.store(true, Ordering::SeqCst);
    }

    /// The `@ViewChanged`/`@Merged` listener: `handleClusterView` (`:289-308`), ordered on the manager.
    pub fn handle_cluster_view(&self, merge_view: bool, new_view_id: i32) {
        let inner = self.inner.clone();
        let fut = self.inner.sequencer.order_on_key("ClusterTopologyManagerImpl", inner.handle_cluster_view_action(merge_view, new_view_id));
        tokio::spawn(fut);
    }

    /// `currentJoiners` (`:122-127`).
    pub fn current_joiners(&self, cache_name: &str) -> Option<Vec<Address>> {
        if !self.status().is_coordinator() {
            return None;
        }
        let entry = self.inner.cache_status_map.lock().unwrap().get(cache_name).cloned();
        entry.map(|e| e.status.lock().unwrap().expected_members().to_vec())
    }

    /// `handleJoin` (`:128-155`): `Ok(None)` is the `null` answer that makes the joiner retry.
    pub async fn handle_join(
        &self,
        cache_name: &str,
        joiner: &Address,
        join_info: &CacheJoinInfo,
        joiner_view_id: i32,
    ) -> Result<Option<CacheStatusResponse>> {
        let i = &self.inner;
        if !i.can_handle_join(joiner_view_id) {
            log::trace!("Delaying join request from {joiner:?} until view {joiner_view_id} is installed (and cluster status is recovered)");
            let timeout = Duration::from_millis(join_info.timeout.max(0) as u64);
            i.join_view_future.wait_for(|| i.can_handle_join(joiner_view_id), timeout).await.map_err(|_| {
                TopologyError(format!(
                    "ISPN000332: Timed out waiting for view {joiner_view_id} on the coordinator (current view {}, status {:?})",
                    i.view_id(),
                    i.status()
                ))
            })?;
        }
        // After we have the right view, obtain the ClusterCacheStatus
        let Some(entry) = i.prepare_join(cache_name, joiner, join_info, joiner_view_id)? else {
            // We have a newer view. Return null so that the joiner is forced to retry
            return Ok(None);
        };
        // nodeCanJoinFuture: creating the initial topology requires a node with a non-zero capacity factor
        if join_info.capacity_factor == 0.0 && entry.status.lock().unwrap().current_topology().is_none() {
            log::trace!("Waiting on initial topology for {join_info:?}");
            let timeout = Duration::from_millis(join_info.timeout.max(0) as u64);
            entry
                .has_initial_topology
                .wait_for(|| entry.status.lock().unwrap().current_topology().is_some(), timeout)
                .await
                .map_err(|_| TopologyError("java.util.concurrent.TimeoutException: Timed out waiting for initial cache topology".into()))?;
        }
        let response = {
            let mut status = entry.status.lock().unwrap();
            let r = status.do_join(joiner, join_info)?;
            if status.current_topology().is_some() {
                entry.has_initial_topology.update();
            }
            r
        };
        Ok(Some(response))
    }

    /// `handleLeave` (`:185-200`).
    pub async fn handle_leave(&self, cache_name: &str, leaver: &Address, _view_id: i32) -> Result<()> {
        let i = &self.inner;
        if !i.status().is_running() {
            log::debug!("Ignoring leave request from {leaver:?} for cache {cache_name}, the local cache manager is shutting down");
            return Ok(());
        }
        let entry = i.cache_status_map.lock().unwrap().get(cache_name).cloned();
        let Some(entry) = entry else {
            // This can happen if we've just become coordinator
            log::trace!("Ignoring leave request from {leaver:?} for cache {cache_name} because it doesn't have a cache status entry");
            return Ok(());
        };
        entry.status.lock().unwrap().do_leave(leaver)
    }

    /// `handleRebalancePhaseConfirm` (`:204-221`); only a `null` throwable is modelled.
    pub async fn handle_rebalance_phase_confirm(
        &self,
        cache_name: &str,
        node: &Address,
        topology_id: i32,
        _view_id: i32,
    ) -> Result<()> {
        let entry = self.inner.cache_status_map.lock().unwrap().get(cache_name).cloned();
        let Some(entry) = entry else {
            log::debug!("Ignoring rebalance confirmation from {node:?} for cache {cache_name} because it doesn't have a cache status entry");
            return Ok(());
        };
        entry.status.lock().unwrap().confirm_rebalance_phase(node, topology_id)
    }

    /// Whether rebalancing is enabled cluster-wide.
    pub fn is_rebalancing_enabled(&self) -> bool {
        self.inner.is_rebalancing_enabled()
    }

    /// `isRebalancingEnabled(cacheName)` (`:530-538`).
    pub fn is_rebalancing_enabled_for(&self, cache_name: Option<&str>) -> bool {
        match cache_name {
            None => self.is_rebalancing_enabled(),
            Some(n) => match self.inner.cache_status_map.lock().unwrap().get(n).cloned() {
                Some(e) => e.status.lock().unwrap().is_rebalance_enabled(),
                None => self.is_rebalancing_enabled(),
            },
        }
    }

    /// `setRebalancingEnabled(boolean)` (`:554-567`).
    pub fn set_rebalancing_enabled(&self, enabled: bool) -> Result<()> {
        *self.inner.global_rebalancing.lock().unwrap() = if enabled { GlobalRebalanceStatus::Enabled } else { GlobalRebalanceStatus::Disabled };
        let entries: Vec<Arc<CacheStatusEntry>> = self.inner.cache_status_map.lock().unwrap().values().cloned().collect();
        for e in entries {
            e.status.lock().unwrap().start_queued_rebalance()?;
        }
        Ok(())
    }

    /// `setRebalancingEnabled(cacheName, enabled)` (`:540-552`).
    pub fn set_cache_rebalancing_enabled(&self, cache_name: &str, enabled: bool) -> Result<()> {
        let entry = self.inner.cache_status_map.lock().unwrap().get(cache_name).cloned();
        match entry {
            Some(e) => e.status.lock().unwrap().set_rebalance_enabled(enabled),
            None => {
                log::debug!("Trying to enable rebalancing for inexistent cache {cache_name}");
                Ok(())
            }
        }
    }

    /// `getRebalancingStatus(cacheName)` (`:584-592`); a null cache name is the global question
    /// `RebalanceStatusRequestCommand.invokeAsync` answers first (`:37-40`).
    pub fn rebalancing_status(&self, cache_name: Option<&str>) -> RebalancingStatus {
        match cache_name {
            None => {
                if self.is_rebalancing_enabled() {
                    RebalancingStatus::Pending
                } else {
                    RebalancingStatus::Suspended
                }
            }
            Some(n) => match self.inner.cache_status_map.lock().unwrap().get(n).cloned() {
                Some(e) => e.status.lock().unwrap().rebalancing_status(),
                None => RebalancingStatus::Pending,
            },
        }
    }

    /// `forceRebalance` (`:568-575`).
    pub fn force_rebalance(&self, cache_name: &str) -> Result<()> {
        let entry = self.inner.cache_status_map.lock().unwrap().get(cache_name).cloned();
        match entry {
            Some(e) => e.status.lock().unwrap().force_rebalance(),
            None => Ok(()),
        }
    }

    /// `setInitialCacheTopologyId` (`:598-604`).
    pub fn set_initial_cache_topology_id(&self, cache_name: &str, cache_mode: CacheMode, topology_id: i32) {
        self.inner.init_cache_status_if_absent(cache_name, cache_mode).status.lock().unwrap().set_initial_topology_id(topology_id);
    }

    /// A snapshot of one cache's coordinator-side state, for tests and diagnostics.
    pub fn with_cache_status<R>(&self, cache_name: &str, f: impl FnOnce(&ClusterCacheStatus) -> R) -> Option<R> {
        let entry = self.inner.cache_status_map.lock().unwrap().get(cache_name).cloned()?;
        let guard = entry.status.lock().unwrap();
        Some(f(&guard))
    }
}
