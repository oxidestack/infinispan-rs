//! Port of `ClusterCacheStatus` (core 14.0.35.Final, `topology/ClusterCacheStatus.java`),
//! `RebalanceConfirmationCollector`, `RebalanceType`, `PersistentUUIDManagerImpl` and the
//! `PreferAvailabilityStrategy` that `whenSplit=ALLOW_READ_WRITES` selects
//! (`partitionhandling/impl/PreferAvailabilityStrategy.java`).
//!
//! The Java class is `synchronized` and calls back into `ClusterTopologyManagerImpl`; here every
//! method takes `&mut self` (the manager wraps it in a lock) and the callbacks are the
//! [`ClusterTopologyHooks`] trait. Out of scope, with the reason: persisted global state
//! (`persistentState`, `restoreCacheTopology`, `setCurrentTopologyAsStable`: MockServer runs without
//! `GlobalState`), conflict resolution (`mergePolicy=NONE`, so `resolveConflictsOnMerge()` is
//! always `false`), `PreferConsistencyStrategy` (`whenSplit=ALLOW_READ_WRITES`) and scattered caches.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::cache_topology::{CacheTopology, ConsistentHash, INITIAL_REBALANCE_ID, INITIAL_TOPOLOGY_ID, Phase};
pub use super::rebalance::{CollectorAction, RebalanceConfirmationCollector, RebalanceType, RebalancingStatus};
use super::messages::{AvailabilityMode, CacheJoinInfo, CacheStatusResponse};
use crate::ch::address::{Address, CapacityFactors};

/// An exception from the topology layer (`CacheException`, `CacheJoinException`,
/// `NullPointerException`...), carrying upstream's message text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyError(pub String);

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TopologyError {}

impl From<String> for TopologyError {
    fn from(s: String) -> Self {
        TopologyError(s)
    }
}

/// The result of a topology operation.
pub type Result<T> = std::result::Result<T, TopologyError>;

/// `ComponentStatus` as `ClusterCacheStatus` uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentStatus {
    /// Created, not started.
    Instantiated,
    /// Running.
    Running,
    /// Stopping.
    Stopping,
    /// Stopped.
    Terminated,
}

/// `PersistentUUIDManagerImpl`: address to persistent UUID and back.
#[derive(Debug, Default)]
pub struct PersistentUuidManager {
    maps: Mutex<(HashMap<Address, Address>, HashMap<Address, Address>)>,
}

impl PersistentUuidManager {
    /// An empty manager.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records that `address` has `persistent_uuid`.
    pub fn add_persistent_address_mapping(&self, address: &Address, persistent_uuid: &Address) {
        let mut m = self.maps.lock().unwrap();
        m.0.insert(address.clone(), persistent_uuid.clone());
        m.1.insert(persistent_uuid.clone(), address.clone());
    }

    /// The persistent UUID of `address`.
    pub fn get_persistent_uuid(&self, address: &Address) -> Option<Address> {
        self.maps.lock().unwrap().0.get(address).cloned()
    }

    /// The address of the node with `persistent_uuid`.
    pub fn get_address(&self, persistent_uuid: &Address) -> Option<Address> {
        self.maps.lock().unwrap().1.get(persistent_uuid).cloned()
    }

    /// `mapAddresses`: a missing mapping is the `NullPointerException` upstream logs as fatal.
    pub fn map_addresses(&self, addresses: &[Address]) -> Result<Vec<Address>> {
        let m = self.maps.lock().unwrap();
        addresses
            .iter()
            .map(|a| {
                m.0.get(a).cloned().ok_or_else(|| {
                    log::error!("Cannot find mapping for address {a:?}");
                    TopologyError("java.lang.NullPointerException".into())
                })
            })
            .collect()
    }
}

/// What `ClusterCacheStatus` calls on `ClusterTopologyManagerImpl`.
pub trait ClusterTopologyHooks: Send + Sync {
    /// Sends the topology that starts a rebalance of `cache_name` to every member.
    fn broadcast_rebalance_start(&self, cache_name: &str, topology: &CacheTopology);
    /// Sends `cache_name`'s new topology and availability mode to every member.
    fn broadcast_topology_update(&self, cache_name: &str, topology: &CacheTopology, availability_mode: Option<AvailabilityMode>);
    /// Sends `cache_name`'s new stable topology to every member.
    fn broadcast_stable_topology_update(&self, cache_name: &str, topology: &CacheTopology);
    /// Forgets `cache_name` after its last member left.
    fn remove_cache_status(&self, cache_name: &str);
    /// Whether rebalancing is enabled cluster-wide.
    fn is_rebalancing_enabled(&self) -> bool;
}

/// What `ClusterCacheStatus` reads from `Transport` (`getMembers`, `getViewId`).
pub trait ViewSource: Send + Sync {
    /// The members of the installed view.
    fn members(&self) -> Vec<Address>;
    /// The id of the installed view.
    fn view_id(&self) -> i32;
}

fn immutable_retain_all(list: &[Address], other: &[Address]) -> Vec<Address> {
    list.iter().filter(|a| other.contains(a)).cloned().collect()
}

fn immutable_remove_all(list: &[Address], other: &[Address]) -> Vec<Address> {
    list.iter().filter(|a| !other.contains(a)).cloned().collect()
}

/// `ClusterTopologyManagerImpl.distLostDataCheck` (`:626-632`).
pub fn dist_lost_data_check(stable_ch: &ConsistentHash, new_members: &[Address]) -> bool {
    for i in 0..stable_ch.num_segments() {
        let owners = stable_ch.locate_owners_for_segment(i);
        if !new_members.iter().any(|m| owners.contains(m)) {
            return true;
        }
    }
    false
}

/// Keeps track of a cache's status: members, current/pending consistent hashes, rebalance status.
pub struct ClusterCacheStatus {
    cache_name: String,
    rebalance_type: RebalanceType,
    hooks: Arc<dyn ClusterTopologyHooks>,
    view: Arc<dyn ViewSource>,
    persistent_uuid_manager: Arc<PersistentUuidManager>,
    initial_topology_id: i32,
    join_info: Option<CacheJoinInfo>,
    expected_members: Vec<Address>,
    capacity_factors: CapacityFactors,
    joiners: Vec<Address>,
    current_topology: Option<CacheTopology>,
    stable_topology: Option<CacheTopology>,
    /// `null` (`None`) after a merge under `PreferAvailabilityStrategy`, until `setAvailabilityMode` changes it
    availability_mode: Option<AvailabilityMode>,
    queued_rebalance_members: Option<Vec<Address>>,
    rebalancing_enabled: bool,
    rebalance_in_progress: bool,
    manually_put_degraded: bool,
    rebalance_confirmation_collector: Option<RebalanceConfirmationCollector>,
    status: ComponentStatus,
}

impl ClusterCacheStatus {
    /// The status of `cache_name` before its first member joined.
    pub fn new(
        cache_name: &str,
        rebalance_type: RebalanceType,
        hooks: Arc<dyn ClusterTopologyHooks>,
        view: Arc<dyn ViewSource>,
        persistent_uuid_manager: Arc<PersistentUuidManager>,
    ) -> Self {
        Self {
            cache_name: cache_name.to_string(),
            rebalance_type,
            hooks,
            view,
            persistent_uuid_manager,
            initial_topology_id: INITIAL_TOPOLOGY_ID,
            join_info: None,
            expected_members: Vec::new(),
            capacity_factors: CapacityFactors::default(),
            joiners: Vec::new(),
            current_topology: None,
            stable_topology: None,
            availability_mode: Some(AvailabilityMode::Available),
            queued_rebalance_members: None,
            rebalancing_enabled: true,
            rebalance_in_progress: false,
            manually_put_degraded: false,
            rebalance_confirmation_collector: None,
            status: ComponentStatus::Instantiated,
        }
    }

    /// The cache's name.
    pub fn cache_name(&self) -> &str {
        &self.cache_name
    }

    /// The join info of the first member, which every joiner must match.
    pub fn join_info(&self) -> Option<&CacheJoinInfo> {
        self.join_info.as_ref()
    }

    /// The members expected to have the cache.
    pub fn expected_members(&self) -> &[Address] {
        &self.expected_members
    }

    /// Each member's capacity factor.
    pub fn capacity_factors(&self) -> &CapacityFactors {
        &self.capacity_factors
    }

    /// The current topology.
    pub fn current_topology(&self) -> Option<&CacheTopology> {
        self.current_topology.as_ref()
    }

    /// The stable topology: the last one with no rebalance in progress.
    pub fn stable_topology(&self) -> Option<&CacheTopology> {
        self.stable_topology.as_ref()
    }

    /// The availability mode.
    pub fn availability_mode(&self) -> Option<AvailabilityMode> {
        self.availability_mode
    }

    /// The members that joined and are not in the topology yet.
    pub fn joiners(&self) -> &[Address] {
        &self.joiners
    }

    /// The component status.
    pub fn status(&self) -> ComponentStatus {
        self.status
    }

    /// Sets the id the first topology gets.
    pub fn set_initial_topology_id(&mut self, id: i32) {
        self.initial_topology_id = id;
    }

    /// Whether the cache was put in degraded mode by hand.
    pub fn is_manually_degraded(&self) -> bool {
        self.manually_put_degraded
    }

    fn factory_kind(&self) -> Result<super::messages::ConsistentHashFactoryKind> {
        self.join_info
            .as_ref()
            .map(|j| j.consistent_hash_factory)
            .ok_or_else(|| TopologyError("java.lang.NullPointerException: no join info yet".into()))
    }

    fn map_addresses(&self, addresses: &[Address]) -> Result<Option<Vec<Address>>> {
        Ok(Some(self.persistent_uuid_manager.map_addresses(addresses)?))
    }

    /// `persistentUUIDManager.mapAddresses`.
    pub(crate) fn persistent_uuids_for(&self, addresses: &[Address]) -> Result<Vec<Address>> {
        self.persistent_uuid_manager.map_addresses(addresses)
    }

    /// `queueRebalance` (`:104-111`).
    pub fn queue_rebalance(&mut self, new_members: &[Address]) -> Result<()> {
        if !new_members.is_empty() && self.total_capacity_factors() != 0.0 {
            log::debug!("Queueing rebalance for cache {} with members {new_members:?}", self.cache_name);
            self.queued_rebalance_members = Some(new_members.to_vec());
            self.start_queued_rebalance()?;
        }
        Ok(())
    }

    /// `updateAvailabilityMode` (`:132-157`).
    pub fn update_availability_mode(
        &mut self,
        actual_members: &[Address],
        new_availability_mode: AvailabilityMode,
        cancel_rebalance: bool,
    ) -> Result<()> {
        let mode_changed = self.set_availability_mode(new_availability_mode);
        self.manually_put_degraded = false;
        let current = self.current_topology.clone().expect("current topology");
        if mode_changed || actual_members != current.actual_members.as_slice() {
            let mut new_pending = current.pending_ch.clone();
            let mut new_phase = current.phase;
            if cancel_rebalance {
                new_pending = None;
                new_phase = Phase::NoRebalance;
                self.rebalance_confirmation_collector = None;
            }
            let new_topology = CacheTopology::new(
                current.topology_id + 1,
                current.rebalance_id,
                false,
                current.current_ch.clone(),
                new_pending,
                None,
                new_phase,
                actual_members.to_vec(),
                self.map_addresses(actual_members)?,
            )?;
            self.set_current_topology(Some(new_topology.clone()));
            self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, Some(new_availability_mode));
        }
        Ok(())
    }

    /// `manuallyUpdateAvailabilityMode` (`:159-163`).
    pub fn manually_update_availability_mode(
        &mut self,
        actual_members: &[Address],
        mode: AvailabilityMode,
        cancel_rebalance: bool,
    ) -> Result<()> {
        self.update_availability_mode(actual_members, mode, cancel_rebalance)?;
        self.manually_put_degraded = mode == AvailabilityMode::DegradedMode;
        Ok(())
    }

    /// `updateTopologiesAfterMerge` (`:170-185`); a `None` `availability_mode` is upstream's `null`.
    pub fn update_topologies_after_merge(
        &mut self,
        current_topology: CacheTopology,
        stable_topology: Option<CacheTopology>,
        availability_mode: Option<AvailabilityMode>,
    ) {
        self.current_topology = Some(current_topology.clone());
        self.stable_topology = stable_topology.clone();
        self.availability_mode = availability_mode;
        self.hooks.broadcast_topology_update(&self.cache_name, &current_topology, self.availability_mode);
        if let Some(s) = stable_topology {
            log::debug!("Updating stable topology for cache {}", self.cache_name);
            self.hooks.broadcast_stable_topology_update(&self.cache_name, &s);
        }
    }

    /// `addMember` (`:192-212`): `true` if the joiner was not already a member.
    fn add_member(&mut self, joiner: &Address, join_info: &CacheJoinInfo) -> Result<bool> {
        if self.expected_members.contains(joiner) {
            return Ok(false);
        }
        if self.join_info.is_none() {
            self.join_info = Some(join_info.clone());
        }
        self.capacity_factors.put(joiner.clone(), join_info.capacity_factor);
        self.expected_members.push(joiner.clone());
        let puuid = join_info
            .persistent_uuid
            .as_ref()
            .ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
        self.persistent_uuid_manager.add_persistent_address_mapping(joiner, puuid);
        self.joiners.push(joiner.clone());
        Ok(true)
    }

    /// `removeMember` (`:217-232`).
    fn remove_member(&mut self, leaver: &Address) -> bool {
        let Some(pos) = self.expected_members.iter().position(|a| a == leaver) else {
            log::trace!("Trying to remove node {leaver:?} from cache {}, but it is not a member", self.cache_name);
            return false;
        };
        self.expected_members.remove(pos);
        self.capacity_factors.0.retain(|(a, _)| a != leaver);
        self.joiners.retain(|a| a != leaver);
        true
    }

    /// `retainMembers` (`:238-252`).
    fn retain_members(&mut self, new_cluster_members: &[Address]) -> bool {
        if self.expected_members.iter().all(|m| new_cluster_members.contains(m)) {
            return false;
        }
        self.expected_members = immutable_retain_all(&self.expected_members, new_cluster_members);
        self.joiners = immutable_retain_all(&self.joiners, new_cluster_members);
        true
    }

    /// `setCurrentTopology` (`:254-266`).
    fn set_current_topology(&mut self, new_topology: Option<CacheTopology>) {
        if let Some(t) = &new_topology {
            let current_members = t.current_ch.as_ref().map(|c| c.members().to_vec()).unwrap_or_default();
            self.joiners = immutable_remove_all(&self.expected_members, &current_members);
        }
        self.current_topology = new_topology;
    }

    fn set_stable_topology(&mut self, t: Option<CacheTopology>) {
        self.stable_topology = t;
    }

    /// `needConsistentHashUpdate` (`:272-277`).
    fn need_consistent_hash_update(&self) -> bool {
        match &self.current_topology {
            Some(t) => self.expected_members.as_slice() != t.members(),
            None => false,
        }
    }

    /// `pruneInvalidMembers` (`:279-281`).
    fn prune_invalid_members(&self, possible: &[Address]) -> Vec<Address> {
        immutable_retain_all(possible, &self.expected_members)
    }

    /// Whether a rebalance is in progress.
    pub fn is_rebalance_in_progress(&self) -> bool {
        self.rebalance_confirmation_collector.is_some()
    }

    /// `getRebalancingStatus` (`:285-296`).
    pub fn rebalancing_status(&self) -> RebalancingStatus {
        if !self.is_rebalance_enabled() {
            RebalancingStatus::Suspended
        } else if self.rebalance_in_progress {
            RebalancingStatus::InProgress
        } else if self.queued_rebalance_members.is_some() {
            RebalancingStatus::Pending
        } else {
            RebalancingStatus::Complete
        }
    }

    /// `confirmRebalancePhase` (`:298-318`).
    pub fn confirm_rebalance_phase(&mut self, member: &Address, received_topology_id: i32) -> Result<()> {
        let Some(current) = &self.current_topology else {
            log::debug!("Ignoring rebalance confirmation from {member:?} for cache {} because the cache has no members", self.cache_name);
            return Ok(());
        };
        if received_topology_id < current.topology_id {
            log::debug!(
                "Ignoring rebalance confirmation from {member:?} for cache {} because the topology id is old ({received_topology_id}, expected {})",
                self.cache_name, current.topology_id
            );
            return Ok(());
        }
        let Some(collector) = &mut self.rebalance_confirmation_collector else {
            return Err(TopologyError(format!(
                "Received invalid rebalance confirmation from {member:?} for cache {}, we don't have a rebalance in progress",
                self.cache_name
            )));
        };
        let action = collector.confirm_phase(member, received_topology_id);
        self.run_collector_action(action)
    }

    fn run_collector_action(&mut self, action: Option<CollectorAction>) -> Result<()> {
        match action {
            Some(CollectorAction::EndRebalance) => self.end_rebalance(),
            Some(CollectorAction::EndReadAllPhase) => self.end_read_all_phase(),
            Some(CollectorAction::EndReadNewPhase) => self.end_read_new_phase(),
            None => Ok(()),
        }
    }

    /// `updateMembers` (`:325-330`).
    fn update_members(&mut self) -> Result<()> {
        if self.rebalance_confirmation_collector.is_some() {
            // We rely on the AvailabilityStrategy updating the current topology beforehand.
            let members = self.current_topology.as_ref().map(|t| t.members().to_vec()).unwrap_or_default();
            let action = self.rebalance_confirmation_collector.as_mut().unwrap().update_members(&members);
            self.run_collector_action(action)?;
        }
        Ok(())
    }

    /// `doHandleClusterView` (`:332-355`).
    pub fn do_handle_cluster_view(&mut self, view_id: i32) -> Result<()> {
        if self.current_topology.is_none() {
            return Ok(());
        }
        let new_cluster_members = self.view.members();
        let new_view_id = self.view.view_id();
        if new_view_id != view_id {
            log::debug!("Cache {} skipping members update for view {view_id}, newer view received: {new_view_id}", self.cache_name);
            return Ok(());
        }
        let cache_members_modified = self.retain_members(&new_cluster_members);
        super::availability::on_cluster_view_change(self, &new_cluster_members)?;
        if cache_members_modified {
            self.update_members()?;
        }
        Ok(())
    }

    /// `endRebalance` (`:357-405`).
    fn end_rebalance(&mut self) -> Result<()> {
        self.rebalance_in_progress = false;
        let Some(current) = self.current_topology.clone() else {
            log::trace!("Rebalance finished because there are no more members in cache {}", self.cache_name);
            return Ok(());
        };
        debug_assert!(current.phase.is_rebalance());
        let current_topology_id = current.topology_id;
        let members = current.members().to_vec();
        let new_topology = match self.rebalance_type {
            RebalanceType::FourPhase => CacheTopology::new(
                current_topology_id + 1,
                current.rebalance_id,
                false,
                current.current_ch.clone(),
                current.pending_ch.clone(),
                None,
                Phase::ReadAllWriteAll,
                members.clone(),
                self.map_addresses(&members)?,
            )?,
            RebalanceType::TwoPhase => CacheTopology::new(
                current_topology_id + 1,
                current.rebalance_id,
                false,
                current.pending_ch.clone(),
                None,
                None,
                Phase::NoRebalance,
                members.clone(),
                self.map_addresses(&members)?,
            )?,
            RebalanceType::None => return Err(TopologyError("java.lang.IllegalStateException".into())),
        };
        self.set_current_topology(Some(new_topology.clone()));
        if new_topology.phase != Phase::NoRebalance {
            self.rebalance_confirmation_collector = Some(RebalanceConfirmationCollector::new(
                &self.cache_name,
                current_topology_id + 1,
                &members,
                CollectorAction::EndReadAllPhase,
            ));
        } else {
            self.rebalance_confirmation_collector = None;
        }
        super::availability::on_rebalance_end(self);
        // TODO upstream: to members only?
        self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, self.availability_mode);
        if new_topology.phase == Phase::NoRebalance {
            self.start_queued_rebalance()?;
        }
        Ok(())
    }

    /// `endReadAllPhase` (`:407-428`).
    fn end_read_all_phase(&mut self) -> Result<()> {
        let current = self.current_topology.clone().expect("current topology");
        debug_assert_eq!(current.phase, Phase::ReadAllWriteAll);
        let members = current.members().to_vec();
        let new_topology = CacheTopology::new(
            current.topology_id + 1,
            current.rebalance_id,
            false,
            current.current_ch.clone(),
            current.pending_ch.clone(),
            None,
            Phase::ReadNewWriteAll,
            members.clone(),
            self.map_addresses(&members)?,
        )?;
        self.set_current_topology(Some(new_topology.clone()));
        self.rebalance_confirmation_collector = Some(RebalanceConfirmationCollector::new(
            &self.cache_name,
            current.topology_id + 1,
            &members,
            CollectorAction::EndReadNewPhase,
        ));
        self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, self.availability_mode);
        Ok(())
    }

    /// `endReadNewPhase` (`:430-451`).
    fn end_read_new_phase(&mut self) -> Result<()> {
        let current = self.current_topology.clone().expect("current topology");
        debug_assert_eq!(current.phase, Phase::ReadNewWriteAll);
        let members = current.members().to_vec();
        let new_topology = CacheTopology::new(
            current.topology_id + 1,
            current.rebalance_id,
            false,
            current.pending_ch.clone(),
            None,
            None,
            Phase::NoRebalance,
            members.clone(),
            self.map_addresses(&members)?,
        )?;
        self.set_current_topology(Some(new_topology.clone()));
        self.rebalance_confirmation_collector = None;
        self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, self.availability_mode);
        self.start_queued_rebalance()
    }

    /// `updateCurrentTopology` (`:453-519`). `new_members` is, as upstream notes, not really used:
    /// `pruneInvalidMembers` uses `expectedMembers`.
    pub fn update_current_topology(&mut self, new_members: &[Address]) -> Result<()> {
        // The current topology might be null just after a joiner became the coordinator
        if self.current_topology.is_none() {
            self.create_initial_cache_topology()?;
        }
        let kind = self.factory_kind()?;
        let current = self.current_topology.clone().unwrap();
        let topology_id = current.topology_id;
        let rebalance_id = current.rebalance_id;
        let current_ch = current.current_ch.clone().expect("current CH");
        let pending_ch = current.pending_ch.clone();

        if !self.need_consistent_hash_update() {
            log::trace!("Cache {} members list was updated, but the cache topology doesn't need to change", self.cache_name);
            return Ok(());
        }
        if new_members.is_empty() {
            log::trace!("Cache {} no longer has any members, removing topology", self.cache_name);
            self.set_current_topology(None);
            self.set_stable_topology(None);
            self.rebalance_confirmation_collector = None;
            self.status = ComponentStatus::Instantiated;
            return Ok(());
        }
        if self.total_capacity_factors() == 0.0 {
            log::debug!("All members have capacity factor 0, delaying topology update");
            return Ok(());
        }

        let mut new_current_members = self.prune_invalid_members(current_ch.members());
        let new_current_ch;
        let mut new_pending_ch = None;
        let mut new_phase = Phase::NoRebalance;
        let actual_members;
        if new_current_members.is_empty() {
            // All the current members left, try to replace them with the joiners
            log::trace!("All current members left, re-initializing status for cache {}", self.cache_name);
            self.rebalance_confirmation_collector = None;
            new_current_members = self.expected_members.clone();
            actual_members = new_current_members.clone();
            let ji = self.join_info.clone().unwrap();
            new_current_ch =
                kind.create(ji.num_owners, ji.num_segments, &new_current_members, Some(&self.capacity_factors))?;
        } else {
            // ReplicatedConsistentHashFactory allocates segments to all its members, so we can't add any members here
            new_current_ch = kind.update_members(&current_ch, &new_current_members, Some(&self.capacity_factors))?;
            let mut am = new_current_members.clone();
            if let Some(p) = &pending_ch {
                new_phase = current.phase;
                let new_pending_members = self.prune_invalid_members(p.members());
                new_pending_ch = Some(kind.update_members(p, &new_pending_members, Some(&self.capacity_factors))?);
                am = self.prune_invalid_members(&new_pending_members);
            }
            actual_members = am;
        }

        // Losing members during state transfer could lead to a state where we have more than two topologies
        // concurrently in the cluster. We need to make sure that all the topologies are compatible - we just
        // remove lost members.
        let new_topology = CacheTopology::new(
            topology_id + 1,
            rebalance_id,
            false,
            Some(new_current_ch),
            new_pending_ch,
            None,
            new_phase,
            actual_members.clone(),
            self.map_addresses(&actual_members)?,
        )?;
        self.set_current_topology(Some(new_topology.clone()));
        if self.rebalance_confirmation_collector.is_some() {
            // The node that will cancel the state transfer because of another topology update won't send topology confirm
            log::debug!("Cancelling topology confirmation {:?} because of another topology update", self.rebalance_confirmation_collector);
            self.rebalance_confirmation_collector = None;
        }
        self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, self.availability_mode);
        Ok(())
    }

    /// `totalCapacityFactors` (`:521-527`).
    fn total_capacity_factors(&self) -> f32 {
        let mut total = 0f32;
        for (_, f) in &self.capacity_factors.0 {
            total += f;
        }
        total
    }

    /// `setAvailabilityMode` (`:529-536`).
    fn set_availability_mode(&mut self, new: AvailabilityMode) -> bool {
        if Some(new) == self.availability_mode {
            return false;
        }
        self.availability_mode = Some(new);
        true
    }

    /// `doMergePartitions` (`:562-593`): `status_responses` is the per-sender recovery data.
    pub fn do_merge_partitions(&mut self, status_responses: &[(Address, CacheStatusResponse)]) -> Result<()> {
        if status_responses.is_empty() {
            return Err(TopologyError("Should have at least one current topology".into()));
        }
        let mut join_infos: Vec<(Address, Option<CacheJoinInfo>)> = Vec::new();
        let mut current_topologies: Vec<CacheTopology> = Vec::new();
        let mut stable_topologies: Vec<CacheTopology> = Vec::new();
        for (sender, response) in status_responses {
            join_infos.push((sender.clone(), response.cache_join_info.clone()));
            if let Some(t) = &response.cache_topology
                && !current_topologies.contains(t)
            {
                current_topologies.push(t.clone());
            }
            if let Some(t) = &response.stable_topology
                && !stable_topologies.contains(t)
            {
                stable_topologies.push(t.clone());
            }
        }
        log::debug!("Recovered {} partition(s) for cache {}", current_topologies.len(), self.cache_name);
        self.recover_members(&join_infos, &current_topologies, &stable_topologies)?;
        super::availability::on_partition_merge(self, status_responses)
    }

    /// `recoverMembers` (`:595-615`).
    fn recover_members(
        &mut self,
        join_infos: &[(Address, Option<CacheJoinInfo>)],
        current_topologies: &[CacheTopology],
        stable_topologies: &[CacheTopology],
    ) -> Result<()> {
        self.expected_members = Vec::new();
        // Try to preserve the member order at least for the first partition
        for topology in stable_topologies {
            self.add_members(topology.members(), join_infos)?;
        }
        for topology in current_topologies {
            self.add_members(topology.members(), join_infos)?;
        }
        // Add the joiners that are not part of any topology at the end
        for (sender, info) in join_infos {
            if !self.expected_members.contains(sender)
                && let Some(i) = info
            {
                self.add_member(sender, i)?;
            }
        }
        Ok(())
    }

    /// `addMembers` (`:617-628`).
    fn add_members(&mut self, to_add: &[Address], join_infos: &[(Address, Option<CacheJoinInfo>)]) -> Result<()> {
        for member in to_add {
            if !self.expected_members.contains(member) {
                // Some members of the stable/current topology may not be members any more
                if let Some((_, Some(info))) = join_infos.iter().find(|(a, _)| a == member) {
                    self.add_member(member, info)?;
                }
            }
        }
        Ok(())
    }

    /// `doJoin` (`:633-684`), without persisted state.
    pub fn do_join(&mut self, joiner: &Address, join_info: &CacheJoinInfo) -> Result<CacheStatusResponse> {
        // A joiner with state can not join a cache without a state.
        if join_info.persistent_state_checksum.is_some() {
            return Err(TopologyError(format!(
                "ISPN000408: Node {joiner:?} with persistent state attempting to join cache {} on cluster without state",
                self.cache_name
            )));
        }
        let is_first_member = self.current_topology.is_none();
        let member_joined = self.add_member(joiner, join_info)?;
        if !member_joined {
            log::trace!("Trying to add node {joiner:?} to cache {}, but it is already a member", self.cache_name);
            return Ok(CacheStatusResponse {
                cache_join_info: None,
                cache_topology: self.current_topology.clone(),
                stable_topology: self.stable_topology.clone(),
                availability_mode: self.availability_mode,
                current: Some(self.expected_members.clone()),
            });
        }
        let current = self.expected_members.clone();
        if self.status == ComponentStatus::Instantiated && is_first_member {
            // This node was the first to join. We need to install the initial CH
            let initial_topology = self.create_initial_cache_topology()?;
            // Change our status
            self.status = ComponentStatus::Running;
            // Don't need to broadcast the initial CH update, just return the cache topology to the joiner
            // But we do need to broadcast the initial topology as the stable topology
            self.hooks.broadcast_stable_topology_update(&self.cache_name, &initial_topology);
        }
        let topology_before_rebalance = self.current_topology.clone();
        // Only trigger availability strategy if we have a topology installed
        if topology_before_rebalance.is_some() {
            super::availability::on_join(self, joiner)?;
        }
        Ok(CacheStatusResponse {
            cache_join_info: None,
            cache_topology: topology_before_rebalance,
            stable_topology: self.stable_topology.clone(),
            availability_mode: self.availability_mode,
            current: Some(current),
        })
    }

    /// `createInitialCacheTopology` (`:706-717`).
    fn create_initial_cache_topology(&mut self) -> Result<CacheTopology> {
        log::trace!("Initializing status for cache {}", self.cache_name);
        let initial_members = self.expected_members.clone();
        let ji = self.join_info.clone().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
        let initial_ch = ji
            .consistent_hash_factory
            .create(ji.num_owners, ji.num_segments, &initial_members, Some(&self.capacity_factors))?;
        let initial_topology = CacheTopology::new(
            self.initial_topology_id,
            INITIAL_REBALANCE_ID,
            false,
            Some(initial_ch),
            None,
            None,
            Phase::NoRebalance,
            initial_members.clone(),
            self.map_addresses(&initial_members)?,
        )?;
        self.set_current_topology(Some(initial_topology.clone()));
        self.set_stable_topology(Some(initial_topology.clone()));
        Ok(initial_topology)
    }

    /// `doLeave` (`:718-730`).
    pub fn do_leave(&mut self, leaver: &Address) -> Result<()> {
        let actual_leaver = self.remove_member(leaver);
        if !actual_leaver {
            return Ok(());
        }
        if self.expected_members.is_empty() {
            self.hooks.remove_cache_status(&self.cache_name);
        }
        if self.current_topology.is_none() {
            return Ok(());
        }
        super::availability::on_graceful_leave(self, leaver)?;
        self.update_members()
    }

    /// `startQueuedRebalance` (`:732-852`).
    pub fn start_queued_rebalance(&mut self) -> Result<()> {
        // We don't have a queued rebalance.
        let Some(queued) = self.queued_rebalance_members.clone() else {
            // We may need to broadcast a stable topology update
            let current = self.current_topology.clone();
            if let Some(current) = current
                && self.stable_topology.as_ref().is_none_or(|s| s.topology_id < current.topology_id)
            {
                self.stable_topology = Some(current.clone());
                log::debug!("Updating stable topology for cache {}", self.cache_name);
                self.hooks.broadcast_stable_topology_update(&self.cache_name, &current);
            }
            return Ok(());
        };
        let cache_topology = self.current_topology.clone();
        if !self.is_rebalance_enabled() {
            log::trace!("Postponing rebalance for cache {}, rebalancing is disabled", self.cache_name);
            return Ok(());
        }
        if self.rebalance_confirmation_collector.is_some() {
            log::trace!("Postponing rebalance for cache {}, there's already a topology change in progress", self.cache_name);
            return Ok(());
        }
        if queued.is_empty() {
            log::trace!("Ignoring request to rebalance cache {}, it doesn't have any member", self.cache_name);
            return Ok(());
        }
        let Some(cache_topology) = cache_topology else {
            self.create_initial_cache_topology()?;
            return Ok(());
        };

        let mut new_members = update_members_preserving_order(cache_topology.members(), &queued);
        self.queued_rebalance_members = None;
        log::trace!("Rebalancing consistent hash for cache {}, members are {new_members:?}", self.cache_name);

        let new_topology_id = cache_topology.topology_id + 1;
        let new_rebalance_id = cache_topology.rebalance_id + 1;
        let mut current_ch = cache_topology.current_ch.clone();
        let Some(mut cur_ch) = current_ch.take() else {
            // There was one node in the cache before, and it left after the rebalance was triggered
            // but before the rebalance actually started.
            log::trace!("Ignoring request to rebalance cache {}, it doesn't have a consistent hash", self.cache_name);
            return Ok(());
        };
        if !new_members.iter().all(|m| self.expected_members.contains(m)) {
            new_members.retain(|m| !self.expected_members.contains(m));
            log::trace!("Ignoring request to rebalance cache {}, we have new leavers: {new_members:?}", self.cache_name);
            return Ok(());
        }

        let kind = self.factory_kind()?;
        // This update will only add the joiners to the CH, we have already checked that we don't have leavers
        let updated_members_ch = kind.update_members(&cur_ch, &new_members, Some(&self.capacity_factors))?;
        let balanced_ch = kind.rebalance(&updated_members_ch)?;
        let remove_members = !cur_ch.members().iter().all(|m| self.expected_members.contains(m));
        if remove_members {
            // Leavers should have been removed before starting a rebalance, but that might have failed
            // e.g. if all the remaining members had capacity factor 0
            log::debug!("Removing unwanted members from the current consistent hash");
            cur_ch = updated_members_ch.clone();
        }

        let mut update_topology = false;
        let mut rebalance = false;
        let mut update_stable_topology = false;
        if self.rebalance_type == RebalanceType::None {
            update_topology = true;
        } else if balanced_ch == cur_ch {
            log::trace!("The balanced CH is the same as the current CH, not rebalancing");
            update_topology = cache_topology.pending_ch.is_some() || remove_members;
            // After a cluster view change that leaves only 1 node, we don't need either a topology update or a rebalance
            // but we must still update the stable topology
            update_stable_topology = cache_topology.pending_ch.is_none()
                && self.stable_topology.as_ref().is_none_or(|s| cache_topology.topology_id != s.topology_id);
        } else {
            rebalance = true;
        }

        if update_topology {
            let members = balanced_ch.members().to_vec();
            let new_topology = CacheTopology::new(
                new_topology_id,
                cache_topology.rebalance_id,
                false,
                Some(balanced_ch),
                None,
                None,
                Phase::NoRebalance,
                members.clone(),
                self.map_addresses(&members)?,
            )?;
            log::trace!("Updating cache {} topology without rebalance", self.cache_name);
            self.set_current_topology(Some(new_topology.clone()));
            self.hooks.broadcast_topology_update(&self.cache_name, &new_topology, self.availability_mode);
        } else if rebalance {
            let new_phase = match self.rebalance_type {
                RebalanceType::FourPhase => Phase::ReadOldWriteAll,
                RebalanceType::TwoPhase => Phase::Transitory,
                RebalanceType::None => return Err(TopologyError("java.lang.IllegalStateException".into())),
            };
            let members = balanced_ch.members().to_vec();
            let new_topology = CacheTopology::new(
                new_topology_id,
                new_rebalance_id,
                false,
                Some(cur_ch),
                Some(balanced_ch),
                None,
                new_phase,
                members.clone(),
                self.map_addresses(&members)?,
            )?;
            log::trace!("Updating cache {} topology for rebalance", self.cache_name);
            self.set_current_topology(Some(new_topology.clone()));
            self.rebalance_in_progress = true;
            debug_assert!(self.rebalance_confirmation_collector.is_none());
            self.rebalance_confirmation_collector = Some(RebalanceConfirmationCollector::new(
                &self.cache_name,
                new_topology.topology_id,
                new_topology.members(),
                CollectorAction::EndRebalance,
            ));
            self.hooks.broadcast_rebalance_start(&self.cache_name, &new_topology);
        } else if update_stable_topology {
            let current = self.current_topology.clone().unwrap();
            self.stable_topology = Some(current.clone());
            self.hooks.broadcast_stable_topology_update(&self.cache_name, &current);
        }
        Ok(())
    }

    /// `isRebalanceEnabled` (`:863-865`).
    pub fn is_rebalance_enabled(&self) -> bool {
        self.rebalancing_enabled && self.hooks.is_rebalancing_enabled()
    }

    /// `setRebalanceEnabled` (`:866-875`).
    pub fn set_rebalance_enabled(&mut self, enabled: bool) -> Result<()> {
        self.rebalancing_enabled = enabled;
        if self.rebalancing_enabled {
            log::debug!("Rebalancing is now enabled for cache {}", self.cache_name);
            self.start_queued_rebalance()?;
        } else {
            log::debug!("Rebalancing is now disabled for cache {}", self.cache_name);
        }
        Ok(())
    }

    /// `forceRebalance` (`:876-878`).
    pub fn force_rebalance(&mut self) -> Result<()> {
        let members = self.current_topology.as_ref().map(|t| t.members().to_vec()).unwrap_or_default();
        self.queue_rebalance(&members)
    }

    /// `forceAvailabilityMode` (`:879-884`).
    pub fn force_availability_mode(&mut self, new_mode: AvailabilityMode) {
        if self.current_topology.is_some() && Some(new_mode) != self.availability_mode {
            super::availability::on_manual_availability_change(self, new_mode);
        }
    }

    /// `resolveConflictsOnMerge` (`:898-906`): `mergePolicy=NONE`, so never.
    pub fn resolve_conflicts_on_merge(&self) -> bool {
        false
    }

    /// `restartConflictResolution` (`:945-972`): no conflict resolution is ever in progress.
    pub fn restart_conflict_resolution(&mut self, _members: &[Address]) -> bool {
        false
    }
}

/// `updateMembersPreservingOrder` (`:853-862`).
fn update_members_preserving_order(old_members: &[Address], new_members: &[Address]) -> Vec<Address> {
    let mut preserving: Vec<Address> = old_members.iter().filter(|a| new_members.contains(a)).cloned().collect();
    for a in new_members {
        if !preserving.contains(a) {
            preserving.push(a.clone());
        }
    }
    preserving
}

impl std::fmt::Debug for ClusterCacheStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCacheStatus")
            .field("cache_name", &self.cache_name)
            .field("members", &self.expected_members)
            .field("joiners", &self.joiners)
            .field("current_topology", &self.current_topology)
            .finish()
    }
}
