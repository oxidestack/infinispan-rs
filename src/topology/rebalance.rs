//! Rebalance bookkeeping (core 14.0.35.Final): `RebalanceType` (`statetransfer/RebalanceType.java`),
//! `RebalancingStatus` (`topology/RebalancingStatus.java`) and `RebalanceConfirmationCollector`
//! (`topology/RebalanceConfirmationCollector.java`), the barrier each of the four rebalance phases waits
//! on before the coordinator installs the next topology.

use super::messages::CacheMode;
use crate::ch::address::Address;

/// `RebalanceType` (`statetransfer/RebalanceType.java`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceType {
    /// No rebalance: a local or invalidation cache.
    None,
    /// The four-phase rebalance of replicated and distributed caches.
    FourPhase,
    /// The two-phase rebalance of scattered caches.
    TwoPhase,
}

impl RebalanceType {
    /// `RebalanceType.from(CacheMode)`.
    pub fn from_cache_mode(m: CacheMode) -> RebalanceType {
        match m {
            CacheMode::Local | CacheMode::InvalidationSync | CacheMode::InvalidationAsync => RebalanceType::None,
            CacheMode::ReplSync | CacheMode::ReplAsync | CacheMode::DistSync | CacheMode::DistAsync => RebalanceType::FourPhase,
            CacheMode::ScatteredSync => RebalanceType::TwoPhase,
        }
    }
}

/// `RebalancingStatus`; the discriminant is the enum ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalancingStatus {
    /// Rebalancing is disabled.
    Suspended = 0,
    /// A rebalance is due but has not started.
    Pending = 1,
    /// A rebalance is in progress.
    InProgress = 2,
    /// No rebalance is in progress or due.
    Complete = 3,
}

/// `RebalanceConfirmationCollector`; completion runs [`CollectorAction`] on the status.
#[derive(Debug, Clone)]
pub struct RebalanceConfirmationCollector {
    cache_name: String,
    topology_id: i32,
    confirmations_needed: Vec<Address>,
    when_completed: CollectorAction,
}

/// The `Runnable` each collector is built with (`this::endRebalance` etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorAction {
    /// `endRebalance`.
    EndRebalance,
    /// `endReadAllPhase`.
    EndReadAllPhase,
    /// `endReadNewPhase`.
    EndReadNewPhase,
}

impl RebalanceConfirmationCollector {
    /// A collector waiting for a confirmation of `topology_id` from each of `members`, then running `when_completed`.
    pub fn new(cache_name: &str, topology_id: i32, members: &[Address], when_completed: CollectorAction) -> Self {
        let mut needed: Vec<Address> = Vec::new();
        for m in members {
            if !needed.contains(m) {
                needed.push(m.clone());
            }
        }
        log::trace!("Initialized topology confirmation collector {topology_id}@{cache_name}, initial list is {needed:?}");
        Self { cache_name: cache_name.to_string(), topology_id, confirmations_needed: needed, when_completed }
    }

    /// `confirmPhase`: `Some(action)` when everyone has confirmed. Like upstream it logs, but does
    /// not return, when the confirmation carries an older topology id (`:41-46`).
    pub fn confirm_phase(&mut self, node: &Address, received_topology_id: i32) -> Option<CollectorAction> {
        if self.topology_id > received_topology_id {
            log::trace!(
                "Ignoring rebalance confirmation with old topology from {node:?} for cache {}, expecting topology id {} but got {}",
                self.cache_name, self.topology_id, received_topology_id
            );
        }
        let Some(pos) = self.confirmations_needed.iter().position(|a| a == node) else {
            log::trace!("Rebalance confirmation collector {}@{} ignored confirmation for {node:?}, already confirmed", self.topology_id, self.cache_name);
            return None;
        };
        self.confirmations_needed.remove(pos);
        if self.confirmations_needed.is_empty() { Some(self.when_completed) } else { None }
    }

    /// `updateMembers`: `Some(action)` only the first time the list becomes empty by this call.
    pub fn update_members(&mut self, new_members: &[Address]) -> Option<CollectorAction> {
        let before = self.confirmations_needed.len();
        self.confirmations_needed.retain(|a| new_members.contains(a));
        let modified = self.confirmations_needed.len() != before;
        if modified && self.confirmations_needed.is_empty() { Some(self.when_completed) } else { None }
    }

    /// The members that have not confirmed yet.
    pub fn confirmations_needed(&self) -> &[Address] {
        &self.confirmations_needed
    }
}

