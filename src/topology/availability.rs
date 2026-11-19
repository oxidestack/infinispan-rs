//! `PreferAvailabilityStrategy` (core 14.0.35.Final,
//! `partitionhandling/impl/PreferAvailabilityStrategy.java`) and `AvailabilityStrategy.
//! ownersConsistentHash`: the strategy `ClusterTopologyManagerImpl.initCacheStatusIfAbsent` picks
//! when `whenSplit` is `ALLOW_READ_WRITES` (the default, `PartitionHandlingConfiguration.java:20-47`).
//! The context is the [`ClusterCacheStatus`] itself, as upstream.

use super::cache_topology::{CacheTopology, ConsistentHash, Phase};
use super::cluster_cache_status::{ClusterCacheStatus, Result, TopologyError, dist_lost_data_check};
use super::messages::{AvailabilityMode, CacheStatusResponse, ConsistentHashFactoryKind};
use crate::ch::address::Address;

/// `AvailabilityStrategy.ownersConsistentHash` (`:21-48`).
pub fn owners_consistent_hash(topology: &CacheTopology, factory: ConsistentHashFactoryKind) -> Result<ConsistentHash> {
    let npe = || TopologyError("java.lang.NullPointerException".into());
    Ok(match topology.phase {
        Phase::NoRebalance | Phase::Transitory | Phase::ConflictResolution | Phase::ReadOldWriteAll => {
            topology.current_ch.clone().ok_or_else(npe)?
        }
        Phase::ReadAllWriteAll => factory.union(
            topology.current_ch.as_ref().ok_or_else(npe)?,
            topology.pending_ch.as_ref().ok_or_else(npe)?,
        )?,
        Phase::ReadNewWriteAll => topology.pending_ch.clone().ok_or_else(npe)?,
    })
}

/// `onJoin` (`:21-24`).
pub fn on_join(ctx: &mut ClusterCacheStatus, _joiner: &Address) -> Result<()> {
    let members = ctx.expected_members().to_vec();
    ctx.queue_rebalance(&members)
}

/// `onGracefulLeave` (`:26-45`).
pub fn on_graceful_leave(ctx: &mut ClusterCacheStatus, leaver: &Address) -> Result<()> {
    let current = ctx.current_topology().cloned().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
    let mut new_members = current.members().to_vec();
    new_members.retain(|a| a != leaver);
    if new_members.is_empty() {
        log::debug!("The last node of cache {} left", ctx.cache_name());
        return ctx.update_current_topology(&new_members);
    }
    if let Some(stable) = ctx.stable_topology()
        && let Some(ch) = &stable.current_ch
        && dist_lost_data_check(ch, &new_members)
    {
        log::warn!("Lost data because of graceful leaver {leaver:?}");
    }
    // We have to do this in case rebalancing is disabled, or there is another rebalance in progress
    ctx.update_current_topology(&new_members)?;
    // If CR is already in progress we have to restart it and a rebalance will occur once CR completes
    if !ctx.restart_conflict_resolution(&new_members) {
        ctx.queue_rebalance(&new_members)?;
    }
    Ok(())
}

/// `onClusterViewChange` (`:47-65`).
pub fn on_cluster_view_change(ctx: &mut ClusterCacheStatus, cluster_members: &[Address]) -> Result<()> {
    let current = ctx.current_topology().cloned().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
    let mut new_members = current.members().to_vec();
    let before = new_members.len();
    new_members.retain(|a| cluster_members.contains(a));
    if new_members.len() == before {
        log::trace!("Cache {} did not lose any members, skipping rebalance", ctx.cache_name());
        return Ok(());
    }
    check_for_lost_data(ctx.cache_name(), ctx.stable_topology(), &new_members)?;
    if !ctx.restart_conflict_resolution(&new_members) {
        // We have to do the update in case rebalancing is disabled, or there is another rebalance in progress
        ctx.update_current_topology(&new_members)?;
        ctx.queue_rebalance(&new_members)?;
    }
    Ok(())
}

/// `checkForLostData` (`:67-76`): the event-log entries become log lines.
fn check_for_lost_data(cache_name: &str, stable: Option<&CacheTopology>, new_members: &[Address]) -> Result<()> {
    let stable = stable.ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
    let stable_members = stable.members();
    let lost: Vec<&Address> = stable_members.iter().filter(|m| !new_members.contains(m)).collect();
    let ch = stable.current_ch.as_ref().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
    if dist_lost_data_check(ch, new_members) {
        log::error!("Cache {cache_name}: lost data because of abrupt leavers {lost:?}");
    } else if lost.len() as f64 >= (stable_members.len() as f64 / 2.0).ceil() {
        log::warn!("Cache {cache_name}: minority partition, lost {lost:?} of {stable_members:?}");
    }
    Ok(())
}

/// `Partition` (`:300-345`).
#[derive(Debug, Clone)]
struct Partition {
    topology: CacheTopology,
    stable_topology: Option<CacheTopology>,
    read_ch: ConsistentHash,
    actual_members: Vec<Address>,
    actual_read_owners: Vec<Address>,
    senders: Vec<Address>,
    conflict_resolution_only: bool,
}

impl Partition {
    fn new(
        sender: &Address,
        topology: CacheTopology,
        stable_topology: Option<CacheTopology>,
        read_ch: ConsistentHash,
        new_members: &[Address],
    ) -> Self {
        let actual_members = topology.actual_members.iter().filter(|a| new_members.contains(a)).cloned().collect();
        let actual_read_owners = read_ch.members().iter().filter(|a| new_members.contains(a)).cloned().collect();
        Self {
            topology,
            stable_topology,
            read_ch,
            actual_members,
            actual_read_owners,
            senders: vec![sender.clone()],
            conflict_resolution_only: false,
        }
    }

    /// `isPreferable` (`:320-338`).
    fn is_preferable(&self, other: Option<&Partition>) -> bool {
        let Some(other) = other else { return true };
        // `isPreferable` (`:320-338`) is a chain of `if (...) return true`; folded into one condition.
        let (o_senders, senders) = (other.senders.len(), self.senders.len());
        o_senders < senders
            || (o_senders == senders && other.actual_read_owners.len() < self.actual_read_owners.len())
            || (o_senders == senders
                && other.actual_read_owners.len() == self.actual_read_owners.len()
                && other.actual_members.len() < self.actual_members.len())
            // Partitions are already sorted in reverse topology order, but we make an explicit check for clarity
            || other.topology.topology_id < self.topology.topology_id
    }
}

/// `computePartitions` (`:211-291`). `status_responses` is in the iteration order of the
/// `HashMap<Address, CacheStatusResponse>` upstream collects into.
fn compute_partitions(status_responses: &[(Address, CacheStatusResponse)], cache_name: &str) -> Result<Vec<Partition>> {
    let all_senders: Vec<Address> = status_responses.iter().map(|(a, _)| a.clone()).collect();
    // The topology is not updated atomically across the cluster, so even members of the same partition
    // can report different topologies
    let mut partitions: Vec<Partition> = Vec::new();
    for (sender, response) in status_responses {
        let Some(topology) = &response.cache_topology else { continue };
        if !topology.members().contains(sender) {
            // The node hasn't properly joined yet, so it can't be part of a partition
            continue;
        }
        let info = response.cache_join_info.as_ref().ok_or_else(|| TopologyError("java.lang.NullPointerException".into()))?;
        let read_ch = owners_consistent_hash(topology, info.consistent_hash_factory)?;
        let p = Partition::new(sender, topology.clone(), response.stable_topology.clone(), read_ch, &all_senders);
        if p.actual_members.is_empty() {
            // None of the members of this partition have send status responses, ignore it
            continue;
        }
        partitions.push(p);
    }
    // Sort partitions in reverse topology id order to simplify the algorithm (stable, as `List.sort`)
    partitions.sort_by_key(|p| std::cmp::Reverse(p.topology.topology_id));
    let mut i = 0;
    while i < partitions.len() {
        log::trace!("Cache {cache_name} keeping partition from {:?}", partitions[i].senders);
        let mut j = i + 1;
        while j < partitions.len() {
            let p_read_members = partitions[j].read_ch.members().to_vec();
            // If read owners don't overlap then we clearly have 2 independent partitions
            if !partitions[i].read_ch.members().iter().any(|m| p_read_members.contains(m)) {
                j += 1;
                continue;
            }
            // Normally the difference between the topology ids seen by different members is 0 or 1.
            // But only rebalance start and phase changes are kept in step,
            // topology updates are fire-and-forget, so the difference can be higher
            // even when communication between nodes is not interrupted.
            let mut fold = false;
            let reference_topology_id = partitions[i].topology.topology_id;
            let topology_id = partitions[j].topology.topology_id;
            if topology_id == reference_topology_id {
                if partitions[j].topology == partitions[i].topology {
                    // The topology is exactly the same, ignore it
                    fold = true;
                }
            } else {
                // topologyId < referenceTopologyId
                // Small topology differences are expected even between properly communicating nodes,
                // but we should consider it a different partition if p's sender might have some entries
                // that the other members lost.
                // Having a majority doesn't matter, because the lost data check ensures p's sender
                // couldn't make any updates without talking to the reference partition's members
                if !dist_lost_data_check(&partitions[j].read_ch, &partitions[i].actual_members) {
                    fold = true;
                } else {
                    partitions[j].conflict_resolution_only = true;
                }
            }
            if fold {
                let removed = partitions.remove(j);
                partitions[i].senders.extend(removed.senders);
            } else {
                j += 1;
            }
        }
        i += 1;
    }
    Ok(partitions)
}

/// `selectPreferredPartition` (`:174-185`).
fn select_preferred_partition(partitions: &[Partition]) -> usize {
    let mut preferred: Option<usize> = None;
    for (idx, p) in partitions.iter().enumerate() {
        // TODO upstream: compare the number of segments owned by the senders
        if !p.conflict_resolution_only && p.is_preferable(preferred.map(|i| &partitions[i])) {
            preferred = Some(idx);
        }
    }
    preferred.expect("a preferred partition")
}

/// `computePreferredTopology` (`:191-194`): ignore the context, only compute the preferred topology.
pub fn compute_preferred_topology(status_responses: &[(Address, CacheStatusResponse)]) -> Result<Option<CacheTopology>> {
    let partitions = compute_partitions(status_responses, "")?;
    Ok(if partitions.is_empty() { None } else { Some(partitions[select_preferred_partition(&partitions)].topology.clone()) })
}

/// `onPartitionMerge` (`:61-173`).
pub fn on_partition_merge(ctx: &mut ClusterCacheStatus, status_responses: &[(Address, CacheStatusResponse)]) -> Result<()> {
    let cache_name = ctx.cache_name().to_string();
    let new_members = ctx.expected_members().to_vec();
    let partitions = compute_partitions(status_responses, &cache_name)?;
    let persistent = |ctx: &ClusterCacheStatus, m: &[Address]| ctx_map_addresses(ctx, m);

    if partitions.is_empty() {
        log::debug!("No current topology, recovered only joiners for cache {cache_name}");
        ctx.update_current_topology(&new_members)?;
        // Then start a rebalance with the expected members
        return ctx.queue_rebalance(&new_members);
    }

    if partitions.len() == 1 {
        // Single partition, only the coordinator changed
        // Usually this means the old coordinator left the cluster, but the other nodes are all alive
        let p = &partitions[0];
        log::debug!("Recovered a single partition for cache {cache_name}: {:?}", p.topology);
        // Cancel any pending rebalance and only use the read CH,
        // because we don't recover the rebalance confirmation status (yet).
        let mut surviving: Vec<Address> = new_members.clone();
        let before = surviving.len();
        surviving.retain(|a| p.read_ch.members().contains(a));
        if surviving.len() != before {
            check_for_lost_data(&cache_name, p.stable_topology.as_ref(), &surviving)?;
        }
        let merged = CacheTopology::new(
            p.topology.topology_id + 1,
            p.topology.rebalance_id + 1,
            false,
            Some(p.read_ch.clone()),
            None,
            None,
            Phase::NoRebalance,
            surviving.clone(),
            Some(persistent(ctx, &surviving)?),
        )?;
        ctx.update_topologies_after_merge(merged, p.stable_topology.clone(), None);
        if surviving.is_empty() {
            // No surviving members, use the expected members instead
            surviving = new_members.clone();
        }
        // Remove leavers first
        if surviving.as_slice() != p.topology.members() {
            ctx.update_current_topology(&surviving)?;
        }
        // Then start a rebalance with the expected members
        return ctx.queue_rebalance(&new_members);
    }

    // Merge with multiple topologies that are not clear descendants of each other
    let preferred = &partitions[select_preferred_partition(&partitions)];
    // Increment the topology id so that it's bigger than any topology that might have been sent by the old
    // coordinator. +1 is enough because there nodes wait for the new JGroups view before answering the status
    // request, and after they have the new view they can't process topology updates with the old view id.
    let mut merge_topology_id = 0;
    let mut merge_rebalance_id = 0;
    for p in &partitions {
        if merge_topology_id <= p.topology.topology_id {
            merge_topology_id = p.topology.topology_id + 1;
        }
        if merge_rebalance_id <= p.topology.rebalance_id {
            merge_rebalance_id = p.topology.rebalance_id + 1;
        }
    }
    // Record all distinct read owners for conflict resolution (never resolved here: mergePolicy=NONE)
    let mut possible_owners: Vec<Address> = Vec::new();
    for p in &partitions {
        for m in p.read_ch.members() {
            if !possible_owners.contains(m) {
                possible_owners.push(m.clone());
            }
        }
    }
    // Remove nodes that have not yet fully rejoined the cluster
    possible_owners.retain(|a| new_members.contains(a));
    let resolve_conflicts = ctx.resolve_conflicts_on_merge() && possible_owners.len() > 1;
    debug_assert!(!resolve_conflicts);

    let mut actual_members = new_members.clone();
    actual_members.retain(|a| preferred.read_ch.members().contains(a));
    for p in &partitions {
        if !std::ptr::eq(p, preferred) {
            log::warn!("Ignoring cache topology from {:?}: {:?}", p.senders, p.topology);
        }
    }
    // Cancel any pending rebalance and only use the read CH,
    // because we don't recover the rebalance confirmation status (yet).
    let merged = CacheTopology::new(
        merge_topology_id,
        merge_rebalance_id,
        false,
        Some(preferred.read_ch.clone()),
        None,
        None,
        Phase::NoRebalance,
        actual_members.clone(),
        Some(persistent(ctx, &actual_members)?),
    )?;
    ctx.update_topologies_after_merge(merged, preferred.stable_topology.clone(), None);
    // First update the CHs to remove any nodes that left from the current topology
    if !preferred.read_ch.members().iter().all(|m| actual_members.contains(m)) {
        check_for_lost_data(&cache_name, preferred.stable_topology.as_ref(), &actual_members)?;
    }
    debug_assert!(!actual_members.is_empty());
    // Initialize the cache with the joiners
    ctx.update_current_topology(&actual_members)?;
    // Then start a rebalance with the merged members
    ctx.queue_rebalance(&new_members)
}

fn ctx_map_addresses(ctx: &ClusterCacheStatus, members: &[Address]) -> Result<Vec<Address>> {
    ctx.persistent_uuids_for(members)
}

/// `onRebalanceEnd` (`:276-279`): nothing, if we need another rebalance it's already in the queue.
pub fn on_rebalance_end(_ctx: &mut ClusterCacheStatus) {}

/// `onManualAvailabilityChange` (`:280-283`): the cache should always be AVAILABLE.
pub fn on_manual_availability_change(_ctx: &mut ClusterCacheStatus, _new_mode: AvailabilityMode) {}
