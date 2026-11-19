//! Port of `ReplicatedConsistentHashFactory` (core 14.0.35.Final,
//! `distribution/ch/impl/ReplicatedConsistentHashFactory.java`): the round-robin replicated CH
//! factory. `StateTransferManagerImpl.pickConsistentHashFactory` (`:144`) picks
//! `SyncReplicatedConsistentHashFactory` for REPL caches, so MockServer never runs this one; it is
//! ported because upstream's `ReplicatedConsistentHashFactoryTest` exercises it.
// Index loops mirror upstream's `for (int i = 0; ...)` so the line-by-line comparison stays easy.
#![allow(clippy::needless_range_loop)]


use std::collections::VecDeque;

use super::address::{Address, CapacityFactors};
use super::replicated_consistent_hash::ReplicatedConsistentHash;
use super::sync_replicated_consistent_hash_factory::compute_members_without_state;

/// `create` (`:33-50`).
pub fn create(
    _num_owners: i32,
    num_segments: i32,
    members: &[Address],
    capacity_factors: Option<&CapacityFactors>,
) -> Result<ReplicatedConsistentHash, String> {
    let without = compute_members_without_state(members, None, capacity_factors)?;
    let mut primary_owners = vec![0i32; num_segments as usize];
    let mut next = 0usize;
    for po in primary_owners.iter_mut() {
        // computeMembersWithoutState ensures that there is at least one member *with* state
        while without.contains(&members[next]) {
            next += 1;
            if next == members.len() {
                next = 0;
            }
        }
        *po = next as i32;
    }
    Ok(ReplicatedConsistentHash::new(members.to_vec(), capacity_factors, without, primary_owners))
}

/// `updateMembers` (`:60-66`).
pub fn update_members(
    base: &ReplicatedConsistentHash,
    new_members: &[Address],
    actual_capacity_factors: Option<&CapacityFactors>,
) -> Result<ReplicatedConsistentHash, String> {
    if new_members == base.members() {
        return Ok(base.clone());
    }
    update_ch(base, new_members, actual_capacity_factors, false)
}

/// `rebalance` (`:178-181`).
pub fn rebalance(base: &ReplicatedConsistentHash) -> Result<ReplicatedConsistentHash, String> {
    update_ch(base, base.members(), base.capacity_factors(), true)
}

/// `union` (`:183-186`).
pub fn union(ch1: &ReplicatedConsistentHash, ch2: &ReplicatedConsistentHash) -> Result<ReplicatedConsistentHash, String> {
    ch1.union(ch2)
}

/// `updateCH` (`:68-150`).
fn update_ch(
    base: &ReplicatedConsistentHash,
    new_members: &[Address],
    actual_capacity_factors: Option<&CapacityFactors>,
    rebalance: bool,
) -> Result<ReplicatedConsistentHash, String> {
    let without = compute_members_without_state(new_members, Some(base.members()), actual_capacity_factors)?;

    let num_segments = base.num_segments();
    let mut primary_owners = vec![0i32; num_segments];
    let mut node_usage = vec![0i32; new_members.len()];
    let mut found_orphans = false;
    for seg in 0..num_segments {
        let po = base.locate_primary_owner_for_segment(seg);
        match new_members.iter().position(|m| m == po) {
            None => {
                primary_owners[seg] = -1;
                found_orphans = true;
            }
            Some(i) => {
                primary_owners[seg] = i as i32;
                node_usage[i] += 1;
            }
        }
    }

    if !found_orphans && !rebalance {
        return Ok(ReplicatedConsistentHash::new(
            new_members.to_vec(),
            actual_capacity_factors,
            without,
            primary_owners,
        ));
    }

    // Exclude members without state by setting their usage to a very high value
    for (i, a) in new_members.iter().enumerate() {
        if without.contains(a) {
            node_usage[i] = i32::MAX;
        }
    }

    // ensure leavers are replaced with existing members so no segments are orphan
    if found_orphans {
        for po in primary_owners.iter_mut() {
            if *po == -1 {
                let least_used = find_least_used_node(&node_usage);
                *po = least_used as i32;
                node_usage[least_used] = node_usage[least_used].wrapping_add(1);
            }
        }
    }

    // ensure even spread of ownership
    let min_segments_per_node = (num_segments / new_members.len()) as i32;
    let mut segments_by_node: Vec<VecDeque<usize>> = vec![VecDeque::new(); new_members.len()];
    for (seg, owner) in primary_owners.iter().enumerate() {
        segments_by_node[*owner as usize].push_back(seg);
    }
    let mut most_used_node = 0usize;
    for node in 0..node_usage.len() {
        while node_usage[node] < min_segments_per_node {
            // we can take segment from any node that has > minSegmentsPerNode + 1, not only the most used
            if node_usage[most_used_node] <= min_segments_per_node + 1 {
                most_used_node = find_most_used_node(&node_usage);
            }
            let seg = segments_by_node[most_used_node].pop_front().expect("segment to take");
            // we don't have to add the segmentId to the new owner's queue
            primary_owners[seg] = node as i32;
            node_usage[most_used_node] -= 1;
            node_usage[node] += 1;
        }
    }

    Ok(ReplicatedConsistentHash::new(new_members.to_vec(), actual_capacity_factors, without, primary_owners))
}

fn find_least_used_node(usage: &[i32]) -> usize {
    let mut res = 0;
    for node in 1..usage.len() {
        if usage[node] < usage[res] {
            res = node;
        }
    }
    res
}

fn find_most_used_node(usage: &[i32]) -> usize {
    let mut res = 0;
    for node in 1..usage.len() {
        if usage[node] > usage[res] {
            res = node;
        }
    }
    res
}
