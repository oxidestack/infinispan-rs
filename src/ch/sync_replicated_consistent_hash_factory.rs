//! Port of `SyncReplicatedConsistentHashFactory` (core 14.0.35.Final,
//! `distribution/ch/impl/SyncReplicatedConsistentHashFactory.java`): a one-owner
//! `SyncConsistentHashFactory` CH, flattened to `ReplicatedConsistentHash.primaryOwners`.

use super::address::{Address, CapacityFactors};
use super::default_consistent_hash::DefaultConsistentHash;
use super::replicated_consistent_hash::ReplicatedConsistentHash;
use super::sync_consistent_hash_factory as sync;

/// `create` (`:30-36`): `numOwners` is ignored; a replicated CH always uses 1 for the placement.
pub fn create(
    _num_owners: i32,
    num_segments: i32,
    members: &[Address],
    capacity_factors: Option<&CapacityFactors>,
) -> Result<ReplicatedConsistentHash, String> {
    let dch = sync::create(1, num_segments, members, capacity_factors)?;
    let without = compute_members_without_state(members, None, capacity_factors)?;
    Ok(replicated_from_default(&dch, without))
}

/// `replicatedFromDefault` (`:49-58`).
fn replicated_from_default(dch: &DefaultConsistentHash, members_without_state: Vec<Address>) -> ReplicatedConsistentHash {
    let members = dch.members();
    let primary_owners: Vec<i32> = (0..dch.num_segments())
        .map(|s| {
            let po = dch.locate_primary_owner_for_segment(s);
            members.iter().position(|m| m == po).map(|i| i as i32).unwrap_or(-1)
        })
        .collect();
    let cf = dch.capacity_factors();
    ReplicatedConsistentHash::new(members.to_vec(), cf.as_ref(), members_without_state, primary_owners)
}

/// `updateMembers` (`:60-67`).
pub fn update_members(
    base: &ReplicatedConsistentHash,
    new_members: &[Address],
    actual_capacity_factors: Option<&CapacityFactors>,
) -> Result<ReplicatedConsistentHash, String> {
    let base_dch = default_from_replicated(base);
    let dch = sync::update_members(&base_dch, new_members, actual_capacity_factors)?;
    let without = compute_members_without_state(new_members, Some(base.members()), actual_capacity_factors)?;
    Ok(replicated_from_default(&dch, without))
}

/// `defaultFromReplicated` (`:69-77`).
fn default_from_replicated(base: &ReplicatedConsistentHash) -> DefaultConsistentHash {
    let owners: Vec<Vec<Address>> =
        (0..base.num_segments()).map(|s| vec![base.locate_primary_owner_for_segment(s).clone()]).collect();
    DefaultConsistentHash::new(1, base.num_segments() as i32, base.members(), base.capacity_factors(), owners)
}

/// `rebalance` (`:79-82`).
pub fn rebalance(base: &ReplicatedConsistentHash) -> Result<ReplicatedConsistentHash, String> {
    create(base.num_owners() as i32, base.num_segments() as i32, base.members(), base.capacity_factors())
}

/// `union` (`:84-87`).
pub fn union(ch1: &ReplicatedConsistentHash, ch2: &ReplicatedConsistentHash) -> Result<ReplicatedConsistentHash, String> {
    ch1.union(ch2)
}

/// `computeMembersWithoutState` (`:89-112`).
pub fn compute_members_without_state(
    new_members: &[Address],
    old_members: Option<&[Address]>,
    capacity_factors: Option<&CapacityFactors>,
) -> Result<Vec<Address>, String> {
    let mut without = Vec::new();
    if let Some(cf) = capacity_factors {
        let mut has_node_with_capacity = false;
        for a in new_members {
            let f = cf.get(a).expect("capacity factor");
            if f != 0.0 && f != 1.0 {
                return Err(format!("Invalid replicated cache capacity factor for node {a:?}"));
            }
            if f == 0.0 || old_members.is_some_and(|o| !o.contains(a)) {
                without.push(a.clone());
            } else {
                has_node_with_capacity = true;
            }
        }
        if !has_node_with_capacity {
            return Err("There must be at least one node with a non-zero capacity factor".into());
        }
    }
    Ok(without)
}
