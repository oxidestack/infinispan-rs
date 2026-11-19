//! The slice of `DefaultConsistentHash` (core 14.0.35.Final,
//! `distribution/ch/impl/DefaultConsistentHash.java`, `AbstractConsistentHash.java`) that
//! `SyncConsistentHashFactory` and `SyncReplicatedConsistentHashFactory` use: construction,
//! `locateOwnersForSegment`, `getCapacityFactors`, equality. Persistence (`ScopedPersistentState`)
//! is out of scope: MockServer does not enable global state.
// Index loops mirror upstream's `for (int i = 0; ...)` so the line-by-line comparison stays easy.
#![allow(clippy::needless_range_loop)]


use super::address::{Address, CapacityFactors};
use super::java_util::HashMapInit;

/// `DefaultConsistentHash`: each segment's owners, the first of them primary.
#[derive(Debug, Clone)]
pub struct DefaultConsistentHash {
    num_owners: i32,
    members: Vec<Address>,
    /// `float[]` aligned with `members` (`AbstractConsistentHash.capacityFactors`), or `None`.
    capacity_factors: Option<Vec<f32>>,
    segment_owners: Vec<Vec<Address>>,
}

impl DefaultConsistentHash {
    /// The public constructor (`:49-62`). Panics with Java's `IllegalArgumentException` text.
    pub fn new(
        num_owners: i32,
        num_segments: i32,
        members: &[Address],
        capacity_factors: Option<&CapacityFactors>,
        segment_owners: Vec<Vec<Address>>,
    ) -> Self {
        assert!(num_segments >= 1, "The number of segments must be strictly positive");
        assert!(num_owners >= 1, "The number of owners must be strictly positive");
        let cf = capacity_factors.map(|m| {
            members.iter().map(|a| m.get(a).expect("capacity factor for member")).collect::<Vec<_>>()
        });
        for s in 0..num_segments as usize {
            assert!(!segment_owners[s].is_empty(), "Segment owner list cannot be null or empty");
        }
        Self { num_owners, members: members.to_vec(), capacity_factors: cf, segment_owners }
    }

    /// The number of segments.
    pub fn num_segments(&self) -> usize {
        self.segment_owners.len()
    }

    /// How many owners each segment should have.
    pub fn num_owners(&self) -> i32 {
        self.num_owners
    }

    /// The members.
    pub fn members(&self) -> &[Address] {
        &self.members
    }

    /// The owners of `segment`, primary first.
    pub fn locate_owners_for_segment(&self, segment: usize) -> &[Address] {
        &self.segment_owners[segment]
    }

    /// The primary owner of `segment`.
    pub fn locate_primary_owner_for_segment(&self, segment: usize) -> &Address {
        &self.segment_owners[segment][0]
    }

    /// `getCapacityFactors()`: a fresh `HashMap` built in member order (`AbstractConsistentHash.java:128`).
    pub fn capacity_factors(&self) -> Option<CapacityFactors> {
        self.capacity_factors.as_ref().map(|cf| {
            let entries = self.members.iter().cloned().zip(cf.iter().copied()).collect();
            CapacityFactors::java_hash_map(entries, HashMapInit::Capacity(self.members.len() as i32))
        })
    }
}

impl PartialEq for DefaultConsistentHash {
    /// `DefaultConsistentHash.equals` (`:197-213`): numOwners, members, per-segment owner lists.
    fn eq(&self, o: &Self) -> bool {
        self.num_owners == o.num_owners && self.members == o.members && self.segment_owners == o.segment_owners
    }
}
