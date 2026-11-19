//! Port of `ReplicatedConsistentHash` (core 14.0.35.Final,
//! `distribution/ch/impl/ReplicatedConsistentHash.java`): every member that holds state owns every
//! segment; only the primary owner (`primaryOwners[segment]`, an index into `members`) is
//! distributed. The persistence constructor (`ScopedPersistentState`) is out of scope.

use super::address::{Address, CapacityFactors};
use super::java_util::HashMapInit;

/// `ReplicatedConsistentHash`: every member with state owns every segment; each segment has one primary owner.
#[derive(Debug, Clone)]
pub struct ReplicatedConsistentHash {
    primary_owners: Vec<i32>,
    members: Vec<Address>,
    members_with_state: Vec<Address>,
    members_without_state: Vec<Address>,
    /// `Immutables.immutableMapCopy(capacityFactors)`: stored in the copy's Java iteration order.
    capacity_factors: Option<CapacityFactors>,
}

impl ReplicatedConsistentHash {
    /// `ReplicatedConsistentHash(List, int[])` (`:49-51`).
    pub fn with_members(members: Vec<Address>, primary_owners: Vec<i32>) -> Self {
        Self::new(members, None, Vec::new(), primary_owners)
    }

    /// The four-argument constructor (`:53-61`).
    pub fn new(
        members: Vec<Address>,
        capacity_factors: Option<&CapacityFactors>,
        members_without_state: Vec<Address>,
        primary_owners: Vec<i32>,
    ) -> Self {
        let members_with_state = compute_members_with_state(&members, &members_without_state);
        Self {
            primary_owners,
            members,
            members_with_state,
            members_without_state,
            capacity_factors: capacity_factors.map(CapacityFactors::copied),
        }
    }

    /// `union` (`:63-107`).
    pub fn union(&self, ch2: &ReplicatedConsistentHash) -> Result<ReplicatedConsistentHash, String> {
        if self.num_segments() != ch2.num_segments() {
            return Err("The consistent hash objects must have the same number of segments".into());
        }
        let mut union_members = self.members.clone();
        for m in &ch2.members {
            if !self.members.contains(m) {
                union_members.push(m.clone());
            }
        }
        let mut union_without = self.members_without_state.clone();
        for m in &ch2.members_without_state {
            if !ch2.members_with_state.contains(m) && !union_without.contains(m) {
                union_without.push(m.clone());
            }
        }
        let primary_owners: Vec<i32> = (0..self.num_segments())
            .map(|s| {
                let po = self.locate_primary_owner_for_segment(s);
                union_members.iter().position(|m| m == po).map(|i| i as i32).unwrap_or(-1)
            })
            .collect();

        let ones = |ms: &[Address]| ms.iter().map(|a| (a.clone(), 1.0f32)).collect::<Vec<_>>();
        let union_cf: Option<CapacityFactors> = match (&self.capacity_factors, &ch2.capacity_factors) {
            (None, None) => None,
            (None, Some(c2)) => {
                // new HashMap<>(ch2.capacityFactors) then put 1.0 for this.members
                let mut entries = c2.0.clone();
                for (a, f) in ones(&self.members) {
                    put_entry(&mut entries, a, f);
                }
                Some(CapacityFactors::java_hash_map(entries, HashMapInit::CopyOf(c2.len())))
            }
            (Some(c1), None) => {
                let mut entries = c1.0.clone();
                for (a, f) in ones(&ch2.members) {
                    put_entry(&mut entries, a, f);
                }
                Some(CapacityFactors::java_hash_map(entries, HashMapInit::CopyOf(c1.len())))
            }
            (Some(c1), Some(c2)) => {
                let mut entries = c1.0.clone();
                for (a, f) in &c2.0 {
                    put_entry(&mut entries, a.clone(), *f);
                }
                Some(CapacityFactors::java_hash_map(entries, HashMapInit::CopyOf(c1.len())))
            }
        };
        Ok(ReplicatedConsistentHash::new(union_members, union_cf.as_ref(), union_without, primary_owners))
    }

    /// The number of segments.
    pub fn num_segments(&self) -> usize {
        self.primary_owners.len()
    }

    /// `getNumOwners`: the number of members that hold state.
    pub fn num_owners(&self) -> usize {
        self.members_with_state.len()
    }

    /// The members, in order.
    pub fn members(&self) -> &[Address] {
        &self.members
    }

    /// The members that hold no state (zero capacity).
    pub fn members_without_state(&self) -> &[Address] {
        &self.members_without_state
    }

    /// Each segment's primary owner, as an index into [`Self::members`].
    pub fn primary_owners(&self) -> &[i32] {
        &self.primary_owners
    }

    /// The members' capacity factors, when set.
    pub fn capacity_factors(&self) -> Option<&CapacityFactors> {
        self.capacity_factors.as_ref()
    }

    /// `locateOwnersForSegment` (`:222-232`): the primary first, then the other stateful members in
    /// member order.
    pub fn locate_owners_for_segment(&self, segment: usize) -> Vec<Address> {
        let primary = self.locate_primary_owner_for_segment(segment).clone();
        let mut owners = Vec::with_capacity(self.members_with_state.len());
        owners.push(primary.clone());
        for m in &self.members_with_state {
            if *m != primary {
                owners.push(m.clone());
            }
        }
        owners
    }

    /// The primary owner of `segment`.
    pub fn locate_primary_owner_for_segment(&self, segment: usize) -> &Address {
        &self.members[self.primary_owners[segment] as usize]
    }

    /// `getSegmentsForOwner` (`:241-251`): every segment for a member with state, else none.
    pub fn segments_for_owner(&self, owner: &Address) -> Vec<usize> {
        if self.members_with_state.contains(owner) { (0..self.num_segments()).collect() } else { Vec::new() }
    }

    /// `getPrimarySegmentsForOwner` (`:253-266`).
    pub fn primary_segments_for_owner(&self, owner: &Address) -> Vec<usize> {
        match self.members.iter().position(|m| m == owner) {
            None => Vec::new(),
            Some(idx) => (0..self.primary_owners.len()).filter(|&i| self.primary_owners[i] == idx as i32).collect(),
        }
    }

    /// Whether `node` holds `segment`: true for any member with state.
    pub fn is_segment_local_to_node(&self, node: &Address, _segment: usize) -> bool {
        self.members_with_state.contains(node)
    }

    /// Always true.
    pub fn is_replicated(&self) -> bool {
        true
    }

    /// `getRoutingTableAsString` (`:268-285`), with `Address.toString()` supplied by the caller.
    pub fn routing_table_as_string(&self, addr_to_string: impl Fn(&Address) -> String) -> String {
        let mut sb = String::new();
        for (i, po) in self.primary_owners.iter().enumerate() {
            if i > 0 {
                sb.push_str(", ");
            }
            sb.push_str(&format!("{i}: {po}"));
        }
        if !self.members_without_state.is_empty() {
            sb.push_str("none:");
            for a in &self.members_without_state {
                sb.push(' ');
                sb.push_str(&addr_to_string(a));
            }
        }
        sb
    }

    /// `hashCode` mixes only members, membersWithoutState and primaryOwners (`:442-449`).
    pub fn java_hash_code(&self) -> i32 {
        fn list_hash(l: &[Address]) -> i32 {
            l.iter().fold(1i32, |h, a| h.wrapping_mul(31).wrapping_add(a.hash_code()))
        }
        let mut result = 1i32;
        result = result.wrapping_mul(31).wrapping_add(list_hash(&self.members));
        result = result.wrapping_mul(31).wrapping_add(list_hash(&self.members_without_state));
        let arr = self.primary_owners.iter().fold(1i32, |h, v| h.wrapping_mul(31).wrapping_add(*v));
        result.wrapping_mul(31).wrapping_add(arr)
    }
}

fn put_entry(entries: &mut Vec<(Address, f32)>, a: Address, f: f32) {
    if let Some(e) = entries.iter_mut().find(|(k, _)| *k == a) {
        e.1 = f;
    } else {
        entries.push((a, f));
    }
}

/// `computeMembersWithState` (`:300-308`).
fn compute_members_with_state(members: &[Address], without: &[Address]) -> Vec<Address> {
    if without.is_empty() {
        members.to_vec()
    } else {
        members.iter().filter(|m| !without.contains(m)).cloned().collect()
    }
}

impl PartialEq for ReplicatedConsistentHash {
    /// `equals` (`:451-474`): members, membersWithoutState, primaryOwners (not capacity factors).
    fn eq(&self, o: &Self) -> bool {
        self.members == o.members
            && self.members_without_state == o.members_without_state
            && self.primary_owners == o.primary_owners
    }
}
