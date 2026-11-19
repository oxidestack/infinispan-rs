//! Port of `CacheTopology` and its `Phase` (core 14.0.35.Final, `topology/CacheTopology.java`),
//! plus the `ConsistentHash` the REPL port can carry.

use super::wire::{ID_NULL, ObjectInput, ObjectOutput, Reusable};
use crate::ch::address::Address;
use crate::ch::replicated_consistent_hash::ReplicatedConsistentHash;
use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{marshall_enum_ordinal, unmarshall_enum_ordinal};

/// The externalizer id of `CacheTopology`.
pub const ID_CACHE_TOPOLOGY: u8 = 37;
/// The id of a cache's first topology.
pub const INITIAL_TOPOLOGY_ID: i32 = 1;
/// The id of a cache's first rebalance.
pub const INITIAL_REBALANCE_ID: i32 = 1;

/// `CacheTopology.Phase` (`:249-290`); the discriminant is the wire ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// `NO_REBALANCE`: no rebalance is in progress; reads and writes go to the current owners.
    NoRebalance = 0,
    /// `TRANSITORY`: a topology installed only to reach the next one (after a merge).
    Transitory = 1,
    /// `CONFLICT_RESOLUTION`: conflicts between partitions are being resolved.
    ConflictResolution = 2,
    /// `READ_OLD_WRITE_ALL`: state is transferred; reads go to the old owners, writes to old and new.
    ReadOldWriteAll = 3,
    /// `READ_ALL_WRITE_ALL`: reads and writes go to old and new owners.
    ReadAllWriteAll = 4,
    /// `READ_NEW_WRITE_ALL`: reads go to the new owners, writes to old and new.
    ReadNewWriteAll = 5,
}

impl Phase {
    /// `Phase.isRebalance`.
    pub fn is_rebalance(self) -> bool {
        matches!(self, Phase::Transitory | Phase::ReadOldWriteAll)
    }

    /// `Phase.valueOf(int)`.
    pub fn from_ordinal(o: u8) -> Result<Phase> {
        Ok(match o {
            0 => Phase::NoRebalance,
            1 => Phase::Transitory,
            2 => Phase::ConflictResolution,
            3 => Phase::ReadOldWriteAll,
            4 => Phase::ReadAllWriteAll,
            5 => Phase::ReadNewWriteAll,
            other => return Err(Error::Malformed(format!("Unknown enum. ordinal {other}"))),
        })
    }

    /// `Enum.name()`, as used in `CacheTopology.toString`.
    pub fn name(self) -> &'static str {
        match self {
            Phase::NoRebalance => "NO_REBALANCE",
            Phase::Transitory => "TRANSITORY",
            Phase::ConflictResolution => "CONFLICT_RESOLUTION",
            Phase::ReadOldWriteAll => "READ_OLD_WRITE_ALL",
            Phase::ReadAllWriteAll => "READ_ALL_WRITE_ALL",
            Phase::ReadNewWriteAll => "READ_NEW_WRITE_ALL",
        }
    }
}

/// The consistent hashes a REPL_SYNC cache uses. (`DefaultConsistentHash` belongs to DIST caches,
/// which this port does not reach: MockServer's cache is REPL_SYNC.)
#[derive(Debug, Clone, PartialEq)]
pub enum ConsistentHash {
    /// A replicated cache's consistent hash.
    Replicated(ReplicatedConsistentHash),
}

impl ConsistentHash {
    /// The members the hash assigns segments to.
    pub fn members(&self) -> &[Address] {
        match self {
            ConsistentHash::Replicated(c) => c.members(),
        }
    }

    /// The replicated consistent hash inside.
    pub fn as_replicated(&self) -> &ReplicatedConsistentHash {
        match self {
            ConsistentHash::Replicated(c) => c,
        }
    }
}

/// `CacheTopology` (`topology/CacheTopology.java`): a cache's consistent hashes and the phase of its rebalance, numbered by a topology id.
#[derive(Debug, Clone)]
pub struct CacheTopology {
    /// The topology id, incremented by every new topology.
    pub topology_id: i32,
    /// The rebalance id, incremented by every rebalance.
    pub rebalance_id: i32,
    /// Whether the topology was restored from persistent state.
    pub restored_from_state: bool,
    /// The consistent hash reads and writes use when no rebalance is in progress.
    pub current_ch: Option<ConsistentHash>,
    /// The consistent hash a rebalance moves to.
    pub pending_ch: Option<ConsistentHash>,
    /// The union of the current and pending hashes, during a rebalance.
    pub union_ch: Option<ConsistentHash>,
    /// The rebalance phase.
    pub phase: Phase,
    /// The members that have the cache running.
    pub actual_members: Vec<Address>,
    /// The persistent UUID of each actual member (`null` when absent).
    pub persistent_uuids: Option<Vec<Address>>,
}

impl CacheTopology {
    /// The widest constructor (`:62-76`), with its two precondition checks.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topology_id: i32,
        rebalance_id: i32,
        restored_topology: bool,
        current_ch: Option<ConsistentHash>,
        pending_ch: Option<ConsistentHash>,
        union_ch: Option<ConsistentHash>,
        phase: Phase,
        actual_members: Vec<Address>,
        persistent_uuids: Option<Vec<Address>>,
    ) -> std::result::Result<Self, String> {
        if let (Some(p), Some(c)) = (&pending_ch, &current_ch)
            && !c.members().iter().all(|m| p.members().contains(m))
        {
            return Err("A cache topology's pending consistent hash must contain all the current consistent hash's members".into());
        }
        if let Some(u) = &persistent_uuids
            && u.len() != actual_members.len()
        {
            return Err("There must be one persistent UUID for each actual member".into());
        }
        Ok(Self {
            topology_id,
            rebalance_id,
            restored_from_state: restored_topology,
            current_ch,
            pending_ch,
            union_ch,
            phase,
            actual_members,
            persistent_uuids,
        })
    }

    /// `getMembers()` (`:112-119`): the pending CH's members, else the current CH's.
    pub fn members(&self) -> &[Address] {
        match (&self.pending_ch, &self.current_ch) {
            (Some(p), _) => p.members(),
            (None, Some(c)) => c.members(),
            _ => &[],
        }
    }

    /// `getReadConsistentHash()` (`:136-161`).
    pub fn read_consistent_hash(&self) -> Option<&ConsistentHash> {
        match self.phase {
            Phase::ConflictResolution | Phase::NoRebalance | Phase::ReadOldWriteAll => self.current_ch.as_ref(),
            Phase::Transitory | Phase::ReadNewWriteAll => self.pending_ch.as_ref(),
            Phase::ReadAllWriteAll => self.union_ch.as_ref(),
        }
    }

    /// `getWriteConsistentHash()` (`:166-186`).
    pub fn write_consistent_hash(&self) -> Option<&ConsistentHash> {
        match self.phase {
            Phase::ConflictResolution | Phase::NoRebalance => self.current_ch.as_ref(),
            Phase::Transitory => self.pending_ch.as_ref(),
            Phase::ReadOldWriteAll | Phase::ReadAllWriteAll | Phase::ReadNewWriteAll => self.union_ch.as_ref(),
        }
    }
}

impl PartialEq for CacheTopology {
    /// `equals` (`:188-207`): persistent UUIDs and `restoredFromState` do not take part.
    fn eq(&self, o: &Self) -> bool {
        self.topology_id == o.topology_id
            && self.rebalance_id == o.rebalance_id
            && self.phase == o.phase
            && self.current_ch == o.current_ch
            && self.pending_ch == o.pending_ch
            && self.union_ch == o.union_ch
            && self.actual_members == o.actual_members
    }
}

impl ObjectOutput {
    /// `writeObject(ConsistentHash)` or `null`.
    pub fn write_consistent_hash(&mut self, ch: Option<&ConsistentHash>) {
        match ch {
            None => self.write_null(),
            Some(ConsistentHash::Replicated(c)) => self.write_replicated_consistent_hash(c),
        }
    }

    /// `writeObject(CacheTopology)` or `null`: `CacheTopology.Externalizer` (`:210-230`).
    pub fn write_cache_topology(&mut self, t: Option<&CacheTopology>) {
        let Some(t) = t else {
            return self.write_null();
        };
        self.write_internal_header(ID_CACHE_TOPOLOGY);
        let t2 = t.clone();
        self.write_reusable(Reusable::Topology(Box::new(t.clone())), true, move |o| {
            o.w.write_int(t2.topology_id);
            o.w.write_int(t2.rebalance_id);
            o.w.write_boolean(t2.restored_from_state);
            o.write_consistent_hash(t2.current_ch.as_ref());
            o.write_consistent_hash(t2.pending_ch.as_ref());
            o.write_consistent_hash(t2.union_ch.as_ref());
            o.write_immutable_address_list(&t2.actual_members);
            match &t2.persistent_uuids {
                None => o.write_null(),
                Some(u) => o.write_array_list_of_addresses(u),
            }
            marshall_enum_ordinal(&mut o.w, Some(t2.phase as u8));
        });
    }
}

impl ObjectInput<'_> {
    /// `readObject()` for a nullable `ConsistentHash`.
    pub fn read_consistent_hash(&mut self) -> Result<Option<ConsistentHash>> {
        let mut peek = self.r;
        let tag = peek.read_unsigned_byte()?;
        if tag == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        let id = peek.read_unsigned_byte()?;
        match id {
            super::wire::ID_REPLICATED_CONSISTENT_HASH => {
                Ok(Some(ConsistentHash::Replicated(self.read_replicated_consistent_hash()?)))
            }
            other => Err(Error::UnknownTag { what: "consistent hash externalizer id (REPL port: only ReplicatedConsistentHash)", value: other as i64 }),
        }
    }

    /// `readObject()` for a nullable `CacheTopology`.
    pub fn read_cache_topology(&mut self) -> Result<Option<CacheTopology>> {
        let tag = {
            let mut peek = self.r;
            peek.read_unsigned_byte()?
        };
        if tag == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        self.expect_internal(ID_CACHE_TOPOLOGY, "CacheTopology header")?;
        match self.read_reusable(true, |o| {
            let topology_id = o.r.read_int()?;
            let rebalance_id = o.r.read_int()?;
            let restored = o.r.read_boolean()?;
            let current = o.read_consistent_hash()?;
            let pending = o.read_consistent_hash()?;
            let union = o.read_consistent_hash()?;
            let actual_members = o.read_address_list()?;
            let persistent = o.read_nullable_address_list()?;
            let phase = Phase::from_ordinal(
                unmarshall_enum_ordinal(&mut o.r)?.ok_or_else(|| Error::Malformed("null phase".into()))?,
            )?;
            let t = CacheTopology::new(topology_id, rebalance_id, restored, current, pending, union, phase, actual_members, persistent)
                .map_err(Error::Malformed)?;
            Ok(Reusable::Topology(Box::new(t)))
        })? {
            Reusable::Topology(t) => Ok(Some(*t)),
            _ => Err(Error::Malformed("back-reference is not a CacheTopology".into())),
        }
    }
}

/// `ConsistentHashFactory` operations over the REPL-reachable hashes. `Sync` (a DIST factory, which
/// builds `DefaultConsistentHash`) is rejected: a REPL_SYNC cache never selects it
/// (`StateTransferManagerImpl.pickConsistentHashFactory`, `:144`).
impl super::messages::ConsistentHashFactoryKind {
    fn unsupported(self) -> String {
        format!("ConsistentHashFactory {self:?} builds DefaultConsistentHash, which this REPL port does not carry")
    }

    /// `create`.
    pub fn create(
        self,
        num_owners: i32,
        num_segments: i32,
        members: &[Address],
        capacity_factors: Option<&crate::ch::address::CapacityFactors>,
    ) -> std::result::Result<ConsistentHash, String> {
        use crate::ch::{replicated_consistent_hash_factory as plain, sync_replicated_consistent_hash_factory as sync};
        use super::messages::ConsistentHashFactoryKind::*;
        match self {
            SyncReplicated => sync::create(num_owners, num_segments, members, capacity_factors).map(ConsistentHash::Replicated),
            Replicated => plain::create(num_owners, num_segments, members, capacity_factors).map(ConsistentHash::Replicated),
            Sync => Err(self.unsupported()),
        }
    }

    /// `updateMembers`.
    pub fn update_members(
        self,
        base: &ConsistentHash,
        new_members: &[Address],
        capacity_factors: Option<&crate::ch::address::CapacityFactors>,
    ) -> std::result::Result<ConsistentHash, String> {
        use crate::ch::{replicated_consistent_hash_factory as plain, sync_replicated_consistent_hash_factory as sync};
        use super::messages::ConsistentHashFactoryKind::*;
        let ConsistentHash::Replicated(b) = base;
        match self {
            SyncReplicated => sync::update_members(b, new_members, capacity_factors).map(ConsistentHash::Replicated),
            Replicated => plain::update_members(b, new_members, capacity_factors).map(ConsistentHash::Replicated),
            Sync => Err(self.unsupported()),
        }
    }

    /// `rebalance`.
    pub fn rebalance(self, base: &ConsistentHash) -> std::result::Result<ConsistentHash, String> {
        use crate::ch::{replicated_consistent_hash_factory as plain, sync_replicated_consistent_hash_factory as sync};
        use super::messages::ConsistentHashFactoryKind::*;
        let ConsistentHash::Replicated(b) = base;
        match self {
            SyncReplicated => sync::rebalance(b).map(ConsistentHash::Replicated),
            Replicated => plain::rebalance(b).map(ConsistentHash::Replicated),
            Sync => Err(self.unsupported()),
        }
    }

    /// `union`.
    pub fn union(self, a: &ConsistentHash, b: &ConsistentHash) -> std::result::Result<ConsistentHash, String> {
        let (ConsistentHash::Replicated(a), ConsistentHash::Replicated(b)) = (a, b);
        match self {
            super::messages::ConsistentHashFactoryKind::Sync => Err(self.unsupported()),
            _ => a.union(b).map(ConsistentHash::Replicated),
        }
    }
}

impl ConsistentHash {
    /// The number of segments.
    pub fn num_segments(&self) -> usize {
        self.as_replicated().num_segments()
    }

    /// `locateOwnersForSegment`.
    pub fn locate_owners_for_segment(&self, segment: usize) -> Vec<Address> {
        self.as_replicated().locate_owners_for_segment(segment)
    }
}
