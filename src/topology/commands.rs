//! The topology `ReplicableCommand`s (core 14.0.35.Final, `commands/topology/`,
//! `topology/HeartBeatCommand.java`) with the `ReplicableCommandExternalizer` framing
//! (`marshall/exts/ReplicableCommandExternalizer.java:84-99`): `ID_INTERNAL`, externalizer id 31,
//! a core-command flag byte, the `short` command id, then `command.writeTo`.

use super::cache_topology::{CacheTopology, ConsistentHash, Phase};
use super::messages::{AvailabilityMode, CacheJoinInfo};
use super::wire::{ObjectInput, ObjectOutput};
use crate::ch::address::Address;
use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{
    marshall_enum_ordinal, marshall_size, marshall_string, unmarshall_enum_ordinal, unmarshall_size, unmarshall_string,
};

/// The externalizer id of `ReplicableCommandExternalizer`, which frames every topology command.
pub const ID_REPLICABLE_COMMAND: u8 = 31;

/// `HeartBeatCommand.COMMAND_ID`.
pub const HEARTBEAT_COMMAND_ID: u8 = 30;
/// `CacheJoinCommand.COMMAND_ID`.
pub const CACHE_JOIN_COMMAND_ID: u8 = 85;
/// `CacheLeaveCommand.COMMAND_ID`.
pub const CACHE_LEAVE_COMMAND_ID: u8 = 86;
/// `RebalancePhaseConfirmCommand.COMMAND_ID`.
pub const REBALANCE_PHASE_CONFIRM_COMMAND_ID: u8 = 87;
/// `RebalanceStatusRequestCommand.COMMAND_ID`.
pub const REBALANCE_STATUS_REQUEST_COMMAND_ID: u8 = 90;
/// `RebalanceStartCommand.COMMAND_ID`.
pub const REBALANCE_START_COMMAND_ID: u8 = 92;
/// `TopologyUpdateCommand.COMMAND_ID`.
pub const TOPOLOGY_UPDATE_COMMAND_ID: u8 = 95;
/// `CacheStatusRequestCommand.COMMAND_ID`.
pub const CACHE_STATUS_REQUEST_COMMAND_ID: u8 = 96;
/// `TopologyUpdateStableCommand.COMMAND_ID`.
pub const TOPOLOGY_UPDATE_STABLE_COMMAND_ID: u8 = 97;

/// The fields `RebalanceStartCommand`/`TopologyUpdateCommand` copy out of a `CacheTopology`
/// (`persistentUUIDs` and `actualMembers` are written with `MarshallUtil.marshallCollection`).
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyFields {
    /// The consistent hash of the current topology.
    pub current_ch: Option<ConsistentHash>,
    /// The consistent hash being rebalanced to, during a rebalance.
    pub pending_ch: Option<ConsistentHash>,
    /// The rebalance phase.
    pub phase: Phase,
    /// The members that have the cache running.
    pub actual_members: Vec<Address>,
    /// The members' persistent UUIDs, in `actual_members` order.
    pub persistent_uuids: Option<Vec<Address>>,
    /// The rebalance id.
    pub rebalance_id: i32,
    /// The topology id.
    pub topology_id: i32,
    /// The JGroups view id the topology was built in.
    pub view_id: i32,
}

impl TopologyFields {
    /// The fields of `t`, sent in view `view_id`.
    pub fn of(t: &CacheTopology, view_id: i32) -> Self {
        Self {
            current_ch: t.current_ch.clone(),
            pending_ch: t.pending_ch.clone(),
            phase: t.phase,
            actual_members: t.actual_members.clone(),
            persistent_uuids: t.persistent_uuids.clone(),
            rebalance_id: t.rebalance_id,
            topology_id: t.topology_id,
            view_id,
        }
    }

    /// What `invokeAsync` builds: `new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH,
    /// phase, actualMembers, persistentUUIDs)` (no union CH).
    pub fn to_cache_topology(&self) -> std::result::Result<CacheTopology, String> {
        CacheTopology::new(
            self.topology_id,
            self.rebalance_id,
            false,
            self.current_ch.clone(),
            self.pending_ch.clone(),
            None,
            self.phase,
            self.actual_members.clone(),
            self.persistent_uuids.clone(),
        )
    }
}

/// A topology command. `origin` is `transient` upstream (set from the message's sender), so it is
/// not part of the wire form and not modelled here.
#[derive(Debug, Clone, PartialEq)]
pub enum TopologyCommand {
    /// joiner to coordinator (id 85)
    CacheJoin {
        /// The cache the command is about.
        cache_name: String,
        /// The joiner's cache configuration.
        join_info: CacheJoinInfo,
        /// The JGroups view id the sender was in.
        view_id: i32,
    },
    /// member to coordinator when a cache stops (id 86)
    CacheLeave {
        /// The cache the command is about.
        cache_name: String,
        /// The JGroups view id the sender was in.
        view_id: i32,
    },
    /// member to coordinator after each phase (id 87); only a `null` throwable is modelled
    RebalancePhaseConfirm {
        /// The cache the command is about.
        cache_name: String,
        /// The topology whose phase the member finished.
        topology_id: i32,
        /// The JGroups view id the sender was in.
        view_id: i32,
    },
    /// member to coordinator at start (id 90)
    RebalanceStatusRequest {
        /// The cache asked about; `None` asks about the whole cluster.
        cache_name: Option<String>,
    },
    /// coordinator to all (id 92)
    RebalanceStart {
        /// The cache the command is about.
        cache_name: String,
        /// The topology to install.
        fields: TopologyFields,
    },
    /// coordinator to all (id 95); the mode is `null` (`None`) for a cache recovered by a merge under
    /// `PreferAvailabilityStrategy` (`updateTopologiesAfterMerge(.., null)`) until its mode changes again
    TopologyUpdate {
        /// The cache the command is about.
        cache_name: String,
        /// The topology to install.
        fields: TopologyFields,
        /// The cache's availability mode.
        availability_mode: Option<AvailabilityMode>,
    },
    /// new coordinator to all during recovery (id 96)
    CacheStatusRequest {
        /// The JGroups view id the sender was in.
        view_id: i32,
    },
    /// coordinator to all (id 97)
    TopologyUpdateStable {
        /// The cache the command is about.
        cache_name: String,
        /// The stable topology.
        fields: TopologyFields,
        /// Whether the topology was restored from persistent state.
        topology_restored: bool,
    },
    /// coordinator to all on every view change (id 30)
    HeartBeat,
}

impl TopologyCommand {
    /// The command's id on the wire, one of the `*_COMMAND_ID` constants.
    pub fn command_id(&self) -> u8 {
        match self {
            Self::CacheJoin { .. } => CACHE_JOIN_COMMAND_ID,
            Self::CacheLeave { .. } => CACHE_LEAVE_COMMAND_ID,
            Self::RebalancePhaseConfirm { .. } => REBALANCE_PHASE_CONFIRM_COMMAND_ID,
            Self::RebalanceStatusRequest { .. } => REBALANCE_STATUS_REQUEST_COMMAND_ID,
            Self::RebalanceStart { .. } => REBALANCE_START_COMMAND_ID,
            Self::TopologyUpdate { .. } => TOPOLOGY_UPDATE_COMMAND_ID,
            Self::CacheStatusRequest { .. } => CACHE_STATUS_REQUEST_COMMAND_ID,
            Self::TopologyUpdateStable { .. } => TOPOLOGY_UPDATE_STABLE_COMMAND_ID,
            Self::HeartBeat => HEARTBEAT_COMMAND_ID,
        }
    }

    /// The marshalled request payload (`marshaller.objectToBuffer(command)`).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut o = ObjectOutput::new();
        o.write_internal_header(ID_REPLICABLE_COMMAND);
        o.w.write_byte(0); // core command, not a module command
        o.w.write_short(self.command_id() as i16);
        self.write_to(&mut o);
        o.into_bytes()
    }

    /// `ReplicableCommand.writeTo`.
    fn write_to(&self, o: &mut ObjectOutput) {
        match self {
            Self::CacheJoin { cache_name, join_info, view_id } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.write_cache_join_info(Some(join_info));
                o.w.write_int(*view_id);
            }
            Self::CacheLeave { cache_name, view_id } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.w.write_int(*view_id);
            }
            Self::RebalancePhaseConfirm { cache_name, topology_id, view_id } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.write_null(); // throwable
                o.w.write_int(*topology_id);
                o.w.write_int(*view_id);
            }
            Self::RebalanceStatusRequest { cache_name } => marshall_string(&mut o.w, cache_name.as_deref()),
            Self::RebalanceStart { cache_name, fields } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.write_consistent_hash(fields.current_ch.as_ref());
                o.write_consistent_hash(fields.pending_ch.as_ref());
                marshall_enum_ordinal(&mut o.w, Some(fields.phase as u8));
                write_collection(o, Some(&fields.actual_members));
                write_collection(o, fields.persistent_uuids.as_deref());
                o.w.write_int(fields.topology_id);
                o.w.write_int(fields.rebalance_id);
                o.w.write_int(fields.view_id);
            }
            Self::TopologyUpdate { cache_name, fields, availability_mode } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.write_consistent_hash(fields.current_ch.as_ref());
                o.write_consistent_hash(fields.pending_ch.as_ref());
                marshall_enum_ordinal(&mut o.w, Some(fields.phase as u8));
                write_collection(o, Some(&fields.actual_members));
                write_collection(o, fields.persistent_uuids.as_deref());
                marshall_enum_ordinal(&mut o.w, availability_mode.map(|m| m as u8));
                o.w.write_int(fields.topology_id);
                o.w.write_int(fields.rebalance_id);
                o.w.write_int(fields.view_id);
            }
            Self::TopologyUpdateStable { cache_name, fields, topology_restored } => {
                marshall_string(&mut o.w, Some(cache_name));
                o.write_consistent_hash(fields.current_ch.as_ref());
                o.write_consistent_hash(fields.pending_ch.as_ref());
                write_collection(o, Some(&fields.actual_members));
                write_collection(o, fields.persistent_uuids.as_deref());
                o.w.write_int(fields.topology_id);
                o.w.write_int(fields.rebalance_id);
                o.w.write_int(fields.view_id);
                o.w.write_boolean(*topology_restored);
            }
            Self::CacheStatusRequest { view_id } => o.w.write_int(*view_id),
            Self::HeartBeat => {}
        }
    }

    /// `ReplicableCommandExternalizer.readObject` for the topology commands.
    pub fn from_bytes(buf: &[u8]) -> Result<TopologyCommand> {
        let mut i = ObjectInput::new(buf);
        i.expect_internal(ID_REPLICABLE_COMMAND, "ReplicableCommand header")?;
        let _core_flag = i.r.read_byte()?;
        let id = i.r.read_short()? as u8;
        let name = |i: &mut ObjectInput| -> Result<String> {
            unmarshall_string(&mut i.r)?.ok_or_else(|| Error::Malformed("null cache name".into()))
        };
        Ok(match id {
            CACHE_JOIN_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                let join_info = i.read_cache_join_info()?.ok_or_else(|| Error::Malformed("null join info".into()))?;
                let view_id = i.r.read_int()?;
                Self::CacheJoin { cache_name, join_info, view_id }
            }
            CACHE_LEAVE_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                Self::CacheLeave { cache_name, view_id: i.r.read_int()? }
            }
            REBALANCE_PHASE_CONFIRM_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                let tag = i.r.read_unsigned_byte()?;
                if tag != super::wire::ID_NULL {
                    return Err(Error::Malformed("RebalancePhaseConfirmCommand with a throwable is not modelled".into()));
                }
                let topology_id = i.r.read_int()?;
                Self::RebalancePhaseConfirm { cache_name, topology_id, view_id: i.r.read_int()? }
            }
            REBALANCE_STATUS_REQUEST_COMMAND_ID => Self::RebalanceStatusRequest { cache_name: unmarshall_string(&mut i.r)? },
            REBALANCE_START_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                let current_ch = i.read_consistent_hash()?;
                let pending_ch = i.read_consistent_hash()?;
                let phase = read_phase(&mut i)?;
                let actual_members = read_collection(&mut i)?.unwrap_or_default();
                let persistent_uuids = read_collection(&mut i)?;
                let topology_id = i.r.read_int()?;
                let rebalance_id = i.r.read_int()?;
                let view_id = i.r.read_int()?;
                Self::RebalanceStart {
                    cache_name,
                    fields: TopologyFields { current_ch, pending_ch, phase, actual_members, persistent_uuids, rebalance_id, topology_id, view_id },
                }
            }
            TOPOLOGY_UPDATE_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                let current_ch = i.read_consistent_hash()?;
                let pending_ch = i.read_consistent_hash()?;
                let phase = read_phase(&mut i)?;
                let actual_members = read_collection(&mut i)?.unwrap_or_default();
                let persistent_uuids = read_collection(&mut i)?;
                let availability_mode = unmarshall_enum_ordinal(&mut i.r)?.map(AvailabilityMode::from_ordinal).transpose()?;
                let topology_id = i.r.read_int()?;
                let rebalance_id = i.r.read_int()?;
                let view_id = i.r.read_int()?;
                Self::TopologyUpdate {
                    cache_name,
                    fields: TopologyFields { current_ch, pending_ch, phase, actual_members, persistent_uuids, rebalance_id, topology_id, view_id },
                    availability_mode,
                }
            }
            TOPOLOGY_UPDATE_STABLE_COMMAND_ID => {
                let cache_name = name(&mut i)?;
                let current_ch = i.read_consistent_hash()?;
                let pending_ch = i.read_consistent_hash()?;
                let actual_members = read_collection(&mut i)?.unwrap_or_default();
                let persistent_uuids = read_collection(&mut i)?;
                let topology_id = i.r.read_int()?;
                let rebalance_id = i.r.read_int()?;
                let view_id = i.r.read_int()?;
                let topology_restored = i.r.read_boolean()?;
                Self::TopologyUpdateStable {
                    cache_name,
                    fields: TopologyFields {
                        current_ch,
                        pending_ch,
                        phase: Phase::NoRebalance,
                        actual_members,
                        persistent_uuids,
                        rebalance_id,
                        topology_id,
                        view_id,
                    },
                    topology_restored,
                }
            }
            CACHE_STATUS_REQUEST_COMMAND_ID => Self::CacheStatusRequest { view_id: i.r.read_int()? },
            HEARTBEAT_COMMAND_ID => Self::HeartBeat,
            other => return Err(Error::UnknownTag { what: "topology command id", value: other as i64 }),
        })
    }
}

fn read_phase(i: &mut ObjectInput) -> Result<Phase> {
    Phase::from_ordinal(unmarshall_enum_ordinal(&mut i.r)?.ok_or_else(|| Error::Malformed("null phase".into()))?)
}

/// `MarshallUtil.marshallCollection(addresses, out)`: size then `writeObject` of each element.
fn write_collection(o: &mut ObjectOutput, c: Option<&[Address]>) {
    match c {
        None => marshall_size(&mut o.w, -1),
        Some(list) => {
            marshall_size(&mut o.w, list.len() as i32);
            for a in list {
                o.write_address(a);
            }
        }
    }
}

fn read_collection(i: &mut ObjectInput) -> Result<Option<Vec<Address>>> {
    let size = unmarshall_size(&mut i.r)?;
    if size < 0 {
        return Ok(None);
    }
    Ok(Some((0..size).map(|_| i.read_address()).collect::<Result<Vec<_>>>()?))
}
