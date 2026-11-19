//! `CacheRpcCommand`s as `CacheRpcCommandExternalizer` frames them (`marshall/exts/CacheRpcCommandExternalizer.java:139-160`):
//! `ID_INTERNAL`, externalizer id 36, the core-command byte, the `short` command id, the cache name as a
//! `ByteString` (length byte + UTF-8), then the command's parameters. A data write travels inside a
//! `SingleRpcCommand` (id 1, `commands/remote/SingleRpcCommand.java:50-57`: `writeObject(command)`).

use super::object::CacheObject;
use crate::marshall::marshall_util::{marshall_size, unmarshall_size};
use crate::statetransfer::chunk::StateChunk;
use crate::statetransfer::int_set::IntSet;
use super::write::{WriteCommand, flags};
use crate::marshall::error::{Error, Result};
use crate::topology::wire::{ID_NULL, ObjectInput, ObjectOutput};

/// The externalizer id of `CacheRpcCommandExternalizer`, which frames every cache command.
pub const ID_CACHE_RPC_COMMAND: u8 = 36;
/// `SingleRpcCommand.COMMAND_ID`: a write command sent to the owners.
pub const SINGLE_RPC_COMMAND_ID: u8 = 1;
/// `ClusteredGetCommand.COMMAND_ID`.
pub const CLUSTERED_GET_COMMAND_ID: u8 = 16;
/// `StateResponseCommand.COMMAND_ID`.
pub const STATE_RESPONSE_COMMAND_ID: u8 = 20;
/// `StateTransferStartCommand.COMMAND_ID`.
pub const STATE_TRANSFER_START_COMMAND_ID: u8 = 116;
/// `StateTransferCancelCommand.COMMAND_ID`.
pub const STATE_TRANSFER_CANCEL_COMMAND_ID: u8 = 117;

/// `StateTransferStartCommand` / `StateTransferCancelCommand` (`commands/statetransfer/
/// AbstractStateTransferCommand.java:55-68`): the segments, then the topology id.
#[derive(Debug, Clone, PartialEq)]
pub struct StateTransferSegments {
    /// The topology the transfer belongs to.
    pub topology_id: i32,
    /// The segments to start or cancel.
    pub segments: IntSet,
}

/// `StateResponseCommand` (`commands/statetransfer/StateResponseCommand.java:104-116`): `pushTransfer`,
/// the chunks as a collection, `applyState`, then the topology id.
#[derive(Debug, Clone, PartialEq)]
pub struct StateResponse {
    /// The topology the state belongs to.
    pub topology_id: i32,
    /// The entries, a chunk per segment.
    pub chunks: Vec<StateChunk>,
    /// Whether the receiver applies the state to its data container.
    pub apply_state: bool,
    /// Whether the provider pushed the state rather than the consumer pulling it.
    pub push_transfer: bool,
}

/// `ClusteredGetCommand` (`commands/remote/ClusteredGetCommand.java:57-145`).
#[derive(Debug, Clone, PartialEq)]
pub struct ClusteredGet {
    /// The key read.
    pub key: CacheObject,
    /// The key's segment, if known.
    pub segment: Option<i32>,
    /// `Flag` bits, the [`crate::commands::write::flags`] constants.
    pub flags: i64,
    /// Whether the read is issued by a write command on the originator.
    pub is_write: bool,
    /// The topology the command was sent in.
    pub topology_id: i32,
}

/// What a [`CacheRpcCommand`] carries.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // short-lived value, never stored in bulk
pub enum CacheRpcPayload {
    /// `SingleRpcCommand`: a write command.
    Single(WriteCommand),
    /// `ClusteredGetCommand`: a remote read.
    ClusteredGet(ClusteredGet),
    /// `StateTransferStartCommand`: asks the provider to send these segments.
    StateStart(StateTransferSegments),
    /// `StateTransferCancelCommand`: asks the provider to stop sending these segments.
    StateCancel(StateTransferSegments),
    /// `StateResponseCommand`: the state of some segments.
    StateResponse(StateResponse),
}

/// A `CacheRpcCommand`: a command addressed to one named cache on another node.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheRpcCommand {
    /// The cache the command is for.
    pub cache_name: String,
    /// The command.
    pub payload: CacheRpcPayload,
}

impl CacheRpcCommand {
    /// A `SingleRpcCommand` carrying `command` to `cache_name`.
    pub fn single(cache_name: &str, command: WriteCommand) -> Self {
        Self { cache_name: cache_name.to_string(), payload: CacheRpcPayload::Single(command) }
    }

    fn command_id(&self) -> u8 {
        match self.payload {
            CacheRpcPayload::Single(_) => SINGLE_RPC_COMMAND_ID,
            CacheRpcPayload::ClusteredGet(_) => CLUSTERED_GET_COMMAND_ID,
            CacheRpcPayload::StateStart(_) => STATE_TRANSFER_START_COMMAND_ID,
            CacheRpcPayload::StateCancel(_) => STATE_TRANSFER_CANCEL_COMMAND_ID,
            CacheRpcPayload::StateResponse(_) => STATE_RESPONSE_COMMAND_ID,
        }
    }

    /// Whether `buf` is a `CacheRpcCommand` frame (`ID_INTERNAL` 36) rather than a topology command (31).
    pub fn is_cache_rpc(buf: &[u8]) -> bool {
        ObjectInput::new(buf).expect_internal(ID_CACHE_RPC_COMMAND, "CacheRpcCommand header").is_ok()
    }

    /// The cache name of a frame, without decoding its payload (a state chunk is large).
    pub fn peek_cache_name(buf: &[u8]) -> Result<String> {
        let mut i = ObjectInput::new(buf);
        i.expect_internal(ID_CACHE_RPC_COMMAND, "CacheRpcCommand header")?;
        let _core = i.r.read_byte()?;
        let _id = i.r.read_short()?;
        let len = i.r.read_unsigned_byte()? as usize;
        let name = i.r.read_raw(len)?;
        String::from_utf8(name.to_vec()).map_err(|e| Error::Malformed(e.to_string()))
    }

    /// `marshaller.objectToBuffer(command)`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut o = ObjectOutput::new();
        o.write_internal_header(ID_CACHE_RPC_COMMAND);
        o.w.write_byte(0);
        o.w.write_short(self.command_id() as i16);
        // ByteString.writeObject
        let name = self.cache_name.as_bytes();
        o.w.write_byte(name.len() as i8);
        o.w.write_raw(name);
        match &self.payload {
            CacheRpcPayload::Single(c) => c.write(&mut o),
            CacheRpcPayload::ClusteredGet(g) => {
                o.write_cache_object(&g.key);
                match g.segment {
                    Some(s) => {
                        o.w.write_boolean(true);
                        o.write_unsigned_int(s as u32);
                    }
                    None => o.w.write_boolean(false),
                }
                o.w.write_long(flags::without_remotable(g.flags));
                o.w.write_boolean(g.is_write);
                o.w.write_int(g.topology_id);
            }
            CacheRpcPayload::StateStart(c) | CacheRpcPayload::StateCancel(c) => {
                o.write_int_set(&c.segments);
                o.w.write_int(c.topology_id);
            }
            CacheRpcPayload::StateResponse(c) => {
                o.w.write_boolean(c.push_transfer);
                marshall_size(&mut o.w, c.chunks.len() as i32);
                for chunk in &c.chunks {
                    o.write_state_chunk(chunk);
                }
                o.w.write_boolean(c.apply_state);
                o.w.write_int(c.topology_id);
            }
        }
        o.into_bytes()
    }

    /// Reads a command from its bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let mut i = ObjectInput::new(buf);
        i.expect_internal(ID_CACHE_RPC_COMMAND, "CacheRpcCommand header")?;
        let _core = i.r.read_byte()?;
        let id = i.r.read_short()? as u8;
        let len = i.r.read_unsigned_byte()? as usize;
        let name = i.r.read_raw(len)?;
        let cache_name = String::from_utf8(name.to_vec()).map_err(|e| Error::Malformed(e.to_string()))?;
        let payload = match id {
            SINGLE_RPC_COMMAND_ID => CacheRpcPayload::Single(WriteCommand::read(&mut i)?),
            CLUSTERED_GET_COMMAND_ID => {
                let key = i.read_cache_object()?;
                let segment = if i.r.read_boolean()? { Some(i.read_unsigned_int()? as i32) } else { None };
                let flags = i.r.read_long()?;
                let is_write = i.r.read_boolean()?;
                let topology_id = i.r.read_int()?;
                CacheRpcPayload::ClusteredGet(ClusteredGet { key, segment, flags, is_write, topology_id })
            }
            STATE_TRANSFER_START_COMMAND_ID | STATE_TRANSFER_CANCEL_COMMAND_ID => {
                let segments = i.read_int_set()?;
                let topology_id = i.r.read_int()?;
                let c = StateTransferSegments { topology_id, segments };
                if id == STATE_TRANSFER_START_COMMAND_ID { CacheRpcPayload::StateStart(c) } else { CacheRpcPayload::StateCancel(c) }
            }
            STATE_RESPONSE_COMMAND_ID => {
                let push_transfer = i.r.read_boolean()?;
                let n = unmarshall_size(&mut i.r)?;
                let chunks = (0..n.max(0)).map(|_| i.read_state_chunk()).collect::<Result<Vec<_>>>()?;
                let apply_state = i.r.read_boolean()?;
                let topology_id = i.r.read_int()?;
                CacheRpcPayload::StateResponse(StateResponse { topology_id, chunks, apply_state, push_transfer })
            }
            other => return Err(Error::UnknownTag { what: "cache rpc command id", value: other as i64 }),
        };
        let _ = ID_NULL;
        Ok(Self { cache_name, payload })
    }
}
