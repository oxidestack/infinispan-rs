//! The six data-write commands (the five of the cache API and `RemoveExpiredCommand`), with `ReplicableCommandExternalizer`'s framing
//! (`marshall/exts/ReplicableCommandExternalizer.java:78-99`): `ID_INTERNAL`, externalizer id 31, a
//! core-command byte, the `short` command id, `writeTo`'s fields, then the `int` topology id every
//! `TopologyAffectedCommand` appends. Field orders are `PutKeyValueCommand.java:83-94`,
//! `ComputeCommand.java:105-114`, `ReplaceCommand.java:67-78`, `RemoveCommand.java:141-151` and
//! `ClearCommand.java:53-56`; the vectors in `tests/vectors/write_commands.hex` pin them.

use std::sync::atomic::{AtomicI64, Ordering};

use super::compute_function::BiFunctionMapper;
use super::metadata::Metadata;
use super::object::CacheObject;
use crate::ch::address::Address;
use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{marshall_enum_ordinal, unmarshall_enum_ordinal};
use crate::topology::wire::{ID_NULL, ObjectInput, ObjectOutput};

/// The externalizer id of `ReplicableCommandExternalizer`, which frames every command.
pub const ID_REPLICABLE_COMMAND: u8 = 31;
/// `ClearCommand.COMMAND_ID`.
pub const CLEAR_COMMAND_ID: u8 = 5;
/// `PutKeyValueCommand.COMMAND_ID`.
pub const PUT_KEY_VALUE_COMMAND_ID: u8 = 8;
/// `RemoveCommand.COMMAND_ID`.
pub const REMOVE_COMMAND_ID: u8 = 10;
/// `ReplaceCommand.COMMAND_ID`.
pub const REPLACE_COMMAND_ID: u8 = 11;
/// `ComputeCommand.COMMAND_ID`.
pub const COMPUTE_COMMAND_ID: u8 = 68;
/// `RemoveExpiredCommand.COMMAND_ID`.
pub const REMOVE_EXPIRED_COMMAND_ID: u8 = 58;

/// `FlagBitSets` (`1L << Flag.ordinal()`, `context/Flag.java`, `context/impl/FlagBitSets.java`): the
/// bits this path reads or sets.
pub mod flags {
    /// `Flag.ZERO_LOCK_ACQUISITION_TIMEOUT`: fail at once when the key's lock is taken.
    pub const ZERO_LOCK_ACQUISITION_TIMEOUT: i64 = 1 << 0;
    /// `Flag.CACHE_MODE_LOCAL`: apply on this node only, without replicating.
    pub const CACHE_MODE_LOCAL: i64 = 1 << 1;
    /// `Flag.SKIP_LOCKING`: write without acquiring the key's lock.
    pub const SKIP_LOCKING: i64 = 1 << 2;
    /// `Flag.SKIP_CACHE_LOAD`: do not load the previous value from a store.
    pub const SKIP_CACHE_LOAD: i64 = 1 << 7;
    /// `Flag.FAIL_SILENTLY`: a lock that cannot be acquired fails the write without an error; never sent to other nodes.
    pub const FAIL_SILENTLY: i64 = 1 << 8;
    /// `Flag.SKIP_OWNERSHIP_CHECK`: write even when this node does not own the key.
    pub const SKIP_OWNERSHIP_CHECK: i64 = 1 << 15;
    /// `Flag.PUT_FOR_STATE_TRANSFER`: a write state transfer applies.
    pub const PUT_FOR_STATE_TRANSFER: i64 = 1 << 12;
    /// `Flag.IGNORE_RETURN_VALUES`: the caller does not need the previous value.
    pub const IGNORE_RETURN_VALUES: i64 = 1 << 16;
    /// `Flag.SKIP_XSITE_BACKUP`: not backed up to another site.
    pub const SKIP_XSITE_BACKUP: i64 = 1 << 17;
    /// `Flag.SKIP_LISTENER_NOTIFICATION`: no listener is notified.
    pub const SKIP_LISTENER_NOTIFICATION: i64 = 1 << 18;
    /// `Flag.COMMAND_RETRY`: the command is a retry after a topology change.
    pub const COMMAND_RETRY: i64 = 1 << 23;
    /// `Flag.BACKUP_WRITE`: the write is a primary owner's copy to a backup.
    pub const BACKUP_WRITE: i64 = 1 << 31;

    /// `FlagBitSets.copyWithoutRemotableFlags`: `FAIL_SILENTLY` never leaves the originator.
    pub fn without_remotable(f: i64) -> i64 {
        f & !FAIL_SILENTLY
    }
}

/// `ValueMatcher` (`commands/write/ValueMatcher.java`); the discriminant is the enum ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueMatcher {
    /// `MATCH_ALWAYS`: the write always applies.
    MatchAlways = 0,
    /// `MATCH_EXPECTED`: applies when the value is the expected one.
    MatchExpected = 1,
    /// `MATCH_EXPECTED_OR_NEW`: applies when the value is the expected or the new one.
    MatchExpectedOrNew = 2,
    /// `MATCH_EXPECTED_OR_NULL`: applies when the value is the expected one or absent.
    MatchExpectedOrNull = 3,
    /// `MATCH_NON_NULL`: applies when a value is present.
    MatchNonNull = 4,
    /// `MATCH_NEVER`: the write never applies.
    MatchNever = 5,
}

impl ValueMatcher {
    /// The matcher with ordinal `o`; an error for an unknown ordinal.
    pub fn from_ordinal(o: u8) -> Result<Self> {
        Ok(match o {
            0 => Self::MatchAlways,
            1 => Self::MatchExpected,
            2 => Self::MatchExpectedOrNew,
            3 => Self::MatchExpectedOrNull,
            4 => Self::MatchNonNull,
            5 => Self::MatchNever,
            other => return Err(Error::UnknownTag { what: "ValueMatcher ordinal", value: other as i64 }),
        })
    }

    /// `matcherForRetry()`.
    pub fn for_retry(self) -> Self {
        match self {
            Self::MatchExpected | Self::MatchExpectedOrNew => Self::MatchExpectedOrNew,
            Self::MatchExpectedOrNull => Self::MatchExpectedOrNull,
            Self::MatchNonNull | Self::MatchAlways => Self::MatchAlways,
            Self::MatchNever => Self::MatchNever,
        }
    }

    /// `matches(existing, expected, new)`, with the value equality supplied by the caller (Java's
    /// `Object.equals`, see `cache::equality`).
    pub fn matches(
        self,
        existing: Option<&CacheObject>,
        expected: &CacheObject,
        new: &CacheObject,
        eq: &dyn Fn(&CacheObject, &CacheObject) -> bool,
    ) -> bool {
        let expected_null = expected.is_null();
        match self {
            Self::MatchAlways => true,
            Self::MatchExpected => match existing {
                None => expected_null,
                Some(e) => eq(e, expected),
            },
            Self::MatchExpectedOrNew => match existing {
                None => expected_null || new.is_null(),
                Some(e) => eq(e, expected) || eq(e, new),
            },
            Self::MatchExpectedOrNull => match existing {
                None => true,
                Some(e) => eq(e, expected),
            },
            Self::MatchNonNull => existing.is_some(),
            Self::MatchNever => false,
        }
    }
}

static NEXT_INVOCATION_ID: AtomicI64 = AtomicI64::new(0);

/// `CommandInvocationId` (`commands/CommandInvocationId.java:36-52`): the originator's address and a
/// per-process counter.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandInvocationId {
    /// The originator; `None` when the peer sent a null address.
    pub address: Option<Address>,
    /// The originator's counter value.
    pub id: i64,
}

impl CommandInvocationId {
    /// A new id for a command `address` originates.
    pub fn generate(address: Address) -> Self {
        Self { address: Some(address), id: NEXT_INVOCATION_ID.fetch_add(1, Ordering::SeqCst) }
    }
}

impl ObjectOutput {
    fn write_invocation_id(&mut self, id: &CommandInvocationId) {
        match &id.address {
            Some(a) => self.write_address(a),
            None => self.write_null(),
        }
        self.w.write_long(id.id);
    }

    fn write_value_matcher(&mut self, m: ValueMatcher) {
        marshall_enum_ordinal(&mut self.w, Some(m as u8));
    }

    /// `PrivateMetadata` is only ever `null` here (IRAC and entry versioning are not part of this path).
    fn write_internal_metadata(&mut self) {
        self.write_null();
    }
}

impl ObjectInput<'_> {
    fn read_invocation_id(&mut self) -> Result<CommandInvocationId> {
        let mut peek = self.r;
        let address = if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            None
        } else {
            Some(self.read_address()?)
        };
        Ok(CommandInvocationId { address, id: self.r.read_long()? })
    }

    fn read_value_matcher(&mut self) -> Result<ValueMatcher> {
        ValueMatcher::from_ordinal(unmarshall_enum_ordinal(&mut self.r)?.ok_or_else(|| Error::Malformed("null ValueMatcher".into()))?)
    }

    fn read_internal_metadata(&mut self) -> Result<()> {
        if self.r.read_unsigned_byte()? != ID_NULL {
            return Err(Error::Malformed("PrivateMetadata is not modelled".into()));
        }
        Ok(())
    }
}

/// `PutKeyValueCommand` (`commands/write/PutKeyValueCommand.java`): `put`, `putIfAbsent` and their variants.
#[derive(Debug, Clone, PartialEq)]
pub struct PutKeyValue {
    /// The key written.
    pub key: CacheObject,
    /// The value written.
    pub value: CacheObject,
    /// The segment the key maps to.
    pub segment: i32,
    /// The entry's expiration and version metadata; `None` for none.
    pub metadata: Option<Metadata>,
    /// When the write applies, given the value in the cache.
    pub value_matcher: ValueMatcher,
    /// Identifies the command, so a retry is recognised.
    pub invocation_id: CommandInvocationId,
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// Whether this is a `putIfAbsent`.
    pub put_if_absent: bool,
    /// Whether the result is the whole entry rather than only the previous value.
    pub return_entry: bool,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// `ComputeCommand` (`commands/write/ComputeCommand.java`): `compute` and `computeIfPresent`.
#[derive(Debug, Clone, PartialEq)]
pub struct Compute {
    /// The key written.
    pub key: CacheObject,
    /// Whether this is a `computeIfPresent`.
    pub compute_if_present: bool,
    /// The remapping function, sent to the owners.
    pub function: BiFunctionMapper,
    /// The segment the key maps to.
    pub segment: i32,
    /// The entry's expiration and version metadata; `None` for none.
    pub metadata: Option<Metadata>,
    /// Identifies the command, so a retry is recognised.
    pub invocation_id: CommandInvocationId,
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// `ReplaceCommand` (`commands/write/ReplaceCommand.java`): `replace`, conditional or not.
#[derive(Debug, Clone, PartialEq)]
pub struct Replace {
    /// The key written.
    pub key: CacheObject,
    /// The value expected, for a conditional replace.
    pub old_value: CacheObject,
    /// The value written.
    pub new_value: CacheObject,
    /// Whether the result is the whole entry rather than only the previous value.
    pub return_entry: bool,
    /// The segment the key maps to.
    pub segment: i32,
    /// The entry's expiration and version metadata; `None` for none.
    pub metadata: Option<Metadata>,
    /// When the write applies, given the value in the cache.
    pub value_matcher: ValueMatcher,
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// Identifies the command, so a retry is recognised.
    pub invocation_id: CommandInvocationId,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// `RemoveCommand` (`commands/write/RemoveCommand.java`): `remove`, conditional or not.
#[derive(Debug, Clone, PartialEq)]
pub struct Remove {
    /// The key written.
    pub key: CacheObject,
    /// The value expected, for a conditional remove.
    pub value: CacheObject,
    /// Whether the result is the whole entry rather than only the previous value.
    pub return_entry: bool,
    /// The segment the key maps to.
    pub segment: i32,
    /// The entry's expiration and version metadata; `None` for none.
    pub metadata: Option<Metadata>,
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// When the write applies, given the value in the cache.
    pub value_matcher: ValueMatcher,
    /// Identifies the command, so a retry is recognised.
    pub invocation_id: CommandInvocationId,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// `RemoveExpiredCommand` (`commands/write/RemoveExpiredCommand.java`): removes an entry found expired in
/// memory, on the primary only when its value and lifespan still match, and on the backups unconditionally.
/// Its value matcher is always `MATCH_EXPECTED_OR_NULL` and is not serialized.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoveExpired {
    /// The key of the expired entry.
    pub key: CacheObject,
    /// The value found expired.
    pub value: CacheObject,
    /// `None` for a max idle expiration
    pub lifespan: Option<i64>,
    /// Whether the entry expired by max idle rather than lifespan.
    pub max_idle: bool,
    /// The segment the key maps to.
    pub segment: i32,
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// Identifies the command, so a retry is recognised.
    pub invocation_id: CommandInvocationId,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// `ClearCommand` (`commands/write/ClearCommand.java`): removes every entry.
#[derive(Debug, Clone, PartialEq)]
pub struct Clear {
    /// `Flag` bits, the [`flags`] constants.
    pub flags: i64,
    /// The topology the command was sent in; a node in a newer one asks for a retry.
    pub topology_id: i32,
}

/// One of the data-write commands, as the dispatcher sends and receives them.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // short-lived value, never stored in bulk
pub enum WriteCommand {
    /// `PutKeyValueCommand`.
    Put(PutKeyValue),
    /// `ComputeCommand`.
    Compute(Compute),
    /// `ReplaceCommand`.
    Replace(Replace),
    /// `RemoveCommand`.
    Remove(Remove),
    /// `RemoveExpiredCommand`.
    RemoveExpired(RemoveExpired),
    /// `ClearCommand`.
    Clear(Clear),
}

impl WriteCommand {
    /// The command's id on the wire, one of the `*_COMMAND_ID` constants.
    pub fn command_id(&self) -> u8 {
        match self {
            Self::Put(_) => PUT_KEY_VALUE_COMMAND_ID,
            Self::Compute(_) => COMPUTE_COMMAND_ID,
            Self::Replace(_) => REPLACE_COMMAND_ID,
            Self::Remove(_) => REMOVE_COMMAND_ID,
            Self::RemoveExpired(_) => REMOVE_EXPIRED_COMMAND_ID,
            Self::Clear(_) => CLEAR_COMMAND_ID,
        }
    }

    /// The command's [`flags`] bits.
    pub fn flags(&self) -> i64 {
        match self {
            Self::Put(c) => c.flags,
            Self::Compute(c) => c.flags,
            Self::Replace(c) => c.flags,
            Self::Remove(c) => c.flags,
            Self::RemoveExpired(c) => c.flags,
            Self::Clear(c) => c.flags,
        }
    }

    /// Replaces the command's flags.
    pub fn set_flags(&mut self, f: i64) {
        match self {
            Self::Put(c) => c.flags = f,
            Self::Compute(c) => c.flags = f,
            Self::Replace(c) => c.flags = f,
            Self::Remove(c) => c.flags = f,
            Self::RemoveExpired(c) => c.flags = f,
            Self::Clear(c) => c.flags = f,
        }
    }

    /// Whether any bit of `mask` is set.
    pub fn has_any_flag(&self, mask: i64) -> bool {
        self.flags() & mask != 0
    }

    /// The topology id the command was sent in.
    pub fn topology_id(&self) -> i32 {
        match self {
            Self::Put(c) => c.topology_id,
            Self::Compute(c) => c.topology_id,
            Self::Replace(c) => c.topology_id,
            Self::Remove(c) => c.topology_id,
            Self::RemoveExpired(c) => c.topology_id,
            Self::Clear(c) => c.topology_id,
        }
    }

    /// Sets the topology id the command is sent in.
    pub fn set_topology_id(&mut self, t: i32) {
        match self {
            Self::Put(c) => c.topology_id = t,
            Self::Compute(c) => c.topology_id = t,
            Self::Replace(c) => c.topology_id = t,
            Self::Remove(c) => c.topology_id = t,
            Self::RemoveExpired(c) => c.topology_id = t,
            Self::Clear(c) => c.topology_id = t,
        }
    }

    /// `SegmentSpecificCommand.getSegment` (a `ClearCommand` has none).
    pub fn segment(&self) -> Option<i32> {
        match self {
            Self::Put(c) => Some(c.segment),
            Self::Compute(c) => Some(c.segment),
            Self::Replace(c) => Some(c.segment),
            Self::Remove(c) => Some(c.segment),
            Self::RemoveExpired(c) => Some(c.segment),
            Self::Clear(_) => None,
        }
    }

    /// The key written; `None` for a clear.
    pub fn key(&self) -> Option<&CacheObject> {
        match self {
            Self::Put(c) => Some(&c.key),
            Self::Compute(c) => Some(&c.key),
            Self::Replace(c) => Some(&c.key),
            Self::Remove(c) => Some(&c.key),
            Self::RemoveExpired(c) => Some(&c.key),
            Self::Clear(_) => None,
        }
    }

    /// The command's value matcher: `MatchExpectedOrNull` for a `RemoveExpired`, `MatchAlways` for a compute or a clear.
    pub fn value_matcher(&self) -> ValueMatcher {
        match self {
            Self::Put(c) => c.value_matcher,
            Self::Replace(c) => c.value_matcher,
            Self::Remove(c) => c.value_matcher,
            // RemoveExpiredCommand's constructor fixes it and the externalizer never writes it
            Self::RemoveExpired(_) => ValueMatcher::MatchExpectedOrNull,
            // ComputeCommand.getValueMatcher / ClearCommand.getValueMatcher
            Self::Compute(_) | Self::Clear(_) => ValueMatcher::MatchAlways,
        }
    }

    /// Sets the value matcher, for the commands that have one.
    pub fn set_value_matcher(&mut self, m: ValueMatcher) {
        match self {
            Self::Put(c) => c.value_matcher = m,
            Self::Replace(c) => c.value_matcher = m,
            Self::Remove(c) => c.value_matcher = m,
            Self::Compute(_) | Self::Clear(_) | Self::RemoveExpired(_) => {}
        }
    }

    /// `isConditional()`.
    pub fn is_conditional(&self) -> bool {
        match self {
            Self::Put(c) => c.put_if_absent,
            Self::Compute(c) => c.compute_if_present,
            Self::Replace(_) => true,
            Self::Remove(c) => !c.value.is_null(),
            // RemoveExpiredCommand.isConditional
            Self::RemoveExpired(_) => true,
            Self::Clear(_) => false,
        }
    }

    /// `isReturnValueExpected()`: `ClearCommand` says no, `ReplaceCommand` always yes, a `put` yes
    /// when conditional or `IGNORE_RETURN_VALUES` is unset (`AbstractDataWriteCommand`).
    pub fn is_return_value_expected(&self) -> bool {
        match self {
            Self::Clear(_) => false,
            Self::Replace(_) => true,
            Self::Put(c) => c.put_if_absent || c.flags & flags::IGNORE_RETURN_VALUES == 0,
            Self::Compute(c) => c.flags & flags::IGNORE_RETURN_VALUES == 0,
            Self::Remove(c) => c.flags & flags::IGNORE_RETURN_VALUES == 0,
            Self::RemoveExpired(c) => c.flags & flags::IGNORE_RETURN_VALUES == 0,
        }
    }

    /// The command's invocation id; `None` for a clear.
    pub fn invocation_id(&self) -> Option<&CommandInvocationId> {
        match self {
            Self::Put(c) => Some(&c.invocation_id),
            Self::Compute(c) => Some(&c.invocation_id),
            Self::Replace(c) => Some(&c.invocation_id),
            Self::Remove(c) => Some(&c.invocation_id),
            Self::RemoveExpired(c) => Some(&c.invocation_id),
            Self::Clear(_) => None,
        }
    }

    /// `ReplicableCommandExternalizer.writeObject`.
    pub fn write(&self, o: &mut ObjectOutput) {
        o.write_internal_header(ID_REPLICABLE_COMMAND);
        o.w.write_byte(0); // core command, not a module command
        o.w.write_short(self.command_id() as i16);
        match self {
            Self::Put(c) => {
                o.write_cache_object(&c.key);
                o.write_cache_object(&c.value);
                o.write_unsigned_int(c.segment as u32);
                o.write_metadata(c.metadata.as_ref());
                o.write_value_matcher(c.value_matcher);
                o.write_invocation_id(&c.invocation_id);
                o.w.write_long(flags::without_remotable(c.flags));
                o.w.write_boolean(c.put_if_absent);
                o.write_internal_metadata();
                o.w.write_boolean(c.return_entry);
                o.w.write_int(c.topology_id);
            }
            Self::Compute(c) => {
                o.write_cache_object(&c.key);
                o.w.write_boolean(c.compute_if_present);
                o.write_bi_function_mapper(&c.function);
                o.write_unsigned_int(c.segment as u32);
                o.write_metadata(c.metadata.as_ref());
                o.write_invocation_id(&c.invocation_id);
                o.w.write_long(flags::without_remotable(c.flags));
                o.write_internal_metadata();
                o.w.write_int(c.topology_id);
            }
            Self::Replace(c) => {
                o.write_cache_object(&c.key);
                o.write_cache_object(&c.old_value);
                o.write_cache_object(&c.new_value);
                o.w.write_boolean(c.return_entry);
                o.write_unsigned_int(c.segment as u32);
                o.write_metadata(c.metadata.as_ref());
                o.write_value_matcher(c.value_matcher);
                o.w.write_long(flags::without_remotable(c.flags));
                o.write_invocation_id(&c.invocation_id);
                o.write_internal_metadata();
                o.w.write_int(c.topology_id);
            }
            Self::Remove(c) => {
                o.write_cache_object(&c.key);
                o.write_cache_object(&c.value);
                o.w.write_boolean(c.return_entry);
                o.write_unsigned_int(c.segment as u32);
                o.write_metadata(c.metadata.as_ref());
                o.w.write_long(flags::without_remotable(c.flags));
                o.write_value_matcher(c.value_matcher);
                o.write_invocation_id(&c.invocation_id);
                o.write_internal_metadata();
                o.w.write_int(c.topology_id);
            }
            // RemoveExpiredCommand.writeTo (`:94-107`): the invocation id comes first
            Self::RemoveExpired(c) => {
                o.write_invocation_id(&c.invocation_id);
                o.write_cache_object(&c.key);
                o.write_cache_object(&c.value);
                o.write_unsigned_int(c.segment as u32);
                match c.lifespan {
                    Some(l) => {
                        o.w.write_boolean(true);
                        o.w.write_long(l);
                    }
                    None => o.w.write_boolean(false),
                }
                o.w.write_boolean(c.max_idle);
                o.w.write_long(flags::without_remotable(c.flags));
                o.write_internal_metadata();
                o.w.write_int(c.topology_id);
            }
            Self::Clear(c) => {
                o.w.write_long(flags::without_remotable(c.flags));
                o.w.write_int(c.topology_id);
            }
        }
    }

    /// `ReplicableCommandExternalizer.readObject` for the write commands.
    pub fn read(i: &mut ObjectInput) -> Result<WriteCommand> {
        i.expect_internal(ID_REPLICABLE_COMMAND, "ReplicableCommand header")?;
        let _core = i.r.read_byte()?;
        let id = i.r.read_short()? as u8;
        Ok(match id {
            PUT_KEY_VALUE_COMMAND_ID => {
                let key = i.read_cache_object()?;
                let value = i.read_cache_object()?;
                let segment = i.read_unsigned_int()? as i32;
                let metadata = i.read_metadata()?;
                let value_matcher = i.read_value_matcher()?;
                let invocation_id = i.read_invocation_id()?;
                let flags = i.r.read_long()?;
                let put_if_absent = i.r.read_boolean()?;
                i.read_internal_metadata()?;
                let return_entry = i.r.read_boolean()?;
                let topology_id = i.r.read_int()?;
                Self::Put(PutKeyValue { key, value, segment, metadata, value_matcher, invocation_id, flags, put_if_absent, return_entry, topology_id })
            }
            COMPUTE_COMMAND_ID => {
                let key = i.read_cache_object()?;
                let compute_if_present = i.r.read_boolean()?;
                let function = i.read_bi_function_mapper()?;
                let segment = i.read_unsigned_int()? as i32;
                let metadata = i.read_metadata()?;
                let invocation_id = i.read_invocation_id()?;
                let flags = i.r.read_long()?;
                i.read_internal_metadata()?;
                let topology_id = i.r.read_int()?;
                Self::Compute(Compute { key, compute_if_present, function, segment, metadata, invocation_id, flags, topology_id })
            }
            REPLACE_COMMAND_ID => {
                let key = i.read_cache_object()?;
                let old_value = i.read_cache_object()?;
                let new_value = i.read_cache_object()?;
                let return_entry = i.r.read_boolean()?;
                let segment = i.read_unsigned_int()? as i32;
                let metadata = i.read_metadata()?;
                let value_matcher = i.read_value_matcher()?;
                let flags = i.r.read_long()?;
                let invocation_id = i.read_invocation_id()?;
                i.read_internal_metadata()?;
                let topology_id = i.r.read_int()?;
                Self::Replace(Replace { key, old_value, new_value, return_entry, segment, metadata, value_matcher, flags, invocation_id, topology_id })
            }
            REMOVE_COMMAND_ID => {
                let key = i.read_cache_object()?;
                let value = i.read_cache_object()?;
                let return_entry = i.r.read_boolean()?;
                let segment = i.read_unsigned_int()? as i32;
                let metadata = i.read_metadata()?;
                let flags = i.r.read_long()?;
                let value_matcher = i.read_value_matcher()?;
                let invocation_id = i.read_invocation_id()?;
                i.read_internal_metadata()?;
                let topology_id = i.r.read_int()?;
                Self::Remove(Remove { key, value, return_entry, segment, metadata, flags, value_matcher, invocation_id, topology_id })
            }
            REMOVE_EXPIRED_COMMAND_ID => {
                let invocation_id = i.read_invocation_id()?;
                let key = i.read_cache_object()?;
                let value = i.read_cache_object()?;
                let segment = i.read_unsigned_int()? as i32;
                let lifespan = if i.r.read_boolean()? { Some(i.r.read_long()?) } else { None };
                let max_idle = i.r.read_boolean()?;
                let flags = i.r.read_long()?;
                i.read_internal_metadata()?;
                let topology_id = i.r.read_int()?;
                Self::RemoveExpired(RemoveExpired { key, value, lifespan, max_idle, segment, flags, invocation_id, topology_id })
            }
            CLEAR_COMMAND_ID => {
                let flags = i.r.read_long()?;
                Self::Clear(Clear { flags, topology_id: i.r.read_int()? })
            }
            other => return Err(Error::UnknownTag { what: "write command id", value: other as i64 }),
        })
    }
}
