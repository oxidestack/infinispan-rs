//! `StateChunk` and the `InternalCacheEntry` kinds it carries.
//!
//! `StateChunk.Externalizer` (`statetransfer/StateChunk.java:60-89`, id 48): `int segmentId`, the
//! entries as one object (an `ArrayList`, or `Collections.emptyList()` for the empty chunk that only
//! marks a segment complete, `OutboundTransferTask.java:136-143`), `boolean isLastChunk`.
//!
//! The eight `InternalCacheEntry` classes `InternalEntryFactoryImpl.create` picks
//! (`container/impl/InternalEntryFactoryImpl.java:46-62`) and their externalizers
//! (`container/entries/*CacheEntry.java`, ids 7-10 and 38-41) are checked against
//! `tests/vectors/state_transfer.hex`.

use crate::commands::metadata::Metadata;
use crate::commands::object::CacheObject;
use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{marshall_size, unmarshall_size};
use crate::topology::wire::{ID_COLLECTIONS, ID_NULL, ObjectInput, ObjectOutput};

/// The externalizer id of `StateChunk`.
pub const ID_STATE_CHUNK: u8 = 48;
const ID_IMMORTAL_ENTRY: u8 = 7;
const ID_MORTAL_ENTRY: u8 = 8;
const ID_TRANSIENT_ENTRY: u8 = 9;
const ID_TRANSIENT_MORTAL_ENTRY: u8 = 10;
const ID_METADATA_IMMORTAL_ENTRY: u8 = 38;
const ID_METADATA_MORTAL_ENTRY: u8 = 39;
const ID_METADATA_TRANSIENT_ENTRY: u8 = 40;
const ID_METADATA_TRANSIENT_MORTAL_ENTRY: u8 = 41;

const ARRAY_LIST: u8 = 0;
const EMPTY_LIST: u8 = 3;

/// An `InternalCacheEntry` of a REPL_SYNC cache. `metadata` is `Some` only for a "stored metadata"
/// entry (`InternalEntryFactoryImpl.isStoreMetadata`: it carries a version); `lifespan`/`max_idle`
/// are `-1` when absent. `PrivateMetadata` is always `null`.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalCacheEntry {
    /// The key.
    pub key: CacheObject,
    /// The value.
    pub value: CacheObject,
    /// The entry's metadata, for an entry that carries a version.
    pub metadata: Option<Metadata>,
    /// wall-clock millis; meaningful when the entry has a lifespan
    pub created: i64,
    /// wall-clock millis; meaningful when the entry has a max idle
    pub last_used: i64,
    /// The lifespan in milliseconds; `-1` for none.
    pub lifespan: i64,
    /// The max idle time in milliseconds; `-1` for none.
    pub max_idle: i64,
}

impl InternalCacheEntry {
    /// An `ImmortalCacheEntry`: no expiration, no metadata.
    pub fn immortal(key: CacheObject, value: CacheObject) -> Self {
        Self { key, value, metadata: None, created: -1, last_used: -1, lifespan: -1, max_idle: -1 }
    }
}

impl ObjectOutput {
    /// Writes `e` with the externalizer of its entry class (immortal, mortal, transient, with or without metadata).
    pub fn write_internal_cache_entry(&mut self, e: &InternalCacheEntry) {
        let mortal = e.lifespan > -1;
        let transient = e.max_idle > -1;
        let id = match (e.metadata.is_some(), mortal, transient) {
            (false, false, false) => ID_IMMORTAL_ENTRY,
            (false, true, false) => ID_MORTAL_ENTRY,
            (false, false, true) => ID_TRANSIENT_ENTRY,
            (false, true, true) => ID_TRANSIENT_MORTAL_ENTRY,
            (true, false, false) => ID_METADATA_IMMORTAL_ENTRY,
            (true, true, false) => ID_METADATA_MORTAL_ENTRY,
            (true, false, true) => ID_METADATA_TRANSIENT_ENTRY,
            (true, true, true) => ID_METADATA_TRANSIENT_MORTAL_ENTRY,
        };
        self.write_internal_header(id);
        self.write_cache_object(&e.key);
        self.write_cache_object(&e.value);
        self.write_null(); // PrivateMetadata
        if e.metadata.is_some() {
            self.write_metadata(e.metadata.as_ref());
            if mortal {
                self.write_unsigned_long(e.created as u64);
            }
            if transient {
                self.write_unsigned_long(e.last_used as u64);
            }
        } else {
            match (mortal, transient) {
                (false, false) => {}
                (true, false) => {
                    self.write_unsigned_long(e.created as u64);
                    self.w.write_long(e.lifespan);
                }
                (false, true) => {
                    self.write_unsigned_long(e.last_used as u64);
                    self.w.write_long(e.max_idle);
                }
                (true, true) => {
                    self.write_unsigned_long(e.created as u64);
                    self.w.write_long(e.lifespan);
                    self.write_unsigned_long(e.last_used as u64);
                    self.w.write_long(e.max_idle);
                }
            }
        }
    }

    /// `StateChunk.Externalizer.writeObject` behind its `ID_INTERNAL` header.
    pub fn write_state_chunk(&mut self, c: &StateChunk) {
        self.write_internal_header(ID_STATE_CHUNK);
        self.w.write_int(c.segment);
        self.write_internal_header(ID_COLLECTIONS);
        if c.entries.is_empty() {
            self.w.write_unsigned_byte(EMPTY_LIST);
        } else {
            self.w.write_unsigned_byte(ARRAY_LIST);
            marshall_size(&mut self.w, c.entries.len() as i32);
            for e in &c.entries {
                self.write_internal_cache_entry(e);
            }
        }
        self.w.write_boolean(c.is_last);
    }
}

impl ObjectInput<'_> {
    /// Reads an `InternalCacheEntry` of any of the eight entry classes.
    pub fn read_internal_cache_entry(&mut self) -> Result<InternalCacheEntry> {
        let tag = self.r.read_unsigned_byte()?;
        if tag != crate::topology::wire::ID_INTERNAL {
            return Err(Error::UnknownTag { what: "InternalCacheEntry header", value: tag as i64 });
        }
        let id = self.r.read_unsigned_byte()?;
        let key = self.read_cache_object()?;
        let value = self.read_cache_object()?;
        if self.r.read_unsigned_byte()? != ID_NULL {
            return Err(Error::Malformed("PrivateMetadata is not modelled".into()));
        }
        let mut e = InternalCacheEntry::immortal(key, value);
        let stored = (ID_METADATA_IMMORTAL_ENTRY..=ID_METADATA_TRANSIENT_MORTAL_ENTRY).contains(&id);
        if stored {
            e.metadata = self.read_metadata()?;
            let m = e.metadata.ok_or_else(|| Error::Malformed("metadata entry without metadata".into()))?;
            e.lifespan = m.lifespan_ms();
            e.max_idle = m.max_idle_ms();
        }
        let (mortal, transient) = match id {
            ID_IMMORTAL_ENTRY | ID_METADATA_IMMORTAL_ENTRY => (false, false),
            ID_MORTAL_ENTRY | ID_METADATA_MORTAL_ENTRY => (true, false),
            ID_TRANSIENT_ENTRY | ID_METADATA_TRANSIENT_ENTRY => (false, true),
            ID_TRANSIENT_MORTAL_ENTRY | ID_METADATA_TRANSIENT_MORTAL_ENTRY => (true, true),
            other => return Err(Error::UnknownTag { what: "InternalCacheEntry externalizer id", value: other as i64 }),
        };
        if mortal {
            e.created = self.read_unsigned_long()? as i64;
            if !stored {
                e.lifespan = self.r.read_long()?;
            }
        }
        if transient {
            e.last_used = self.read_unsigned_long()? as i64;
            if !stored {
                e.max_idle = self.r.read_long()?;
            }
        }
        Ok(e)
    }

    /// Reads a `StateChunk`.
    pub fn read_state_chunk(&mut self) -> Result<StateChunk> {
        self.expect_internal(ID_STATE_CHUNK, "StateChunk header")?;
        let segment = self.r.read_int()?;
        self.expect_internal(ID_COLLECTIONS, "StateChunk entries")?;
        let kind = self.r.read_unsigned_byte()?;
        let entries = match kind {
            EMPTY_LIST => Vec::new(),
            ARRAY_LIST => {
                let n = unmarshall_size(&mut self.r)?;
                (0..n.max(0)).map(|_| self.read_internal_cache_entry()).collect::<Result<Vec<_>>>()?
            }
            other => return Err(Error::UnknownTag { what: "StateChunk entries collection", value: other as i64 }),
        };
        let is_last = self.r.read_boolean()?;
        Ok(StateChunk { segment, entries, is_last })
    }
}

/// `StateChunk`: entries of one segment, and whether more chunks of it follow.
#[derive(Debug, Clone, PartialEq)]
pub struct StateChunk {
    /// The segment the entries belong to.
    pub segment: i32,
    /// The entries.
    pub entries: Vec<InternalCacheEntry>,
    /// Whether this is the segment's last chunk.
    pub is_last: bool,
}
