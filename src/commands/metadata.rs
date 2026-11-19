//! `Metadata` as `EmbeddedMetadata.Externalizer` writes it (`metadata/EmbeddedMetadata.java:331-396`,
//! externalizer id 57) and `NumericVersion.Externalizer` (id 58, `container/versioning/NumericVersion.java:74-96`).
//!
//! `VersionInterceptor` (first in a non-transactional chain, `InterceptorChainFactory.java:124-126`)
//! stamps a `NumericVersion` on every `ReplaceCommand` whose metadata carries none
//! (`interceptors/impl/VersionInterceptor.java:25-37`); [`NumericVersionGenerator`] makes those versions.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::marshall::error::{Error, Result};
use crate::topology::wire::{ID_NULL, ObjectInput, ObjectOutput};

/// The externalizer id of `EmbeddedMetadata`.
pub const ID_EMBEDDED_METADATA: u8 = 57;
/// The externalizer id of `NumericVersion`.
pub const ID_NUMERIC_VERSION: u8 = 58;

/// The four `EmbeddedMetadata` classes, by their externalizer number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// `EmbeddedMetadata` (0)
    Immortal,
    /// `EmbeddedExpirableMetadata` (1): lifespan and max idle, milliseconds (-1 = none)
    Expirable {
        /// The lifespan in milliseconds.
        lifespan: i64,
        /// The max idle time in milliseconds.
        max_idle: i64,
    },
    /// `EmbeddedLifespanExpirableMetadata` (2)
    Lifespan(i64),
    /// `EmbeddedMaxIdleExpirableMetadata` (3)
    MaxIdle(i64),
}

/// `NumericVersion` (`container/versioning/NumericVersion.java`): an entry version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericVersion(pub i64);

/// `EmbeddedMetadata`: an entry's expiration and optional version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    /// How the entry expires.
    pub expiry: Expiry,
    /// The entry's version, if it has one.
    pub version: Option<NumericVersion>,
}

impl Metadata {
    /// `EmbeddedMetadata` with no expiry: not what the cache API sends (see [`Metadata::DEFAULT`]).
    pub const IMMORTAL: Metadata = Metadata { expiry: Expiry::Immortal, version: None };

    /// What `Cache.put(k, v)`, `putIfAbsent`, `compute` and `replace` send when the caller sets no
    /// lifespan: `EmbeddedExpirableMetadata(-1, -1)` (every command vector shows it).
    pub const DEFAULT: Metadata = Metadata { expiry: Expiry::Expirable { lifespan: -1, max_idle: -1 }, version: None };

    /// The max idle time in milliseconds; `-1` for none.
    pub fn max_idle_ms(&self) -> i64 {
        match self.expiry {
            Expiry::Expirable { max_idle, .. } | Expiry::MaxIdle(max_idle) => max_idle,
            _ => -1,
        }
    }

    /// The lifespan in milliseconds; `-1` for none.
    pub fn lifespan_ms(&self) -> i64 {
        match self.expiry {
            Expiry::Expirable { lifespan, .. } | Expiry::Lifespan(lifespan) => lifespan,
            _ => -1,
        }
    }
}

impl ObjectOutput {
    /// `writeObject(Metadata)`.
    pub fn write_metadata(&mut self, m: Option<&Metadata>) {
        let Some(m) = m else { return self.write_null() };
        self.write_internal_header(ID_EMBEDDED_METADATA);
        match m.expiry {
            Expiry::Immortal => self.w.write_unsigned_byte(0),
            Expiry::Expirable { lifespan, max_idle } => {
                self.w.write_unsigned_byte(1);
                self.w.write_long(lifespan);
                self.w.write_long(max_idle);
            }
            Expiry::Lifespan(l) => {
                self.w.write_unsigned_byte(2);
                self.w.write_long(l);
            }
            Expiry::MaxIdle(i) => {
                self.w.write_unsigned_byte(3);
                self.w.write_long(i);
            }
        }
        match m.version {
            None => self.write_null(),
            Some(v) => {
                self.write_internal_header(ID_NUMERIC_VERSION);
                self.w.write_long(v.0);
            }
        }
    }
}

impl ObjectInput<'_> {
    /// `readObject()` for a `Metadata`.
    pub fn read_metadata(&mut self) -> Result<Option<Metadata>> {
        let mut peek = self.r;
        if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        self.expect_internal(ID_EMBEDDED_METADATA, "Metadata header")?;
        let expiry = match self.r.read_byte()? {
            0 => Expiry::Immortal,
            1 => Expiry::Expirable { lifespan: self.r.read_long()?, max_idle: self.r.read_long()? },
            2 => Expiry::Lifespan(self.r.read_long()?),
            3 => Expiry::MaxIdle(self.r.read_long()?),
            other => return Err(Error::UnknownTag { what: "EmbeddedMetadata kind", value: other as i64 }),
        };
        let mut peek = self.r;
        let version = if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            None
        } else {
            self.expect_internal(ID_NUMERIC_VERSION, "EntryVersion header")?;
            Some(NumericVersion(self.r.read_long()?))
        };
        Ok(Some(Metadata { expiry, version }))
    }
}

/// `NumericVersionGenerator` (`container/versioning/NumericVersionGenerator.java:34-67`): a clustered
/// version is `[view id (2 bytes)][rank (2 bytes)][counter (4 bytes)]`.
#[derive(Debug, Default)]
pub struct NumericVersionGenerator {
    counter: AtomicU32,
}

impl NumericVersionGenerator {
    /// `RankCalculator.updateRank`: prefix `(viewId << 48) | (rank << 32)` with `rank` the 1-based
    /// position in the view; the counter is an `int` that is incremented before use.
    pub fn generate_new(&self, view_id: i32, rank: i32) -> NumericVersion {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst).wrapping_add(1) as i32 as i64;
        let prefix = ((view_id as i64) << 48) | ((rank as i64) << 32);
        NumericVersion(prefix | counter)
    }
}
