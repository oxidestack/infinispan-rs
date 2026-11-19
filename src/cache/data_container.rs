//! The bounded data container (`container/impl/BoundedSegmentedDataContainer.java`, `maxCount` + REMOVE)
//! and the factory that picks it (`factories/DataContainerFactory.java:36-110`), wired to the W-TinyLFU
//! policy of `caffeine-rs`.
//!
//! Upstream's container keeps its entries in a Caffeine `Cache` built with `maximumSize(maxCount)`;
//! Caffeine decides which entry to evict, the container's eviction listener removes it from the
//! per-segment maps and `EvictionManagerImpl.onEntryEviction` raises `@CacheEntriesEvicted` on the local
//! notifier (`BoundedSegmentedDataContainer.java:48-83`, `AbstractInternalDataContainer.java:408-415, 528-550`).
//! Here [`Eviction`] is that Caffeine cache's *policy* over the keys, the container keeps the entries, and
//! what the policy dropped is reported through [`crate::cache::container::DataContainer::drain_evicted`] for the
//! engine to notify. Eviction is per node and nothing about it goes on the wire; since W-TinyLFU is not LRU,
//! two replicas that saw the same writes in a different order can hold different entries.
//!
//! `caffeine-rs`' `put_evicting` returns what a write evicted and `remove`/`invalidate_all` take keys out of the
//! policy (`RemovalCause.EXPLICIT`), so a removed key frees its capacity and no victim scan is needed.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use caffeine::{BoundedCache, JavaHashCode};

use super::container::key_bytes;
use crate::commands::object::CacheObject;
use crate::marshall::primitives::PrimitiveValue;
use crate::notify::cache_notifier::CacheMode;

/// `EvictionStrategy`: what a full cache does (`MemoryConfiguration.whenFull()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionStrategy {
    /// no bound
    None,
    /// the application evicts (`cache.evict`)
    Manual,
    /// the container drops entries (Caffeine `maximumSize`)
    Remove,
    /// writes fail when full
    Exception,
}

impl EvictionStrategy {
    /// Whether the strategy bounds the container.
    pub fn is_enabled(self) -> bool {
        matches!(self, EvictionStrategy::Remove | EvictionStrategy::Exception)
    }
    /// Whether a full container fails writes rather than evicting.
    pub fn is_exception_based(self) -> bool {
        self == EvictionStrategy::Exception
    }
}

/// `MemoryConfiguration`, reduced to a heap cache bounded by entry count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryConfig {
    /// `maxCount`; negative is unbounded
    pub max_count: i64,
    /// What happens when the container reaches `max_count`.
    pub when_full: EvictionStrategy,
    /// `storageType(OFF_HEAP)`: not ported
    pub off_heap: bool,
}

impl MemoryConfig {
    /// The default: heap, unbounded.
    pub const UNBOUNDED: MemoryConfig = MemoryConfig { max_count: -1, when_full: EvictionStrategy::None, off_heap: false };

    /// `memory().maxCount(n)` with the default strategy for a bounded cache, `REMOVE`.
    pub fn max_count(n: i64) -> Self {
        MemoryConfig { max_count: n, when_full: if n > 0 { EvictionStrategy::Remove } else { EvictionStrategy::None }, off_heap: false }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self::UNBOUNDED
    }
}

/// The classes `DataContainerFactory.construct` returns, for the heap containers this port reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerKind {
    /// `DefaultDataContainer` (not segmented, unbounded)
    DefaultDataContainer,
    /// `DefaultDataContainer.boundedDataContainer` (not segmented, `maxCount`)
    BoundedDataContainer,
    /// `DefaultSegmentedDataContainer`
    DefaultSegmentedDataContainer,
    /// `BoundedSegmentedDataContainer`
    BoundedSegmentedDataContainer,
}

/// A memory configuration this port has no container for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedContainer(pub String);

impl std::fmt::Display for UnsupportedContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UnsupportedContainer {}

/// `CacheMode.needsStateTransfer()`: every clustered mode but invalidation.
fn needs_state_transfer(mode: CacheMode) -> bool {
    mode.is_replicated() || mode.is_distributed() || mode.is_scattered()
}

/// `DataContainerFactory`: picks and builds the data container for a cache's mode and memory configuration.
pub struct DataContainerFactory;

impl DataContainerFactory {
    /// Which container `construct` builds (`DataContainerFactory.java:36-110`). Off-heap containers, L1 and
    /// `EXCEPTION` eviction are not ported.
    pub fn kind(mode: CacheMode, memory: &MemoryConfig) -> Result<ContainerKind, UnsupportedContainer> {
        let should_segment = needs_state_transfer(mode);
        if memory.off_heap {
            return Err(UnsupportedContainer("off-heap data containers are not ported".into()));
        }
        let strategy = memory.when_full;
        // handle case when < 0 value signifies unbounded container or when we are not removal based
        if strategy.is_exception_based() {
            return Err(UnsupportedContainer("EvictionStrategy.EXCEPTION is not ported".into()));
        }
        if !strategy.is_enabled() {
            return Ok(if should_segment { ContainerKind::DefaultSegmentedDataContainer } else { ContainerKind::DefaultDataContainer });
        }
        if memory.max_count <= 0 {
            return Err(UnsupportedContainer("a REMOVE cache needs a positive maxCount".into()));
        }
        Ok(if should_segment { ContainerKind::BoundedSegmentedDataContainer } else { ContainerKind::BoundedDataContainer })
    }

    /// `construct`: the container for a cache of this mode and memory configuration.
    pub fn construct(mode: CacheMode, memory: &MemoryConfig) -> Result<super::container::DataContainer, UnsupportedContainer> {
        Ok(match Self::kind(mode, memory)? {
            ContainerKind::BoundedSegmentedDataContainer | ContainerKind::BoundedDataContainer => super::container::DataContainer::bounded(memory.max_count as u64),
            ContainerKind::DefaultSegmentedDataContainer | ContainerKind::DefaultDataContainer => super::container::DataContainer::default(),
        })
    }
}

/// A key as the policy sees it: its canonical bytes (equality) and Java's `hashCode()` (the frequency sketch).
#[derive(Debug, Clone)]
pub struct KeyHash {
    bytes: Arc<Vec<u8>>,
    hash: i32,
}

impl KeyHash {
    /// The canonical bytes of the key.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The hash of `key`'s canonical bytes.
    pub fn of(key: &CacheObject) -> Self {
        KeyHash { bytes: Arc::new(key_bytes(key)), hash: java_hash_code(key) }
    }
}

impl PartialEq for KeyHash {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for KeyHash {}

impl Hash for KeyHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl JavaHashCode for KeyHash {
    fn java_hash_code(&self) -> i32 {
        self.hash
    }
}

/// `Object.hashCode()` of a cache key. `String`, the boxed primitives, `byte[]` (`Arrays.hashCode`) and
/// `long[]` are exact. A user object's `hashCode` lives in its class; its serialized form is hashed instead,
/// so the sketch sees a different (but stable) value than the JVM would.
pub fn java_hash_code(key: &CacheObject) -> i32 {
    fn long_hash(v: i64) -> i32 {
        (v ^ ((v as u64) >> 32) as i64) as i32
    }
    match key {
        CacheObject::Null => 0,
        CacheObject::Primitive(p) => match p {
            PrimitiveValue::Str(s) => s.encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32)),
            PrimitiveValue::IntObj(i) => *i,
            PrimitiveValue::ShortObj(i) => *i as i32,
            PrimitiveValue::ByteObj(i) => *i as i32,
            PrimitiveValue::CharObj(i) => *i as i32,
            PrimitiveValue::LongObj(l) => long_hash(*l),
            PrimitiveValue::BooleanObj(b) => if *b { 1231 } else { 1237 },
            PrimitiveValue::ByteArray(b) => b.iter().fold(1i32, |h, x| h.wrapping_mul(31).wrapping_add(*x as i8 as i32)),
            PrimitiveValue::LongArray(l) => l.iter().fold(1i32, |h, x| h.wrapping_mul(31).wrapping_add(long_hash(*x))),
            other => fnv(&format!("{other:?}").into_bytes()),
        },
        CacheObject::User(u) => fnv(u.bytes()),
    }
}

fn fnv(bytes: &[u8]) -> i32 {
    bytes.iter().fold(0x811c9dc5u32, |h, b| (h ^ *b as u32).wrapping_mul(0x01000193)) as i32
}

/// The Caffeine cache's eviction policy over this container's keys.
pub struct Eviction {
    policy: BoundedCache<KeyHash>,
    max_count: u64,
}

impl Eviction {
    /// A policy keeping at most `max_count` keys.
    pub fn new(max_count: u64) -> Self {
        Eviction { policy: BoundedCache::with_max_count(max_count), max_count }
    }

    /// `Policy.Eviction.getMaximum()`
    pub fn capacity(&self) -> u64 {
        self.max_count
    }

    /// Keys the policy holds.
    pub fn policy_len(&self) -> usize {
        self.policy.len()
    }

    /// A read: `Cache.getIfPresent` records an access.
    pub fn touch(&mut self, key: &KeyHash) {
        self.policy.get(key);
    }

    /// A write (`Cache.put`/`compute`). Returns the keys the policy evicted to stay within the bound
    /// (`RemovalCause.SIZE`; the written key itself if it was not admitted).
    pub fn write(&mut self, key: &KeyHash) -> Vec<KeyHash> {
        self.policy.put_evicting(key.clone())
    }

    /// `Cache.invalidate` (`RemovalCause.EXPLICIT`): the key leaves the policy and gives its capacity back.
    pub fn remove(&mut self, key: &KeyHash) {
        self.policy.remove(key);
    }

    /// `Cache.invalidateAll()`
    pub fn clear(&mut self) {
        self.policy.invalidate_all();
    }
}
