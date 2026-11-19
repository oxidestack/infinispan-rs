//! The data container of a REPL_SYNC cache: every entry of every segment on this node (`DefaultDataContainer`
//! / `SegmentedDataContainer`'s observable behaviour, or `BoundedSegmentedDataContainer` when bounded, see
//! [`super::data_container`]). Keys are compared by
//! their marshalled form, which is canonical for the `String`/primitive keys MockServer uses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::data_container::{Eviction, KeyHash};
use super::time_service::{SystemTimeService, TimeService};
use crate::commands::metadata::{Expiry, Metadata};
use crate::commands::object::CacheObject;
use crate::statetransfer::chunk::InternalCacheEntry;
use crate::topology::wire::ObjectOutput;

/// An entry in the data container (`InternalCacheEntry`).
#[derive(Debug, Clone)]
pub struct StoredEntry {
    /// The key.
    pub key: CacheObject,
    /// The value.
    pub value: CacheObject,
    /// The entry's expiration and version metadata.
    pub metadata: Option<Metadata>,
    /// The key's segment.
    pub segment: i32,
    /// wall-clock millis the entry was created (`InternalCacheEntry.getCreated`)
    pub created: i64,
    /// wall-clock millis it was last used (not refreshed on read: max idle is not enforced)
    pub last_used: i64,
}

/// `TimeService.wallClockTime()` of the system clock.
pub fn wall_clock_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl StoredEntry {
    /// `ExpiryHelper.isExpiredMortal(lifespan, created, now)`: `lifespan > -1 && created > -1 && lifespan + created <= now`.
    pub fn expired(&self, now: i64) -> bool {
        let l = self.metadata.map(|m| m.lifespan_ms()).unwrap_or(-1);
        l >= 0 && now >= self.created + l
    }

    /// The `InternalCacheEntry` upstream's `InternalEntryFactoryImpl` would hold for this entry: stored
    /// metadata only when it carries a version.
    pub fn to_internal(&self) -> InternalCacheEntry {
        let m = self.metadata;
        InternalCacheEntry {
            key: self.key.clone(),
            value: self.value.clone(),
            metadata: m.filter(|m| m.version.is_some()),
            created: self.created,
            last_used: self.last_used,
            lifespan: m.map(|m| m.lifespan_ms()).unwrap_or(-1),
            max_idle: m.map(|m| m.max_idle_ms()).unwrap_or(-1),
        }
    }
}

/// The metadata a received `InternalCacheEntry` is stored with (`InternalMetadataImpl.actual`).
pub fn metadata_of(e: &InternalCacheEntry) -> Metadata {
    Metadata {
        expiry: Expiry::Expirable { lifespan: e.lifespan, max_idle: e.max_idle },
        version: e.metadata.and_then(|m| m.version),
    }
}

/// The canonical byte form of a key.
pub fn key_bytes(key: &CacheObject) -> Vec<u8> {
    let mut o = ObjectOutput::new();
    o.write_cache_object(key);
    o.into_bytes()
}

/// What the container holds: the entries and, for a bounded one, the eviction policy over their keys.
#[derive(Default)]
struct Inner {
    map: HashMap<Vec<u8>, StoredEntry>,
    eviction: Option<Eviction>,
    /// bounded only: the policy's view of each held key, so a removal does not rehash it
    hashes: HashMap<Vec<u8>, KeyHash>,
}

impl Inner {
    /// A key left the container: it leaves the policy too (no ghost capacity).
    fn forget(&mut self, kb: &[u8]) {
        if let Some(kh) = self.hashes.remove(kb)
            && let Some(ev) = self.eviction.as_mut()
        {
            ev.remove(&kh);
        }
    }
}

/// `DefaultSegmentedDataContainer` or, when built with [`DataContainer::bounded`],
/// `BoundedSegmentedDataContainer` (see [`super::data_container`]).
#[derive(Default)]
pub struct DataContainer {
    inner: Mutex<Inner>,
    /// entries the policy evicted, until the engine notifies them
    evicted: Mutex<Vec<StoredEntry>>,
    time: RwLock<Option<Arc<dyn TimeService>>>,
}

impl DataContainer {
    /// A container bounded to `max_count` entries by Caffeine's W-TinyLFU (`maxCount` + REMOVE).
    pub fn bounded(max_count: u64) -> Self {
        DataContainer { inner: Mutex::new(Inner { map: HashMap::new(), eviction: Some(Eviction::new(max_count)), hashes: HashMap::new() }), evicted: Mutex::new(Vec::new()), time: RwLock::new(None) }
    }

    /// Replace this container's clock (`TestingUtil.replaceComponent(manager, TimeService.class, ts, true)`).
    pub fn set_time_service(&self, ts: Arc<dyn TimeService>) {
        *self.time.write().unwrap() = Some(ts);
    }

    /// `TimeService.wallClockTime()` of this node.
    pub fn now(&self) -> i64 {
        match self.time.read().unwrap().as_ref() {
            Some(ts) => ts.wall_clock_time(),
            None => SystemTimeService.wall_clock_time(),
        }
    }

    /// Whether the container evicts entries beyond a maximum count.
    pub fn is_bounded(&self) -> bool {
        self.inner.lock().unwrap().eviction.is_some()
    }

    /// `BoundedSegmentedDataContainer.capacity()`: `None` for an unbounded container.
    pub fn capacity(&self) -> Option<u64> {
        self.inner.lock().unwrap().eviction.as_ref().map(|e| e.capacity())
    }

    /// A read that counts as an access for the eviction policy (`InternalDataContainer.get`).
    pub fn get(&self, key: &CacheObject) -> Option<StoredEntry> {
        self.lookup(key, true)
    }

    /// A read that does not touch the eviction policy (`InternalDataContainer.peek`, which the write path
    /// uses to wrap an entry).
    pub fn peek(&self, key: &CacheObject) -> Option<StoredEntry> {
        self.lookup(key, false)
    }

    fn lookup(&self, key: &CacheObject, touch: bool) -> Option<StoredEntry> {
        let kb = key_bytes(key);
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        match inner.map.get(&kb) {
            Some(e) if e.expired(now) => {
                inner.map.remove(&kb);
                inner.forget(&kb);
                None
            }
            Some(e) => {
                let entry = e.clone();
                if touch {
                    let Inner { eviction, hashes, .. } = &mut *inner;
                    if let (Some(ev), Some(kh)) = (eviction.as_mut(), hashes.get(&kb)) {
                        ev.touch(kh);
                    }
                }
                Some(entry)
            }
            None => None,
        }
    }

    /// The raw entry, expired or not and without touching the eviction policy: what the write path sees
    /// when it wraps an entry (`InternalDataContainer.peek`, which does not check expiration).
    pub fn peek_including_expired(&self, key: &CacheObject) -> Option<StoredEntry> {
        self.inner.lock().unwrap().map.get(&key_bytes(key)).cloned()
    }

    /// Stores `value` under `key`, created now, evicting an entry if the container is full.
    pub fn put(&self, key: CacheObject, value: CacheObject, metadata: Option<Metadata>, segment: i32) {
        let now = self.now();
        self.put_with_times(key, value, metadata, segment, now, now);
    }

    /// A put that keeps the timestamps it was given (state transfer: `InternalMetadataImpl`).
    pub fn put_with_times(&self, key: CacheObject, value: CacheObject, metadata: Option<Metadata>, segment: i32, created: i64, last_used: i64) {
        let kb = key_bytes(&key);
        let mut inner = self.inner.lock().unwrap();
        let kh = inner.eviction.is_some().then(|| KeyHash::of(&key));
        inner.map.insert(kb.clone(), StoredEntry { key, value, metadata, segment, created, last_used });
        let Some(kh) = kh else { return };
        inner.hashes.insert(kb, kh.clone());
        let victims = inner.eviction.as_mut().expect("bounded").write(&kh);
        if !victims.is_empty() {
            // `evictionListener(.. cause == SIZE)`: the entries the policy evicted leave the container
            let mut gone = Vec::with_capacity(victims.len());
            for v in victims {
                inner.hashes.remove(v.bytes());
                if let Some(e) = inner.map.remove(v.bytes()) {
                    gone.push(e);
                }
            }
            gone.sort_by_cached_key(|e| key_bytes(&e.key));
            drop(inner);
            self.evicted.lock().unwrap().extend(gone);
        }
    }

    /// The entries evicted since the last call, for `EvictionManager.onEntryEviction`.
    pub fn drain_evicted(&self) -> Vec<StoredEntry> {
        std::mem::take(&mut *self.evicted.lock().unwrap())
    }

    /// The live entries of one segment, in key order (`SegmentedDataContainer.iterator(segments)`).
    pub fn segment_entries(&self, segment: i32) -> Vec<StoredEntry> {
        let now = self.now();
        let mut v: Vec<(Vec<u8>, StoredEntry)> =
            self.inner.lock().unwrap().map.iter().filter(|(_, e)| e.segment == segment && !e.expired(now)).map(|(k, e)| (k.clone(), e.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v.into_iter().map(|(_, e)| e).collect()
    }

    /// `removeSegments`: drop every entry of segments this node no longer owns.
    pub fn remove_segments(&self, segments: &dyn Fn(i32) -> bool) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let before = inner.map.len();
        inner.map.retain(|_, e| !segments(e.segment));
        let Inner { map, hashes, eviction } = &mut *inner;
        hashes.retain(|k, kh| {
            let live = map.contains_key(k);
            if !live && let Some(ev) = eviction.as_mut() {
                ev.remove(kh);
            }
            live
        });
        before - inner.map.len()
    }

    /// Removes `key`'s entry and returns it.
    pub fn remove(&self, key: &CacheObject) -> Option<StoredEntry> {
        let kb = key_bytes(key);
        let mut inner = self.inner.lock().unwrap();
        inner.forget(&kb);
        inner.map.remove(&kb)
    }

    /// Remove everything, returning what was there, expired entries included and in key order
    /// (`commitClearCommand` iterates `iteratorIncludingExpired` and notifies each entry).
    pub fn clear(&self) -> Vec<StoredEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.hashes.clear();
        if let Some(ev) = inner.eviction.as_mut() {
            ev.clear();
        }
        let mut v: Vec<(Vec<u8>, StoredEntry)> = inner.map.drain().collect();
        drop(inner);
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v.into_iter().map(|(_, e)| e).collect()
    }

    /// The number of entries, expired ones included.
    pub fn len(&self) -> usize {
        let now = self.now();
        self.inner.lock().unwrap().map.values().filter(|e| !e.expired(now)).count()
    }

    /// Whether the container holds no entry.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every entry, in key order.
    pub fn entries(&self) -> Vec<StoredEntry> {
        let now = self.now();
        self.inner.lock().unwrap().map.values().filter(|e| !e.expired(now)).cloned().collect()
    }
}
