//! Listener event filters and converters (`notifications/cachelistener/filter/`): `CacheEventFilter`,
//! `CacheEventConverter`, `CacheEventFilterConverter`, `EventType`, and `KeyValueFilter` adapted to a
//! filter (`KeyValueFilterAsCacheEventFilter`).
//!
//! In a replicated cache with non-zero capacity a cluster listener is a *local* listener on every node
//! (`CacheNotifierImpl.java:1278-1282`), so filters and converters run in-process and never travel; the
//! Externalizers (`KeyValueFilterAsCacheEventFilter.Externalizer`, ids and ProtoStream adapters) matter
//! only for DIST caches and are not ported.

use super::event::EventKind;
use crate::commands::metadata::Metadata;
use crate::commands::object::CacheObject;

/// `org.infinispan.notifications.cachelistener.filter.EventType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventType {
    /// `isRetry()`
    pub retry: bool,
    /// `isPreEvent()`
    pub pre: bool,
    /// `getType()`
    pub kind: EventKind,
}

impl EventType {
    /// Whether the event is a creation.
    pub fn is_create(&self) -> bool {
        self.kind == EventKind::CacheEntryCreated
    }
    /// Whether the event is a modification.
    pub fn is_modified(&self) -> bool {
        self.kind == EventKind::CacheEntryModified
    }
    /// Whether the event is a removal.
    pub fn is_remove(&self) -> bool {
        self.kind == EventKind::CacheEntryRemoved
    }
    /// Whether the event is an expiration.
    pub fn is_expired(&self) -> bool {
        self.kind == EventKind::CacheEntryExpired
    }
}

/// `CacheEventFilter.accept(key, oldValue, oldMetadata, newValue, newMetadata, eventType)`.
pub trait CacheEventFilter: Send + Sync {
    /// Whether the event passes the filter.
    fn accept(&self, key: &CacheObject, old_value: &CacheObject, old_metadata: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, event_type: EventType) -> bool;
}

/// `CacheEventConverter.convert(...)`: the returned object replaces the event's value.
pub trait CacheEventConverter: Send + Sync {
    /// The value the listener receives instead of the event's.
    fn convert(&self, key: &CacheObject, old_value: &CacheObject, old_metadata: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, event_type: EventType) -> CacheObject;
}

/// `CacheEventFilterConverter.filterAndConvert(...)`: a null result drops the event. A listener
/// registered with one object as both filter and converter calls only this method
/// (`BaseCacheEntryListenerInvocation.filterAndConvert`, `CacheNotifierImpl.java:1726`).
pub trait CacheEventFilterConverter: CacheEventFilter + CacheEventConverter {
    /// The converted value, or [`CacheObject::Null`] to drop the event.
    fn filter_and_convert(&self, key: &CacheObject, old_value: &CacheObject, old_metadata: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, event_type: EventType) -> CacheObject;
}

/// `org.infinispan.filter.KeyValueFilter`.
pub trait KeyValueFilter: Send + Sync {
    /// Whether the entry passes the filter.
    fn accept(&self, key: &CacheObject, value: &CacheObject, metadata: Option<&Metadata>) -> bool;
}

/// `KeyValueFilterAsCacheEventFilter`: filters on the new value and metadata alone.
pub struct KeyValueFilterAsCacheEventFilter<F: KeyValueFilter>(pub F);

impl<F: KeyValueFilter> CacheEventFilter for KeyValueFilterAsCacheEventFilter<F> {
    fn accept(&self, key: &CacheObject, _old_value: &CacheObject, _old_metadata: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, _event_type: EventType) -> bool {
        self.0.accept(key, new_value, new_metadata)
    }
}
