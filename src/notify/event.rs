//! The events a cache listener receives: `Event.Type` and `EventImpl`
//! (`notifications/cachelistener/event/Event.java`, `event/impl/EventImpl.java`).
//!
//! One struct stands for every `Event` subinterface; which fields are meaningful follows the kind
//! (`CacheEntryEvent` has a key and a value, `CacheEntryModifiedEvent` also the old value, `isCreated`,
//! `CacheEntriesEvictedEvent` the entries, `TransactionalEvent` the transaction id). `Event.Type` and the
//! method annotation of the same name (`@CacheEntryCreated` is the annotation for
//! `CACHE_ENTRY_CREATED`) share [`EventKind`], as upstream's `CacheNotifierImpl` maps one to the other
//! (`allowedListeners`, `:154-171`).

use crate::commands::metadata::Metadata;
use crate::commands::object::CacheObject;

/// `Event.Type`, and the listener method annotation that selects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EventKind {
    /// `@CacheEntryCreated`.
    CacheEntryCreated,
    /// `@CacheEntryModified`.
    CacheEntryModified,
    /// `@CacheEntryRemoved`.
    CacheEntryRemoved,
    /// `@CacheEntryActivated`.
    CacheEntryActivated,
    /// `@CacheEntryPassivated`.
    CacheEntryPassivated,
    /// `@CacheEntryLoaded`.
    CacheEntryLoaded,
    /// `@CacheEntryVisited`.
    CacheEntryVisited,
    /// `@CacheEntriesEvicted`.
    CacheEntriesEvicted,
    /// `@CacheEntryExpired`.
    CacheEntryExpired,
    /// `@CacheEntryInvalidated`.
    CacheEntryInvalidated,
    /// `@TransactionRegistered`.
    TransactionRegistered,
    /// `@TransactionCompleted`.
    TransactionCompleted,
    /// `@DataRehashed`.
    DataRehashed,
    /// `@TopologyChanged`.
    TopologyChanged,
    /// `@PartitionStatusChanged`.
    PartitionStatusChanged,
    /// `@PersistenceAvailabilityChanged`.
    PersistenceAvailabilityChanged,
}

impl EventKind {
    /// Every kind, in `CacheNotifierImpl`'s `listenersMap` order.
    pub const ALL: [EventKind; 16] = [
        EventKind::CacheEntryCreated,
        EventKind::CacheEntryRemoved,
        EventKind::CacheEntryVisited,
        EventKind::CacheEntryModified,
        EventKind::CacheEntryActivated,
        EventKind::CacheEntryPassivated,
        EventKind::CacheEntryLoaded,
        EventKind::CacheEntriesEvicted,
        EventKind::CacheEntryExpired,
        EventKind::TransactionRegistered,
        EventKind::TransactionCompleted,
        EventKind::CacheEntryInvalidated,
        EventKind::DataRehashed,
        EventKind::TopologyChanged,
        EventKind::PartitionStatusChanged,
        EventKind::PersistenceAvailabilityChanged,
    ];

    /// The `Event.Type` constant name.
    pub fn type_name(self) -> &'static str {
        match self {
            EventKind::CacheEntryCreated => "CACHE_ENTRY_CREATED",
            EventKind::CacheEntryModified => "CACHE_ENTRY_MODIFIED",
            EventKind::CacheEntryRemoved => "CACHE_ENTRY_REMOVED",
            EventKind::CacheEntryActivated => "CACHE_ENTRY_ACTIVATED",
            EventKind::CacheEntryPassivated => "CACHE_ENTRY_PASSIVATED",
            EventKind::CacheEntryLoaded => "CACHE_ENTRY_LOADED",
            EventKind::CacheEntryVisited => "CACHE_ENTRY_VISITED",
            EventKind::CacheEntriesEvicted => "CACHE_ENTRY_EVICTED",
            EventKind::CacheEntryExpired => "CACHE_ENTRY_EXPIRED",
            EventKind::CacheEntryInvalidated => "CACHE_ENTRY_INVALIDATED",
            EventKind::TransactionRegistered => "TRANSACTION_REGISTERED",
            EventKind::TransactionCompleted => "TRANSACTION_COMPLETED",
            EventKind::DataRehashed => "DATA_REHASHED",
            EventKind::TopologyChanged => "TOPOLOGY_CHANGED",
            EventKind::PartitionStatusChanged => "PARTITION_STATUS_CHANGED",
            EventKind::PersistenceAvailabilityChanged => "PERSISTENCE_AVAILABILITY_CHANGED",
        }
    }

    /// The annotation's simple name, as `testListenerMethodValidity` prints it.
    pub fn annotation_name(self) -> &'static str {
        match self {
            EventKind::CacheEntryCreated => "org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated",
            EventKind::CacheEntryModified => "org.infinispan.notifications.cachelistener.annotation.CacheEntryModified",
            EventKind::CacheEntryRemoved => "org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved",
            EventKind::CacheEntryActivated => "org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated",
            EventKind::CacheEntryPassivated => "org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated",
            EventKind::CacheEntryLoaded => "org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded",
            EventKind::CacheEntryVisited => "org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited",
            EventKind::CacheEntriesEvicted => "org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted",
            EventKind::CacheEntryExpired => "org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired",
            EventKind::CacheEntryInvalidated => "org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated",
            EventKind::TransactionRegistered => "org.infinispan.notifications.cachelistener.annotation.TransactionRegistered",
            EventKind::TransactionCompleted => "org.infinispan.notifications.cachelistener.annotation.TransactionCompleted",
            EventKind::DataRehashed => "org.infinispan.notifications.cachelistener.annotation.DataRehashed",
            EventKind::TopologyChanged => "org.infinispan.notifications.cachelistener.annotation.TopologyChanged",
            EventKind::PartitionStatusChanged => "org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged",
            EventKind::PersistenceAvailabilityChanged => "org.infinispan.notifications.cachelistener.annotation.PersistenceAvailabilityChanged",
        }
    }

    /// The event interface's class name (`CacheEntryCreatedEvent`, ...), as the validity message prints it.
    pub fn event_class_name(self) -> String {
        let simple = match self {
            EventKind::CacheEntriesEvicted => "CacheEntriesEvictedEvent".to_string(),
            other => format!("{}Event", other.annotation_name().rsplit('.').next().unwrap()),
        };
        format!("org.infinispan.notifications.cachelistener.event.{simple}")
    }

    /// Whether the event extends `CacheEntryEvent` (a key and a value).
    pub fn is_cache_entry_event(self) -> bool {
        matches!(
            self,
            EventKind::CacheEntryCreated
                | EventKind::CacheEntryModified
                | EventKind::CacheEntryRemoved
                | EventKind::CacheEntryActivated
                | EventKind::CacheEntryPassivated
                | EventKind::CacheEntryLoaded
                | EventKind::CacheEntryVisited
                | EventKind::CacheEntryExpired
                | EventKind::CacheEntryInvalidated
        )
    }

    /// Whether the event is about a transaction.
    pub fn is_transactional_event(self) -> bool {
        matches!(self, EventKind::TransactionRegistered | EventKind::TransactionCompleted)
    }
}

/// `EventImpl`. [`CacheObject::Null`] stands for Java `null`.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// The event's type.
    pub kind: EventKind,
    /// `isPre()`
    pub pre: bool,
    /// `getCache().getName()`
    pub cache_name: String,
    /// `CacheEntryEvent.getKey()`
    pub key: CacheObject,
    /// `CacheEntryEvent.getValue()`: for a post removal, null; for a pre event the value before the change
    pub value: CacheObject,
    /// `EventImpl.getNewValue()`
    pub new_value: CacheObject,
    /// `getOldValue()` (modified, removed, invalidated)
    pub old_value: CacheObject,
    /// `CacheEntryEvent.getMetadata()`
    pub metadata: Option<Metadata>,
    /// `getOldMetadata()`
    pub old_metadata: Option<Metadata>,
    /// `isOriginLocal()`
    pub origin_local: bool,
    /// `isCommandRetried()`: the command carried `COMMAND_RETRY`
    pub command_retried: bool,
    /// `CacheEntryModifiedEvent.isCreated()`; upstream never sets it from `CacheNotifierImpl`
    pub created: bool,
    /// `CacheEntriesEvictedEvent.getEntries()`
    pub entries: Vec<(CacheObject, CacheObject)>,
    /// `TransactionalEvent.getGlobalTransaction()`
    pub transaction_id: Option<String>,
    /// `TransactionCompletedEvent.isTransactionSuccessful()`
    pub transaction_successful: bool,
}

impl Event {
    /// `EventImpl.createEvent(cache, type)`.
    pub fn new(cache_name: &str, kind: EventKind) -> Self {
        Event {
            kind,
            pre: false,
            cache_name: cache_name.to_string(),
            key: CacheObject::Null,
            value: CacheObject::Null,
            new_value: CacheObject::Null,
            old_value: CacheObject::Null,
            metadata: None,
            old_metadata: None,
            origin_local: true,
            command_retried: false,
            created: false,
            entries: Vec::new(),
            transaction_id: None,
            transaction_successful: false,
        }
    }

    /// `Event.getType()`
    pub fn get_type(&self) -> EventKind {
        self.kind
    }
}

/// A listener method's parameter type, reduced to what `testListenerMethodValidity` checks:
/// `m.getParameterTypes()[0].isAssignableFrom(allowedParameter)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventClass {
    /// `Event`
    Event,
    /// `CacheEntryEvent`
    CacheEntryEvent,
    /// `TransactionalEvent`
    TransactionalEvent,
    /// The event interface of exactly this kind (`CacheEntryRemovedEvent`)
    Concrete(EventKind),
    /// Any other class (`String`): assignable from no event
    Other(String),
}

impl EventClass {
    /// `Class.isAssignableFrom(eventClassOf(kind))`.
    pub fn is_assignable_from(&self, kind: EventKind) -> bool {
        match self {
            EventClass::Event => true,
            EventClass::CacheEntryEvent => kind.is_cache_entry_event(),
            EventClass::TransactionalEvent => kind.is_transactional_event(),
            EventClass::Concrete(k) => *k == kind,
            EventClass::Other(_) => false,
        }
    }
}
