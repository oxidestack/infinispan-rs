//! The `ClusterListener` of `AbstractClusterListenerUtilTest`, shared by the cluster listener tests.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use infinispan::cache::encoder_cache::Cache;
use infinispan::commands::metadata::Metadata;
use infinispan::commands::object::CacheObject;
use infinispan::marshall::primitives::PrimitiveValue;
use infinispan::notify::cache_notifier::ListenerFilters;
use infinispan::notify::event::{Event, EventKind};
use infinispan::notify::filter::{CacheEventConverter, CacheEventFilter, CacheEventFilterConverter, EventType, KeyValueFilter};
use infinispan::notify::listener_registration::{ListenerAttrs, ListenerDefinition, ListenerMethod};

use super::repl::s;

/// `ClusterListener`: `@Listener(clustered = true)` with created, modified, removed and expired handlers.
#[derive(Default)]
pub struct ClusterListener {
    pub events: Mutex<Vec<Event>>,
}

impl ClusterListener {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn definition(self: &Arc<Self>) -> Arc<ListenerDefinition> {
        let methods = [
            ("onCreatedEvent", EventKind::CacheEntryCreated),
            ("onModifiedEvent", EventKind::CacheEntryModified),
            ("onRemoveEvent", EventKind::CacheEntryRemoved),
            ("onExpireEvent", EventKind::CacheEntryExpired),
        ]
        .into_iter()
        .map(|(name, kind)| {
            let me = self.clone();
            ListenerMethod::new(name, &[kind], move |e| me.events.lock().unwrap().push(e.clone()))
        })
        .collect();
        ListenerDefinition::new("ClusterListener", ListenerAttrs::clustered(), methods)
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

pub fn listen_with(cache: &Cache, filters: ListenerFilters) -> Arc<ClusterListener> {
    let l = ClusterListener::new();
    cache.add_cache_listener_filtered(&l.definition(), filters).unwrap();
    l
}

/// `cache.addListener(listener())`.
pub fn listen(cache: &Cache) -> Arc<ClusterListener> {
    let l = ClusterListener::new();
    cache.add_cache_listener(&l.definition()).unwrap();
    l
}

// --- the filters and converters of AbstractClusterListenerUtilTest ---------------------------------------

/// `filter(keyToFilter)`: `Objects.equals(k, keyToFilter)`.
pub struct KeyEquals(pub CacheObject);

impl CacheEventFilter for KeyEquals {
    fn accept(&self, key: &CacheObject, _: &CacheObject, _: Option<&Metadata>, _: &CacheObject, _: Option<&Metadata>, _: EventType) -> bool {
        *key == self.0
    }
}

/// `LifespanFilter`: only entries with a lifespan longer than the threshold.
pub struct LifespanFilter(pub i64);

impl KeyValueFilter for LifespanFilter {
    fn accept(&self, _: &CacheObject, _: &CacheObject, metadata: Option<&Metadata>) -> bool {
        match metadata {
            None => false,
            Some(m) => m.lifespan_ms() > self.0,
        }
    }
}

/// `NewLifespanLargerFilter`
pub struct NewLifespanLargerFilter;

impl CacheEventFilter for NewLifespanLargerFilter {
    fn accept(&self, _: &CacheObject, _: &CacheObject, old: Option<&Metadata>, _: &CacheObject, new: Option<&Metadata>, _: EventType) -> bool {
        // If neither metadata is provided don't raise the event (this will preclude all creations and removals)
        let (Some(old), Some(new)) = (old, new) else { return true };
        new.lifespan_ms() > old.lifespan_ms()
    }
}

/// `LifespanConverter`
pub struct LifespanConverter {
    pub return_original_value_or_null: bool,
    pub lifespan_threshold: i64,
}

impl CacheEventConverter for LifespanConverter {
    fn convert(&self, _: &CacheObject, _: &CacheObject, _: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, _: EventType) -> CacheObject {
        if let Some(m) = new_metadata {
            let lifespan = m.lifespan_ms();
            if lifespan > self.lifespan_threshold {
                return long(lifespan);
            }
        }
        if self.return_original_value_or_null { new_value.clone() } else { CacheObject::Null }
    }
}

/// `StringTruncator`
pub struct StringTruncator {
    pub beginning: usize,
    pub length: usize,
}

impl CacheEventConverter for StringTruncator {
    fn convert(&self, _: &CacheObject, _: &CacheObject, _: Option<&Metadata>, new_value: &CacheObject, _: Option<&Metadata>, _: EventType) -> CacheObject {
        match new_value.as_str() {
            Some(v) if v.len() > self.beginning + self.length => s(&v[self.beginning..self.beginning + self.length]),
            _ => new_value.clone(),
        }
    }
}

/// `StringAppender`: `oldValue + oldLifespan + newValue + newLifespan`, "null" for a missing metadata.
pub struct StringAppender;

impl CacheEventConverter for StringAppender {
    fn convert(&self, _: &CacheObject, old_value: &CacheObject, old_metadata: Option<&Metadata>, new_value: &CacheObject, new_metadata: Option<&Metadata>, _: EventType) -> CacheObject {
        let show = |v: &CacheObject| v.as_str().map(String::from).unwrap_or_else(|| "null".into());
        let life = |m: Option<&Metadata>| m.map(|m| m.lifespan_ms().to_string()).unwrap_or_else(|| "null".into());
        s(&format!("{}{}{}{}", show(old_value), life(old_metadata), show(new_value), life(new_metadata)))
    }
}

/// `FilterConverter`: `filterAndConvert` is the only method that may be called.
pub struct FilterConverter {
    pub throw_exception_on_non_filter_and_converter_methods: bool,
    pub converted_value: String,
}

impl CacheEventFilter for FilterConverter {
    fn accept(&self, _: &CacheObject, _: &CacheObject, _: Option<&Metadata>, _: &CacheObject, _: Option<&Metadata>, _: EventType) -> bool {
        assert!(!self.throw_exception_on_non_filter_and_converter_methods, "Method should not have been invoked!");
        true
    }
}

impl CacheEventConverter for FilterConverter {
    fn convert(&self, _: &CacheObject, _: &CacheObject, _: Option<&Metadata>, _: &CacheObject, _: Option<&Metadata>, _: EventType) -> CacheObject {
        assert!(!self.throw_exception_on_non_filter_and_converter_methods, "Method should not have been invoked!");
        s(&self.converted_value)
    }
}

impl CacheEventFilterConverter for FilterConverter {
    fn filter_and_convert(&self, _: &CacheObject, _: &CacheObject, _: Option<&Metadata>, _: &CacheObject, _: Option<&Metadata>, _: EventType) -> CacheObject {
        s(&self.converted_value)
    }
}

pub fn long(v: i64) -> CacheObject {
    CacheObject::Primitive(PrimitiveValue::LongObj(v))
}

pub fn filtered(f: impl CacheEventFilter + 'static) -> ListenerFilters {
    ListenerFilters { filter: Some(Arc::new(f)), ..Default::default() }
}

pub fn converted(c: impl CacheEventConverter + 'static) -> ListenerFilters {
    ListenerFilters { converter: Some(Arc::new(c)), ..Default::default() }
}

