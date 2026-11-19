//! The user-facing cache API on one node: what `CacheImpl`/`EncoderCache` do before a command enters the
//! interceptor chain (`cache/impl/CacheImpl.java`, `EncoderCache.java:661-686`): build the command with
//! its segment, default metadata, matcher, flags and a fresh `CommandInvocationId`, run it through the
//! [`CacheEngine`], and turn the outcome into the Java return value.
//!
//! Matchers per operation (`PutKeyValueCommand`/`ReplaceCommand`/`RemoveCommand` constructors):
//! `put` `MATCH_ALWAYS`; `putIfAbsent` `MATCH_EXPECTED`; `replace(k, v)` `MATCH_NON_NULL`;
//! `replace(k, old, new)` and `remove(k, v)` `MATCH_EXPECTED`; `remove(k)` `MATCH_ALWAYS`.

use std::sync::Arc;
use std::time::Duration;

use super::write_pipeline::{CacheEngine, CacheError, EntryListener, Outcome};
use crate::ch::address::Address;
use crate::commands::compute_function::{BiFunctionMapper, CacheLambda};
use crate::commands::metadata::{Expiry, Metadata};
use crate::commands::object::CacheObject;
use crate::commands::write::*;
use crate::marshall::primitives::PrimitiveValue;
use crate::notify::cache_notifier::ListenerFilters;
use crate::notify::listener_registration::{ListenerDefinition, NotifierError};

/// `EncoderCache`: the application's handle on a clustered cache.
#[derive(Clone)]
pub struct Cache {
    engine: Arc<CacheEngine>,
    flags: i64,
}

impl Cache {
    /// The cache running on `engine`.
    pub fn new(engine: Arc<CacheEngine>) -> Self {
        Self { engine, flags: 0 }
    }

    /// The engine the cache runs on.
    pub fn engine(&self) -> &Arc<CacheEngine> {
        &self.engine
    }

    /// The cache's name.
    pub fn name(&self) -> &str {
        &self.engine.cfg.name
    }

    /// The local node's address.
    pub fn address(&self) -> Address {
        self.engine.address()
    }

    /// `AdvancedCache.withFlags`.
    pub fn with_flags(&self, flags: i64) -> Cache {
        Cache { engine: self.engine.clone(), flags: self.flags | flags }
    }

    /// Registers `l`, called after each change applied on this node.
    pub fn add_listener(&self, l: Arc<dyn EntryListener>) {
        self.engine.add_listener(l);
    }

    /// `Cache.addListener(listener)`: register an annotated listener (see [`crate::notify`]).
    pub fn add_cache_listener(&self, l: &Arc<ListenerDefinition>) -> Result<(), NotifierError> {
        self.engine.add_cache_listener(l, ListenerFilters::default())
    }

    /// `Listenable.addListener(listener, filter, converter)`.
    pub fn add_cache_listener_filtered(&self, l: &Arc<ListenerDefinition>, filters: ListenerFilters) -> Result<(), NotifierError> {
        self.engine.add_cache_listener(l, filters)
    }

    /// `Cache.removeListener(listener)`.
    pub fn remove_cache_listener(&self, l: &Arc<ListenerDefinition>) -> Result<(), NotifierError> {
        self.engine.notifier().remove_listener(l)
    }

    /// `AdvancedCache.getListeners()`.
    pub fn cache_listeners(&self) -> Vec<Arc<ListenerDefinition>> {
        self.engine.notifier().get_listeners()
    }

    fn id(&self) -> CommandInvocationId {
        CommandInvocationId::generate(self.address())
    }

    fn visible(&self, value: CacheObject) -> Option<CacheObject> {
        if self.flags & flags::IGNORE_RETURN_VALUES != 0 || value.is_null() { None } else { Some(value) }
    }

    async fn run(&self, cmd: WriteCommand) -> Result<Outcome, CacheError> {
        self.engine.execute(cmd).await
    }

    /// `put(key, value)`: stores `value` and returns the previous one.
    pub async fn put(&self, key: &CacheObject, value: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        self.put_with_metadata(key, value, Metadata::DEFAULT).await
    }

    /// `put(key, value, lifespan, unit)`.
    pub async fn put_with_lifespan(&self, key: &CacheObject, value: &CacheObject, lifespan: Duration) -> Result<Option<CacheObject>, CacheError> {
        let m = Metadata { expiry: Expiry::Expirable { lifespan: lifespan.as_millis() as i64, max_idle: -1 }, version: None };
        self.put_with_metadata(key, value, m).await
    }

    /// `put` with the expiration and version in `metadata`.
    pub async fn put_with_metadata(&self, key: &CacheObject, value: &CacheObject, metadata: Metadata) -> Result<Option<CacheObject>, CacheError> {
        let o = self
            .run(WriteCommand::Put(PutKeyValue {
                key: key.clone(),
                value: value.clone(),
                segment: self.engine.segment_of(key)?,
                metadata: Some(metadata),
                value_matcher: ValueMatcher::MatchAlways,
                invocation_id: self.id(),
                flags: self.flags,
                put_if_absent: false,
                return_entry: false,
                topology_id: -1,
            }))
            .await?;
        Ok(self.visible(o.value))
    }

    /// `putIfAbsent`: the existing value, or `None` when the value was stored.
    pub async fn put_if_absent(&self, key: &CacheObject, value: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        let o = self
            .run(WriteCommand::Put(PutKeyValue {
                key: key.clone(),
                value: value.clone(),
                segment: self.engine.segment_of(key)?,
                metadata: Some(Metadata::DEFAULT),
                value_matcher: ValueMatcher::MatchExpected,
                invocation_id: self.id(),
                flags: self.flags,
                put_if_absent: true,
                return_entry: false,
                topology_id: -1,
            }))
            .await?;
        Ok(if o.value.is_null() { None } else { Some(o.value) })
    }

    /// `compute(key, lambda)`: the new value (`None` when the lambda removed or never created it).
    pub async fn compute(&self, key: &CacheObject, function: CacheLambda) -> Result<Option<CacheObject>, CacheError> {
        let o = self
            .run(WriteCommand::Compute(Compute {
                key: key.clone(),
                compute_if_present: false,
                function: BiFunctionMapper::for_default_cache(function),
                segment: self.engine.segment_of(key)?,
                metadata: Some(Metadata::DEFAULT),
                invocation_id: self.id(),
                flags: self.flags,
                topology_id: -1,
            }))
            .await?;
        Ok(if o.value.is_null() { None } else { Some(o.value) })
    }

    /// `replace(key, value)`: the previous value, `None` (and nothing stored) when the key was absent.
    pub async fn replace(&self, key: &CacheObject, value: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        let o = self.replace_command(key, CacheObject::Null, value, ValueMatcher::MatchNonNull).await?;
        Ok(self.visible(o.value))
    }

    /// `replace(key, oldValue, newValue)`.
    pub async fn replace_if(&self, key: &CacheObject, old: &CacheObject, new: &CacheObject) -> Result<bool, CacheError> {
        let o = self.replace_command(key, old.clone(), new, ValueMatcher::MatchExpected).await?;
        Ok(o.successful && o.value == CacheObject::Primitive(PrimitiveValue::BooleanObj(true)))
    }

    async fn replace_command(&self, key: &CacheObject, old: CacheObject, new: &CacheObject, matcher: ValueMatcher) -> Result<Outcome, CacheError> {
        self.run(WriteCommand::Replace(Replace {
            key: key.clone(),
            old_value: old,
            new_value: new.clone(),
            return_entry: false,
            segment: self.engine.segment_of(key)?,
            metadata: Some(Metadata::DEFAULT),
            value_matcher: matcher,
            flags: self.flags,
            invocation_id: self.id(),
            topology_id: -1,
        }))
        .await
    }

    /// `remove(key)`: the removed value.
    pub async fn remove(&self, key: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        let o = self.remove_command(key, CacheObject::Null, ValueMatcher::MatchAlways).await?;
        Ok(self.visible(o.value))
    }

    /// `remove(key, value)`.
    pub async fn remove_if(&self, key: &CacheObject, value: &CacheObject) -> Result<bool, CacheError> {
        let o = self.remove_command(key, value.clone(), ValueMatcher::MatchExpected).await?;
        Ok(o.successful && o.value == CacheObject::Primitive(PrimitiveValue::BooleanObj(true)))
    }

    async fn remove_command(&self, key: &CacheObject, value: CacheObject, matcher: ValueMatcher) -> Result<Outcome, CacheError> {
        self.run(WriteCommand::Remove(Remove {
            key: key.clone(),
            value,
            return_entry: false,
            segment: self.engine.segment_of(key)?,
            metadata: None,
            flags: self.flags,
            value_matcher: matcher,
            invocation_id: self.id(),
            topology_id: -1,
        }))
        .await
    }

    /// `evict(key)`: node-local, not replicated (see [`CacheEngine::evict`]).
    pub fn evict(&self, key: &CacheObject) -> bool {
        self.engine.evict(key)
    }

    /// `clear()`: removes every entry, on every node.
    pub async fn clear(&self) -> Result<(), CacheError> {
        self.run(WriteCommand::Clear(Clear { flags: self.flags, topology_id: -1 })).await.map(|_| ())
    }

    /// `get(key)`.
    pub async fn get(&self, key: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        self.engine.get_with_flags(key, self.flags).await
    }

    /// `containsKey(key)`.
    pub async fn contains_key(&self, key: &CacheObject) -> Result<bool, CacheError> {
        Ok(self.get(key).await?.is_some())
    }

    /// `size()`: local for REPL.
    pub fn size(&self) -> usize {
        self.engine.size()
    }

    /// Whether the cache holds no entry on this node.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// The keys on this node.
    pub fn keys(&self) -> Vec<CacheObject> {
        self.engine.keys()
    }

    /// The entries on this node.
    pub fn entries(&self) -> Vec<(CacheObject, CacheObject)> {
        self.engine.entries()
    }
}
