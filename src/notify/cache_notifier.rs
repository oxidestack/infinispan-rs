//! `CacheNotifierImpl` (`notifications/cachelistener/CacheNotifierImpl.java`): the per-cache registry of
//! listener invocations and the `notify*` methods that build an event, run each listener's filter and
//! converter, and call the listener.
//!
//! What a REPL_SYNC non-transactional cache needs is ported in full: every `notify*` entry point
//! (created, modified, removed, visited, activated, passivated, loaded, invalidated, evicted, expired,
//! transaction registered/completed), `Observation`, `primaryOnly`, `SKIP_LISTENER_NOTIFICATION`,
//! `PUT_FOR_STATE_TRANSFER` and the commit-type gate (an entry is notified only on a node that commits
//! it), the filter/converter/filter-converter pipeline of `BaseCacheEntryListenerInvocation`, and the
//! cluster-listener rules of `addListenerInternal` (below).
//!
//! **Cluster listeners** (`@Listener(clustered = true)`, `:1443-1445`, `:1278-1282`, `:1027-1119`):
//! - observation is forced to `POST`, whatever the annotation says (`adjustCacheInvocationBuilder`);
//! - a class observing only `PRE` is refused (ISPN000364), after its invocations were added;
//! - an invalidation cache refuses them, also after the invocations were added;
//! - `clusterListenerOnPrimaryOnly()` is `mode.isDistributed() || isScattered() ||
//!   (mode.isReplicated() && zeroCapacity)`. For a replicated cache with capacity the listener is
//!   therefore an ordinary **local** listener on the node it was added to: it fires for writes that
//!   originate on this node and for backup writes that arrive from the primary alike (the comment in
//!   MockServer's `InfinispanStateBackend.java:295-300` says "remote writes only"; it is wrong), and no
//!   `ClusterListenerReplicateCallable` or `MultiClusterEventCommand` goes on the wire;
//! - a `@CacheEntriesEvicted` method is never registered (see `listener_registration`).
//!
//! Not ported: listeners on primary-only caches (DIST, scattered, zero-capacity REPL: they need the
//! cluster executor and `ClusterEventManager`), `includeCurrentState` (initial state transfer through
//! the publisher manager), indexed filters (`FilterIndexingServiceProvider`), data conversion
//! (`DataConversion`/`EncoderRegistry`; keys and values are delivered as stored), the async notification
//! executor (a listener with `sync = false` runs on the calling thread), the transaction manager
//! suspend/resume around invocations, and `ListenerHolder`/class-loader/`Subject` plumbing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::event::{Event, EventKind};
use super::filter::{CacheEventConverter, CacheEventFilter, CacheEventFilterConverter, EventType};
use super::listener_registration::{ListenerDefinition, ListenerHandler, NotifierError, Observation, collect_invocations, test_listener_class_validity};
use crate::commands::metadata::Metadata;
use crate::commands::object::CacheObject;
use crate::commands::write::flags;

/// `CacheMode`, reduced to what listener registration asks of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    /// `LOCAL`.
    Local,
    /// `REPL_SYNC`.
    ReplSync,
    /// `REPL_ASYNC`.
    ReplAsync,
    /// `INVALIDATION_SYNC`.
    InvalSync,
    /// `INVALIDATION_ASYNC`.
    InvalAsync,
    /// `DIST_SYNC`.
    DistSync,
    /// `DIST_ASYNC`.
    DistAsync,
    /// `SCATTERED_SYNC`.
    ScatteredSync,
}

impl CacheMode {
    /// Whether the mode is replicated.
    pub fn is_replicated(self) -> bool {
        matches!(self, CacheMode::ReplSync | CacheMode::ReplAsync)
    }
    /// Whether the mode is distributed.
    pub fn is_distributed(self) -> bool {
        matches!(self, CacheMode::DistSync | CacheMode::DistAsync)
    }
    /// Whether the mode is invalidation.
    pub fn is_invalidation(self) -> bool {
        matches!(self, CacheMode::InvalSync | CacheMode::InvalAsync)
    }
    /// Whether the mode is scattered.
    pub fn is_scattered(self) -> bool {
        matches!(self, CacheMode::ScatteredSync)
    }
}

/// The parts of `Configuration`/`GlobalConfiguration` the notifier reads.
#[derive(Debug, Clone)]
pub struct NotifierConfig {
    /// The cache's name.
    pub cache_name: String,
    /// The cache's mode.
    pub cache_mode: CacheMode,
    /// `clustering().hash().capacityFactor()`
    pub capacity_factor: f32,
    /// `globalConfiguration.isZeroCapacityNode()`
    pub zero_capacity_node: bool,
}

impl NotifierConfig {
    /// The configuration of `cache_name` in `cache_mode`, with capacity factor 1.
    pub fn new(cache_name: &str, cache_mode: CacheMode) -> Self {
        NotifierConfig { cache_name: cache_name.to_string(), cache_mode, capacity_factor: 1.0, zero_capacity_node: false }
    }
}

/// `InvocationContext`, reduced to the two facts the notifier reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvocationContext {
    /// `isOriginLocal()`
    pub origin_local: bool,
    /// `isInTxScope()`
    pub in_tx_scope: bool,
}

impl InvocationContext {
    /// `new NonTxInvocationContext(null)` seen from the originator.
    pub const LOCAL: InvocationContext = InvocationContext { origin_local: true, in_tx_scope: false };
    /// A context on a node that received the command from another.
    pub const REMOTE: InvocationContext = InvocationContext { origin_local: false, in_tx_scope: false };
}

/// The `FlagAffectedCommand` a notification is raised for: its flags and, when known, its segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CommandInfo {
    /// The command's `Flag` bits.
    pub flags: i64,
    /// The segment of the command's key.
    pub segment: Option<i32>,
}

impl CommandInfo {
    /// Whether any bit of `mask` is set.
    pub fn has_any_flag(&self, mask: i64) -> bool {
        self.flags & mask != 0
    }
}

/// `ClusteringDependentLogic`, reduced to the two questions the notifier asks.
pub trait ClusteringDependentLogic: Send + Sync {
    /// `commitType(command, ctx, segment, removed).isLocal()`: whether this node commits the entry
    /// (and so raises the event). `ReplicationLogic`: only when this node is a write owner of the
    /// segment, or `SKIP_OWNERSHIP_CHECK` is set (`ClusteringDependentLogic.java:294-309`).
    fn commit_is_local(&self, key: &CacheObject, command: Option<&CommandInfo>, removed: bool) -> bool;
    /// `getCacheTopology().getDistribution(key).isPrimary()`
    fn is_local_primary_owner(&self, key: &CacheObject) -> bool;
}

/// `ClusteringDependentLogic.LocalLogic`: every commit is local, and this node is every key's primary.
pub struct LocalLogic;

impl ClusteringDependentLogic for LocalLogic {
    fn commit_is_local(&self, _key: &CacheObject, _command: Option<&CommandInfo>, _removed: bool) -> bool {
        true
    }
    fn is_local_primary_owner(&self, _key: &CacheObject) -> bool {
        true
    }
}

/// One registered `(listener, method, annotation)`: `BaseCacheEntryListenerInvocation`.
struct Invocation {
    target: Arc<ListenerDefinition>,
    method_name: String,
    handler: ListenerHandler,
    annotation: EventKind,
    filter: Option<Arc<dyn CacheEventFilter>>,
    converter: Option<Arc<dyn CacheEventConverter>>,
    /// a filter that is also the converter: `filterAndConvert` is the only method called
    filter_and_convert: Option<Arc<dyn CacheEventFilterConverter>>,
    only_primary: bool,
    clustered: bool,
    identifier: u64,
    sync: bool,
    observation: Observation,
}

/// What a listener was registered with besides its definition.
#[derive(Default, Clone)]
pub struct ListenerFilters {
    /// Accepts or drops each event.
    pub filter: Option<Arc<dyn CacheEventFilter>>,
    /// Replaces each event's value.
    pub converter: Option<Arc<dyn CacheEventConverter>>,
    /// one object as filter *and* converter (`addListener(l, fc, fc)`)
    pub filter_and_convert: Option<Arc<dyn CacheEventFilterConverter>>,
}

/// `CacheNotifierImpl`: holds a cache's listeners and raises their events.
pub struct CacheNotifier {
    cfg: NotifierConfig,
    logic: Arc<dyn ClusteringDependentLogic>,
    /// `listenersMap`: `CopyOnWriteArrayList`s, so a snapshot is taken before invoking
    lists: RwLock<HashMap<EventKind, Vec<Arc<Invocation>>>>,
    ids: AtomicU64,
}

impl CacheNotifier {
    /// A notifier for the cache `cfg` describes, asking `logic` which node is primary for a key.
    pub fn new(cfg: NotifierConfig, logic: Arc<dyn ClusteringDependentLogic>) -> Self {
        let lists = EventKind::ALL.iter().map(|k| (*k, Vec::new())).collect();
        CacheNotifier { cfg, logic, lists: RwLock::new(lists), ids: AtomicU64::new(1) }
    }

    /// The cache's name.
    pub fn cache_name(&self) -> &str {
        &self.cfg.cache_name
    }

    /// `clusterListenerOnPrimaryOnly()` (`:1278-1282`).
    pub fn cluster_listener_on_primary_only(&self) -> bool {
        let mode = self.cfg.cache_mode;
        let zero_capacity = self.cfg.capacity_factor == 0.0 || self.cfg.zero_capacity_node;
        mode.is_distributed() || mode.is_scattered() || (mode.is_replicated() && zero_capacity)
    }

    // ------------------------------------------------------------------ registration

    /// `addListener(listener)`.
    pub fn add_listener(&self, listener: &Arc<ListenerDefinition>) -> Result<(), NotifierError> {
        self.add_listener_filtered(listener, ListenerFilters::default())
    }

    /// `addListener(listener, filter, converter)`.
    pub fn add_listener_filtered(&self, listener: &Arc<ListenerDefinition>, filters: ListenerFilters) -> Result<(), NotifierError> {
        self.add_listener_internal(listener, filters)
    }

    /// `AbstractListenerImpl.getListeners()`: the distinct targets with at least one invocation.
    pub fn get_listeners(&self) -> Vec<Arc<ListenerDefinition>> {
        let lists = self.lists.read().unwrap();
        let mut out: Vec<Arc<ListenerDefinition>> = Vec::new();
        for kind in EventKind::ALL {
            for li in &lists[&kind] {
                if !out.iter().any(|t| Arc::ptr_eq(t, &li.target)) {
                    out.push(li.target.clone());
                }
            }
        }
        out
    }

    /// `hasListener(annotation)`.
    pub fn has_listener(&self, kind: EventKind) -> bool {
        !self.lists.read().unwrap()[&kind].is_empty()
    }

    /// How many invocations are registered for an annotation (`n.cacheEntryVisitedListeners.size()`).
    pub fn invocation_count(&self, kind: EventKind) -> usize {
        self.lists.read().unwrap()[&kind].len()
    }

    /// `removeListener(listener)`: `removeListenerFromMaps` validates the class first.
    pub fn remove_listener(&self, listener: &Arc<ListenerDefinition>) -> Result<(), NotifierError> {
        test_listener_class_validity(listener)?;
        let mut lists = self.lists.write().unwrap();
        for list in lists.values_mut() {
            list.retain(|li| !Arc::ptr_eq(&li.target, listener));
        }
        // clusterListenerIDs: only primary-only listeners are tracked there, and none can be registered
        Ok(())
    }

    /// `AbstractListenerImpl.stop()`: "removes all listeners from the notifier".
    pub fn stop(&self) {
        for list in self.lists.write().unwrap().values_mut() {
            list.clear();
        }
    }

    fn add_listener_internal(&self, listener: &Arc<ListenerDefinition>, filters: ListenerFilters) -> Result<(), NotifierError> {
        let l = test_listener_class_validity(listener)?;
        if l.include_current_state {
            return Err(NotifierError::NotPorted("includeCurrentState (initial state through the publisher manager) is not ported".into()));
        }
        let only_primary = if l.clustered { self.cluster_listener_on_primary_only() } else { l.primary_only };
        if l.clustered && only_primary {
            return Err(NotifierError::NotPorted(
                "cluster listeners on a primary-only cache (DIST, scattered, zero-capacity REPL) need the cluster executor and are not ported".into(),
            ));
        }
        let identifier = self.ids.fetch_add(1, Ordering::Relaxed);
        // adjustCacheInvocationBuilder: a cluster listener observes POST whatever the annotation says
        let observation = if l.clustered { Observation::Post } else { l.observation };
        let found = collect_invocations(listener, &l)?;
        let filter_and_convert = filters.filter_and_convert.clone().filter(|_| filters.converter.is_none());
        {
            let mut lists = self.lists.write().unwrap();
            for (kind, m) in &found {
                lists.get_mut(kind).expect("every kind has a list").push(Arc::new(Invocation {
                    target: listener.clone(),
                    method_name: m.name.clone(),
                    handler: m.handler.clone(),
                    annotation: *kind,
                    filter: filters.filter.clone(),
                    converter: filters.converter.clone(),
                    filter_and_convert: filter_and_convert.clone(),
                    only_primary,
                    clustered: l.clustered,
                    identifier,
                    sync: l.sync,
                    observation,
                }));
            }
        }
        if found.is_empty() {
            // Log.noAnnotateMethodsFoundInListener (ISPN000133)
            log::warn!("ISPN000133: No methods annotated with public methods annotated with method-level event annotations found in {}! Ignoring listener.", listener.class_name);
        }
        if !found.is_empty() && l.clustered {
            if l.observation == Observation::Pre {
                return Err(NotifierError::Cache(format!(
                    "ISPN000364: Attempted to register cluster listener of class {}, but listener is annotated as only observing pre events!",
                    listener.class_name
                )));
            } else if self.cfg.cache_mode.is_invalidation() {
                return Err(NotifierError::UnsupportedOperation("Cluster listeners cannot be used with Invalidation Caches!".into()));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ notification

    fn snapshot(&self, kind: EventKind) -> Vec<Arc<Invocation>> {
        self.lists.read().unwrap()[&kind].clone()
    }

    /// `isNotificationAllowed`: there are listeners and the command does not skip them.
    fn is_notification_allowed(&self, command: Option<&CommandInfo>, kind: EventKind) -> bool {
        self.has_listener(kind) && command.is_none_or(|c| !c.has_any_flag(flags::SKIP_LISTENER_NOTIFICATION))
    }

    /// `EventImpl` of the created/modified/removed/invalidated family (`configureEvent`, `:529-551`).
    #[allow(clippy::too_many_arguments)]
    fn configure(&self, e: &mut Event, key: &CacheObject, value: &CacheObject, metadata: Option<Metadata>, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>, previous_value: &CacheObject, previous_metadata: Option<Metadata>) {
        e.origin_local = ctx.origin_local;
        e.pre = pre;
        e.value = if pre { previous_value.clone() } else { value.clone() };
        e.new_value = value.clone();
        e.old_value = previous_value.clone();
        e.old_metadata = previous_metadata;
        e.metadata = metadata;
        if command.is_some_and(|c| c.has_any_flag(flags::COMMAND_RETRY)) {
            e.command_retried = true;
        }
        e.key = key.clone();
    }

    /// `notifyCacheEntryCreated` (`:407-443`).
    pub fn notify_cache_entry_created(&self, key: &CacheObject, value: &CacheObject, metadata: Option<Metadata>, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, EventKind::CacheEntryCreated) {
            return Ok(());
        }
        let state_transfer = command.is_some_and(|c| c.has_any_flag(flags::PUT_FOR_STATE_TRANSFER));
        if !self.logic.commit_is_local(key, command, false) || state_transfer {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryCreated) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryCreated);
            self.configure(&mut e, key, value, metadata, pre, ctx, command, &CacheObject::Null, None);
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntryModified` (`:445-481`).
    #[allow(clippy::too_many_arguments)]
    pub fn notify_cache_entry_modified(&self, key: &CacheObject, value: &CacheObject, metadata: Option<Metadata>, previous_value: &CacheObject, previous_metadata: Option<Metadata>, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, EventKind::CacheEntryModified) {
            return Ok(());
        }
        let state_transfer = command.is_some_and(|c| c.has_any_flag(flags::PUT_FOR_STATE_TRANSFER));
        if !self.logic.commit_is_local(key, command, false) || state_transfer {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryModified) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryModified);
            self.configure(&mut e, key, value, metadata, pre, ctx, command, previous_value, previous_metadata);
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntryRemoved` (`:483-526`). Unlike created/modified it has no state-transfer check.
    pub fn notify_cache_entry_removed(&self, key: &CacheObject, previous_value: &CacheObject, previous_metadata: Option<Metadata>, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, EventKind::CacheEntryRemoved) {
            return Ok(());
        }
        if !self.logic.commit_is_local(key, command, true) {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryRemoved) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryRemoved);
            if pre {
                self.configure(&mut e, key, previous_value, previous_metadata, true, ctx, command, previous_value, previous_metadata);
            } else {
                // upstream keeps previousMetadata as the event's metadata when pre=false (`:501-504`)
                self.configure(&mut e, key, &CacheObject::Null, previous_metadata, false, ctx, command, previous_value, previous_metadata);
            }
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// The visited/loaded/activated family (`configureEvent(.., key, value, pre, ctx)`, `:553-563`).
    fn notify_simple_entry(&self, kind: EventKind, key: &CacheObject, value: &CacheObject, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, kind) {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(kind) {
            let mut e = Event::new(&self.cfg.cache_name, kind);
            e.pre = pre;
            e.key = key.clone();
            e.value = value.clone();
            e.origin_local = ctx.origin_local;
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntryVisited`.
    pub fn notify_cache_entry_visited(&self, key: &CacheObject, value: &CacheObject, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        self.notify_simple_entry(EventKind::CacheEntryVisited, key, value, pre, ctx, command)
    }

    /// `notifyCacheEntryLoaded`.
    pub fn notify_cache_entry_loaded(&self, key: &CacheObject, value: &CacheObject, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        self.notify_simple_entry(EventKind::CacheEntryLoaded, key, value, pre, ctx, command)
    }

    /// `notifyCacheEntryActivated`.
    pub fn notify_cache_entry_activated(&self, key: &CacheObject, value: &CacheObject, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        self.notify_simple_entry(EventKind::CacheEntryActivated, key, value, pre, ctx, command)
    }

    /// `notifyCacheEntryPassivated` (`:741-769`): no context, so the event's origin stays local.
    pub fn notify_cache_entry_passivated(&self, key: &CacheObject, value: &CacheObject, pre: bool, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, EventKind::CacheEntryPassivated) {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryPassivated) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryPassivated);
            e.pre = pre;
            e.key = key.clone();
            e.value = value.clone();
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntryInvalidated` (`:664-686`).
    pub fn notify_cache_entry_invalidated(&self, key: &CacheObject, value: &CacheObject, metadata: Option<Metadata>, pre: bool, ctx: &InvocationContext, command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if !self.is_notification_allowed(command, EventKind::CacheEntryInvalidated) {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryInvalidated) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryInvalidated);
            self.configure(&mut e, key, value, metadata, pre, ctx, command, value, metadata);
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntryExpired` (`:631-662`): origin local, never pre, and no command, so no retry flag.
    pub fn notify_cache_entry_expired(&self, key: &CacheObject, value: &CacheObject, metadata: Option<Metadata>, _ctx: Option<&InvocationContext>) -> Result<(), NotifierError> {
        if !self.has_listener(EventKind::CacheEntryExpired) {
            return Ok(());
        }
        let primary = self.logic.is_local_primary_owner(key);
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntryExpired) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntryExpired);
            e.key = key.clone();
            e.value = value.clone();
            e.metadata = metadata;
            e.origin_local = true;
            e.pre = false;
            record(&mut first_error, li.invoke_local(e, primary));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyCacheEntriesEvicted` (`:597-629`): one event with all the entries; it goes through the
    /// plain `invoke(Event)`, so no filter or converter applies.
    pub fn notify_cache_entries_evicted(&self, entries: &[(CacheObject, CacheObject)], command: Option<&CommandInfo>) -> Result<(), NotifierError> {
        if entries.is_empty() || !self.is_notification_allowed(command, EventKind::CacheEntriesEvicted) {
            return Ok(());
        }
        let mut first_error = None;
        for li in self.snapshot(EventKind::CacheEntriesEvicted) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::CacheEntriesEvicted);
            e.entries = entries.to_vec();
            record(&mut first_error, li.invoke_plain(&e));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyTransactionRegistered` (`:801-818`).
    pub fn notify_transaction_registered(&self, transaction: &str, is_origin_local: bool) -> Result<(), NotifierError> {
        if !self.has_listener(EventKind::TransactionRegistered) {
            return Ok(());
        }
        let mut first_error = None;
        for li in self.snapshot(EventKind::TransactionRegistered) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::TransactionRegistered);
            e.origin_local = is_origin_local;
            e.transaction_id = Some(transaction.to_string());
            record(&mut first_error, li.invoke_plain(&e));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// `notifyTransactionCompleted` (`:771-799`).
    pub fn notify_transaction_completed(&self, transaction: &str, successful: bool, ctx: &InvocationContext) -> Result<(), NotifierError> {
        if !self.has_listener(EventKind::TransactionCompleted) {
            return Ok(());
        }
        let mut first_error = None;
        for li in self.snapshot(EventKind::TransactionCompleted) {
            let mut e = Event::new(&self.cfg.cache_name, EventKind::TransactionCompleted);
            e.origin_local = ctx.origin_local;
            e.transaction_id = Some(transaction.to_string());
            e.transaction_successful = successful;
            record(&mut first_error, li.invoke_plain(&e));
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn record(first: &mut Option<NotifierError>, r: Result<(), NotifierError>) {
    if let Err(e) = r {
        first.get_or_insert(e);
    }
}

impl std::fmt::Debug for Invocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{} for {:?} (id={}, onlyPrimary={}, clustered={})", self.target.class_name, self.method_name, self.annotation, self.identifier, self.only_primary, self.clustered)
    }
}

impl Invocation {
    /// The entry point for local listeners firing events: `invoke(EventWrapper, isLocalNodePrimaryOwner)`
    /// (`:1752-1760`): filter first, then convert, then call.
    fn invoke_local(&self, event: Event, is_local_primary: bool) -> Result<(), NotifierError> {
        match self.should_invoke(event, is_local_primary) {
            Some(resulting) => {
                // invokeNoChecks(wrapped, false, filterAndConvert, false)
                let skip_converter = self.filter_and_convert.is_some();
                let event = if skip_converter { resulting } else { self.convert_value(resulting) };
                self.call(&event)
            }
            None => Ok(()),
        }
    }

    /// `invoke(Event)` (`:1738-1743`): only the observation is checked.
    fn invoke_plain(&self, event: &Event) -> Result<(), NotifierError> {
        if self.observation.should_invoke(event.pre) { self.call(event) } else { Ok(()) }
    }

    fn call(&self, event: &Event) -> Result<(), NotifierError> {
        match (self.handler)(event) {
            Ok(()) => Ok(()),
            Err(cause) if self.sync => Err(NotifierError::Listener(format!(
                "ISPN000280: Caught exception [{cause}] while invoking method [{}] on listener instance: {}",
                self.method_name, self.target.class_name
            ))),
            Err(cause) => {
                // Log.unableToInvokeListenerMethod: an async listener's failure is logged only
                log::warn!("ISPN000281: Unable to invoke method {} on Object instance {}: {cause}", self.method_name, self.target.class_name);
                Ok(())
            }
        }
    }

    /// `shouldInvoke(event, isLocalNodePrimaryOwner)` (`:1800-1830`).
    fn should_invoke(&self, event: Event, is_local_primary: bool) -> Option<Event> {
        if self.only_primary && !is_local_primary {
            return None;
        }
        if !self.observation.should_invoke(event.pre) {
            return None;
        }
        let Some(event_type) = event_type_of(&event) else { return Some(event) };
        if let Some(fc) = &self.filter_and_convert {
            let v = fc.filter_and_convert(&event.key, &event.old_value, event.old_metadata.as_ref(), &event.value, event.metadata.as_ref(), event_type);
            return if v.is_null() { None } else { Some(with_value(event, v)) };
        }
        if let Some(filter) = &self.filter {
            let accept = filter.accept(&event.key, &event.old_value, event.old_metadata.as_ref(), &event.value, event.metadata.as_ref(), event_type);
            if !accept {
                return None;
            }
            // converter == null: convertEventToRequestFormat(eventImpl, filter, null, eventImpl.getValue())
        }
        Some(event)
    }

    /// `convertValue(converter, event)` (`:1906-1940`): the converter's result becomes the event's value,
    /// a null result included.
    fn convert_value(&self, event: Event) -> Event {
        let Some(converter) = &self.converter else { return event };
        let Some(event_type) = event_type_of(&event) else { return event };
        let new_value = converter.convert(&event.key, &event.old_value, event.old_metadata.as_ref(), &event.value, event.metadata.as_ref(), event_type);
        with_value(event, new_value)
    }
}

/// `getEvent(EventImpl)` (`:1834-1848`): events with a single key/value pair can be filtered.
fn event_type_of(event: &Event) -> Option<EventType> {
    event.kind.is_cache_entry_event().then_some(EventType { retry: event.command_retried, pre: event.pre, kind: event.kind })
}

/// `convertEventToRequestFormat(.., newValue)` with identity data conversion: a clone whose value is `v`.
fn with_value(mut event: Event, v: CacheObject) -> Event {
    event.value = v;
    event
}
