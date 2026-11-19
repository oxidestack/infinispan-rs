//! The REPL_SYNC non-transactional write path, folded from `InterceptorChainFactory`'s chain
//! (`factories/InterceptorChainFactory.java:122-282`) into one explicit pipeline that keeps the same
//! observable steps:
//!
//! 1. `VersionInterceptor` stamps a `NumericVersion` on a `ReplaceCommand` without one;
//! 2. the originator computes the key's segment (`HashFunctionPartitioner`) and, when it is not the
//!    segment's primary owner, forwards the *same command* to the primary and waits for the answer
//!    (`BaseDistributionInterceptor.invokeRemotely`, `:686-715`);
//! 3. the primary locks the key (`NonTransactionalLockingInterceptor`), applies the command
//!    (`CallInterceptor`, `:167-376`), and when it succeeded broadcasts it to every member with
//!    `IGNORE_RETURN_VALUES | BACKUP_WRITE` and `MATCH_ALWAYS` (`primaryReturnHandler`, `:228-280`);
//!    every other member, the originator included, applies it without locking;
//! 4. a failed conditional write answers `UnsuccessfulResponse` carrying the return value
//!    (`DefaultResponseGenerator.getResponse`, `remoting/responses/DefaultResponseGenerator.java:17-29`);
//! 5. `ClearCommand` is broadcast by its originator, which then clears locally, with no primary and no
//!    lock (`BaseDistributionInterceptor.visitClearCommand`, `:109-122`).
//!
//! Failure handling kept from `StateTransferInterceptor`: an answer of `OutdatedTopologyException`,
//! `CacheNotFoundResponse` or a suspected target makes the originator wait for a newer topology and
//! retry with the matcher `matcherForRetry()` (`:735-745`).

use std::sync::Arc;
use std::time::Duration;

use super::container::{DataContainer, StoredEntry, key_bytes};
use super::data_container::{DataContainerFactory, MemoryConfig};
use super::equality::java_equals;
use super::locking::{DEFAULT_LOCK_ACQUISITION_TIMEOUT, KeyLocks};
use crate::ch::address::Address;
use crate::ch::hash_function_partitioner::HashFunctionPartitioner;
use crate::ch::murmur_hash3::HashKey;
use crate::commands::metadata::{Metadata, NumericVersionGenerator};
use crate::commands::object::{CacheObject, InternalCacheValue};
use crate::commands::rpc::{CacheRpcCommand, CacheRpcPayload, ClusteredGet};
use crate::commands::write::*;
use crate::marshall::primitives::PrimitiveValue;
use crate::notify::cache_notifier::{CacheMode, CacheNotifier, ClusteringDependentLogic, CommandInfo, InvocationContext, ListenerFilters, NotifierConfig};
use crate::notify::event::{Event, EventKind};
use crate::notify::listener_registration::{ListenerAttrs, ListenerDefinition, ListenerMethod, NotifierError};
use crate::topology::cache_topology::CacheTopology;
use crate::topology::messages::{ExceptionKind, Response, ResponseValue, TIMEOUT_EXCEPTION};
use crate::topology::support::BoxFuture;
use crate::topology::transport::RemoteError;
use crate::statetransfer::chunk::InternalCacheEntry;
use crate::statetransfer::commit_manager::{CommitFlag, CommitManager};
use crate::statetransfer::manager::StateTransferManager;
use crate::topology::wire::{ObjectInput, ObjectOutput};

/// `CallInterceptor.CLOCK_BUFFER` (`:132`): how much earlier than its own clock a primary accepts a peer's expiration.
const CLOCK_BUFFER: i64 = 100;

/// `ExpiryHelper.isExpiredMortal`: `lifespan > -1 && created > -1 && lifespan + created <= now`.
fn is_expired_mortal(lifespan: i64, created: i64, now: i64) -> bool {
    lifespan > -1 && created > -1 && lifespan + created <= now
}

/// `ClusteringConfiguration.remoteTimeout` default (`ClusteringConfiguration.java:21-22`).
pub const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(15);

/// Why a cache operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// `org.infinispan.util.concurrent.TimeoutException`.
    Timeout(String),
    /// `SuspectException`.
    Suspect(String),
    /// Any other failure of a remote node or of the transport.
    Remote(String),
    /// The cache has no topology yet (not started).
    NotRunning,
    /// A value or key this port cannot hash or marshal.
    Unsupported(String),
    /// The compute function failed (`UserRaisedFunctionalException`).
    Function(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Timeout(m) | CacheError::Remote(m) | CacheError::Unsupported(m) | CacheError::Function(m) => f.write_str(m),
            CacheError::Suspect(m) => write!(f, "SuspectException: {m}"),
            CacheError::NotRunning => f.write_str("the cache is not running"),
        }
    }
}

impl std::error::Error for CacheError {}

/// The slice of `Transport`/`RpcManager` the data path uses. Requests and answers are marshalled
/// bytes: `None` is the `null` answer a command without an expected return value produces
/// (`JGroupsTransport.processResponse` turns it into `SUCCESSFUL_EMPTY_RESPONSE`, `:1563-1565`).
pub type RpcResult = Result<Option<Response>, RemoteError>;

/// The slice of `Transport`/`RpcManager` a cache uses: its view and request-response calls.
pub trait CacheTransport: Send + Sync {
    /// The local node's address.
    fn address(&self) -> Address;
    /// The id of the installed view.
    fn view_id(&self) -> i32;
    /// The members of the installed view.
    fn members(&self) -> Vec<Address>;
    /// Sends `request` to `target` and waits up to `timeout` for its answer.
    fn invoke(&self, target: &Address, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, RpcResult>;
    /// To every member but the local node, collecting every answer.
    fn invoke_on_all(&self, request: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Vec<(Address, RpcResult)>>;
}

/// Where a cache reads its current topology.
pub type TopologySource = Arc<dyn Fn() -> Option<CacheTopology> + Send + Sync>;

/// A cache's configuration: the parts of `Configuration` this port reads.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// The cache's name.
    pub name: String,
    /// `ClusteringConfiguration.hash().numSegments` (default 256).
    pub num_segments: i32,
    /// `LockingConfiguration.lockAcquisitionTimeout` (default 10 s).
    pub lock_acquisition_timeout: Duration,
    /// `ClusteringConfiguration.remoteTimeout` (default 15 s).
    pub remote_timeout: Duration,
    /// `StateTransferConfiguration.timeout`
    pub state_transfer_timeout: Duration,
    /// `StateTransferConfiguration.chunkSize`
    pub state_chunk_size: usize,
    /// `StateTransferConfiguration.awaitInitialTransfer`: whether joining waits for the initial transfer.
    pub await_initial_transfer: bool,
    /// `memory()`: unbounded by default; `maxCount` + REMOVE gives a `BoundedSegmentedDataContainer`.
    pub memory: MemoryConfig,
}

impl CacheConfig {
    /// The default configuration of the cache `name`.
    pub fn new(name: &str) -> Self {
        Self { name: name.into(), num_segments: 256, lock_acquisition_timeout: DEFAULT_LOCK_ACQUISITION_TIMEOUT, remote_timeout: DEFAULT_REMOTE_TIMEOUT, state_transfer_timeout: crate::statetransfer::manager::DEFAULT_STATE_TRANSFER_TIMEOUT, state_chunk_size: crate::statetransfer::provider::DEFAULT_CHUNK_SIZE, await_initial_transfer: true, memory: MemoryConfig::UNBOUNDED }
    }

    /// `memory().maxCount(n)` (strategy REMOVE): at most `n` entries, chosen for eviction by W-TinyLFU.
    pub fn with_max_count(mut self, n: i64) -> Self {
        self.memory = MemoryConfig::max_count(n);
        self
    }
}

/// What a command returned and whether it took effect (`command.isSuccessful()`).
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// The value the command returns.
    pub value: CacheObject,
    /// Whether the command took effect.
    pub successful: bool,
}

/// Post-notification of an applied change (the local-listener semantics build on this).
#[derive(Debug, Clone, PartialEq)]
pub struct EntryEvent {
    /// What happened to the entry.
    pub kind: EntryEventKind,
    /// The entry's key.
    pub key: CacheObject,
    /// the new value (created/modified) or the removed value
    pub value: CacheObject,
    /// The value before the change.
    pub old_value: CacheObject,
    /// `ctx.isOriginLocal()`
    pub origin_local: bool,
}

/// What an [`EntryEvent`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryEventKind {
    /// The entry was created.
    Created,
    /// The entry's value changed.
    Modified,
    /// The entry was removed.
    Removed,
}

/// A listener of the changes applied on a node.
pub trait EntryListener: Send + Sync {
    /// Called after each change.
    fn on_event(&self, event: &EntryEvent);
}

/// A point a test can hold a command at: after the command was applied on this node (and, on the
/// primary, replicated), before `EntryWrappingInterceptor` decides whether the topology change in
/// between forces a retry. This is where upstream's `DelayInterceptor`s block
/// (`ReplCommandRetryTest`, `.after(EntryWrappingInterceptor.class)` + `invokeNextThenAccept`).
pub trait AfterApplyHook: Send + Sync {
    /// Awaited after `command` was applied on `local`; the command continues once it resolves.
    fn after_apply(self: Arc<Self>, local: &Address, command: &WriteCommand, origin_local: bool) -> BoxFuture<'static, ()>;
}

/// Points on the originator at which a test can hold a command (upstream's `BaseAsyncInterceptor`s placed
/// after `EntryWrappingInterceptor` / before `CallInterceptor`).
pub trait ExecuteHook: Send + Sync {
    /// A write issued on this node, before it is dispatched under the topology read for it.
    fn before_write(self: Arc<Self>, _local: &Address, _command: &WriteCommand) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// A `get` issued on this node, after its value was looked up and before it is returned.
    fn after_get_lookup(self: Arc<Self>, _local: &Address, _key: &CacheObject) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// A chunk of state about to be applied on this node (`StateTransferLatchInterceptor`).
    fn before_state_apply(self: Arc<Self>, _local: &Address, _segment: i32) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

struct Distribution {
    topology_id: i32,
    primary: Address,
    write_owners: Vec<Address>,
    read_owners: Vec<Address>,
}

struct Perform {
    value: CacheObject,
    successful: bool,
    non_existent: bool,
    mutation: Option<Mutation>,
}

/// A change staged by [`CacheEngine::perform`], applied by [`CacheEngine::commit`].
enum Mutation {
    Put { key: CacheObject, value: CacheObject, metadata: Option<Metadata>, segment: i32, existed: bool, old: CacheObject, old_metadata: Option<Metadata>, flags: i64 },
    Remove { key: CacheObject, removed: CacheObject, removed_metadata: Option<Metadata>, expired: bool, flags: i64 },
    Clear { flags: i64 },
    /// A received state transfer entry (`PutKeyValueCommand` with `STATE_TRANSFER_FLAGS`): keeps the
    /// timestamps it was sent with.
    StatePut { entry: InternalCacheEntry, segment: i32 },
}

/// A cache's data path on one node: the container, the locks and the write, read and replication logic.
pub struct CacheEngine {
    /// The configuration.
    pub cfg: CacheConfig,
    transport: Arc<dyn CacheTransport>,
    topology: TopologySource,
    /// The node's entries.
    pub container: DataContainer,
    locks: KeyLocks,
    partitioner: HashFunctionPartitioner,
    versions: NumericVersionGenerator,
    notifier: Arc<CacheNotifier>,
    hook: std::sync::Mutex<Option<Arc<dyn AfterApplyHook>>>,
    /// Orders the state transfer's writes against the cache's own.
    pub commit_manager: Arc<CommitManager>,
    execute_hook: std::sync::Mutex<Option<Arc<dyn ExecuteHook>>>,
    state_transfer: std::sync::OnceLock<Arc<StateTransferManager>>,
    /// `ExpirationManagerImpl.expiring`: keys with a `RemoveExpiredCommand` in flight
    expiring: std::sync::Mutex<std::collections::HashSet<Vec<u8>>>,
}

/// `MurmurHash3.hash(Object)` input for a key. Only keys whose `hashCode()` is determined by the value
/// itself are supported (`String`, `byte[]`, `Integer`, `Long`, `Boolean`, ...).
pub fn hash_key(key: &CacheObject) -> Result<HashKey, CacheError> {
    match key {
        CacheObject::Primitive(PrimitiveValue::Str(s)) => Ok(HashKey::string(s)),
        CacheObject::Primitive(PrimitiveValue::ByteArray(b)) => Ok(HashKey::Bytes(b.clone())),
        CacheObject::Primitive(PrimitiveValue::LongArray(l)) => Ok(HashKey::Longs(l.clone())),
        CacheObject::Primitive(PrimitiveValue::IntObj(i)) => Ok(HashKey::JavaHashCode(*i)),
        CacheObject::Primitive(PrimitiveValue::ShortObj(i)) => Ok(HashKey::JavaHashCode(*i as i32)),
        CacheObject::Primitive(PrimitiveValue::ByteObj(i)) => Ok(HashKey::JavaHashCode(*i as i32)),
        CacheObject::Primitive(PrimitiveValue::CharObj(i)) => Ok(HashKey::JavaHashCode(*i as i32)),
        CacheObject::Primitive(PrimitiveValue::LongObj(l)) => Ok(HashKey::JavaHashCode((*l ^ ((*l as u64) >> 32) as i64) as i32)),
        CacheObject::Primitive(PrimitiveValue::BooleanObj(b)) => Ok(HashKey::JavaHashCode(if *b { 1231 } else { 1237 })),
        other => Err(CacheError::Unsupported(format!("cannot compute a segment for key {other:?}"))),
    }
}

fn display_key(key: &CacheObject) -> String {
    key.as_str().map(String::from).unwrap_or_else(|| format!("{key:?}"))
}

impl CacheEngine {
    /// The engine of the cache `cfg` describes, sending over `transport` in the topology `topology` returns.
    pub fn new(cfg: CacheConfig, transport: Arc<dyn CacheTransport>, topology: TopologySource) -> Arc<Self> {
        let partitioner = HashFunctionPartitioner::new(cfg.num_segments).expect("positive segment count");
        let container = DataContainerFactory::construct(CacheMode::ReplSync, &cfg.memory).expect("a container this port can build");
        Arc::new_cyclic(|me| {
            let notifier = Arc::new(CacheNotifier::new(NotifierConfig::new(&cfg.name, CacheMode::ReplSync), Arc::new(EngineLogic(me.clone()))));
            Self {
                cfg,
                transport,
                topology,
                container,
                locks: KeyLocks::default(),
                partitioner,
                versions: NumericVersionGenerator::default(),
                notifier,
                hook: std::sync::Mutex::new(None),
                commit_manager: Arc::new(CommitManager::new()),
                execute_hook: std::sync::Mutex::new(None),
                state_transfer: std::sync::OnceLock::new(),
                expiring: std::sync::Mutex::new(std::collections::HashSet::new()),
            }
        })
    }

    /// The local node's address.
    pub fn address(&self) -> Address {
        self.transport.address()
    }

    /// This cache's `CacheNotifier` (`cache.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class)`).
    pub fn notifier(&self) -> &Arc<CacheNotifier> {
        &self.notifier
    }

    /// Register a post-notification observer of created/modified/removed entries: a plain listener
    /// (not clustered) with those three methods, delivered through the [`CacheNotifier`] like any other.
    pub fn add_listener(&self, l: Arc<dyn EntryListener>) {
        let handler = move |e: &Event| {
            let kind = match e.kind {
                EventKind::CacheEntryCreated => EntryEventKind::Created,
                EventKind::CacheEntryModified => EntryEventKind::Modified,
                EventKind::CacheEntryRemoved => EntryEventKind::Removed,
                _ => return,
            };
            // the removed value is the event's old value once the entry is gone
            let value = if kind == EntryEventKind::Removed { e.old_value.clone() } else { e.value.clone() };
            l.on_event(&EntryEvent { kind, key: e.key.clone(), value, old_value: e.old_value.clone(), origin_local: e.origin_local });
        };
        let def = ListenerDefinition::new(
            "EntryListenerAdapter",
            ListenerAttrs { observation: crate::notify::listener_registration::Observation::Post, ..ListenerAttrs::default() },
            vec![ListenerMethod::new("on_event", &[EventKind::CacheEntryCreated, EventKind::CacheEntryModified, EventKind::CacheEntryRemoved], handler)],
        );
        self.notifier.add_listener(&def).expect("the adapter is a valid listener");
    }

    /// `AdvancedCache.addListener(listener, filter, converter)`.
    pub fn add_cache_listener(&self, listener: &Arc<ListenerDefinition>, filters: ListenerFilters) -> Result<(), NotifierError> {
        self.notifier.add_listener_filtered(listener, filters)
    }

    /// Replace this node's clock for this cache (`TestingUtil.replaceComponent(manager, TimeService.class, ts, true)`).
    pub fn set_time_service(&self, ts: Arc<dyn super::time_service::TimeService>) {
        self.container.set_time_service(ts);
    }

    /// `LockManager.isLocked(key)` on this node.
    pub fn is_locked(&self, key: &CacheObject) -> bool {
        self.locks.is_locked(&key_bytes(key))
    }

    /// `AdvancedCache.lock(key)` for a non-transactional caller: take this node's lock on the key and
    /// hold it until the guard is dropped (only the primary's lock is ever contended).
    pub async fn lock_key(&self, key: &CacheObject, timeout: Duration) -> Result<super::locking::KeyLockGuard, CacheError> {
        self.locks.lock(&key_bytes(key), &display_key(key), "test", timeout).await.map_err(|e| CacheError::Timeout(e.message))
    }

    /// Sets the hook commands pass on the originator, or removes it with `None`.
    pub fn set_execute_hook(&self, hook: Option<Arc<dyn ExecuteHook>>) {
        *self.execute_hook.lock().unwrap() = hook;
    }

    fn execute_hook(&self) -> Option<Arc<dyn ExecuteHook>> {
        self.execute_hook.lock().unwrap().clone()
    }

    /// Sets the hook commands pass after being applied, or removes it with `None`.
    pub fn set_after_apply_hook(&self, hook: Option<Arc<dyn AfterApplyHook>>) {
        *self.hook.lock().unwrap() = hook;
    }

    /// The id of the topology this engine currently runs commands under.
    pub fn topology_id(&self) -> Option<i32> {
        (self.topology)().map(|t| t.topology_id)
    }

    /// The segment `key` maps to.
    pub fn segment_of(&self, key: &CacheObject) -> Result<i32, CacheError> {
        Ok(self.partitioner.get_segment(&hash_key(key)?))
    }

    fn current_topology(&self) -> Result<CacheTopology, CacheError> {
        (self.topology)().ok_or(CacheError::NotRunning)
    }

    fn distribution(&self, topology: &CacheTopology, segment: i32) -> Result<Distribution, CacheError> {
        let write = topology.write_consistent_hash().ok_or(CacheError::NotRunning)?.as_replicated();
        let read = topology.read_consistent_hash().ok_or(CacheError::NotRunning)?.as_replicated();
        let seg = segment as usize;
        Ok(Distribution {
            topology_id: topology.topology_id,
            primary: write.locate_primary_owner_for_segment(seg).clone(),
            write_owners: write.locate_owners_for_segment(seg),
            read_owners: read.locate_owners_for_segment(seg),
        })
    }

    /// `(primary owner, write owners)` of the key's segment in the current topology.
    pub fn owners_of(&self, key: &CacheObject) -> Result<(Address, Vec<Address>), CacheError> {
        let topology = self.current_topology()?;
        let dist = self.distribution(&topology, self.segment_of(key)?)?;
        Ok((dist.primary, dist.write_owners))
    }

    // ------------------------------------------------------------------ originator

    /// Run a write command issued on this node and return its outcome.
    pub async fn execute(self: &Arc<Self>, mut cmd: WriteCommand) -> Result<Outcome, CacheError> {
        self.stamp_version(&mut cmd);
        let deadline = tokio::time::Instant::now() + self.cfg.remote_timeout;
        loop {
            let topology = self.current_topology()?;
            cmd.set_topology_id(topology.topology_id);
            if let Some(h) = self.execute_hook() {
                h.before_write(&self.address(), &cmd).await;
            }
            let attempt = if cmd.has_any_flag(flags::CACHE_MODE_LOCAL) {
                // handleNonTxWriteCommand: a local-mode command is applied here and goes nowhere
                self.perform(&cmd, true).map_err(Attempt::Fail).map(|p| {
                    self.commit(p.mutation, true);
                    Outcome { value: p.value, successful: p.successful }
                })
            } else if matches!(cmd, WriteCommand::Clear(_)) {
                self.clear_on_originator(&cmd).await
            } else {
                let segment = cmd.segment().expect("data write has a segment");
                let dist = self.distribution(&topology, segment)?;
                if dist.primary == self.address() {
                    self.apply_on_primary(cmd.clone(), true, &dist).await
                } else {
                    self.forward_to_primary(&cmd, &dist.primary).await
                }
            };
            match attempt {
                Err(Attempt::Retry) => {
                    // StateTransferInterceptor.handleExceptionOnNonTxWriteReturn: retry with the
                    // next topology, the COMMAND_RETRY flag and the retry matcher (`:323-336`)
                    cmd.set_value_matcher(cmd.value_matcher().for_retry());
                    cmd.set_flags(cmd.flags() | flags::COMMAND_RETRY);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    if !self.wait_for_topology_after(topology.topology_id, deadline).await {
                        return Err(CacheError::Timeout(format!("ISPN000476: Timed out waiting for topology {}", topology.topology_id + 1)));
                    }
                }
                Err(Attempt::Fail(e)) => return Err(e),
                Ok(o) => return Ok(o),
            }
        }
    }

    async fn wait_for_topology_after(&self, id: i32, deadline: tokio::time::Instant) -> bool {
        loop {
            if (self.topology)().is_some_and(|t| t.topology_id > id) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// `VersionInterceptor.visitReplaceCommand` (`interceptors/impl/VersionInterceptor.java:25-37`).
    fn stamp_version(&self, cmd: &mut WriteCommand) {
        if let WriteCommand::Replace(r) = cmd {
            let m = r.metadata.get_or_insert(Metadata::DEFAULT);
            if m.version.is_none() {
                let members = self.transport.members();
                let rank = members.iter().position(|a| *a == self.address()).map(|i| i as i32 + 1).unwrap_or(0);
                m.version = Some(self.versions.generate_new(self.transport.view_id(), rank));
            }
        }
    }

    async fn forward_to_primary(&self, cmd: &WriteCommand, primary: &Address) -> Result<Outcome, Attempt> {
        let request = CacheRpcCommand::single(&self.cfg.name, cmd.clone()).to_bytes();
        let answer = self.transport.invoke(primary, request, self.cfg.remote_timeout).await;
        match answer {
            Err(RemoteError::Suspect(_)) => Err(Attempt::Retry),
            Err(RemoteError::Timeout) => Err(Attempt::Fail(CacheError::Timeout(format!(
                "ISPN000476: Timed out waiting for responses for request from {}",
                self.address_string()
            )))),
            Err(RemoteError::Other(m)) => Err(Attempt::Fail(CacheError::Remote(m))),
            Ok(None) => Ok(Outcome { value: CacheObject::Null, successful: true }),
            Ok(Some(r)) => response_to_outcome(r),
        }
    }

    fn address_string(&self) -> String {
        format!("{:?}", self.address())
    }

    /// `visitClearCommand` on the originator: remote first, then locally.
    async fn clear_on_originator(&self, cmd: &WriteCommand) -> Result<Outcome, Attempt> {
        let request = CacheRpcCommand::single(&self.cfg.name, cmd.clone()).to_bytes();
        for (_, r) in self.transport.invoke_on_all(request, self.cfg.remote_timeout).await {
            match r {
                Ok(Some(Response::Exception { kind, message })) => return Err(Attempt::Fail(exception_error(kind, message))),
                Ok(_) | Err(RemoteError::Suspect(_)) => {}
                Err(RemoteError::Timeout) => return Err(Attempt::Fail(CacheError::Timeout("ISPN000476: Timed out waiting for responses".into()))),
                Err(RemoteError::Other(m)) => return Err(Attempt::Fail(CacheError::Remote(m))),
            }
        }
        self.clear_local(true, cmd.flags());
        Ok(Outcome { value: CacheObject::Null, successful: true })
    }

    /// `CallInterceptor.visitClearCommand` and `commitClearCommand`: a pre-notification for every live entry
    /// before the clear (`CallInterceptor.java:481-491`), then the entries are removed one by one, expired
    /// ones included, each followed by a post `CacheEntryRemoved` (`ClusteringDependentLogic.java:244-259`).
    /// There is no "cleared" event: a listener sees N per-key removals on every node.
    fn clear_local(&self, origin_local: bool, flags: i64) {
        let ctx = Self::ctx(origin_local);
        let info = CommandInfo { flags, segment: None };
        if self.notifier.has_listener(EventKind::CacheEntryRemoved) {
            for e in self.container.entries() {
                Self::report(self.notifier.notify_cache_entry_removed(&e.key, &e.value, e.metadata, true, &ctx, Some(&info)));
            }
        }
        // EntryWrappingInterceptor commits the ClearCacheEntry: the container is emptied entry by entry
        for e in self.container.clear() {
            Self::report(self.notifier.notify_cache_entry_removed(&e.key, &e.value, e.metadata, false, &ctx, Some(&info)));
        }
    }

    fn ctx(origin_local: bool) -> InvocationContext {
        InvocationContext { origin_local, in_tx_scope: false }
    }

    /// A synchronous listener that threw: upstream propagates the `CacheListenerException` to the writer;
    /// here the write has already been applied and replicated, so it is logged.
    fn report(r: Result<(), NotifierError>) {
        if let Err(e) = r {
            log::warn!("{e}");
        }
    }

    // ------------------------------------------------------------------ primary

    /// `CacheTopologyUtil.checkTopology` (`util/CacheTopologyUtil.java:27-39`): a command stamped with
    /// another topology id than this node's current one is refused with `OutdatedTopologyException`.
    fn check_topology_at_start(&self, cmd: &WriteCommand, current_id: i32) -> Result<(), Attempt> {
        if cmd.has_any_flag(flags::SKIP_OWNERSHIP_CHECK | flags::CACHE_MODE_LOCAL) {
            return Ok(());
        }
        if cmd.topology_id() >= 0 && cmd.topology_id() != current_id { Err(Attempt::Retry) } else { Ok(()) }
    }

    /// `EntryWrappingInterceptor.checkTopology` (`:624-669`), run once the command was applied: when the
    /// topology moved on during the execution and the key gained an owner since this node started
    /// executing it, the command must be retried so the new owner sees it.
    fn check_topology_after_apply(&self, cmd: &WriteCommand, start_owners: &[Address]) -> Result<(), Attempt> {
        if cmd.has_any_flag(flags::CACHE_MODE_LOCAL) {
            return Ok(());
        }
        let Ok(now) = self.current_topology() else { return Ok(()) };
        if cmd.topology_id() == -1 || now.topology_id == cmd.topology_id() {
            return Ok(());
        }
        let Some(segment) = cmd.segment() else { return Ok(()) };
        let owners_added = match self.distribution(&now, segment) {
            Ok(d) => !d.write_owners.iter().all(|o| start_owners.contains(o)),
            Err(_) => false,
        };
        if owners_added { Err(Attempt::Retry) } else { Ok(()) }
    }

    async fn run_hook(&self, cmd: &WriteCommand, origin_local: bool) {
        let hook = self.hook.lock().unwrap().clone();
        if let Some(h) = hook {
            h.after_apply(&self.address(), cmd, origin_local).await;
        }
    }

    async fn apply_on_primary(self: &Arc<Self>, mut cmd: WriteCommand, origin_local: bool, dist: &Distribution) -> Result<Outcome, Attempt> {
        self.check_topology_at_start(&cmd, dist.topology_id)?;
        let key = cmd.key().expect("data write has a key").clone();
        let _guard = if cmd.has_any_flag(flags::SKIP_LOCKING) {
            None
        } else {
            let timeout = if cmd.has_any_flag(flags::ZERO_LOCK_ACQUISITION_TIMEOUT) { Duration::ZERO } else { self.cfg.lock_acquisition_timeout };
            let requestor = cmd.invocation_id().map(|i| format!("CommandInvocation:{}:{}", i.address.as_ref().map(|a| format!("{a:?}")).unwrap_or_else(|| "local".into()), i.id)).unwrap_or_default();
            Some(
                self.locks
                    .lock(&key_bytes(&key), &display_key(&key), &requestor, timeout)
                    .await
                    .map_err(|e| Attempt::Fail(CacheError::Timeout(e.message)))?,
            )
        };
        let result = self.perform(&cmd, origin_local).map_err(Attempt::Fail)?;
        // RemoveCommand.shouldReplicate: a remove of a missing key is not replicated
        let replicate = result.successful && !(result.non_existent && matches!(cmd, WriteCommand::Remove(_)));
        if replicate && dist.write_owners.len() > 1 {
            self.broadcast_backup(&mut cmd).await?;
        }
        self.run_hook(&cmd, origin_local).await;
        self.check_topology_after_apply(&cmd, &dist.write_owners)?;
        self.commit(result.mutation, origin_local);
        Ok(Outcome { value: result.value, successful: result.successful })
    }

    /// `primaryReturnHandler`'s synchronous branch: `invokeCommandOnAll` with the matcher reset to
    /// `MATCH_ALWAYS` and `IGNORE_RETURN_VALUES | BACKUP_WRITE`.
    async fn broadcast_backup(&self, cmd: &mut WriteCommand) -> Result<(), Attempt> {
        let mut backup = cmd.clone();
        backup.set_value_matcher(ValueMatcher::MatchAlways);
        backup.set_flags(backup.flags() | flags::IGNORE_RETURN_VALUES | flags::BACKUP_WRITE);
        if let Ok(t) = self.current_topology() {
            backup.set_topology_id(t.topology_id);
        }
        let request = CacheRpcCommand::single(&self.cfg.name, backup).to_bytes();
        for (_, r) in self.transport.invoke_on_all(request, self.cfg.remote_timeout).await {
            match r {
                // ignoreLeavers: a member that left or does not run the cache is not an error
                Ok(Some(Response::CacheNotFound)) | Err(RemoteError::Suspect(_)) => {}
                Ok(Some(Response::Exception { kind: ExceptionKind::OutdatedTopology { .. }, .. })) => return Err(Attempt::Retry),
                Ok(Some(Response::Exception { kind, message })) => return Err(Attempt::Fail(exception_error(kind, message))),
                Ok(_) => {}
                Err(RemoteError::Timeout) => {
                    return Err(Attempt::Fail(CacheError::Timeout(format!(
                        "ISPN000476: Timed out waiting for responses for request from {}",
                        self.address_string()
                    ))));
                }
                Err(RemoteError::Other(m)) => return Err(Attempt::Fail(CacheError::Remote(m))),
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ CallInterceptor

    fn eq(a: &CacheObject, b: &CacheObject) -> bool {
        java_equals(a, b)
    }

    /// Run `cmd` against the entries of this node: `CallInterceptor.visit*Command`. Nothing is written
    /// to the container yet: like upstream's invocation context, the change is staged in the returned
    /// [`Perform`] and only [`CacheEngine::commit`] makes it visible, after the primary replicated it and
    /// `EntryWrappingInterceptor.checkTopology` accepted the topology (so a retried command starts from
    /// the unchanged container).
    fn perform(&self, cmd: &WriteCommand, origin_local: bool) -> Result<Perform, CacheError> {
        let p = self.perform_command(cmd)?;
        if let Some(m) = &p.mutation {
            self.notify_pre(m, cmd, origin_local);
        }
        Ok(p)
    }

    /// The pre-notifications `CallInterceptor` raises while it applies a command to the context entry
    /// (`performPut` `:211-213`, `performRemove` `:296`, compute `:411-424`, clear `:481-491`).
    fn notify_pre(&self, m: &Mutation, cmd: &WriteCommand, origin_local: bool) {
        let ctx = Self::ctx(origin_local);
        let info = CommandInfo { flags: cmd.flags(), segment: cmd.segment() };
        match m {
            Mutation::Put { key, value, metadata, existed, old, old_metadata, flags: f, .. } => {
                if f & flags::PUT_FOR_STATE_TRANSFER != 0 {
                    return;
                }
                let r = if *existed {
                    self.notifier.notify_cache_entry_modified(key, value, *metadata, old, *old_metadata, true, &ctx, Some(&info))
                } else {
                    self.notifier.notify_cache_entry_created(key, value, *metadata, true, &ctx, Some(&info))
                };
                Self::report(r);
            }
            // performRemove(.., notifyRemove = false) for an expiration: no pre event
            Mutation::Remove { expired: true, .. } => {}
            Mutation::Remove { key, removed, removed_metadata, .. } => {
                Self::report(self.notifier.notify_cache_entry_removed(key, removed, *removed_metadata, true, &ctx, Some(&info)));
            }
            Mutation::Clear { .. } => {
                if self.notifier.has_listener(EventKind::CacheEntryRemoved) {
                    for e in self.container.entries() {
                        Self::report(self.notifier.notify_cache_entry_removed(&e.key, &e.value, e.metadata, true, &ctx, Some(&info)));
                    }
                }
            }
            Mutation::StatePut { .. } => {}
        }
    }

    fn perform_command(&self, cmd: &WriteCommand) -> Result<Perform, CacheError> {
        let ok = |value: CacheObject, mutation: Option<Mutation>| Perform { value, successful: true, non_existent: false, mutation };
        let fail = |value: CacheObject| Perform { value, successful: false, non_existent: false, mutation: None };
        let opt = |o: &Option<CacheObject>| o.clone().unwrap_or(CacheObject::Null);
        match cmd {
            WriteCommand::Put(c) => {
                if c.value_matcher == ValueMatcher::MatchNever {
                    return Ok(fail(CacheObject::Null));
                }
                let prev_entry = self.container.peek(&c.key);
                let prev_meta = prev_entry.as_ref().and_then(|e| e.metadata);
                let prev = prev_entry.map(|e| e.value);
                if !c.value_matcher.matches(prev.as_ref(), &CacheObject::Null, &c.value, &Self::eq) {
                    return Ok(fail(opt(&prev)));
                }
                // performPut: the previous value, except when retrying a putIfAbsent (expected: null)
                let response = if c.value_matcher != ValueMatcher::MatchExpectedOrNew { opt(&prev) } else { CacheObject::Null };
                let mutation = Mutation::Put {
                    key: c.key.clone(),
                    value: c.value.clone(),
                    metadata: c.metadata,
                    segment: c.segment,
                    existed: prev.is_some(),
                    old: opt(&prev),
                    old_metadata: prev_meta,
                    flags: c.flags,
                };
                Ok(ok(response, Some(mutation)))
            }
            WriteCommand::Replace(c) => {
                if c.value_matcher == ValueMatcher::MatchNever {
                    return Ok(fail(CacheObject::Null));
                }
                let prev_entry = self.container.peek(&c.key);
                let prev_meta = prev_entry.as_ref().and_then(|e| e.metadata);
                let prev = prev_entry.map(|e| e.value);
                if c.value_matcher.matches(prev.as_ref(), &c.old_value, &c.new_value, &Self::eq) {
                    // notifyCacheEntryModified(..., expectedValue == null ? prevValue : expectedValue, ...)
                    let old = if c.old_value.is_null() { opt(&prev) } else { c.old_value.clone() };
                    let mutation = Mutation::Put {
                        key: c.key.clone(),
                        value: c.new_value.clone(),
                        metadata: c.metadata,
                        segment: c.segment,
                        existed: true,
                        old,
                        old_metadata: prev_meta,
                        flags: c.flags,
                    };
                    let response = if c.old_value.is_null() { opt(&prev) } else { CacheObject::Primitive(PrimitiveValue::BooleanObj(true)) };
                    return Ok(ok(response, Some(mutation)));
                }
                Ok(fail(if c.old_value.is_null() { opt(&prev) } else { CacheObject::Primitive(PrimitiveValue::BooleanObj(false)) }))
            }
            WriteCommand::Remove(c) => {
                if c.value_matcher == ValueMatcher::MatchNever {
                    return Ok(fail(CacheObject::Null));
                }
                let conditional = !c.value.is_null();
                let boolean = |b: bool| CacheObject::Primitive(PrimitiveValue::BooleanObj(b));
                let prev_entry = self.container.peek(&c.key);
                let prev_meta = prev_entry.as_ref().and_then(|e| e.metadata);
                let prev = prev_entry.map(|e| e.value);
                let Some(prev) = prev else {
                    // nothing to remove
                    return Ok(if c.value_matcher.matches(None, &c.value, &CacheObject::Null, &Self::eq) {
                        Perform { value: if conditional { boolean(true) } else { CacheObject::Null }, successful: true, non_existent: true, mutation: None }
                    } else {
                        Perform { value: boolean(false), successful: false, non_existent: true, mutation: None }
                    });
                };
                if !c.value_matcher.matches(Some(&prev), &c.value, &CacheObject::Null, &Self::eq) {
                    return Ok(fail(boolean(false)));
                }
                let response = if conditional {
                    boolean(true)
                } else if c.value_matcher != ValueMatcher::MatchExpectedOrNew {
                    prev.clone()
                } else {
                    c.value.clone()
                };
                Ok(ok(response, Some(Mutation::Remove { key: c.key.clone(), removed: prev, removed_metadata: prev_meta, expired: false, flags: c.flags })))
            }
            WriteCommand::Compute(c) => {
                let prev_entry = self.container.peek(&c.key);
                let prev_meta = prev_entry.as_ref().and_then(|e| e.metadata);
                let prev = prev_entry.map(|e| e.value);
                if c.compute_if_present && prev.is_none() {
                    return Ok(fail(CacheObject::Null));
                }
                let new = c.function.function.apply(prev.as_ref()).map_err(|e| CacheError::Function(e.to_string()))?;
                match (prev, new) {
                    (None, None) => Ok(ok(CacheObject::Null, None)),
                    (Some(old), Some(new)) => Ok(ok(
                        new.clone(),
                        Some(Mutation::Put { key: c.key.clone(), value: new, metadata: c.metadata, segment: c.segment, existed: true, old, old_metadata: prev_meta, flags: c.flags }),
                    )),
                    (Some(old), None) => Ok(ok(CacheObject::Null, Some(Mutation::Remove { key: c.key.clone(), removed: old, removed_metadata: prev_meta, expired: false, flags: c.flags }))),
                    (None, Some(new)) => Ok(ok(
                        new.clone(),
                        Some(Mutation::Put { key: c.key.clone(), value: new, metadata: c.metadata, segment: c.segment, existed: false, old: CacheObject::Null, old_metadata: None, flags: c.flags }),
                    )),
                }
            }
            WriteCommand::RemoveExpired(c) => {
                let boolean = |b: bool| CacheObject::Primitive(PrimitiveValue::BooleanObj(b));
                // the raw entry: the write path sees an expired entry too
                let raw = self.container.peek_including_expired(&c.key);
                let removal = |e: &StoredEntry| Mutation::Remove { key: c.key.clone(), removed: e.value.clone(), removed_metadata: e.metadata, expired: true, flags: c.flags };
                // When it isn't the primary owner, just accept the removal as is, trusting the primary did the appropriate checks
                if c.flags & flags::BACKUP_WRITE != 0 {
                    return Ok(ok(boolean(true), raw.as_ref().map(removal)));
                }
                let Some(e) = raw else {
                    // Nothing to remove since the entry doesn't exist - assume command was successful
                    return Ok(ok(boolean(true), None));
                };
                let matcher = cmd.value_matcher();
                let value_matches = matcher.matches(Some(&e.value), &c.value, &CacheObject::Null, &Self::eq);
                let lifespan = e.metadata.map(|m| m.lifespan_ms()).unwrap_or(-1);
                let remove = match c.lifespan {
                    // a max idle expiration (lifespan null) skips the lifespan checks
                    None => value_matches,
                    // the entry must have a positive lifespan, the one the expiring node saw, the value must still be
                    // the same, and it must still be expired within CLOCK_BUFFER of this node's clock
                    Some(l) => lifespan > 0 && lifespan == l && value_matches && is_expired_mortal(l, e.created, self.container.now() + CLOCK_BUFFER),
                };
                if remove { Ok(ok(boolean(true), Some(removal(&e)))) } else { Ok(fail(boolean(false))) }
            }
            WriteCommand::Clear(_) => Ok(ok(CacheObject::Null, Some(Mutation::Clear { flags: cmd.flags() }))),
        }
    }

    /// `EntryWrappingInterceptor`'s commit of the context entries (`ClusteringDependentLogic.commitEntry`),
    /// followed by the post notifications (`NotifyHelper.entryCommitted`: none for state transfer).
    fn commit(&self, mutation: Option<Mutation>, origin_local: bool) {
        let Some(m) = mutation else { return };
        let fire = |f: i64| f & (flags::SKIP_LISTENER_NOTIFICATION | flags::PUT_FOR_STATE_TRANSFER) == 0;
        let op = |f: i64| if f & flags::PUT_FOR_STATE_TRANSFER != 0 { CommitFlag::PutForStateTransfer } else { CommitFlag::Normal };
        let ctx = Self::ctx(origin_local);
        match m {
            Mutation::Put { key, value, metadata, segment, existed, old, old_metadata, flags } => {
                // CommitManager.commit: a normal commit during state transfer is remembered so a later
                // state transfer entry for the key is dropped
                let applied = self.commit_manager.commit(&key_bytes(&key), op(flags), segment, || self.container.put(key.clone(), value.clone(), metadata, segment));
                if applied && fire(flags) {
                    let info = CommandInfo { flags, segment: Some(segment) };
                    let r = if existed {
                        self.notifier.notify_cache_entry_modified(&key, &value, metadata, &old, old_metadata, false, &ctx, Some(&info))
                    } else {
                        self.notifier.notify_cache_entry_created(&key, &value, metadata, false, &ctx, Some(&info))
                    };
                    Self::report(r);
                }
            }
            Mutation::Remove { key, removed, removed_metadata, expired, flags } => {
                let segment = self.segment_of(&key).unwrap_or(-1);
                let applied = self.commit_manager.commit(&key_bytes(&key), op(flags), segment, || {
                    self.container.remove(&key);
                });
                if applied && fire(flags) {
                    if expired {
                        // NotifyHelper.entryCommitted: a RemoveExpiredCommand raises `expired`, not `removed`
                        Self::report(self.notifier.notify_cache_entry_expired(&key, &removed, removed_metadata, Some(&ctx)));
                    } else {
                        let info = CommandInfo { flags, segment: Some(segment) };
                        Self::report(self.notifier.notify_cache_entry_removed(&key, &removed, removed_metadata, false, &ctx, Some(&info)));
                    }
                }
            }
            Mutation::Clear { flags } => {
                // EntryWrappingInterceptor.visitClearCommand: no key may be written by state transfer from now on
                // until the current rebalance ends
                self.commit_manager.stop_track(CommitFlag::PutForStateTransfer);
                self.clear_local(origin_local, flags);
            }
            Mutation::StatePut { entry, segment } => {
                let metadata = crate::cache::container::metadata_of(&entry);
                self.commit_manager.commit(&key_bytes(&entry.key), CommitFlag::PutForStateTransfer, segment, || {
                    self.container.put_with_times(entry.key.clone(), entry.value.clone(), Some(metadata), segment, entry.created, entry.last_used)
                });
                // NotifyHelper.java:27-29, CacheNotifierImpl.java:417-418: PUT_FOR_STATE_TRANSFER fires no notification
            }
        }
        self.flush_evictions();
    }

    /// `EvictionManagerImpl.onEntryEviction` (`eviction/impl/EvictionManagerImpl.java:40`): the entries the
    /// bounded container evicted are announced on this node's notifier, as one `@CacheEntriesEvicted` event.
    /// A cluster listener never registered an evicted handler, so only plain listeners see it.
    fn flush_evictions(&self) {
        let evicted = self.container.drain_evicted();
        if evicted.is_empty() {
            return;
        }
        let entries: Vec<(CacheObject, CacheObject)> = evicted.into_iter().map(|e| (e.key, e.value)).collect();
        Self::report(self.notifier.notify_cache_entries_evicted(&entries, None));
    }

    /// `StateConsumerImpl.doApplyState` for one entry: a put with `STATE_TRANSFER_FLAGS`, which skips locking,
    /// ownership checks and replication and goes straight to the commit.
    pub(crate) fn apply_state_entry(&self, entry: InternalCacheEntry, segment: i32) {
        self.commit(Some(Mutation::StatePut { entry, segment }), false);
    }

    /// Attach the state transfer manager (before the cache joins).
    pub fn set_state_transfer(&self, m: Arc<StateTransferManager>) {
        let _ = self.state_transfer.set(m);
    }

    /// The cache's state transfer, once started.
    pub fn state_transfer(&self) -> Option<Arc<StateTransferManager>> {
        self.state_transfer.get().cloned()
    }

    // ------------------------------------------------------------------ remote side

    /// The answer a node sends for a cache RPC (`PerCacheInboundInvocationHandler` +
    /// `DefaultResponseGenerator`). `None` is the `null` answer.
    pub async fn handle_rpc(self: &Arc<Self>, request: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
        self.handle_rpc_from(None, request).await
    }

    /// As [`CacheEngine::handle_rpc`], knowing the node the request came from (state transfer needs it).
    pub async fn handle_rpc_from(self: &Arc<Self>, origin: Option<&Address>, request: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
        let command = CacheRpcCommand::from_bytes(request).map_err(|e| CacheError::Remote(e.to_string()))?;
        // PerCacheInboundInvocationHandler / DefaultTopologyRunnable.beforeInvoke: a command sent under a topology this
        // node has not installed yet waits for it, and one sent before this node was a member is ignored
        let topology_id = match &command.payload {
            CacheRpcPayload::Single(c) => Some(c.topology_id()),
            CacheRpcPayload::ClusteredGet(g) => Some(g.topology_id),
            _ => None,
        };
        if let (Some(id), Some(m)) = (topology_id, self.state_transfer()) {
            if m.lock.topology_future(id.max(0), self.cfg.remote_timeout).await.is_err() {
                return Err(CacheError::Timeout(format!("ISPN000: Timed out waiting for topology {id}")));
            }
            if m.is_command_sent_before_first_topology(id) {
                return Ok(Some(Self::response_bytes(&Response::CacheNotFound)));
            }
        }
        let response = match command.payload {
            CacheRpcPayload::Single(cmd) => self.handle_remote_write(cmd).await,
            CacheRpcPayload::ClusteredGet(g) => self.handle_clustered_get(g),
            payload @ (CacheRpcPayload::StateStart(_) | CacheRpcPayload::StateCancel(_) | CacheRpcPayload::StateResponse(_)) => {
                match (self.state_transfer(), origin) {
                    (Some(m), Some(origin)) => m.handle(origin, payload).await,
                    // the cache is not (yet) running: PerCacheInboundInvocationHandler answers CacheNotFoundResponse
                    _ => Some(Response::CacheNotFound),
                }
            }
        };
        Ok(response.map(|r| Self::response_bytes(&r)))
    }

    fn response_bytes(r: &Response) -> Vec<u8> {
        let mut o = ObjectOutput::new();
        o.write_response(r);
        o.into_bytes()
    }

    async fn handle_remote_write(self: &Arc<Self>, cmd: WriteCommand) -> Option<Response> {
        let Ok(topology) = self.current_topology() else { return Some(Response::CacheNotFound) };
        if matches!(cmd, WriteCommand::Clear(_)) {
            self.clear_local(false, cmd.flags());
            return None;
        }
        let dist = match self.distribution(&topology, cmd.segment().expect("segment")) {
            Ok(d) => d,
            Err(e) => return Some(exception_response(&e)),
        };
        let me = self.address();
        let backup = cmd.has_any_flag(flags::BACKUP_WRITE);
        if backup && !dist.write_owners.contains(&me) {
            // BaseDistributionInterceptor.handleNonTxWriteCommand: not an owner (zero capacity node)
            return None;
        }
        let outcome = if dist.primary == me && !backup {
            self.apply_on_primary(cmd.clone(), false, &dist).await
        } else if let Err(e) = self.check_topology_at_start(&cmd, dist.topology_id) {
            Err(e)
        } else {
            // a backup applying the primary's command (or a node that was sent one it does not own)
            match self.perform(&cmd, false) {
                Err(e) => Err(Attempt::Fail(e)),
                Ok(p) => {
                    self.run_hook(&cmd, false).await;
                    self.check_topology_after_apply(&cmd, &dist.write_owners).map(|_| {
                        self.commit(p.mutation, false);
                        Outcome { value: p.value, successful: p.successful }
                    })
                }
            }
        };
        match outcome {
            Err(Attempt::Retry) => Some(Response::Exception { kind: ExceptionKind::OutdatedTopology { retry_same_topology: false }, message: None }),
            Err(Attempt::Fail(e)) => Some(exception_response(&e)),
            Ok(o) => {
                // DefaultResponseGenerator.getResponse
                if !cmd.is_return_value_expected() {
                    return None;
                }
                let value = match o.value {
                    CacheObject::Null => None,
                    CacheObject::Primitive(PrimitiveValue::BooleanObj(b)) => Some(Box::new(ResponseValue::Boolean(b))),
                    other => Some(Box::new(ResponseValue::Object(other))),
                };
                Some(if o.successful { Response::Successful(value) } else { Response::Unsuccessful(value) })
            }
        }
    }

    /// `ClusteredGetCommand.invokeAsync`: a local read of the entry.
    fn handle_clustered_get(&self, g: ClusteredGet) -> Option<Response> {
        if self.current_topology().is_err() {
            return Some(Response::CacheNotFound);
        }
        // read through the interceptor chain: `EntryFactoryImpl.getFromContainer` peeks (no eviction access)
        let value = self.container.peek(&g.key).map(|e| {
            let mortal = e.metadata.map(|m| m.lifespan_ms()).filter(|l| *l >= 0).map(|l| (0, l));
            Box::new(ResponseValue::InternalCacheValue(InternalCacheValue { value: e.value, mortal }))
        });
        Some(Response::Successful(value))
    }

    // ------------------------------------------------------------------ reads

    /// `get`: a local read on a read owner, otherwise a `ClusteredGetCommand` to the owners.
    pub async fn get(self: &Arc<Self>, key: &CacheObject) -> Result<Option<CacheObject>, CacheError> {
        self.get_with_flags(key, 0).await
    }

    /// `get` with flags: `CACHE_MODE_LOCAL`/`SKIP_REMOTE_LOOKUP` never leave the node.
    pub async fn get_with_flags(self: &Arc<Self>, key: &CacheObject, f: i64) -> Result<Option<CacheObject>, CacheError> {
        let value = self.get_lookup(key, f).await?;
        if let Some(h) = self.execute_hook() {
            h.after_get_lookup(&self.address(), key).await;
        }
        Ok(value)
    }

    async fn get_lookup(self: &Arc<Self>, key: &CacheObject, f: i64) -> Result<Option<CacheObject>, CacheError> {
        if f & (flags::CACHE_MODE_LOCAL | (1 << 9)) != 0 {
            return Ok(self.read_local(key));
        }
        let topology = self.current_topology()?;
        let segment = self.segment_of(key)?;
        let dist = self.distribution(&topology, segment)?;
        let me = self.address();
        if dist.read_owners.contains(&me) {
            return Ok(self.read_local(key));
        }
        for owner in dist.read_owners.iter().filter(|o| **o != me) {
            let request = CacheRpcCommand {
                cache_name: self.cfg.name.clone(),
                payload: CacheRpcPayload::ClusteredGet(ClusteredGet { key: key.clone(), segment: Some(segment), flags: 0, is_write: false, topology_id: topology.topology_id }),
            }
            .to_bytes();
            match self.transport.invoke(owner, request, self.cfg.remote_timeout).await {
                Ok(Some(Response::Successful(Some(v)))) => {
                    if let ResponseValue::InternalCacheValue(icv) = *v {
                        return Ok(Some(icv.value));
                    }
                }
                Ok(Some(Response::Successful(None))) | Ok(None) => return Ok(None),
                Ok(_) | Err(_) => continue,
            }
        }
        Ok(None)
    }

    /// A read of this node's data container. An entry that is expired by this node's clock reads as absent and
    /// starts a `RemoveExpiredCommand` without making the reader wait: `handlePossibleExpiration` ->
    /// `ClusterExpirationManager.entryExpiredInMemory`, where `waitOnLifespanExpiration(isWrite = false)` is false
    /// (`expiration/impl/ClusterExpirationManager.java:355-380`, `ExpirationManagerImpl.java:240-260`).
    fn read_local(self: &Arc<Self>, key: &CacheObject) -> Option<CacheObject> {
        if let Some(e) = self.container.peek_including_expired(key).filter(|e| e.expired(self.container.now())) {
            self.expire_in_memory(e);
            return None;
        }
        // `EntryFactoryImpl.getFromContainer` reads with `container.peek` (`container/impl/EntryFactoryImpl.java:285-286`):
        // a read through the cache API is not an access for the Caffeine eviction policy
        self.container.peek(key).map(|e| e.value)
    }

    /// `ClusterExpirationManager.handleLifespanExpireEntry`: one expiration per key at a time, issued with
    /// `ZERO_LOCK_ACQUISITION_TIMEOUT` (`cacheToUse(false)`) and, as `CacheImpl.removeLifespanExpired` adds,
    /// `SKIP_CACHE_LOAD | SKIP_XSITE_BACKUP`. Max idle expiration is not modelled.
    fn expire_in_memory(self: &Arc<Self>, e: StoredEntry) {
        let Some(lifespan) = e.metadata.map(|m| m.lifespan_ms()).filter(|l| *l >= 0) else { return };
        let kb = key_bytes(&e.key);
        if !self.expiring.lock().unwrap().insert(kb.clone()) {
            return; // There is a pending expiration removal for this key
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.expiring.lock().unwrap().remove(&kb);
            return;
        };
        let cmd = WriteCommand::RemoveExpired(RemoveExpired {
            key: e.key.clone(),
            value: e.value.clone(),
            lifespan: Some(lifespan),
            max_idle: false,
            segment: e.segment,
            flags: flags::ZERO_LOCK_ACQUISITION_TIMEOUT | flags::SKIP_CACHE_LOAD | flags::SKIP_XSITE_BACKUP,
            invocation_id: CommandInvocationId::generate(self.address()),
            topology_id: -1,
        });
        let me = self.clone();
        rt.spawn(async move {
            // a timeout is taken as a concurrent write; the entry is tried again on the next read
            let _ = me.execute(cmd).await;
            me.expiring.lock().unwrap().remove(&kb);
        });
    }

    /// `Cache.evict(key)` (`EvictCommand`): drop the entry from this node's data container only. It is not
    /// replicated (a peer keeps its copy) and, unlike a write, raises no listener event here. Returns whether
    /// the key was held.
    pub fn evict(&self, key: &CacheObject) -> bool {
        self.container.remove(key).is_some()
    }

    /// `size()`: local for a REPL cache (`CallInterceptor.java:625-644`).
    pub fn size(&self) -> usize {
        self.container.len()
    }

    /// The keys on this node.
    pub fn keys(&self) -> Vec<CacheObject> {
        self.container.entries().into_iter().map(|e| e.key).collect()
    }

    /// The entries on this node.
    pub fn entries(&self) -> Vec<(CacheObject, CacheObject)> {
        self.container.entries().into_iter().map(|e| (e.key, e.value)).collect()
    }

    /// The value of `key` on this node, without remote reads or listeners.
    pub fn local_get(&self, key: &CacheObject) -> Option<CacheObject> {
        self.container.peek(key).map(|e| e.value)
    }

    /// Decode a request only to see what it is (tests, interceptors).
    pub fn decode(request: &[u8]) -> Option<CacheRpcCommand> {
        let _ = ObjectInput::new(request);
        CacheRpcCommand::from_bytes(request).ok()
    }
}

enum Attempt {
    /// Wait for a newer topology and run the command again.
    Retry,
    Fail(CacheError),
}

fn response_to_outcome(r: Response) -> Result<Outcome, Attempt> {
    let value = |v: Option<Box<ResponseValue>>| match v.map(|b| *b) {
        None => Ok(CacheObject::Null),
        Some(ResponseValue::Boolean(b)) => Ok(CacheObject::Primitive(PrimitiveValue::BooleanObj(b))),
        Some(ResponseValue::Object(o)) => Ok(o),
        Some(other) => Err(Attempt::Fail(CacheError::Remote(format!("unexpected response value {other:?}")))),
    };
    match r {
        Response::Successful(v) => Ok(Outcome { value: value(v)?, successful: true }),
        Response::Unsuccessful(v) => Ok(Outcome { value: value(v)?, successful: false }),
        Response::CacheNotFound => Err(Attempt::Retry),
        Response::Exception { kind: ExceptionKind::OutdatedTopology { .. }, .. } => Err(Attempt::Retry),
        Response::Exception { kind, message } => Err(Attempt::Fail(exception_error(kind, message))),
    }
}

fn exception_error(kind: ExceptionKind, message: Option<String>) -> CacheError {
    let m = message.unwrap_or_default();
    match kind {
        ExceptionKind::Generic(c) if c == TIMEOUT_EXCEPTION => CacheError::Timeout(m),
        ExceptionKind::SuspectException => CacheError::Suspect(m),
        _ => CacheError::Remote(m),
    }
}

fn exception_response(e: &CacheError) -> Response {
    match e {
        CacheError::Timeout(m) => Response::Exception { kind: ExceptionKind::Generic(TIMEOUT_EXCEPTION), message: Some(m.clone()) },
        CacheError::Suspect(m) => Response::Exception { kind: ExceptionKind::SuspectException, message: Some(m.clone()) },
        other => Response::Exception { kind: ExceptionKind::CacheException, message: Some(other.to_string()) },
    }
}

/// The node as the state consumer and provider see it.
pub struct EngineEnv(pub std::sync::Weak<CacheEngine>);

impl EngineEnv {
    fn engine(&self) -> Option<Arc<CacheEngine>> {
        self.0.upgrade()
    }
}

impl crate::statetransfer::env::StateTransferEnv for EngineEnv {
    fn address(&self) -> Address {
        self.engine().expect("engine alive").address()
    }

    fn invoke(&self, target: &Address, command: CacheRpcCommand, timeout: Duration) -> BoxFuture<'static, RpcResult> {
        match self.engine() {
            Some(e) => e.transport.invoke(target, command.to_bytes(), timeout),
            None => Box::pin(async { Err(RemoteError::Other("cache stopped".into())) }),
        }
    }

    fn send(&self, target: &Address, command: CacheRpcCommand) {
        if let Some(e) = self.engine() {
            let fut = e.transport.invoke(target, command.to_bytes(), e.cfg.remote_timeout);
            tokio::spawn(async move {
                let _ = fut.await;
            });
        }
    }

    fn apply_entries(&self, segment: i32, entries: Vec<InternalCacheEntry>) -> BoxFuture<'static, ()> {
        let engine = self.engine();
        Box::pin(async move {
            let Some(e) = engine else { return };
            if let Some(h) = e.execute_hook() {
                h.before_state_apply(&e.address(), segment).await;
            }
            for entry in entries {
                e.apply_state_entry(entry, segment);
            }
        })
    }

    fn segment_entries(&self, segment: i32) -> Vec<InternalCacheEntry> {
        self.engine().map(|e| e.container.segment_entries(segment).iter().map(|s| s.to_internal()).collect()).unwrap_or_default()
    }

    fn remove_segments_not(&self, keep: &dyn Fn(i32) -> bool) {
        if let Some(e) = self.engine() {
            e.container.remove_segments(&|s| !keep(s));
        }
    }

    fn is_running(&self) -> bool {
        self.engine().is_some()
    }
}

/// `ClusteringDependentLogic.ReplicationLogic` as the notifier sees it.
struct EngineLogic(std::sync::Weak<CacheEngine>);

impl ClusteringDependentLogic for EngineLogic {
    /// `clusterCommitType` (`ClusteringDependentLogic.java:294-309`): the entry is committed here when
    /// the command skips the ownership check or this node is a write owner of the segment.
    fn commit_is_local(&self, key: &CacheObject, command: Option<&CommandInfo>, _removed: bool) -> bool {
        let Some(e) = self.0.upgrade() else { return true };
        if command.is_some_and(|c| c.has_any_flag(flags::SKIP_OWNERSHIP_CHECK)) {
            return true;
        }
        let Ok(topology) = e.current_topology() else { return true };
        let Some(segment) = command.and_then(|c| c.segment).or_else(|| e.segment_of(key).ok()) else { return true };
        match e.distribution(&topology, segment) {
            Ok(d) => d.write_owners.contains(&e.address()),
            Err(_) => true,
        }
    }

    fn is_local_primary_owner(&self, key: &CacheObject) -> bool {
        let Some(e) = self.0.upgrade() else { return true };
        e.owners_of(key).map(|(primary, _)| primary == e.address()).unwrap_or(true)
    }
}
