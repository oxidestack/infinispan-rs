//! The value objects topology commands carry and answer with (core 14.0.35.Final):
//! `CacheJoinInfo`, `CacheStatusResponse`, `ManagerStatusResponse`, `AvailabilityMode`, and the
//! `Response` wrappers (`remoting/responses/`).

use super::cache_topology::CacheTopology;
use super::wire::{ID_NULL, ObjectInput, ObjectOutput, Reusable};
use crate::ch::address::Address;
use crate::ch::java_util::{HashMapInit, hash_map_iteration_order};
use crate::marshall::error::{Error, Result};
use crate::marshall::global::{self, Value};
use crate::marshall::marshall_util::{marshall_enum_ordinal, marshall_size, unmarshall_enum_ordinal, unmarshall_size};
use crate::marshall::primitives::PrimitiveValue;

/// The externalizer id of `SuccessfulResponse`.
pub const ID_SUCCESSFUL_RESPONSE: u8 = 4;
/// The externalizer id of `UnsuccessfulResponse`.
pub const ID_UNSUCCESSFUL_RESPONSE: u8 = 6;
/// The externalizer id of `CacheJoinInfo`.
pub const ID_CACHE_JOIN_INFO: u8 = 49;
/// The externalizer id of `ReplicatedConsistentHashFactory`.
pub const ID_REPLICATED_CONSISTENT_HASH_FACTORY: u8 = 51;
/// The externalizer id of `SyncConsistentHashFactory`.
pub const ID_SYNC_CONSISTENT_HASH_FACTORY: u8 = 52;
/// The externalizer id of `CacheNotFoundResponse`.
pub const ID_CACHE_NOT_FOUND_RESPONSE: u8 = 59;
/// The externalizer id of `CacheStatusResponse`.
pub const ID_CACHE_STATUS_RESPONSE: u8 = 72;
/// The externalizer id of `ManagerStatusResponse`.
pub const ID_MANAGER_STATUS_RESPONSE: u8 = 81;
/// The externalizer id of `Optional`.
pub const ID_OPTIONAL: u8 = 88;
/// The externalizer id of `AvailabilityMode`.
pub const ID_AVAILABILITY_MODE: u8 = 95;
/// The externalizer id of `SyncReplicatedConsistentHashFactory`.
pub const ID_SYNC_REPLICATED_CONSISTENT_HASH_FACTORY: u8 = 96;

/// `CacheMode` (`configuration/cache/CacheMode.java`); the discriminant is the enum ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    /// `LOCAL`: not clustered.
    Local = 0,
    /// `REPL_SYNC`: every node holds every entry; writes wait for the owners.
    ReplSync = 1,
    /// `REPL_ASYNC`: every node holds every entry; writes do not wait.
    ReplAsync = 2,
    /// `INVALIDATION_SYNC`: a write invalidates the other nodes' copies, waiting for them.
    InvalidationSync = 3,
    /// `INVALIDATION_ASYNC`: a write invalidates the other nodes' copies without waiting.
    InvalidationAsync = 4,
    /// `DIST_SYNC`: each entry on `num_owners` nodes; writes wait for the owners.
    DistSync = 5,
    /// `DIST_ASYNC`: each entry on `num_owners` nodes; writes do not wait.
    DistAsync = 6,
    /// `SCATTERED_SYNC`: scattered cache.
    ScatteredSync = 7,
}

impl CacheMode {
    /// The mode with ordinal `o`; an error for an unknown ordinal.
    pub fn from_ordinal(o: u8) -> Result<Self> {
        use CacheMode::*;
        Ok(match o {
            0 => Local,
            1 => ReplSync,
            2 => ReplAsync,
            3 => InvalidationSync,
            4 => InvalidationAsync,
            5 => DistSync,
            6 => DistAsync,
            7 => ScatteredSync,
            other => return Err(Error::Malformed(format!("Unknown enum. ordinal {other}"))),
        })
    }
}

/// `AvailabilityMode` (`partitionhandling/AvailabilityMode.java`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailabilityMode {
    /// `AVAILABLE`: the cache serves reads and writes.
    Available = 0,
    /// `DEGRADED_MODE`: after a partition, the cache refuses access to entries whose owners it lost.
    DegradedMode = 1,
}

impl AvailabilityMode {
    /// The mode with ordinal `o`; an error for an unknown ordinal.
    pub fn from_ordinal(o: u8) -> Result<Self> {
        match o {
            0 => Ok(AvailabilityMode::Available),
            1 => Ok(AvailabilityMode::DegradedMode),
            other => Err(Error::Malformed(format!("Unknown enum. ordinal {other}"))),
        }
    }

    /// `AvailabilityMode.min` (`:25-29`).
    pub fn min(self, other: AvailabilityMode) -> AvailabilityMode {
        if self == AvailabilityMode::DegradedMode || other == AvailabilityMode::DegradedMode {
            AvailabilityMode::DegradedMode
        } else {
            AvailabilityMode::Available
        }
    }
}

/// The `ConsistentHashFactory` implementations whose externalizers carry no state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistentHashFactoryKind {
    /// `SyncReplicatedConsistentHashFactory`.
    SyncReplicated,
    /// `ReplicatedConsistentHashFactory`.
    Replicated,
    /// `SyncConsistentHashFactory`.
    Sync,
}

impl ConsistentHashFactoryKind {
    fn id(self) -> u8 {
        match self {
            Self::SyncReplicated => ID_SYNC_REPLICATED_CONSISTENT_HASH_FACTORY,
            Self::Replicated => ID_REPLICATED_CONSISTENT_HASH_FACTORY,
            Self::Sync => ID_SYNC_CONSISTENT_HASH_FACTORY,
        }
    }
}

/// `CacheJoinInfo` (`topology/CacheJoinInfo.java`).
#[derive(Debug, Clone, PartialEq)]
pub struct CacheJoinInfo {
    /// The factory that builds the cache's consistent hashes.
    pub consistent_hash_factory: ConsistentHashFactoryKind,
    /// The number of segments.
    pub num_segments: i32,
    /// The number of owners of each segment.
    pub num_owners: i32,
    /// The state transfer timeout in milliseconds.
    pub timeout: i64,
    /// The cache mode.
    pub cache_mode: CacheMode,
    /// The node's capacity factor, its share of the segments relative to the others.
    pub capacity_factor: f32,
    /// `PersistentUUID` (an [`Address::Persistent`]).
    pub persistent_uuid: Option<Address>,
    /// The checksum of the node's persistent state, if it has any.
    pub persistent_state_checksum: Option<i32>,
}

/// `CacheStatusResponse` (`topology/CacheStatusResponse.java`).
#[derive(Debug, Clone, PartialEq)]
pub struct CacheStatusResponse {
    /// The node's join info for the cache.
    pub cache_join_info: Option<CacheJoinInfo>,
    /// The node's current topology for the cache.
    pub cache_topology: Option<CacheTopology>,
    /// The node's stable topology for the cache.
    pub stable_topology: Option<CacheTopology>,
    /// The node's availability mode for the cache.
    pub availability_mode: Option<AvailabilityMode>,
    /// The members the node knows of in the current topology.
    pub current: Option<Vec<Address>>,
}

impl CacheStatusResponse {
    /// `CacheStatusResponse.empty()`.
    pub fn empty() -> Self {
        Self { cache_join_info: None, cache_topology: None, stable_topology: None, availability_mode: None, current: None }
    }

    /// Whether the response is [`Self::empty`].
    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }
}

/// `ManagerStatusResponse` (`topology/ManagerStatusResponse.java`): the per-cache statuses of one
/// node, in the iteration order of the `HashMap` that carried them.
#[derive(Debug, Clone, PartialEq)]
pub struct ManagerStatusResponse {
    /// Each cache's status, by name.
    pub caches: Vec<(String, CacheStatusResponse)>,
    /// Whether rebalancing is enabled on the node.
    pub rebalancing_enabled: bool,
}

/// `String.hashCode()` over UTF-16 code units.
pub fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

impl ManagerStatusResponse {
    /// Build with `caches` ordered as a `new HashMap<>()` filled in `insertion` order would iterate.
    pub fn with_hash_map_order(insertion: Vec<(String, CacheStatusResponse)>, rebalancing_enabled: bool) -> Self {
        let hashes: Vec<i32> = insertion.iter().map(|(k, _)| java_string_hash(k)).collect();
        let order = hash_map_iteration_order(&hashes, HashMapInit::Default);
        let caches = order.into_iter().map(|i| insertion[i].clone()).collect();
        Self { caches, rebalancing_enabled }
    }
}

/// The `Response` kinds topology RPCs answer with.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// `SuccessfulResponse` wrapping a value; `None` is `SUCCESSFUL_EMPTY_RESPONSE`.
    Successful(Option<Box<ResponseValue>>),
    /// `UnsuccessfulResponse` (`EMPTY` when `None`).
    Unsuccessful(Option<Box<ResponseValue>>),
    /// `CacheNotFoundResponse.INSTANCE`.
    CacheNotFound,
    /// `ExceptionResponse` carrying one of the `ThrowableExternalizer` kinds the topology layer raises
    /// (message only: no cause and no suppressed exceptions).
    Exception {
        /// The exception's class.
        kind: ExceptionKind,
        /// The exception's message.
        message: Option<String>,
    },
}

/// Fully qualified name of `org.infinispan.IllegalLifecycleStateException`, which the externalizer
/// does not number and so writes as a generic throwable.
pub const ILLEGAL_LIFECYCLE_STATE_EXCEPTION: &str = "org.infinispan.IllegalLifecycleStateException";

/// `org.infinispan.util.concurrent.TimeoutException`: not the `java.util.concurrent.TimeoutException`
/// the externalizer numbers (23), so it is written by name (the vector `resp:exception-timeout` shows it).
pub const TIMEOUT_EXCEPTION: &str = "org.infinispan.util.concurrent.TimeoutException";

/// `ThrowableExternalizer` ids (`marshall/exts/ThrowableExternalizer.java:50-76`) for the exceptions
/// the topology layer throws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionKind {
    /// `org.infinispan.commons.CacheException`.
    CacheException,
    /// `org.infinispan.topology.CacheJoinException`.
    CacheJoinException,
    /// `org.infinispan.remoting.transport.jgroups.SuspectException`.
    SuspectException,
    /// `OutdatedTopologyException` (18): `true` for `RETRY_SAME_TOPOLOGY` (`topologyIdDelta == 0`).
    OutdatedTopology {
        /// Whether the command is retried in the same topology.
        retry_same_topology: bool,
    },
    /// `UNKNOWN` (-1): a class the externalizer does not number, written by name.
    Generic(&'static str),
}

impl ExceptionKind {
    fn id(self) -> i16 {
        match self {
            ExceptionKind::CacheException => 3,
            ExceptionKind::CacheJoinException => 6,
            ExceptionKind::SuspectException => 22,
            ExceptionKind::OutdatedTopology { .. } => 18,
            ExceptionKind::Generic(_) => -1,
        }
    }
}

/// The externalizer id of `ExceptionResponse`.
pub const ID_EXCEPTION_RESPONSE: u8 = 5;
/// The externalizer id of `ThrowableExternalizer`.
pub const ID_EXCEPTIONS: u8 = 146;

/// What a topology command's `invokeAsync` returns inside a `SuccessfulResponse`.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // short-lived value, never stored in bulk
pub enum ResponseValue {
    /// A `CacheStatusResponse`.
    CacheStatus(CacheStatusResponse),
    /// A `ManagerStatusResponse`.
    ManagerStatus(ManagerStatusResponse),
    /// `Boolean`, e.g. `handleRebalancePhaseConfirm`'s completion.
    Boolean(bool),
    /// `RebalanceStatusRequestCommand`'s answer (an `EnumExternalizer` enum).
    RebalancingStatus(super::cluster_cache_status::RebalancingStatus),
    /// A cache value (`null`-free): a primitive other than `Boolean`, or a user object. Cache
    /// commands answer with these (`SuccessfulResponse.create(returnValue)`).
    Object(crate::commands::object::CacheObject),
    /// `ClusteredGetCommand`'s answer: an `InternalCacheValue` (`ImmortalCacheValue`/`MortalCacheValue`).
    InternalCacheValue(crate::commands::object::InternalCacheValue),
}

/// The externalizer id of the internal enums (`Ids.INTERNAL_ENUMS`).
pub const ID_INTERNAL_ENUMS: u8 = 148;
/// The externalizer `writeObject(Class)` goes through (`Ids.CLASS`).
pub const ID_CLASS: u8 = 144;
const REBALANCING_STATUS_CLASS: &str = "org.infinispan.topology.RebalancingStatus";

impl super::cluster_cache_status::RebalancingStatus {
    fn name(self) -> &'static str {
        use super::cluster_cache_status::RebalancingStatus::*;
        match self {
            Suspended => "SUSPENDED",
            Pending => "PENDING",
            InProgress => "IN_PROGRESS",
            Complete => "COMPLETE",
        }
    }

    fn from_name(n: &str) -> Result<Self> {
        use super::cluster_cache_status::RebalancingStatus::*;
        Ok(match n {
            "SUSPENDED" => Suspended,
            "PENDING" => Pending,
            "IN_PROGRESS" => InProgress,
            "COMPLETE" => Complete,
            other => return Err(Error::Malformed(format!("No enum constant RebalancingStatus.{other}"))),
        })
    }
}

impl ObjectOutput {
    fn write_nullable_string(&mut self, s: &str) {
        global::write_nullable(&mut self.w, &Value::Primitive(PrimitiveValue::Str(s.to_string())));
    }

    fn write_availability_mode(&mut self, m: Option<AvailabilityMode>) {
        match m {
            None => self.write_null(),
            Some(m) => {
                self.write_internal_header(ID_AVAILABILITY_MODE);
                marshall_enum_ordinal(&mut self.w, Some(m as u8));
            }
        }
    }

    /// `writeObject(CacheJoinInfo)` or `null`: `CacheJoinInfo.Externalizer` (`:157-172`).
    pub fn write_cache_join_info(&mut self, i: Option<&CacheJoinInfo>) {
        let Some(i) = i else { return self.write_null() };
        self.write_internal_header(ID_CACHE_JOIN_INFO);
        self.write_internal_header(i.consistent_hash_factory.id());
        self.w.write_int(i.num_segments);
        self.w.write_int(i.num_owners);
        self.w.write_long(i.timeout);
        marshall_enum_ordinal(&mut self.w, Some(i.cache_mode as u8));
        self.w.write_float(i.capacity_factor);
        match &i.persistent_uuid {
            None => self.write_null(),
            Some(a) => self.write_address(a),
        }
        // writeObject(Optional<Integer>): OptionalExternalizer
        self.write_internal_header(ID_OPTIONAL);
        match i.persistent_state_checksum {
            None => self.w.write_byte(0),
            Some(c) => {
                self.w.write_byte(1);
                global::write_nullable(&mut self.w, &Value::Primitive(PrimitiveValue::IntObj(c)));
            }
        }
    }

    /// `writeObject(CacheStatusResponse)` or `null`: `CacheStatusResponse.Externalizer`.
    pub fn write_cache_status_response(&mut self, r: &CacheStatusResponse) {
        self.write_internal_header(ID_CACHE_STATUS_RESPONSE);
        self.write_cache_join_info(r.cache_join_info.as_ref());
        self.write_cache_topology(r.cache_topology.as_ref());
        self.write_cache_topology(r.stable_topology.as_ref());
        self.write_availability_mode(r.availability_mode);
        // MarshallUtil.marshallCollection(current): null is the NULL_VALUE size
        match &r.current {
            None => marshall_size(&mut self.w, -1),
            Some(list) => {
                marshall_size(&mut self.w, list.len() as i32);
                for a in list {
                    self.write_address(a);
                }
            }
        }
    }

    /// `writeObject(ManagerStatusResponse)`: `ManagerStatusResponse.Externalizer` (reusing, with
    /// children, identity equality).
    pub fn write_manager_status_response(&mut self, r: &ManagerStatusResponse) {
        self.write_internal_header(ID_MANAGER_STATUS_RESPONSE);
        let key = self.next_identity();
        let r2 = r.clone();
        self.write_reusable(key, true, move |o| {
            // writeObject(Map<String, CacheStatusResponse>): HashMap via MapExternalizer
            o.write_internal_header(super::wire::ID_MAPS);
            o.w.write_unsigned_byte(0); // HASHMAP
            marshall_size(&mut o.w, r2.caches.len() as i32);
            for (name, status) in &r2.caches {
                o.write_nullable_string(name);
                o.write_cache_status_response(status);
            }
            o.w.write_boolean(r2.rebalancing_enabled);
        });
    }

    /// `writeObject(Response)`.
    pub fn write_response(&mut self, r: &Response) {
        match r {
            Response::CacheNotFound => self.write_internal_header(ID_CACHE_NOT_FOUND_RESPONSE),
            Response::Exception { kind, message } => {
                self.write_internal_header(ID_EXCEPTION_RESPONSE);
                self.write_internal_header(ID_EXCEPTIONS);
                self.w.write_short(kind.id());
                match kind {
                    ExceptionKind::OutdatedTopology { retry_same_topology } => self.w.write_boolean(*retry_same_topology),
                    _ => {
                        if let ExceptionKind::Generic(class) = kind {
                            self.w.write_utf(class);
                        }
                        // writeMessageAndCause: message, cause (null), suppressed (empty array)
                        crate::marshall::marshall_util::marshall_string(&mut self.w, message.as_deref());
                        self.write_null();
                        marshall_size(&mut self.w, 0);
                    }
                }
            }
            Response::Successful(v) => {
                self.write_internal_header(ID_SUCCESSFUL_RESPONSE);
                self.write_response_value(v.as_deref());
            }
            Response::Unsuccessful(v) => {
                self.write_internal_header(ID_UNSUCCESSFUL_RESPONSE);
                self.write_response_value(v.as_deref());
            }
        }
    }

    fn write_response_value(&mut self, v: Option<&ResponseValue>) {
        match v {
            None => self.write_null(),
            Some(ResponseValue::CacheStatus(c)) => self.write_cache_status_response(c),
            Some(ResponseValue::ManagerStatus(m)) => self.write_manager_status_response(m),
            Some(ResponseValue::Boolean(b)) => {
                global::write_nullable(&mut self.w, &Value::Primitive(PrimitiveValue::BooleanObj(*b)))
            }
            Some(ResponseValue::Object(o)) => self.write_cache_object(o),
            Some(ResponseValue::InternalCacheValue(v)) => self.write_internal_cache_value(v),
            Some(ResponseValue::RebalancingStatus(rs)) => {
                // EnumExternalizer: writeObject(e.getClass()); writeUTF(e.name())
                self.write_internal_header(ID_INTERNAL_ENUMS);
                self.write_internal_header(ID_CLASS);
                self.w.write_utf(REBALANCING_STATUS_CLASS);
                self.w.write_utf(rs.name());
            }
        }
    }
}

impl ObjectInput<'_> {
    fn read_nullable_string(&mut self) -> Result<String> {
        match global::read_nullable(&mut self.r)? {
            Value::Primitive(PrimitiveValue::Str(s)) => Ok(s),
            other => Err(Error::Malformed(format!("expected String, got {other:?}"))),
        }
    }

    fn read_availability_mode(&mut self) -> Result<Option<AvailabilityMode>> {
        let mut peek = self.r;
        if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        self.expect_internal(ID_AVAILABILITY_MODE, "AvailabilityMode header")?;
        let o = unmarshall_enum_ordinal(&mut self.r)?.ok_or_else(|| Error::Malformed("null enum".into()))?;
        Ok(Some(AvailabilityMode::from_ordinal(o)?))
    }

    /// `readObject()` for a nullable `CacheJoinInfo`.
    pub fn read_cache_join_info(&mut self) -> Result<Option<CacheJoinInfo>> {
        let mut peek = self.r;
        if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        self.expect_internal(ID_CACHE_JOIN_INFO, "CacheJoinInfo header")?;
        if self.r.read_unsigned_byte()? != super::wire::ID_INTERNAL {
            return Err(Error::Malformed("ConsistentHashFactory is not an internal object".into()));
        }
        let consistent_hash_factory = match self.r.read_unsigned_byte()? {
            ID_SYNC_REPLICATED_CONSISTENT_HASH_FACTORY => ConsistentHashFactoryKind::SyncReplicated,
            ID_REPLICATED_CONSISTENT_HASH_FACTORY => ConsistentHashFactoryKind::Replicated,
            ID_SYNC_CONSISTENT_HASH_FACTORY => ConsistentHashFactoryKind::Sync,
            other => return Err(Error::UnknownTag { what: "ConsistentHashFactory externalizer id", value: other as i64 }),
        };
        let num_segments = self.r.read_int()?;
        let num_owners = self.r.read_int()?;
        let timeout = self.r.read_long()?;
        let cache_mode = CacheMode::from_ordinal(
            unmarshall_enum_ordinal(&mut self.r)?.ok_or_else(|| Error::Malformed("null cache mode".into()))?,
        )?;
        let capacity_factor = self.r.read_float()?;
        let mut peek = self.r;
        let persistent_uuid = if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            None
        } else {
            Some(self.read_address()?)
        };
        self.expect_internal(ID_OPTIONAL, "Optional header")?;
        let persistent_state_checksum = if self.r.read_byte()? == 1 {
            match global::read_nullable(&mut self.r)? {
                Value::Primitive(PrimitiveValue::IntObj(c)) => Some(c),
                other => return Err(Error::Malformed(format!("expected Integer, got {other:?}"))),
            }
        } else {
            None
        };
        Ok(Some(CacheJoinInfo {
            consistent_hash_factory,
            num_segments,
            num_owners,
            timeout,
            cache_mode,
            capacity_factor,
            persistent_uuid,
            persistent_state_checksum,
        }))
    }

    /// `readObject()` for a `CacheStatusResponse`.
    pub fn read_cache_status_response(&mut self) -> Result<CacheStatusResponse> {
        self.expect_internal(ID_CACHE_STATUS_RESPONSE, "CacheStatusResponse header")?;
        let cache_join_info = self.read_cache_join_info()?;
        let cache_topology = self.read_cache_topology()?;
        let stable_topology = self.read_cache_topology()?;
        let availability_mode = self.read_availability_mode()?;
        let size = unmarshall_size(&mut self.r)?;
        let current = if size < 0 {
            None
        } else {
            Some((0..size).map(|_| self.read_address()).collect::<Result<Vec<_>>>()?)
        };
        Ok(CacheStatusResponse { cache_join_info, cache_topology, stable_topology, availability_mode, current })
    }

    /// `readObject()` for a `ManagerStatusResponse`.
    pub fn read_manager_status_response(&mut self) -> Result<ManagerStatusResponse> {
        self.expect_internal(ID_MANAGER_STATUS_RESPONSE, "ManagerStatusResponse header")?;
        let mut out: Option<ManagerStatusResponse> = None;
        let slot = &mut out;
        self.read_reusable(true, |o| {
            o.expect_internal(super::wire::ID_MAPS, "Map header")?;
            let kind = o.r.read_unsigned_byte()?;
            if kind != 0 {
                return Err(Error::UnknownTag { what: "MapExternalizer kind (expected HASHMAP)", value: kind as i64 });
            }
            let size = unmarshall_size(&mut o.r)?;
            if size < 0 {
                return Err(Error::Malformed("null caches map".into()));
            }
            let mut caches = Vec::new();
            for _ in 0..size {
                let name = o.read_nullable_string()?;
                caches.push((name, o.read_cache_status_response()?));
            }
            let rebalancing_enabled = o.r.read_boolean()?;
            *slot = Some(ManagerStatusResponse { caches, rebalancing_enabled });
            Ok(Reusable::Identity(0))
        })?;
        out.ok_or_else(|| Error::Malformed("back-reference to a ManagerStatusResponse".into()))
    }

    /// `readObject()` for a `Response` carrying topology values.
    pub fn read_response(&mut self) -> Result<Response> {
        if self.r.read_unsigned_byte()? != super::wire::ID_INTERNAL {
            return Err(Error::Malformed("response is not an internal object".into()));
        }
        match self.r.read_unsigned_byte()? {
            ID_CACHE_NOT_FOUND_RESPONSE => Ok(Response::CacheNotFound),
            ID_EXCEPTION_RESPONSE => {
                self.expect_internal(ID_EXCEPTIONS, "Throwable header")?;
                let id = self.r.read_short()?;
                if id == 18 {
                    let retry_same_topology = self.r.read_boolean()?;
                    return Ok(Response::Exception { kind: ExceptionKind::OutdatedTopology { retry_same_topology }, message: None });
                }
                let kind = match id {
                    3 => ExceptionKind::CacheException,
                    6 => ExceptionKind::CacheJoinException,
                    22 => ExceptionKind::SuspectException,
                    -1 => {
                        let class = self.r.read_utf()?;
                        if class == ILLEGAL_LIFECYCLE_STATE_EXCEPTION {
                            ExceptionKind::Generic(ILLEGAL_LIFECYCLE_STATE_EXCEPTION)
                        } else if class == TIMEOUT_EXCEPTION {
                            ExceptionKind::Generic(TIMEOUT_EXCEPTION)
                        } else {
                            return Err(Error::Malformed(format!("unsupported exception class {class}")));
                        }
                    }
                    other => return Err(Error::UnknownTag { what: "ThrowableExternalizer id", value: other as i64 }),
                };
                let message = crate::marshall::marshall_util::unmarshall_string(&mut self.r)?;
                let cause_tag = self.r.read_unsigned_byte()?;
                let suppressed = unmarshall_size(&mut self.r)?;
                if cause_tag != ID_NULL || suppressed > 0 {
                    return Err(Error::Malformed("exceptions with a cause or suppressed exceptions are not modelled".into()));
                }
                Ok(Response::Exception { kind, message })
            }
            ID_SUCCESSFUL_RESPONSE => Ok(Response::Successful(self.read_response_value()?.map(Box::new))),
            ID_UNSUCCESSFUL_RESPONSE => Ok(Response::Unsuccessful(self.read_response_value()?.map(Box::new))),
            other => Err(Error::UnknownTag { what: "Response externalizer id", value: other as i64 }),
        }
    }

    fn read_response_value(&mut self) -> Result<Option<ResponseValue>> {
        let mut peek = self.r;
        let tag = peek.read_unsigned_byte()?;
        if tag == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        if tag == super::wire::ID_PRIMITIVE || tag == 0x05 {
            return match self.read_cache_object()? {
                crate::commands::object::CacheObject::Primitive(PrimitiveValue::BooleanObj(b)) => Ok(Some(ResponseValue::Boolean(b))),
                other => Ok(Some(ResponseValue::Object(other))),
            };
        }
        match peek.read_unsigned_byte()? {
            ID_INTERNAL_ENUMS => {
                self.expect_internal(ID_INTERNAL_ENUMS, "enum header")?;
                self.expect_internal(ID_CLASS, "Class header")?;
                let class = self.r.read_utf()?;
                if class != REBALANCING_STATUS_CLASS {
                    return Err(Error::Malformed(format!("unsupported enum class {class}")));
                }
                let name = self.r.read_utf()?;
                Ok(Some(ResponseValue::RebalancingStatus(super::cluster_cache_status::RebalancingStatus::from_name(&name)?)))
            }
            crate::commands::object::ID_IMMORTAL_VALUE | crate::commands::object::ID_MORTAL_VALUE => {
                Ok(Some(ResponseValue::InternalCacheValue(self.read_internal_cache_value()?)))
            }
            ID_CACHE_STATUS_RESPONSE => Ok(Some(ResponseValue::CacheStatus(self.read_cache_status_response()?))),
            ID_MANAGER_STATUS_RESPONSE => Ok(Some(ResponseValue::ManagerStatus(self.read_manager_status_response()?))),
            other => Err(Error::UnknownTag { what: "response value externalizer id", value: other as i64 }),
        }
    }
}
