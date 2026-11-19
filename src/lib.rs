//! An unofficial Rust port of the parts of [Infinispan](https://infinispan.org/) 14.0.35.Final that a clustered
//! `REPL_SYNC` cache reaches: marshalling, consistent hashing, cache topology and rebalance, state transfer, the
//! write commands, listeners and expiration, over the [`jgroups`] crate's channel. A node built from it joins a
//! cluster of Java Infinispan nodes and speaks their wire protocol; the byte forms are checked against vectors
//! recorded from the real Infinispan.
//!
//! # Example
//!
//! Two nodes of an in-memory cluster (the same pipeline, every request and answer marshalled, no network) share a
//! replicated cache:
//!
//! ```
//! use infinispan::cache::testing::ReplCluster;
//! use infinispan::commands::object::CacheObject;
//!
//! # #[tokio::main(flavor = "multi_thread")]
//! # async fn main() {
//! let cluster = ReplCluster::start(2, &["demo"]).await;
//! let key = CacheObject::string("greeting");
//! let value = CacheObject::string("hello");
//!
//! cluster.cache(0, "demo").put(&key, &value).await.unwrap();
//! assert_eq!(cluster.cache(1, "demo").get(&key).await.unwrap(), Some(value));
//! # }
//! ```
//!
//! # Modules
//!
//! `marshall` is the first ported surface: the `GlobalMarshaller` byte
//! envelope, the JDK Object Serialization codec Infinispan's "user value" path rides on top of,
//! and the deserialization allow-list gate.
//!
//! `remoting` is the `JGroupsTransport` surface: fire-and-forget and correlated
//! request/response dispatch over a real `jgroups-rs` channel, via `jgroups::channel::JChannel`'s
//! `UpHandler`/`send_message` and the `RequestCorrelator.Header` that crate already
//! ports. `factories` is the small slice of Infinispan's startup sequence the cluster node needs —
//! `GlobalComponentRegistry`/`GlobalConfigurationManagerImpl`/`InternalCacheRegistryImpl` — enough
//! to connect a channel and register the implicit `org.infinispan.CONFIG` cache.
//!
//! `ch` and `topology` are the cluster-topology half of the port: MurmurHash3 segment
//! placement, `SyncConsistentHashFactory`/`SyncReplicatedConsistentHashFactory`/`ReplicatedConsistentHash`
//! (byte-exact against the real Java classes), `CacheTopology` and the topology commands, and the
//! coordinator ([`topology::cluster_topology_manager`]) and per-node ([`topology::local_topology_manager`])
//! managers that drive the four-phase rebalance over a [`topology::transport::TopologyTransport`] (the
//! real jgroups-rs channel in [`topology::jgroups_transport`], an in-memory cluster in
//! [`topology::testing`]).

//! `commands` and `cache` are the REPL_SYNC non-transactional write path:
//! `SingleRpcCommand`-wrapped `PutKeyValue`/`Compute`/`Replace`/`Remove`/`Clear` commands and
//! `ClusteredGet` with byte-exact externalizers (against `tests/vectors/write_commands.hex`, from the real
//! Infinispan marshaller), the `BiFunctionMapper`/lambda that carries MockServer's `compute`, and
//! [`cache::write_pipeline::CacheEngine`], the interceptor chain folded into one pipeline: segment,
//! forwarding to the primary, key lock, apply, broadcast to the backups, `UnsuccessfulResponse` for a
//! failed conditional write, and the topology-change retries. [`cache::testing::ReplCluster`] runs it
//! over the in-memory topology cluster, marshalling every request and answer.

//! `statetransfer` is the join half of the cache: a joining node requests every segment it will own
//! ([`statetransfer::consumer`]), the members push them as chunks ([`statetransfer::provider`],
//! [`statetransfer::outbound_task`]), the joiner applies them without locks, replication or listener
//! notifications and never over a newer write ([`statetransfer::commit_manager`]), and only then confirms
//! the rebalance phase. Chunks, entries and the three commands are byte-checked against
//! `tests/vectors/state_transfer.hex`, which also records what a real two-node Java cluster stores in a
//! joiner.

//! `notify` and the bounded container are the observation half of the cache. [`notify::cache_notifier`]
//! is `CacheNotifierImpl`: annotated listeners ([`notify::listener_registration`]), filters and converters
//! ([`notify::filter`]), and every `notify*` entry point. In a replicated cache a clustered listener is a local
//! listener on the node it was added to, post events only, that fires for local writes and incoming backup
//! writes alike, receives `clear()` as one removal per entry, and never gets an evicted event. The engine owns a
//! notifier and raises pre/post events (with metadata, retry flag and origin) from the write pipeline, and
//! `expired` events from `RemoveExpiredCommand` (byte-checked against `tests/vectors/expiry.hex`).
//! [`cache::data_container`] is `BoundedSegmentedDataContainer` over `caffeine-rs`' W-TinyLFU policy
//! (`maxCount` + REMOVE) with the factory that picks it.

#![warn(missing_docs)]

pub mod cache;
pub mod ch;
pub mod commands;
pub mod factories;
pub mod marshall;
pub mod notify;
pub mod remoting;
pub mod statetransfer;
pub mod topology;
