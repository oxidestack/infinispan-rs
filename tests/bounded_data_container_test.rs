//! The bounded data container: `DataContainerFactoryTest`'s heap cases for the modes this port
//! reaches, and the behaviour MockServer relies on from an `expectations` cache with `maxCount` + REMOVE:
//! each node holds at most `maxCount` entries, which ones is up to Caffeine's W-TinyLFU, nothing about an
//! eviction travels, and the eviction event reaches plain listeners only: a clustered listener has no evicted
//! handler (`AbstractListenerImpl.java:216-222`).
//!
//! `DataContainerFactoryTest` (10 tests upstream) is ported for its 5 heap-container cases, with `REPL_SYNC`
//! where it uses `DIST_ASYNC` (both need state transfer, so both segment); its off-heap and L1 cases are not
//! ported (no off-heap containers, no L1).

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::repl::*;
use infinispan::cache::container::DataContainer;
use infinispan::cache::data_container::{ContainerKind, DataContainerFactory, EvictionStrategy, MemoryConfig, java_hash_code};
use infinispan::cache::testing::{Intercepted, ReplCluster, RpcInterceptor};
use infinispan::cache::write_pipeline::CacheConfig;
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::commands::write::WriteCommand;
use infinispan::notify::cache_notifier::CacheMode;
use infinispan::notify::event::{Event, EventKind};
use infinispan::notify::listener_registration::{ListenerAttrs, ListenerDefinition, ListenerMethod};
use infinispan::topology::support::BoxFuture;

// --- DataContainerFactoryTest ----------------------------------------------------------------------------

#[test]
fn test_default_configuration_data_container() {
    assert_eq!(ContainerKind::DefaultDataContainer, DataContainerFactory::kind(CacheMode::Local, &MemoryConfig::UNBOUNDED).unwrap());
}

/// `testDefaultSegmented`, with REPL_SYNC for DIST_ASYNC.
#[test]
fn test_default_segmented() {
    assert_eq!(ContainerKind::DefaultSegmentedDataContainer, DataContainerFactory::kind(CacheMode::ReplSync, &MemoryConfig::UNBOUNDED).unwrap());
}

#[test]
fn test_eviction_remove_not_segmented() {
    let memory = MemoryConfig { when_full: EvictionStrategy::Remove, ..MemoryConfig::max_count(1000) };
    assert_eq!(ContainerKind::BoundedDataContainer, DataContainerFactory::kind(CacheMode::Local, &memory).unwrap());
}

/// `testEvictionRemoveSegmented`, with REPL_SYNC for DIST_ASYNC.
#[test]
fn test_eviction_remove_segmented() {
    let memory = MemoryConfig::max_count(1000);
    assert_eq!(ContainerKind::BoundedSegmentedDataContainer, DataContainerFactory::kind(CacheMode::ReplSync, &memory).unwrap());
    let c = DataContainerFactory::construct(CacheMode::ReplSync, &memory).unwrap();
    assert!(c.is_bounded());
    assert_eq!(Some(1000), c.capacity());
    assert!(!DataContainerFactory::construct(CacheMode::ReplSync, &MemoryConfig::UNBOUNDED).unwrap().is_bounded());
}

#[test]
fn off_heap_and_exception_eviction_are_refused() {
    let off_heap = MemoryConfig { off_heap: true, ..MemoryConfig::UNBOUNDED };
    assert!(DataContainerFactory::kind(CacheMode::ReplSync, &off_heap).is_err());
    let exception = MemoryConfig { when_full: EvictionStrategy::Exception, ..MemoryConfig::max_count(10) };
    assert!(DataContainerFactory::kind(CacheMode::ReplSync, &exception).is_err());
}

// --- the container ---------------------------------------------------------------------------------------

#[test]
fn java_hash_codes_of_keys() {
    assert_eq!(java_hash_code(&s("abc")), 96354);
    assert_eq!(java_hash_code(&s("")), 0);
    assert_eq!(java_hash_code(&s("Aa")), java_hash_code(&s("BB")));
    assert_eq!(java_hash_code(&CacheObject::Primitive(infinispan::marshall::primitives::PrimitiveValue::LongObj(1 << 32)) ), 1);
}

fn fill(c: &DataContainer, n: usize) -> usize {
    let mut evicted = 0;
    for i in 0..n {
        c.put(s(&format!("key-{i}")), s("v"), None, 0);
        evicted += c.drain_evicted().len();
    }
    evicted
}

#[test]
fn a_bounded_container_never_holds_more_than_max_count_entries() {
    for max in [1u64, 5, 100, 1000] {
        let c = DataContainer::bounded(max);
        let evicted = fill(&c, 3000);
        assert!(c.len() as u64 <= max, "max {max}: {} entries", c.len());
        assert_eq!(3000, c.len() + evicted, "every entry is either held or was reported as evicted (max {max})");
        assert!(c.len() as u64 >= max.saturating_sub(1) || max == 1, "the container stays full under pressure (max {max}: {})", c.len());
    }
}

/// A removed key leaves the policy (`RemovalCause.EXPLICIT`), so it takes no capacity and a
/// container with churn never evicts a live entry while it is under its bound.
#[test]
fn removed_keys_no_longer_take_capacity() {
    let c = DataContainer::bounded(100);
    for round in 0..50 {
        for i in 0..60 {
            c.put(s(&format!("k-{round}-{i}")), s("v"), None, 0);
        }
        for i in 0..60 {
            assert!(c.remove(&s(&format!("k-{round}-{i}"))).is_some());
        }
    }
    assert_eq!(0, c.len());
    assert!(c.drain_evicted().is_empty(), "removal churn evicted a live entry");
    for i in 0..100 {
        c.put(s(&format!("live-{i}")), s("v"), None, 0);
    }
    assert_eq!(100, c.len());
    assert!(c.drain_evicted().is_empty(), "ghosts took capacity");
    // clear and remove_segments free capacity too
    c.clear();
    for i in 0..100 {
        c.put(s(&format!("again-{i}")), s("v"), None, 7);
    }
    assert_eq!(100, c.remove_segments(&|seg| seg == 7));
    for i in 0..100 {
        c.put(s(&format!("final-{i}")), s("v"), None, 0);
    }
    assert_eq!(100, c.len());
    assert!(c.drain_evicted().is_empty());
}

#[test]
fn an_unbounded_container_evicts_nothing() {
    let c = DataContainer::default();
    assert_eq!(0, fill(&c, 2000));
    assert_eq!(2000, c.len());
}

/// W-TinyLFU is not LRU: entries that are read again and again survive a flood of one-off writes.
#[test]
fn frequently_read_entries_survive_a_flood_of_cold_writes() {
    let c = DataContainer::bounded(100);
    let hot: Vec<CacheObject> = (0..10).map(|i| s(&format!("hot-{i}"))).collect();
    for h in &hot {
        c.put(h.clone(), s("v"), None, 0);
    }
    for round in 0..20_000 {
        c.put(s(&format!("cold-{round}")), s("v"), None, 0);
        for h in &hot {
            c.get(h);
        }
        c.drain_evicted();
    }
    let alive = hot.iter().filter(|h| c.peek(h).is_some()).count();
    assert!(alive >= 9, "{alive} of the 10 hot keys survived");
    assert!(c.len() <= 100);
}

#[test]
fn a_peek_does_not_count_as_an_access_but_the_entry_is_returned() {
    let c = DataContainer::bounded(10);
    c.put(s("k"), s("v"), None, 3);
    assert_eq!(c.peek(&s("k")).unwrap().segment, 3);
    assert_eq!(c.get(&s("k")).unwrap().value, s("v"));
    assert!(c.peek(&s("absent")).is_none());
}

// --- the engine ------------------------------------------------------------------------------------------

const EXPECTATIONS: &str = "expectations";
const SCENARIO_STATES: &str = "scenarioStates";
const BLOBS: &str = "blobs";
const MAX: i64 = 50;

/// MockServer's layout: only `expectations` is bounded.
fn mockserver_config(cfg: CacheConfig) -> CacheConfig {
    if cfg.name == EXPECTATIONS { cfg.with_max_count(MAX) } else { cfg }
}

async fn mockserver_cluster() -> ReplCluster {
    ReplCluster::start_with(3, &[EXPECTATIONS, SCENARIO_STATES, BLOBS], mockserver_config).await
}

/// A listener class with a created handler and an evicted handler, `clustered` or plain, recording the events.
fn listener(clustered: bool, seen: &Arc<Mutex<Vec<Event>>>) -> Arc<ListenerDefinition> {
    let (a, b) = (seen.clone(), seen.clone());
    let attrs = if clustered { ListenerAttrs::clustered() } else { ListenerAttrs::default() };
    ListenerDefinition::new(
        if clustered { "ClusteredWithEvicted" } else { "PlainWithEvicted" },
        attrs,
        vec![
            ListenerMethod::new("onCreated", &[EventKind::CacheEntryCreated], move |e| a.lock().unwrap().push(e.clone())),
            ListenerMethod::new("onEvicted", &[EventKind::CacheEntriesEvicted], move |e| b.lock().unwrap().push(e.clone())),
        ],
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_expectations_cache_is_bounded_and_every_node_holds_at_most_max_count() {
    let c = mockserver_cluster().await;
    for i in 0..400 {
        c.cache(0, EXPECTATIONS).put(&s(&format!("e{i}")), &s("expectation")).await.unwrap();
        c.cache(1, SCENARIO_STATES).put(&s(&format!("s{i}")), &s("state")).await.unwrap();
        c.cache(2, BLOBS).put(&s(&format!("b{i}")), &s("blob")).await.unwrap();
    }
    for node in 0..3 {
        let held = c.cache(node, EXPECTATIONS).engine().container.len();
        assert!(held as i64 <= MAX, "node {node} holds {held} expectations");
        assert!(held as i64 >= MAX - 5, "node {node} holds {held} expectations");
        assert_eq!(400, c.cache(node, SCENARIO_STATES).engine().container.len());
        assert_eq!(400, c.cache(node, BLOBS).engine().container.len());
        assert!(!c.cache(node, SCENARIO_STATES).engine().container.is_bounded());
        assert_eq!(Some(MAX as u64), c.cache(node, EXPECTATIONS).engine().container.capacity());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plain_listener_gets_the_evicted_event_and_a_cluster_listener_never_does() {
    let c = mockserver_cluster().await;
    let cache0 = c.cache(0, EXPECTATIONS);
    let (plain_seen, cluster_seen) = (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new())));
    cache0.add_cache_listener(&listener(false, &plain_seen)).unwrap();
    cache0.add_cache_listener(&listener(true, &cluster_seen)).unwrap();

    for i in 0..200 {
        cache0.put(&s(&format!("e{i}")), &s("expectation")).await.unwrap();
    }

    let held = cache0.engine().container.len();
    let plain = plain_seen.lock().unwrap().clone();
    let evicted_events: Vec<&Event> = plain.iter().filter(|e| e.kind == EventKind::CacheEntriesEvicted).collect();
    // a plain listener observes BOTH: every create arrives as a pre and a post event
    let created_plain = plain.iter().filter(|e| e.kind == EventKind::CacheEntryCreated && !e.pre).count();
    let created_pre = plain.iter().filter(|e| e.kind == EventKind::CacheEntryCreated && e.pre).count();
    assert_eq!(created_pre, 200);
    let evicted_keys: usize = evicted_events.iter().map(|e| e.entries.len()).sum();
    assert_eq!(created_plain, 200);
    assert!(!evicted_events.is_empty());
    assert_eq!(200, held + evicted_keys, "every entry is either held or was announced as evicted");
    for e in &evicted_events {
        assert!(!e.pre);
        for (k, v) in &e.entries {
            assert_eq!(*v, s("expectation"));
            assert!(cache0.engine().container.peek(k).is_none(), "an evicted entry is gone from the container");
        }
    }

    // the cluster listener saw every create, and nothing about eviction, which it cannot observe
    let cluster = cluster_seen.lock().unwrap().clone();
    assert_eq!(200, cluster.len());
    assert!(cluster.iter().all(|e| e.kind == EventKind::CacheEntryCreated && !e.pre));
    assert_eq!(1, cache0.engine().notifier().invocation_count(EventKind::CacheEntriesEvicted), "only the plain listener has an evicted handler registered");
}

/// Counts what travels: an eviction is local, so after N writes only N backup broadcasts were sent.
struct CountCommands {
    puts: AtomicUsize,
    other: AtomicUsize,
}

impl RpcInterceptor for CountCommands {
    fn intercept(&self, _from: &Address, _to: &Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
        match &command.payload {
            CacheRpcPayload::Single(WriteCommand::Put(_)) => self.puts.fetch_add(1, Ordering::SeqCst),
            _ => self.other.fetch_add(1, Ordering::SeqCst),
        };
        Box::pin(async { Intercepted::Deliver })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn eviction_sends_nothing_on_the_wire() {
    let c = mockserver_cluster().await;
    let counter = Arc::new(CountCommands { puts: AtomicUsize::new(0), other: AtomicUsize::new(0) });
    c.set_interceptor(Some(counter.clone()));
    // node 0 is the primary of some keys and a backup of the rest; count the puts it takes part in
    for i in 0..300 {
        c.cache(0, EXPECTATIONS).put(&s(&format!("wire{i}")), &s("v")).await.unwrap();
    }
    assert_eq!(0, counter.other.load(Ordering::SeqCst), "no remove, evict or invalidate command was sent");
    // each put is (at most) one forward to the primary and one broadcast of two backup copies
    let puts = counter.puts.load(Ordering::SeqCst);
    assert!(puts <= 300 * 3, "{puts} put requests for 300 writes");
    assert!(c.cache(0, EXPECTATIONS).engine().container.len() as i64 <= MAX);
}

/// A key that was evicted on one node is still held by nodes that did not evict it: eviction is per node.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_after_eviction_misses_locally_on_the_evicting_node() {
    let c = ReplCluster::start_with(2, &[EXPECTATIONS], |cfg| cfg.with_max_count(5)).await;
    let cache0 = c.cache(0, EXPECTATIONS);
    for i in 0..60 {
        cache0.put(&s(&format!("k{i}")), &s("v")).await.unwrap();
    }
    let held: Vec<bool> = (0..60).map(|i| cache0.engine().container.peek(&s(&format!("k{i}"))).is_some()).collect();
    assert_eq!(held.iter().filter(|h| **h).count(), cache0.engine().container.len());
    let evicted = (0..60).find(|i| !held[*i]).expect("something was evicted");
    // a REPL read is a local read: the evicted key is a miss
    assert_eq!(None, cache0.get(&s(&format!("k{evicted}"))).await.unwrap());
}

/// A joiner that receives more entries than its `maxCount` evicts while applying them, without notifications
/// for the transfer itself but with the eviction announced locally.
#[tokio::test(flavor = "multi_thread")]
async fn a_bounded_joiner_evicts_while_applying_state() {
    let c = ReplCluster::start(1, &[EXPECTATIONS]).await;
    for i in 0..120 {
        c.cache(0, EXPECTATIONS).put(&s(&format!("t{i}")), &s("v")).await.unwrap();
    }
    assert_eq!(120, c.cache(0, EXPECTATIONS).engine().container.len());
    let idx = c.add_node_and_join(&[EXPECTATIONS], |cfg| cfg.with_max_count(MAX)).await;
    c.nodes.wait_for_no_rebalance(&[0, idx], EXPECTATIONS, Duration::from_secs(30)).await.unwrap();
    let held = c.cache(idx, EXPECTATIONS).engine().container.len();
    assert!(held as i64 <= MAX && held > 0, "{held}");
    assert_eq!(120, c.cache(0, EXPECTATIONS).engine().container.len());
}
