//! The nine expiration cases of `AbstractClusterListenerTest` (14.0.35.Final) for a three-node REPL_SYNC cache.
//! Each node runs on a [`ControlledTimeService`] of its own (`injectTimeServices`), advanced by hand.
//!
//! An entry that a read finds expired by the reading node's clock reads as absent, and that node sends a
//! `RemoveExpiredCommand` to the primary owner without making the reader wait (`ClusterExpirationManager`);
//! the primary removes it when the value and lifespan it was told still match and the entry is expired within
//! 100 ms of the primary's own clock (`CallInterceptor.visitRemoveExpiredCommand`), replicates the removal, and
//! every node that applies it raises `@CacheEntryExpired` (`NotifyHelper.entryCommitted`). A listener on any
//! node therefore sees one expired event per expiration, even when only another node's clock moved.
//!
//! `testListenerOnBackupOwnerNodeBackupRead` listens on cache 0, not on the backup, exactly as upstream writes it.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::repl::*;
use infinispan::cache::encoder_cache::Cache;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::time_service::ControlledTimeService;
use infinispan::commands::object::CacheObject;
use infinispan::notify::cache_notifier::ListenerFilters;
use infinispan::notify::event::EventKind;
use infinispan::notify::filter::{CacheEventConverter, CacheEventFilter, KeyValueFilterAsCacheEventFilter};
use common::cluster_listener::*;

const CACHE_NAME: &str = "cluster-listener";
const FIRST_VALUE: &str = "first-value";

struct Fixture {
    c: ReplCluster,
    ts: [Arc<ControlledTimeService>; 3],
}

impl Fixture {
    /// `createCacheManagers` + `injectTimeServices`
    async fn new() -> Self {
        let c = ReplCluster::start(3, &[CACHE_NAME]).await;
        let ts = [0, 1, 2].map(|i| {
            let ts = Arc::new(ControlledTimeService::new());
            c.set_time_service(i, ts.clone());
            ts
        });
        Fixture { c, ts }
    }

    fn cache(&self, i: usize) -> Cache {
        self.c.cache(i, CACHE_NAME)
    }

    fn advance_time_services(&self, millis: i64) {
        for t in &self.ts {
            t.advance(millis);
        }
    }
}

async fn put_expiring(cache: &Cache, key: &CacheObject, value: &str, lifespan_ms: u64) {
    cache.put_with_lifespan(key, &s(value), Duration::from_millis(lifespan_ms)).await.unwrap();
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `verifySimpleExpirationEvents`
async fn verify_simple_expiration_events(listener: &ClusterListener, expected_num_events: usize, key: &CacheObject, expected_value: &CacheObject) {
    eventually("the expiration events", || listener.len() >= expected_num_events).await;
    let event = listener.events()[expected_num_events - 1].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(*key, event.key);
    assert_eq!(*expected_value, event.value);
}

/// `key + "-expiring"`
fn expiring(key: &CacheObject) -> String {
    format!("{}-expiring", key.as_str().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_listener_on_primary_node_read_primary() {
    let f = Fixture::new().await;
    let cache0 = f.cache(0);
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    let expected = expiring(&key);
    put_expiring(&cache0, &key, &expected, 10).await;

    let cl = listen(&cache0);
    f.ts[0].advance(11);

    assert_eq!(None, cache0.get(&key).await.unwrap());

    eventually("one expiration", || cl.len() >= 1).await;
    assert_eq!(1, cl.len());
    let event = cl.events()[0].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(key, event.key);
    assert_eq!(s(&expected), event.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_listener_on_primary_node_read_backup() {
    let f = Fixture::new().await;
    let (cache0, cache1) = (f.cache(0), f.cache(1));
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    let expected = expiring(&key);
    put_expiring(&cache0, &key, &expected, 10).await;

    let cl = listen(&cache0);
    f.ts[1].advance(11);

    assert_eq!(None, cache1.get(&key).await.unwrap());

    eventually("one expiration", || cl.len() >= 1).await;
    assert_eq!(1, cl.len());
    let event = cl.events()[0].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(key, event.key);
    assert_eq!(s(&expected), event.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_listener_on_backup_owner_node_primary_read() {
    let f = Fixture::new().await;
    let (cache0, cache1) = (f.cache(0), f.cache(1));
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    let expected = expiring(&key);
    put_expiring(&cache0, &key, &expected, 10).await;

    let cl = listen(&cache1);
    f.advance_time_services(11);

    assert_eq!(None, cache0.get(&key).await.unwrap());

    eventually("one expiration", || cl.len() >= 1).await;
    assert_eq!(1, cl.len());
    let event = cl.events()[0].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(key, event.key);
    assert_eq!(s(&expected), event.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_listener_on_backup_owner_node_backup_read() {
    let f = Fixture::new().await;
    let (cache0, cache1) = (f.cache(0), f.cache(1));
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    let expected = expiring(&key);
    put_expiring(&cache0, &key, &expected, 10).await;

    // upstream adds this one to cache0, not to the backup
    let cl = listen(&cache0);
    f.ts[1].advance(11);

    assert_eq!(None, cache1.get(&key).await.unwrap());

    eventually("one expiration", || cl.len() >= 1).await;
    assert_eq!(1, cl.len());
    let event = cl.events()[0].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(key, event.key);
    assert_eq!(s(&expected), event.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all_expire() {
    let f = Fixture::new().await;
    let cache0 = f.cache(0);
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    let expected = expiring(&key);
    put_expiring(&cache0, &key, &expected, 1000).await;

    let cl = listen(&cache0);
    f.advance_time_services(1001);

    assert_eq!(None, cache0.get(&key).await.unwrap());
    assert_eq!(None, f.cache(1).get(&key).await.unwrap());
    assert_eq!(None, f.cache(2).get(&key).await.unwrap());

    eventually("an expiration", || cl.len() >= 1).await;
    // We can't assert size here, this is because expiration is done asynchronously. As such you could have more
    // than 1 expire command come at the same time, although from different nodes. Currently we assume a null is
    // okay to say it was expired, so you can get multiple expirations
    let event = cl.events()[0].clone();
    assert_eq!(EventKind::CacheEntryExpired, event.get_type());
    assert_eq!(key, event.key);
    assert_eq!(s(&expected), event.value);
}

async fn test_expiration_filter(f: &Fixture, key_to_filter_out: &str, key_to_filter_out_lifespan: u64, key_to_use: &CacheObject, key_to_use_lifespan: u64, filter: impl CacheEventFilter + 'static) {
    let cache0 = f.cache(0);

    // put from a non-owner
    put_expiring(&cache0, &s(key_to_filter_out), FIRST_VALUE, key_to_filter_out_lifespan).await;

    let cl = listen_with(&cache0, ListenerFilters { filter: Some(Arc::new(filter)), ..Default::default() });

    f.advance_time_services(key_to_filter_out_lifespan as i64 + 1);

    assert_eq!(None, cache0.get(&s(key_to_filter_out)).await.unwrap());
    // We should not have gotten the message since it was filtered
    assert_eq!(cl.len(), 0);

    let expected_value = expiring(key_to_use);
    put_expiring(&cache0, key_to_use, &expected_value, key_to_use_lifespan).await;

    f.advance_time_services(key_to_use_lifespan as i64 + 1);

    assert_eq!(None, cache0.get(key_to_use).await.unwrap());

    verify_simple_expiration_events(&cl, 2, key_to_use, &s(&expected_value)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_expiration_filter_not_owner() {
    let f = Fixture::new().await;
    let key = key_with_primary(&f.c, CACHE_NAME, 1);
    let common_lifespan = 1000;
    test_expiration_filter(&f, "filter-me", common_lifespan, &key, common_lifespan, KeyEquals(key.clone())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_expiration_metadata_filter_not_owner() {
    let f = Fixture::new().await;
    let key = key_with_primary(&f.c, CACHE_NAME, 1);
    test_expiration_filter(&f, "filter-me", 50, &key, 1000, KeyValueFilterAsCacheEventFilter(LifespanFilter(100))).await;
}

async fn test_expiration_converter(f: &Fixture, key: &CacheObject, value: &str, expected_value: CacheObject, lifespan: u64, converter: impl CacheEventConverter + 'static) {
    let cache0 = f.cache(0);
    put_expiring(&cache0, key, value, lifespan).await;

    let cl = listen_with(&cache0, ListenerFilters { converter: Some(Arc::new(converter)), ..Default::default() });

    f.advance_time_services(lifespan as i64 + 1);

    assert_eq!(None, cache0.get(key).await.unwrap());

    // clusterListener.hasIncludeState() ? 2 : 1
    verify_simple_expiration_events(&cl, 1, key, &expected_value).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_expiration_converter_not_owner() {
    let f = Fixture::new().await;
    let key = key_with_primary(&f.c, CACHE_NAME, 1);
    test_expiration_converter(&f, &key, FIRST_VALUE, s(&FIRST_VALUE[0..2]), 1000, StringTruncator { beginning: 0, length: 2 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_expiration_converter_success_not_owner() {
    let f = Fixture::new().await;
    let key = key_with_primary(&f.c, CACHE_NAME, 1);
    let lifespan = 25000;
    test_expiration_converter(&f, &key, FIRST_VALUE, long(lifespan as i64), lifespan, LifespanConverter { return_original_value_or_null: true, lifespan_threshold: 500 }).await;
}

// --- the mechanics the tests above rely on ---------------------------------------------------

/// The primary refuses an expiration when the entry was rewritten meanwhile: another lifespan, or another value.
#[tokio::test(flavor = "multi_thread")]
async fn a_remove_expired_for_an_entry_that_changed_is_not_applied() {
    use infinispan::commands::write::{CommandInvocationId, RemoveExpired, WriteCommand, flags};
    let f = Fixture::new().await;
    let cache0 = f.cache(0);
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    put_expiring(&cache0, &key, "current", 1000).await;
    let cl = listen(&cache0);
    let expire = |value: &str, lifespan: i64| {
        WriteCommand::RemoveExpired(RemoveExpired {
            key: key.clone(),
            value: s(value),
            lifespan: Some(lifespan),
            max_idle: false,
            segment: cache0.engine().segment_of(&key).unwrap(),
            flags: flags::ZERO_LOCK_ACQUISITION_TIMEOUT | flags::SKIP_CACHE_LOAD,
            invocation_id: CommandInvocationId::generate(cache0.address()),
            topology_id: -1,
        })
    };
    f.advance_time_services(5000);
    // the value is not the one the expiring node saw
    let o = cache0.engine().execute(expire("stale", 1000)).await.unwrap();
    assert!(!o.successful);
    // the lifespan is not the one the expiring node saw
    let o = cache0.engine().execute(expire("current", 777)).await.unwrap();
    assert!(!o.successful);
    assert_eq!(cl.len(), 0);
    assert!(cache0.engine().container.peek_including_expired(&key).is_some(), "the entry is still stored (expired, not removed)");
    // the entry the expiring node saw: removed on every node, one expired event on each listener
    let o = cache0.engine().execute(expire("current", 1000)).await.unwrap();
    assert!(o.successful);
    assert_eq!(cl.len(), 1);
    for i in 0..3 {
        assert!(f.cache(i).engine().container.peek_including_expired(&key).is_none(), "node {i}");
    }
}

/// The primary accepts an expiration up to 100 ms before its own clock says so (`CLOCK_BUFFER`), not more.
#[tokio::test(flavor = "multi_thread")]
async fn the_primary_accepts_a_peers_expiration_within_the_clock_buffer_only() {
    use infinispan::commands::write::{CommandInvocationId, RemoveExpired, WriteCommand};
    let f = Fixture::new().await;
    let cache0 = f.cache(0);
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    put_expiring(&cache0, &key, "v", 1000).await;
    let expire = || {
        WriteCommand::RemoveExpired(RemoveExpired {
            key: key.clone(),
            value: s("v"),
            lifespan: Some(1000),
            max_idle: false,
            segment: cache0.engine().segment_of(&key).unwrap(),
            flags: 0,
            invocation_id: CommandInvocationId::generate(cache0.address()),
            topology_id: -1,
        })
    };
    f.ts[0].advance(800); // 200 ms short of expiring: more than the buffer
    assert!(!cache0.engine().execute(expire()).await.unwrap().successful);
    f.ts[0].advance(101); // 99 ms short: within the buffer
    assert!(cache0.engine().execute(expire()).await.unwrap().successful);
}

/// An expiration of an entry that is already gone succeeds without raising anything.
#[tokio::test(flavor = "multi_thread")]
async fn expiring_a_missing_entry_raises_nothing() {
    let f = Fixture::new().await;
    let cache0 = f.cache(0);
    let key = key_with_primary(&f.c, CACHE_NAME, 0);
    put_expiring(&cache0, &key, "v", 1000).await;
    let cl = listen(&cache0);
    f.advance_time_services(1001);
    assert_eq!(None, cache0.get(&key).await.unwrap());
    eventually("the first expiration", || cl.len() == 1).await;
    assert_eq!(None, f.cache(1).get(&key).await.unwrap());
    assert_eq!(None, f.cache(2).get(&key).await.unwrap());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(1, cl.len());
}
