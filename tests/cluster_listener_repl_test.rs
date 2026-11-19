//! Port of `notifications/cachelistener/cluster/ClusterListenerReplTest` and the
//! `AbstractClusterListenerTest` / `AbstractClusterListenerNonTxTest` it inherits (14.0.35.Final), for a
//! non-transactional REPL_SYNC cache of three nodes.
//!
//! In a replicated cache with capacity a `@Listener(clustered = true)` listener is a local listener on the node
//! it is added to (`CacheNotifierImpl.java:1278-1282`): nothing is installed on the other nodes, it fires for
//! the writes of its own node and for the backup writes the primary sends it alike, and `clear()` reaches it
//! as one removal per entry. Upstream's `MagicKey(cacheA, cacheB)` (primary `cacheA`, backup `cacheB`) is
//! [`key_with_primary`]; in REPL every other node is a backup.
//!
//! Coverage (41 upstream tests; 29 in this file with the rest in the other files named):
//! - the 29 create/filter/converter/lifecycle/previous-value tests below;
//! - the 3 retry tests (`testPrimaryOwnerGoesDown*`) in `cluster_listener_retry_test.rs`;
//! - the 9 expiration tests in `cluster_listener_expiration_test.rs`.
//!
//! The listener and the filters/converters of `AbstractClusterListenerUtilTest` are in `common/cluster_listener.rs`.
//!
//! `ClusterListenerWithIncludeCurrentState` and the transactional, DIST and async variants are out of scope.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::repl::*;
use common::cluster_listener::*;
use infinispan::cache::encoder_cache::Cache;
use infinispan::cache::testing::ReplCluster;
use infinispan::commands::object::CacheObject;
use infinispan::notify::cache_notifier::ListenerFilters;
use infinispan::notify::event::EventKind;
use infinispan::notify::filter::{CacheEventConverter, CacheEventFilter, KeyValueFilterAsCacheEventFilter};
use infinispan::notify::listener_registration::{ListenerAttrs, ListenerDefinition};

pub const CACHE_NAME: &str = "cluster-listener";
pub const FIRST_VALUE: &str = "first-value";
pub const SECOND_VALUE: &str = "second-value";
const WAIT: Duration = Duration::from_secs(20);

// --- helpers of AbstractClusterListenerUtilTest ----------------------------------------------------------

async fn put(cache: &Cache, key: &CacheObject, value: &str, lifespan: Option<i64>) {
    match lifespan {
        Some(ms) => cache.put_with_lifespan(key, &s(value), Duration::from_millis(ms as u64)).await.unwrap(),
        None => cache.put(key, &s(value)).await.unwrap(),
    };
}

async fn verify_simple_insertion(cache: &Cache, key: &CacheObject, value: &str, lifespan: Option<i64>, listener: &ClusterListener, expected: CacheObject) {
    put(cache, key, value, lifespan).await;
    verify_simple_insertion_events(listener, key, &expected);
}

async fn verify_simple_modification(cache: &Cache, key: &CacheObject, value: &str, lifespan: Option<i64>, listener: &ClusterListener, expected: CacheObject) {
    put(cache, key, value, lifespan).await;
    verify_simple_modification_events(listener, key, &expected);
}

fn verify_simple_insertion_events(listener: &ClusterListener, key: &CacheObject, expected: &CacheObject) {
    let events = listener.events();
    assert_eq!(1, events.len(), "{events:?}");
    assert_eq!(EventKind::CacheEntryCreated, events[0].get_type());
    assert_eq!(*key, events[0].key);
    assert_eq!(*expected, events[0].value);
}

fn verify_simple_modification_events(listener: &ClusterListener, key: &CacheObject, expected: &CacheObject) {
    let events = listener.events();
    assert_eq!(1, events.len(), "{events:?}");
    assert_eq!(EventKind::CacheEntryModified, events[0].get_type());
    assert_eq!(*key, events[0].key);
    assert_eq!(*expected, events[0].value);
}

async fn cluster() -> ReplCluster {
    ReplCluster::start(3, &[CACHE_NAME]).await
}

// --- AbstractClusterListenerTest -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_create_from_non_owner_with_listener_not_owner() {
    let c = cluster().await;
    let cl = listen(&c.cache(0, CACHE_NAME));
    let key = key_with_primary(&c, CACHE_NAME, 1);
    verify_simple_insertion(&c.cache(2, CACHE_NAME), &key, FIRST_VALUE, None, &cl, s(FIRST_VALUE)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_from_non_owner_with_listener_as_backup_owner() {
    let c = cluster().await;
    let cl = listen(&c.cache(0, CACHE_NAME));
    let key = key_with_primary(&c, CACHE_NAME, 1);
    verify_simple_insertion(&c.cache(2, CACHE_NAME), &key, FIRST_VALUE, None, &cl, s(FIRST_VALUE)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_local_node_owner_and_cluster_listener() {
    let c = cluster().await;
    let cl = listen(&c.cache(0, CACHE_NAME));
    let key = key_with_primary(&c, CACHE_NAME, 0);
    verify_simple_insertion(&c.cache(0, CACHE_NAME), &key, FIRST_VALUE, None, &cl, s(FIRST_VALUE)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_local_node_non_owner_and_cluster_listener() {
    let c = cluster().await;
    let cl = listen(&c.cache(0, CACHE_NAME));
    let key = key_with_primary(&c, CACHE_NAME, 1);
    verify_simple_insertion(&c.cache(0, CACHE_NAME), &key, FIRST_VALUE, None, &cl, s(FIRST_VALUE)).await;
}

async fn test_filter(c: &ReplCluster, key_to_filter_out: &str, key_to_use: &CacheObject, lifespan: Option<i64>, filter: impl CacheEventFilter + 'static) {
    let cache0 = c.cache(0, CACHE_NAME);
    let cl = listen_with(&cache0, filtered(filter));
    put(&cache0, &s(key_to_filter_out), FIRST_VALUE, None).await;
    // We should not have gotten the message since it was filtered
    assert_eq!(cl.len(), 0);
    verify_simple_insertion(&cache0, key_to_use, FIRST_VALUE, lifespan, &cl, s(FIRST_VALUE)).await;
}

async fn test_simple_filter(c: &ReplCluster, key: CacheObject) {
    test_filter(c, "filter-me", &key.clone(), None, KeyEquals(key)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_filter_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    test_simple_filter(&c, key).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_filter_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    test_simple_filter(&c, key).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_filter_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    test_filter(&c, "filter-me", &key, Some(1000), KeyValueFilterAsCacheEventFilter(LifespanFilter(100))).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_filter_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    test_filter(&c, "filter-me", &key, Some(1000), KeyValueFilterAsCacheEventFilter(LifespanFilter(100))).await;
}

async fn test_converter(c: &ReplCluster, key: &CacheObject, value: &str, resulting_value: CacheObject, lifespan: Option<i64>, converter: impl CacheEventConverter + 'static) {
    let cache0 = c.cache(0, CACHE_NAME);
    let cl = listen_with(&cache0, converted(converter));
    verify_simple_insertion(&cache0, key, value, lifespan, &cl, resulting_value).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_converter_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    test_converter(&c, &key, FIRST_VALUE, s(&FIRST_VALUE[0..2]), None, StringTruncator { beginning: 0, length: 2 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_converter_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    test_converter(&c, &key, FIRST_VALUE, s(&FIRST_VALUE[0..2]), None, StringTruncator { beginning: 0, length: 2 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_success_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    let lifespan = 25000;
    test_converter(&c, &key, FIRST_VALUE, long(lifespan), Some(lifespan), LifespanConverter { return_original_value_or_null: true, lifespan_threshold: 500 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_success_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    let lifespan = 25000;
    test_converter(&c, &key, FIRST_VALUE, long(lifespan), Some(lifespan), LifespanConverter { return_original_value_or_null: true, lifespan_threshold: 500 }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_no_pass_return_original_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    test_converter(&c, &key, FIRST_VALUE, s(FIRST_VALUE), Some(25000), LifespanConverter { return_original_value_or_null: true, lifespan_threshold: i64::MAX }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_no_pass_return_original_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    test_converter(&c, &key, FIRST_VALUE, s(FIRST_VALUE), Some(25000), LifespanConverter { return_original_value_or_null: true, lifespan_threshold: i64::MAX }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_no_pass_return_null_not_owner() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 1);
    test_converter(&c, &key, FIRST_VALUE, CacheObject::Null, Some(25000), LifespanConverter { return_original_value_or_null: false, lifespan_threshold: i64::MAX }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_converter_no_pass_return_null_local_only() {
    let c = cluster().await;
    let key = key_with_primary(&c, CACHE_NAME, 0);
    test_converter(&c, &key, FIRST_VALUE, CacheObject::Null, Some(25000), LifespanConverter { return_original_value_or_null: false, lifespan_threshold: i64::MAX }).await;
}

/// Upstream kills node 0 (the listener's) and, in REPL, nothing was installed on the others, so their
/// listener counts do not change (`cacheMode.isDistributed() ? 1 : 0`).
#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_listener_node_goes_down() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(1, CACHE_NAME), c.cache(2, CACHE_NAME));
    let _cl = listen(&c.cache(0, CACHE_NAME));

    let cache1_listener_size = cache1.cache_listeners().len();
    let cache2_listener_size = cache2.cache_listeners().len();

    c.kill_node(0);
    c.nodes.wait_for_no_rebalance(&[1, 2], CACHE_NAME, WAIT).await.unwrap();

    assert_eq!(cache1.cache_listeners().len(), cache1_listener_size);
    assert_eq!(cache2.cache_listeners().len(), cache2_listener_size);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_node_comes_up_with_cluster_listener_already_installed() {
    let c = cluster().await;
    let cl = listen(&c.cache(0, CACHE_NAME));

    let idx = c.add_node_and_join(&[CACHE_NAME], |c| c).await;
    c.nodes.wait_for_no_rebalance(&[0, 1, 2, idx], CACHE_NAME, WAIT).await.unwrap();

    // state transfer into the joiner raised nothing
    assert_eq!(cl.len(), 0);
    let key = key_with_primary(&c, CACHE_NAME, idx);
    verify_simple_insertion(&c.cache(idx, CACHE_NAME), &key, FIRST_VALUE, None, &cl, s(FIRST_VALUE)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_node_comes_up_with_cluster_listener_already_installed_filter_and_converter() {
    let c = cluster().await;
    let key_to_filter = s("filter-me");
    let cl = listen_with(
        &c.cache(0, CACHE_NAME),
        ListenerFilters { filter: Some(Arc::new(KeyEquals(key_to_filter.clone()))), converter: Some(Arc::new(StringTruncator { beginning: 0, length: 3 })), ..Default::default() },
    );

    let idx = c.add_node_and_join(&[CACHE_NAME], |c| c).await;
    c.nodes.wait_for_no_rebalance(&[0, 1, 2, idx], CACHE_NAME, WAIT).await.unwrap();

    let cache3 = c.cache(idx, CACHE_NAME);
    let key = key_with_primary(&c, CACHE_NAME, idx);
    cache3.put(&key, &s(FIRST_VALUE)).await.unwrap();
    // Should be filtered
    assert_eq!(cl.len(), 0);
    verify_simple_insertion(&cache3, &key_to_filter, FIRST_VALUE, None, &cl, s(&FIRST_VALUE[0..3])).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_cluster_listener_removed() {
    let c = cluster().await;
    let (cache0, cache1, cache2) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME), c.cache(2, CACHE_NAME));
    let initial0 = cache0.cache_listeners().len();
    let initial1 = cache1.cache_listeners().len();
    let initial2 = cache2.cache_listeners().len();

    let cl = ClusterListener::new();
    let def = cl.definition();
    cache0.add_cache_listener(&def).unwrap();

    // Adding a cluster listener adds it to this node only: nothing is installed remotely in a REPL cache
    assert_eq!(cache0.cache_listeners().len(), initial0 + 1);
    assert_eq!(cache1.cache_listeners().len(), initial1);
    assert_eq!(cache2.cache_listeners().len(), initial2);

    cache0.remove_cache_listener(&def).unwrap();

    assert_eq!(cache0.cache_listeners().len(), initial0);
    assert_eq!(cache1.cache_listeners().len(), initial1);
    assert_eq!(cache2.cache_listeners().len(), initial2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_listener_removed_with_multiple_installed_on_same_node() {
    let c = cluster().await;
    let (cache0, cache1, cache2) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME), c.cache(2, CACHE_NAME));
    let initial0 = cache0.cache_listeners().len();
    let initial1 = cache1.cache_listeners().len();
    let initial2 = cache2.cache_listeners().len();

    let cluster_listener = ClusterListener::new();
    let def1 = cluster_listener.definition();
    cache0.add_cache_listener(&def1).unwrap();
    assert_eq!(cache0.cache_listeners().len(), initial0 + 1);
    assert_eq!(cache1.cache_listeners().len(), initial1);
    assert_eq!(cache2.cache_listeners().len(), initial2);

    let cluster_listener2 = ClusterListener::new();
    cache0.add_cache_listener(&cluster_listener2.definition()).unwrap();
    // Adding a second cluster listener adds one more on this node
    assert_eq!(cache0.cache_listeners().len(), initial0 + 2);
    assert_eq!(cache1.cache_listeners().len(), initial1);
    assert_eq!(cache2.cache_listeners().len(), initial2);

    let key = key_with_primary(&c, CACHE_NAME, 2);
    cache1.put(&key, &s(FIRST_VALUE)).await.unwrap();

    // Both listeners should have been notified
    assert_eq!(cluster_listener.len(), 1);
    assert_eq!(cluster_listener2.len(), 1);
    verify_simple_insertion_events(&cluster_listener, &key, &s(FIRST_VALUE));
    verify_simple_insertion_events(&cluster_listener2, &key, &s(FIRST_VALUE));

    cache0.remove_cache_listener(&def1).unwrap();
    assert_eq!(cache0.cache_listeners().len(), initial0 + 1);
    assert_eq!(cache1.cache_listeners().len(), initial1);
    assert_eq!(cache2.cache_listeners().len(), initial2);

    // Change the value again to make sure other listener is still working properly
    cache2.put(&key, &s(SECOND_VALUE)).await.unwrap();

    let events = cluster_listener2.events();
    assert_eq!(events.len(), 2);
    assert_eq!(EventKind::CacheEntryModified, events[1].get_type());
    assert_eq!(key, events[1].key);
    assert_eq!(s(SECOND_VALUE), events[1].value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_member_leaves_that_cluster_listener_not_notified() {
    let c = cluster().await;
    let (cache0, cache1) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME));
    let key = key_with_primary(&c, CACHE_NAME, 1);
    cache1.put(&key, &s("some-key")).await.unwrap();

    let cl = listen(&cache0);

    c.kill_node(1);
    c.nodes.wait_for_no_rebalance(&[0, 2], CACHE_NAME, WAIT).await.unwrap();

    // hasIncludeState() ? 1 : 0
    assert_eq!(0, cl.len());
}

async fn previous_value_converter(listener_node: usize, key_primary: usize) {
    let c = cluster().await;
    let (cache0, listener_cache) = (c.cache(0, CACHE_NAME), c.cache(listener_node, CACHE_NAME));
    let previous_value = "myOldValue";
    let previous_expiration = 10000000;
    let key = key_with_primary(&c, CACHE_NAME, key_primary);

    put(&cache0, &key, previous_value, Some(previous_expiration)).await;

    let cl = listen_with(&listener_cache, converted(StringAppender));

    let new_value = "myBrandSpankingNewValue";
    let new_expiration = 314159;
    verify_simple_modification(&cache0, &key, new_value, Some(new_expiration), &cl, s(&format!("{previous_value}{previous_expiration}{new_value}{new_expiration}"))).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_converter_event_raised_local_node() {
    previous_value_converter(0, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_converter_event_raised_non_owner_node() {
    previous_value_converter(2, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_converter_event_raised_backup_owner_node() {
    previous_value_converter(1, 0).await;
}

async fn previous_value_filter(listener_node: usize) {
    let c = cluster().await;
    let (cache0, listener_cache) = (c.cache(0, CACHE_NAME), c.cache(listener_node, CACHE_NAME));
    let previous_value = "myOldValue";
    let previous_expiration = 10000000;
    let key = key_with_primary(&c, CACHE_NAME, 0);

    put(&cache0, &key, previous_value, Some(previous_expiration)).await;

    let cl = listen_with(&listener_cache, filtered(NewLifespanLargerFilter));

    // This event is ignored because lifespan is shorter
    put(&cache0, &key, previous_value, Some(previous_expiration - 100)).await;

    let new_value = "myBrandSpankingNewValue";
    let new_expiration = 314159265;
    verify_simple_modification(&cache0, &key, new_value, Some(new_expiration), &cl, s(new_value)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_filter_event_raised_local_node() {
    previous_value_filter(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_filter_event_raised_non_owner_node() {
    previous_value_filter(2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_previous_value_filter_event_raised_backup_owner_node() {
    previous_value_filter(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cache_event_filter_converter() {
    let c = cluster().await;
    let cache0 = c.cache(0, CACHE_NAME);
    let converted_value = "my-value";
    let fc = Arc::new(FilterConverter { throw_exception_on_non_filter_and_converter_methods: true, converted_value: converted_value.into() });
    let cl = listen_with(&cache0, ListenerFilters { filter_and_convert: Some(fc), ..Default::default() });

    let key = key_with_primary(&c, CACHE_NAME, 0);
    verify_simple_insertion(&cache0, &key, "doesn't-matter", None, &cl, s(converted_value)).await;
}

// --- the semantics the tests above only imply ------------------------------------------------

/// A listener fires for the writes of its own node and for the backup writes of the others, exactly once each,
/// and never remotely registered: `isOriginLocal` tells the two apart. (Upstream's comment in MockServer
/// claims "remote writes only"; `CacheNotifierImpl.java:1278-1282` makes it a local listener.)
#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_listener_fires_for_local_writes_and_incoming_backup_writes() {
    let c = cluster().await;
    let (cache0, cache1) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME));
    let cl = listen(&cache0);

    let own = key_with_primary(&c, CACHE_NAME, 0);
    cache0.put(&own, &s(FIRST_VALUE)).await.unwrap();
    let events = cl.events();
    assert_eq!(events.len(), 1, "a write issued here: {events:?}");
    assert!(events[0].origin_local);

    let remote = s("written-on-node-1");
    cache1.put(&remote, &s(FIRST_VALUE)).await.unwrap();
    let events = cl.events();
    assert_eq!(events.len(), 2, "a write issued on another node arrives as a backup write: {events:?}");
    assert_eq!(events[1].key, remote);
    assert!(!events[1].origin_local);
    // post events only: a cluster listener is forced to POST observation
    assert!(events.iter().all(|e| !e.pre));
}

/// `clear()` reaches a listener as one post removal per entry on each node, not a "cleared" event
/// (`CallInterceptor.java:481-491`, `ClusteringDependentLogic.java:244-259`).
#[tokio::test(flavor = "multi_thread")]
async fn clear_fires_one_removal_per_entry_on_each_node() {
    let c = cluster().await;
    let (cache0, cache1, cache2) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME), c.cache(2, CACHE_NAME));
    for i in 0..5 {
        cache0.put(&s(&format!("k{i}")), &s(&format!("v{i}"))).await.unwrap();
    }
    let listeners: Vec<Arc<ClusterListener>> = [&cache0, &cache1, &cache2].iter().map(|cache| listen(cache)).collect();

    cache1.clear().await.unwrap();

    for (i, cl) in listeners.iter().enumerate() {
        let events = cl.events();
        assert_eq!(events.len(), 5, "node {i}: {events:?}");
        assert!(events.iter().all(|e| e.kind == EventKind::CacheEntryRemoved && !e.pre));
        let mut keys: Vec<String> = events.iter().map(|e| e.key.as_str().unwrap().to_string()).collect();
        keys.sort();
        assert_eq!(keys, ["k0", "k1", "k2", "k3", "k4"]);
        for e in &events {
            // a post removal carries no value but the old one
            assert_eq!(e.value, CacheObject::Null);
            let n = &e.key.as_str().unwrap()[1..];
            assert_eq!(e.old_value, s(&format!("v{n}")));
        }
        // origin: the node that cleared fires local events, the others remote
        assert!(events.iter().all(|e| e.origin_local == (i == 1)), "node {i}");
    }
    assert!(cache0.engine().container.is_empty());
}

/// Removal and put-if-absent of a missing key raise events as upstream does: remove of a missing key raises none.
#[tokio::test(flavor = "multi_thread")]
async fn remove_of_a_missing_key_raises_nothing_and_remove_raises_a_post_removal() {
    let c = cluster().await;
    let cache0 = c.cache(0, CACHE_NAME);
    let cl = listen(&cache0);
    cache0.remove(&s("absent")).await.unwrap();
    assert_eq!(cl.len(), 0);
    cache0.put(&s("k"), &s("v")).await.unwrap();
    cache0.remove(&s("k")).await.unwrap();
    let events = cl.events();
    assert_eq!(events.iter().map(|e| e.kind).collect::<Vec<_>>(), [EventKind::CacheEntryCreated, EventKind::CacheEntryRemoved]);
    assert_eq!(events[1].old_value, s("v"));
    assert_eq!(events[1].value, CacheObject::Null);
}

/// The same listener class registered with `includeCurrentState` is not ported, as the notifier documents.
#[tokio::test(flavor = "multi_thread")]
async fn include_current_state_is_refused_not_silently_ignored() {
    let c = cluster().await;
    let cache0 = c.cache(0, CACHE_NAME);
    let def = ListenerDefinition::new("WithState", ListenerAttrs { include_current_state: true, ..ListenerAttrs::clustered() }, vec![]);
    assert!(matches!(cache0.add_cache_listener(&def), Err(infinispan::notify::listener_registration::NotifierError::NotPorted(_))));
}

/// A listener belongs to the cache it was added to: MockServer registers one on `expectations`, `scenarioStates`
/// and the `crud-<ns>` caches and none on `blobs`, whose writes then raise nothing anywhere.
#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_listener_sees_only_its_own_cache() {
    let c = ReplCluster::start(2, &["expectations", "blobs"]).await;
    let on_expectations = listen(&c.cache(0, "expectations"));
    let blobs = c.cache(0, "blobs");
    assert!(blobs.cache_listeners().is_empty(), "no listener was ever registered on blobs");
    blobs.put(&s("b"), &s("blob")).await.unwrap();
    c.cache(1, "blobs").put(&s("b2"), &s("blob")).await.unwrap();
    assert_eq!(0, on_expectations.len());
    c.cache(1, "expectations").put(&s("e"), &s("expectation")).await.unwrap();
    assert_eq!(1, on_expectations.len());
}
