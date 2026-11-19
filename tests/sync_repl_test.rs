//! Port of `replication/SyncReplTest` (REPL_SYNC, two nodes). `ReplicatedControlledConsistentHashFactory(0)`
//! with one segment makes `cache(0)` the primary of every key; here a key with that primary is picked.
//! `testMixingSyncAndAsyncOnSameTransport` is not ported: its second half needs a `REPL_ASYNC` cache
//! (`sendToAll`), and MockServer only uses `REPL_SYNC`; its first half (a put is a synchronous
//! `invokeCommandOnAll`) is `sync_put_is_a_synchronous_broadcast`.

mod common;

use common::repl::*;
use infinispan::cache::testing::{Intercepted, ReplCluster, RpcInterceptor};
use infinispan::cache::write_pipeline::CacheConfig;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::commands::write::{WriteCommand, flags};
use infinispan::topology::support::BoxFuture;
use std::sync::{Arc, Mutex};

const CACHE: &str = "replSync";

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_operation() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let (k, v) = (key_with_primary(&c, CACHE, 0), vw("value", 1));

    assert_eq!(cache1.get(&k).await.unwrap(), None);
    assert_eq!(cache2.get(&k).await.unwrap(), None);

    cache1.put(&k, &v).await.unwrap();
    assert_eq!(cache1.get(&k).await.unwrap(), Some(v.clone()));
    assert_eq!(cache2.get(&k).await.unwrap(), Some(v), "should have replicated");

    cache2.remove(&k).await.unwrap();
    assert!(cache1.is_empty());
    assert!(cache2.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_caches_on_shared_transport() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    assert!(cache1.is_empty() && cache2.is_empty());

    // a second cache on the same two nodes
    for i in 0..2 {
        c.start_cache_with(i, "newCache", CacheConfig::new("newCache")).await.unwrap();
    }
    c.nodes.wait_for_no_rebalance(&[0, 1], "newCache", std::time::Duration::from_secs(20)).await.unwrap();
    let (alt1, alt2) = (c.cache(0, "newCache"), c.cache(1, "newCache"));
    assert!(alt1.is_empty() && alt2.is_empty());

    let (k, v) = (s("k"), vw("v", 1));
    cache1.put(&k, &v).await.unwrap();
    assert_eq!(cache1.get(&k).await.unwrap(), Some(v.clone()));
    assert_eq!(cache2.get(&k).await.unwrap(), Some(v.clone()));
    assert!(alt1.is_empty() && alt2.is_empty());

    alt1.put(&k, &vw("value2", 1)).await.unwrap();
    assert_eq!(alt1.get(&k).await.unwrap(), Some(vw("value2", 1)));
    assert_eq!(alt2.get(&k).await.unwrap(), Some(vw("value2", 1)));
    assert_eq!(cache1.get(&k).await.unwrap(), Some(v.clone()));
    assert_eq!(cache2.get(&k).await.unwrap(), Some(v));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicate_to_non_existent_cache() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    // the second cache runs on node 0 only
    c.start_cache_with(0, "newCache2", CacheConfig::new("newCache2")).await.unwrap();
    c.nodes.wait_for_no_rebalance(&[0], "newCache2", std::time::Duration::from_secs(20)).await.unwrap();
    let alt1 = c.cache(0, "newCache2");
    assert!(alt1.is_empty());

    let (k, v) = (s("k"), vw("v", 1));
    cache1.put(&k, &v).await.unwrap();
    assert_eq!(cache1.get(&k).await.unwrap(), Some(v.clone()));
    assert_eq!(cache2.get(&k).await.unwrap(), Some(v.clone()));
    assert!(alt1.is_empty());

    alt1.put(&k, &vw("value2", 1)).await.unwrap();
    assert_eq!(alt1.get(&k).await.unwrap(), Some(vw("value2", 1)));
    assert_eq!(cache1.get(&k).await.unwrap(), Some(v.clone()));
    assert_eq!(cache2.get(&k).await.unwrap(), Some(v));
}

#[derive(Default)]
struct Seen(Mutex<Vec<i64>>);

impl RpcInterceptor for Seen {
    fn intercept(&self, _from: &infinispan::ch::address::Address, _to: &infinispan::ch::address::Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
        if let CacheRpcPayload::Single(w @ WriteCommand::Put(_)) = &command.payload {
            self.0.lock().unwrap().push(w.flags());
        }
        Box::pin(async { Intercepted::Deliver })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_put_is_a_synchronous_broadcast() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let seen = Arc::new(Seen::default());
    c.set_interceptor(Some(seen.clone()));
    let k = key_with_primary(&c, CACHE, 0);
    // the put only returns once the backup applied it (the answer of a sync `invokeCommandOnAll`)
    c.cache(0, CACHE).put(&k, &vw("v", 1)).await.unwrap();
    assert_eq!(c.cache(1, CACHE).engine().local_get(&k), Some(vw("v", 1)));
    let flags_sent = seen.0.lock().unwrap().clone();
    assert_eq!(flags_sent.len(), 1);
    assert_ne!(flags_sent[0] & flags::BACKUP_WRITE, 0);
    c.set_interceptor(None);
}
