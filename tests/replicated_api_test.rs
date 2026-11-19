//! Port of `replication/ReplicatedAPITest` (core 14.0.35.Final, REPL_SYNC, two nodes), over the in-memory
//! cluster. Adaptations:
//! - upstream's keys happen to have `cache(0)` as primary owner; here the key is chosen so it does
//!   (`common::repl::key_with_primary`), because `testPutIfAbsent`, `testRemoveIfPresent` and the
//!   replace tests assert the outcome of an operation executed on the primary;
//! - `remove` uses a key whose primary is `cache(1)`, the node that alone holds the value at first;
//! - `put()`'s `putAll` half is not ported: `PutMapCommand` is not a command MockServer issues.
//! - values are `VersionedWrapper<String>` user objects (what MockServer stores) instead of plain strings.

mod common;

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::commands::write::flags::CACHE_MODE_LOCAL;

const CACHE: &str = "replication";

async fn cluster() -> ReplCluster {
    ReplCluster::start(2, &[CACHE]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn put() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = s("key");
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), None);
    cache1.put(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn remove() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    // the first half needs the node that holds the value to be the primary: a remove of a key the
    // primary does not have is not replicated (`RemoveCommand.shouldReplicate`)
    let key = key_with_primary(&c, CACHE, 1);
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache1.get(&key).await.unwrap(), None);

    cache1.remove(&key).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), None);

    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value", 1)).await.unwrap();
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)));

    cache1.remove(&key).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_if_absent() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = key_with_primary(&c, CACHE, 0);
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("valueOld", 1)).await.unwrap();
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("valueOld", 1)));
    assert_eq!(cache1.get(&key).await.unwrap(), None);

    cache1.put_if_absent(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)));

    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value3", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value3", 1)));

    cache1.put_if_absent(&key, &vw("value4", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value3", 1)), "should not invalidate cache2");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_if_present() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = key_with_primary(&c, CACHE, 0);
    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value1", 1)).await.unwrap();
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value2", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    cache1.remove_if(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)), "should not remove");
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)), "should not remove");

    cache1.remove_if(&key, &vw("value1", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clear() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = s("key");
    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value1", 1)).await.unwrap();
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value2", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    cache1.clear().await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replace() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = key_with_primary(&c, CACHE, 0);
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value2", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    // should do nothing since there is nothing to replace on cache1 (the primary)
    cache1.replace(&key, &vw("value1", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("valueN", 1)).await.unwrap();
    cache1.replace(&key, &vw("value1", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value1", 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replace_with_old_val() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = key_with_primary(&c, CACHE, 0);
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value2", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    assert!(!cache1.replace_if(&key, &vw("valueOld", 1), &vw("value1", 1)).await.unwrap());
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("valueN", 1)).await.unwrap();
    assert!(!cache1.replace_if(&key, &vw("valueOld", 1), &vw("value1", 1)).await.unwrap());
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("valueN", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    assert!(cache1.replace_if(&key, &vw("valueN", 1), &vw("value1", 1)).await.unwrap());
    // the replace executed identically on both of them
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value1", 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_local_only_clear() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = s("key");
    cache1.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value1", 1)).await.unwrap();
    cache2.with_flags(CACHE_MODE_LOCAL).put(&key, &vw("value2", 1)).await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), Some(vw("value1", 1)));
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));

    cache1.with_flags(CACHE_MODE_LOCAL).clear().await.unwrap();
    assert_eq!(cache1.get(&key).await.unwrap(), None);
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value2", 1)));
}

/// `Cache.evict(key)` (`EvictCommand`) is node-local: the evicting node loses the entry, its peer keeps it, and a
/// key it never held is reported as such.
#[tokio::test(flavor = "multi_thread")]
async fn test_evict_is_node_local() {
    let c = cluster().await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let key = key_with_primary(&c, CACHE, 0);
    cache1.put(&key, &vw("value", 1)).await.unwrap();
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)), "replicated");

    assert!(cache1.evict(&key));
    assert_eq!(cache1.get(&key).await.unwrap(), None, "the evicting node lost it");
    assert_eq!(cache2.get(&key).await.unwrap(), Some(vw("value", 1)), "the peer keeps it");
    assert!(!cache1.evict(&key), "nothing left to evict");
}
