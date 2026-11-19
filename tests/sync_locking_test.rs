//! Port of the non-transactional parts of `replication/SyncLockingTest` (REPL_SYNC, two nodes).
//!
//! Upstream runs it pessimistic-transactional (`TransactionManager`, `AdvancedCache.lock(k)` inside a
//! tx); a non-tx cache locks one key at a time, on the primary owner, for the duration of one command
//! (`AbstractLockingInterceptor.visitNonTxDataWriteCommand`). What carries over, with `lock_key` standing
//! in for `AdvancedCache.lock`:
//! - `testLocksReleasedWithoutExplicitUnlock`: after a write no node holds a lock on the key;
//! - `testConcurrentNonTxLocking` (same node and other node): a write blocked by a lock held on the
//!   primary times out (`lockAcquisitionTimeout`), and succeeds once the lock is released;
//! - `testLocksReleasedWithNoMods`: a read leaves nothing locked;
//! - `testReplaceNonExistentKey`: `replace` of a missing key changes nothing and leaves nothing locked.
//!
//! `testConcurrentTxLocking` (and the tx halves above) need transactions and are not ported.

mod common;

use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::write_pipeline::{CacheConfig, CacheError};

const CACHE: &str = "testcache";

async fn cluster() -> ReplCluster {
    ReplCluster::start_with(2, &[CACHE], |mut c: CacheConfig| {
        c.lock_acquisition_timeout = Duration::from_millis(300);
        c
    })
    .await
}

fn assert_not_locked(c: &ReplCluster, key: &infinispan::commands::object::CacheObject) {
    for i in 0..2 {
        assert!(!c.cache(i, CACHE).engine().is_locked(key), "node {i} still holds a lock");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_locks_released_without_explicit_unlock() {
    let c = cluster().await;
    let k = s("key");
    for origin in 0..2 {
        c.cache(origin, CACHE).put(&k, &vw("Infinispan", 1)).await.unwrap();
        assert_not_locked(&c, &k);
        c.cache(origin, CACHE).remove(&k).await.unwrap();
        assert_not_locked(&c, &k);
    }
}

async fn concurrent_locking(same_node: bool) {
    let c = cluster().await;
    let k = key_with_primary(&c, CACHE, 0);
    assert_eq!(c.cache(0, CACHE).get(&k).await.unwrap(), None);
    // lock the key where it is contended: on its primary, node 0
    let guard = c.cache(0, CACHE).engine().lock_key(&k, Duration::from_secs(1)).await.unwrap();

    // a concurrent write, from the same node or from the other one, must time out
    let writer = c.cache(if same_node { 0 } else { 1 }, CACHE);
    let blocked = writer.put(&k, &vw("JBC", 1)).await;
    assert!(matches!(blocked, Err(CacheError::Timeout(ref m)) if m.contains("ISPN000299")), "{blocked:?}");

    // the lock holder writes, then lets go
    drop(guard);
    c.cache(0, CACHE).put(&k, &vw("Infinispan", 1)).await.unwrap();
    assert_not_locked(&c, &k);
    assert_eq!(c.cache(1, CACHE).get(&k).await.unwrap(), Some(vw("Infinispan", 1)));

    c.cache(1, CACHE).remove(&k).await.unwrap();
    assert!(c.cache(0, CACHE).is_empty());
    assert!(c.cache(1, CACHE).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_non_tx_locking() {
    concurrent_locking(false).await;
    concurrent_locking(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_locks_released_with_no_mods() {
    let c = cluster().await;
    let k = s("key");
    assert_eq!(c.cache(0, CACHE).get(&k).await.unwrap(), None);
    assert_eq!(c.cache(1, CACHE).get(&k).await.unwrap(), None);
    assert_not_locked(&c, &k);
    assert!(c.cache(0, CACHE).is_empty() && c.cache(1, CACHE).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replace_non_existent_key() {
    let c = cluster().await;
    let k = s("key");
    let cache1 = c.cache(0, CACHE);
    assert_eq!(cache1.replace(&k, &vw("blah", 1)).await.unwrap(), None);
    assert_eq!(cache1.get(&k).await.unwrap(), None);
    assert!(!cache1.replace_if(&k, &vw("Vladimir", 1), &vw("Blagojevic", 1)).await.unwrap());
    assert_eq!(cache1.get(&k).await.unwrap(), None);
    assert_not_locked(&c, &k);
    assert!(c.cache(0, CACHE).is_empty() && c.cache(1, CACHE).is_empty());
}
