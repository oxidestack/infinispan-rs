//! Port of the listener-adjacent, non-transactional parts of `replication/SyncCacheListenerTest`
//! (REPL_SYNC, two nodes). Full listener semantics (`@Listener(clustered)`, pre events, `Event` types) are
//! covered elsewhere; this exercises the post-notification hook the write pipeline fires:
//! - `testRemoteCacheListener`: a listener on `cache(1)` sees the write replicated from `cache(0)`
//!   as *not* originating locally (`e.isOriginLocal()` is false);
//! - `testSyncRepl` / `testSyncReplMap`: a listener on `cache(0)` that reads the key while the write that
//!   fired it still holds the key lock does not block, and nothing is locked afterwards;
//! - `simpleReplicationTest` without the transaction.
//!
//! `testSyncTxRepl` and `testSyncTxReplMap` need transactions and are not ported.

mod common;

use std::sync::{Arc, Mutex};

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::write_pipeline::{CacheEngine, EntryEvent, EntryEventKind, EntryListener};
use infinispan::commands::object::CacheObject;

const CACHE: &str = "cache";

/// `RemoteListener`: `@CacheEntryRemoved @CacheEntryModified`, asserting the event is not local.
#[derive(Default)]
struct RemoteListener(Mutex<Vec<EntryEvent>>);

impl EntryListener for RemoteListener {
    fn on_event(&self, e: &EntryEvent) {
        if matches!(e.kind, EntryEventKind::Removed | EntryEventKind::Modified) {
            assert!(!e.origin_local, "entry was removed on remote cache so isLocal should be false");
            self.0.lock().unwrap().push(e.clone());
        }
    }
}

/// `LocalListener`: on every modification reads the key from its own node.
struct LocalListener {
    engine: Arc<CacheEngine>,
    key: CacheObject,
    reads: Mutex<Vec<Option<CacheObject>>>,
}

impl EntryListener for LocalListener {
    fn on_event(&self, e: &EntryEvent) {
        if e.kind == EntryEventKind::Modified {
            // test out if we can get the read lock since there is a write lock going as well
            self.reads.lock().unwrap().push(self.engine.local_get(&self.key));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_cache_listener() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let lis = Arc::new(RemoteListener::default());
    cache2.add_listener(lis.clone());

    let age = s("age");
    // create (not observed), modify (observed, remote), remove (observed, remote)
    cache1.put(&age, &vw("38", 1)).await.unwrap();
    assert_eq!(cache2.get(&age).await.unwrap(), Some(vw("38", 1)));
    cache1.put(&age, &vw("39", 2)).await.unwrap();
    cache1.remove(&age).await.unwrap();
    let seen = lis.0.lock().unwrap().clone();
    assert_eq!(seen.iter().map(|e| e.kind).collect::<Vec<_>>(), vec![EntryEventKind::Modified, EntryEventKind::Removed]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sync_repl() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let age = s("age");
    let lis = Arc::new(LocalListener { engine: cache1.engine().clone(), key: age.clone(), reads: Mutex::new(vec![]) });
    cache1.add_listener(lis.clone());

    cache1.put(&age, &vw("38", 1)).await.unwrap();
    cache1.put(&age, &vw("38", 2)).await.unwrap(); // a modification fires the listener
    // value on cache2 must be 38
    assert_eq!(cache2.get(&age).await.unwrap(), Some(vw("38", 2)));
    assert_eq!(lis.reads.lock().unwrap().len(), 1);
    assert_eq!(lis.reads.lock().unwrap()[0], Some(vw("38", 2)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sync_repl_map() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    let (cache1, cache2) = (c.cache(0, CACHE), c.cache(1, CACHE));
    let age = s("age");
    assert!(!cache1.engine().is_locked(&age));
    let lis = Arc::new(LocalListener { engine: cache1.engine().clone(), key: age.clone(), reads: Mutex::new(vec![]) });
    cache1.add_listener(lis);
    cache1.put(&age, &vw("38", 1)).await.unwrap();
    cache1.put(&s("name"), &vw("Ben", 1)).await.unwrap();
    assert!(!cache1.engine().is_locked(&age));
    assert_eq!(cache2.get(&age).await.unwrap(), Some(vw("38", 1)));
    assert!(!cache1.engine().is_locked(&age));
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_replication_test() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    c.cache(0, CACHE).put(&s("key"), &vw("value", 1)).await.unwrap();
    assert_eq!(c.cache(1, CACHE).get(&s("key")).await.unwrap(), Some(vw("value", 1)));
}
