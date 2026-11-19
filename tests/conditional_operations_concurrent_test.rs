//! Port of `api/ConditionalOperationsConcurrentTest` (core 14.0.35.Final): the atomic semantics of
//! `replace(k, old, new)`, `remove(k, v)` and `putIfAbsent` when several threads, each on its own
//! node in turn, race on one key. Upstream synchronises the movers with a `CyclicBarrier` whose
//! barrier action checks the state; here the test driver plays the barrier: every round has a
//! "before" phase (all movers read the key), a check, an "execute" phase (all movers run the
//! operation concurrently), and a check. The default upstream shape is 2 nodes / 10 operations /
//! 2 threads; a 3-node / 12-operation / 3-thread run is added. Runs on REPL_SYNC (upstream: DIST_SYNC).

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::repl::*;
use infinispan::cache::encoder_cache::Cache;
use infinispan::cache::testing::ReplCluster;
use infinispan::commands::object::CacheObject;

const CACHE: &str = "cond";

#[derive(Clone, Copy)]
enum Op {
    Replace,
    ConditionalRemove,
    PutIfAbsent,
}

impl Op {
    /// `beforeOperation`
    async fn before(self, cache: &Cache, key: &CacheObject) {
        match self {
            Op::Replace => {}
            Op::PutIfAbsent => {
                let _ = cache.remove(key).await;
            }
            Op::ConditionalRemove => {
                let _ = cache.put(key, &s("someValue")).await;
            }
        }
    }

    /// `execute`: did this thread's operation succeed?
    async fn execute(self, cache: &Cache, key: &CacheObject, existing: &Option<CacheObject>, target: &str) -> bool {
        let exist = existing.clone().unwrap_or(CacheObject::Null);
        match self {
            Op::Replace => cache.replace_if(key, &exist, &s(target)).await.unwrap_or(false),
            Op::ConditionalRemove => cache.remove_if(key, &exist).await.unwrap_or(false),
            Op::PutIfAbsent => cache.put_if_absent(key, &s(target)).await.map(|o| o.is_none()).unwrap_or(false),
        }
    }

    /// `validateTargetValueForSuccess`
    fn valid_success(self, target: &str, stored: &Option<CacheObject>) -> bool {
        match self {
            Op::ConditionalRemove => stored.is_none(),
            _ => stored.as_ref().and_then(|v| v.as_str()) == Some(target),
        }
    }
}

async fn run(op: Op, nodes: usize, operations: usize, threads: usize) {
    let c = Arc::new(ReplCluster::start(nodes, &[CACHE]).await);
    let key = s("thisIsTheKeyForConcurrentAccess");
    c.cache(0, CACHE).put(&key, &s("initialValue")).await.unwrap();
    let moves: Vec<String> = (0..operations).map(|i| format!("v_{i}")).collect();
    let mut pick: Vec<usize> = (0..threads).collect();

    let mut round = 0;
    while round * threads < operations {
        // the targets of this round: thread t moves to validMoves[round * threads + t]
        let targets: Vec<Option<String>> = (0..threads).map(|t| moves.get(round * threads + t).cloned()).collect();
        let active: Vec<usize> = (0..threads).filter(|t| targets[*t].is_some()).collect();
        if active.len() != threads {
            break; // upstream's movers stop when one runs out of moves
        }

        // phase 1: before + read the current value from this thread's next cache
        let mut readers = Vec::new();
        for &t in &active {
            pick[t] = (pick[t] + 1) % nodes;
            let cache = c.cache(pick[t], CACHE);
            let (key, zero) = (key.clone(), c.cache(0, CACHE));
            readers.push(tokio::spawn(async move {
                op.before(&zero, &key).await; // upstream: operation.beforeOperation(caches.get(0))
                cache.get(&key).await.unwrap()
            }));
        }
        let mut existing = Vec::new();
        for r in readers {
            existing.push(r.await.unwrap());
        }
        // checkBeforeState: every mover expects what is stored
        let stored = c.cache(0, CACHE).get(&key).await.unwrap();
        for e in &existing {
            assert_eq!(e, &stored, "some cache expected a different value than what is stored");
        }

        // phase 2: all movers execute at the same time
        let mut execs = Vec::new();
        for (i, &t) in active.iter().enumerate() {
            let cache = c.cache(pick[t], CACHE);
            let (key, ex, target) = (key.clone(), existing[i].clone(), targets[t].clone().unwrap());
            execs.push(tokio::spawn(async move { op.execute(&cache, &key, &ex, &target).await }));
        }
        let mut results = Vec::new();
        for e in execs {
            results.push(e.await.unwrap());
        }

        // checkAfterState
        let current = c.cache(0, CACHE).get(&key).await.unwrap();
        let unique: HashSet<_> = active.iter().map(|t| targets[*t].clone()).collect();
        assert_eq!(unique.len(), threads, "test bug");
        for n in 0..nodes {
            assert_eq!(c.cache(n, CACHE).get(&key).await.unwrap(), current, "not all the caches see the same value (node {n})");
        }
        let successes = results.iter().filter(|r| **r).count();
        assert_eq!(successes, 1, "{successes} threads assume a successful operation (CAS should succeed on a single thread only)");
        for (i, &t) in active.iter().enumerate() {
            let target = targets[t].as_ref().unwrap();
            if results[i] {
                assert!(op.valid_success(target, &current), "operation successful but the stored value {current:?} is not the successful thread's {target}");
            } else {
                assert_ne!(current.as_ref().and_then(|v| v.as_str()), Some(target.as_str()), "operation not successful but the stored value matches the write attempt");
            }
        }
        for n in 0..nodes {
            assert!(!c.cache(n, CACHE).engine().is_locked(&key), "lock on the entry wasn't cleaned up on node {n}");
        }
        round += 1;
    }
    assert!(round > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_replace() {
    run(Op::Replace, 2, 10, 2).await;
    run(Op::Replace, 3, 12, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_conditional_remove() {
    run(Op::ConditionalRemove, 2, 10, 2).await;
    run(Op::ConditionalRemove, 3, 12, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_if_absent() {
    run(Op::PutIfAbsent, 2, 10, 2).await;
    run(Op::PutIfAbsent, 3, 12, 3).await;
}
