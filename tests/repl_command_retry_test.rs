//! Port of `statetransfer/ReplCommandRetryTest.testRetryAfterJoinNonTransactional` (REPL_SYNC): a `put` is
//! replicated to `c2` and held there; `c3` joins and the topology changes; when `c2` lets go it answers
//! `OutdatedTopologyException` (`EntryWrappingInterceptor.checkTopology`: an owner was added while the
//! command ran) and `c1`, the originator and primary, retries the command at the new topology on every
//! member; a join of `c4` during the retry forces a second retry. Counters are asserted exactly as
//! upstream: `c1` 1, `c2` 3, `c3` 2, `c4` 1.
//!
//! Adaptations: `ReplicatedControlledConsistentHashFactory(0)` (coordinator always primary) is replaced
//! by a key whose primary is `c1` in the topology of every step (found with a scratch cluster, the
//! addresses being deterministic). `DelayInterceptor`/`CheckPoint` become an [`AfterApplyHook`] and
//! [`CheckPoint`] below. The four transactional variants (`LockControlCommand`, `PrepareCommand`,
//! `CommitCommand`) need transactions and are not ported.

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::write_pipeline::{AfterApplyHook, CacheConfig};
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::commands::write::{WriteCommand, flags};
use infinispan::topology::support::BoxFuture;
use tokio::sync::Notify;

const CACHE: &str = "retryCache";
const WAIT: Duration = Duration::from_secs(15);

/// `org.infinispan.test.concurrent.CheckPoint`, reduced to what the test needs.
#[derive(Default)]
struct CheckPoint {
    events: Mutex<HashSet<String>>,
    changed: Notify,
}

impl CheckPoint {
    fn trigger(&self, event: &str) {
        self.events.lock().unwrap().insert(event.to_string());
        self.changed.notify_waiters();
    }

    async fn await_event(&self, event: &str, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.events.lock().unwrap().contains(event) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.events.lock().unwrap().contains(event);
            }
        }
    }
}

/// `DelayInterceptor(PutKeyValueCommand.class)`: blocks every non-state-transfer put after it ran.
struct DelayHook {
    counter: AtomicI32,
    checkpoint: CheckPoint,
    name: String,
}

impl DelayHook {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self { counter: AtomicI32::new(0), checkpoint: CheckPoint::default(), name: name.into() })
    }

    fn counter(&self) -> i32 {
        self.counter.load(Ordering::SeqCst)
    }

    async fn wait_until_blocked(&self, count: i32) {
        assert!(self.checkpoint.await_event(&format!("blocked_{count}_on_{}", self.name), Duration::from_secs(5)).await, "{} never blocked {count}", self.name);
    }

    async fn unblock(&self, count: i32) {
        self.wait_until_blocked(count).await;
        self.checkpoint.trigger(&format!("resume_{count}_on_{}", self.name));
    }
}

impl AfterApplyHook for DelayHook {
    fn after_apply(self: Arc<Self>, _local: &Address, command: &WriteCommand, _origin_local: bool) -> BoxFuture<'static, ()> {
        let me = self;
        let is_put = matches!(command, WriteCommand::Put(_)) && !command.has_any_flag(flags::PUT_FOR_STATE_TRANSFER);
        Box::pin(async move {
            if !is_put {
                return;
            }
            let my_count = me.counter.fetch_add(1, Ordering::SeqCst) + 1;
            me.checkpoint.trigger(&format!("blocked_{my_count}_on_{}", me.name));
            assert!(me.checkpoint.await_event(&format!("resume_{my_count}_on_{}", me.name), WAIT).await, "command {my_count} on {} was never resumed", me.name);
        })
    }
}

fn install(c: &ReplCluster, idx: usize, name: &str) -> Arc<DelayHook> {
    let h = DelayHook::new(name);
    c.cache(idx, CACHE).engine().set_after_apply_hook(Some(h.clone()));
    h
}

/// Add node `idx` to a view with every earlier node, start it and join the cache.
async fn add_node(c: &ReplCluster) -> usize {
    let idx = c.nodes.add_node();
    let members: Vec<usize> = (0..=idx).collect();
    c.nodes.install_view(&members, false);
    c.nodes.start_node(idx).await;
    c.start_cache_with(idx, CACHE, CacheConfig::new(CACHE)).await.expect("cache starts");
    idx
}

async fn wait_for_state_transfer(c: &ReplCluster, expected_topology_id: i32, nodes: &[usize]) {
    c.nodes.wait_for_no_rebalance(nodes, CACHE, Duration::from_secs(20)).await.unwrap();
    for i in nodes {
        let t = c.nodes.cache_topology(*i, CACHE).unwrap();
        assert_eq!(t.topology_id, expected_topology_id, "wrong topology on node {i}");
    }
}

fn topology_id(c: &ReplCluster, idx: usize) -> i32 {
    c.nodes.cache_topology(idx, CACHE).unwrap().topology_id
}

/// A key whose primary is node 0 with 1, 2, 3 and 4 members (scratch cluster, deterministic addresses).
async fn key_with_stable_coordinator_primary() -> CacheObject {
    let scratch = ReplCluster::new();
    let candidates: Vec<CacheObject> = (0..400).map(|i| s(&format!("k{i}"))).collect();
    let mut ok: Vec<bool> = vec![true; candidates.len()];
    for step in 0..4 {
        add_node(&scratch).await;
        let members: Vec<usize> = (0..=step).collect();
        scratch.nodes.wait_for_no_rebalance(&members, CACHE, Duration::from_secs(20)).await.unwrap();
        for (i, k) in candidates.iter().enumerate() {
            let (p, _) = scratch.cache(0, CACHE).engine().owners_of(k).unwrap();
            if p != scratch.address(0) {
                ok[i] = false;
            }
        }
    }
    let i = ok.iter().position(|b| *b).expect("a key whose primary stays node 0");
    candidates[i].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_retry_after_join_non_transactional() {
    let key = key_with_stable_coordinator_primary().await;
    let c = Arc::new(ReplCluster::new());

    let c1 = add_node(&c).await;
    let di1 = install(&c, c1, "c1");
    let initial_topology_id = topology_id(&c, 0);

    let c2 = add_node(&c).await;
    let di2 = install(&c, c2, "c2");
    wait_for_state_transfer(&c, initial_topology_id + 4, &[0, 1]).await;

    let cache1 = c.cache(0, CACHE);
    let (k, v) = (key.clone(), vw("v", 1));
    let put = tokio::spawn(async move { cache1.put(&k, &v).await });

    // The command is replicated to c2, and blocks in the DelayInterceptor on c2
    di2.wait_until_blocked(1).await;

    // c3 joins, topology id changes
    let c3 = add_node(&c).await;
    let di3 = install(&c, c3, "c3");
    wait_for_state_transfer(&c, initial_topology_id + 8, &[0, 1, 2]).await;

    // Unblock the replicated command on c2.
    di2.unblock(1).await;

    // c2 answers OutdatedTopologyException, and c1 retries the command.
    // c1 sends the command to c2 and c3, blocking on both in the DelayInterceptor
    di2.wait_until_blocked(2).await;
    di3.wait_until_blocked(1).await;

    // Unblock the command with the new topology id on c2
    di2.unblock(2).await;

    // c4 joins, topology id changes
    let c4 = add_node(&c).await;
    let di4 = install(&c, c4, "c4");
    wait_for_state_transfer(&c, initial_topology_id + 12, &[0, 1, 2, 3]).await;

    // Unblock the command with the new topology id on c3.
    di3.unblock(1).await;

    // c3 sends an OutdatedTopologyException, and c1 retries the command.
    // c1 sends the command to c2, c3, and c4, blocking everywhere in the DelayInterceptor.
    // Unblock every node except c1
    di2.unblock(3).await;
    di3.unblock(2).await;
    di4.unblock(1).await;

    // Now c1 blocks
    di1.unblock(1).await;

    let retval = tokio::time::timeout(Duration::from_secs(10), put).await.expect("the put finishes").unwrap().unwrap();
    assert_eq!(retval, None);

    // 1 for the last retry
    assert_eq!(di1.counter(), 1);
    // 1 for the initial invocation + 1 for each retry
    assert_eq!(di2.counter(), 3);
    // 1 for each retry
    assert_eq!(di3.counter(), 2);
    // just the last retry
    assert_eq!(di4.counter(), 1);

    // and every member holds the value
    for i in 0..4 {
        assert_eq!(c.cache(i, CACHE).engine().local_get(&key), Some(vw("v", 1)), "node {i}");
    }
}
