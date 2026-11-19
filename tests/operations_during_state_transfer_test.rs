//! Port of `statetransfer/OperationsDuringStateTransferTest` (infinispan-core 14.0.35.Final), the
//! `REPL_SYNC` non-transactional instance (the DIST and the transactional instances are not reached by
//! MockServer). `numSegments(10)` is not applied (the join info of the in-memory cluster has 256 segments).
//!
//! Upstream blocks a command in a custom interceptor placed after `EntryWrappingInterceptor` on node B;
//! [`ExecuteHook`] is that seam: it holds a write on its originator before dispatch, a `get` after its
//! lookup, and a chunk of state before it is applied.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::write_pipeline::{CacheConfig, ExecuteHook};
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::commands::write::{WriteCommand, flags};
use infinispan::topology::support::BoxFuture;
use tokio::sync::Notify;

const CACHE: &str = "ops";
const WAIT: Duration = Duration::from_secs(10);

/// A latch pair: `started` fires when the held operation arrives, the operation continues on `proceed`.
struct Latch {
    started: Notify,
    proceed: Notify,
    started_flag: AtomicBool,
    proceed_flag: AtomicBool,
}

impl Latch {
    fn new() -> Arc<Self> {
        Arc::new(Self { started: Notify::new(), proceed: Notify::new(), started_flag: false.into(), proceed_flag: false.into() })
    }

    async fn hold(&self) {
        self.started_flag.store(true, Ordering::SeqCst);
        self.started.notify_waiters();
        while !self.proceed_flag.load(Ordering::SeqCst) {
            let _ = tokio::time::timeout(Duration::from_millis(20), self.proceed.notified()).await;
        }
    }

    async fn wait_started(&self) {
        let r = tokio::time::timeout(WAIT, async {
            while !self.started_flag.load(Ordering::SeqCst) {
                let _ = tokio::time::timeout(Duration::from_millis(20), self.started.notified()).await;
            }
        })
        .await;
        r.expect("the held operation arrived");
    }

    fn proceed(&self) {
        self.proceed_flag.store(true, Ordering::SeqCst);
        self.proceed.notify_waiters();
    }
}

#[derive(Clone, Copy)]
enum Op {
    Remove,
    Put,
    Replace,
}

/// Holds the first matching write on node `node` (the originator), or the first state apply, or the first get.
struct Hold {
    node: Address,
    write: Option<(Op, Arc<Latch>)>,
    apply: Option<Arc<Latch>>,
    get: Option<Arc<Latch>>,
}

impl ExecuteHook for Hold {
    fn before_write(self: Arc<Self>, local: &Address, command: &WriteCommand) -> BoxFuture<'static, ()> {
        let hit = match (&self.write, command) {
            (Some((Op::Remove, l)), WriteCommand::Remove(_)) => Some(l.clone()),
            (Some((Op::Put, l)), WriteCommand::Put(p)) if p.flags & flags::PUT_FOR_STATE_TRANSFER == 0 => Some(l.clone()),
            (Some((Op::Replace, l)), WriteCommand::Replace(_)) => Some(l.clone()),
            _ => None,
        };
        let me = *local == self.node;
        Box::pin(async move {
            if let (true, Some(l)) = (me, hit)
                && !l.started_flag.load(Ordering::SeqCst)
            {
                l.hold().await;
            }
        })
    }

    fn after_get_lookup(self: Arc<Self>, local: &Address, _key: &CacheObject) -> BoxFuture<'static, ()> {
        let hit = if *local == self.node { self.get.clone() } else { None };
        Box::pin(async move {
            // only the first get is blocked
            if let Some(l) = hit
                && !l.started_flag.load(Ordering::SeqCst)
            {
                l.hold().await;
            }
        })
    }

    fn before_state_apply(self: Arc<Self>, local: &Address, _segment: i32) -> BoxFuture<'static, ()> {
        let hit = if *local == self.node { self.apply.clone() } else { None };
        Box::pin(async move {
            if let Some(l) = hit {
                l.hold().await;
            }
        })
    }
}

fn await_no_initial(c: CacheConfig) -> CacheConfig {
    CacheConfig { await_initial_transfer: false, ..c }
}

/// Node 0 holds `myKey`; node 1 joined without a rebalance (the coordinator refuses to start one) and is not
/// a member yet. Returns the cluster.
async fn node_b_not_yet_a_member(hook: impl FnOnce(Address) -> Hold) -> (Arc<ReplCluster>, Hold) {
    let c = Arc::new(ReplCluster::start(1, &[CACHE]).await);
    c.cache(0, CACHE).put(&s("myKey"), &s("myValue")).await.unwrap();
    // do not allow coordinator to send topology updates to node B
    c.nodes.topology_node(0).ctm.set_rebalancing_enabled(false).unwrap();
    let idx = c.add_started_node();
    c.nodes.start_node(idx).await;
    let hold = hook(c.address(idx));
    (c, hold)
}

async fn join_b(c: &ReplCluster, hold: Arc<Hold>) {
    c.start_cache_with(1, CACHE, await_no_initial(CacheConfig::new(CACHE))).await.expect("joined");
    c.cache(1, CACHE).engine().set_execute_hook(Some(hold));
}

fn assert_b_is_not_a_member(c: &ReplCluster) {
    let t = c.nodes.cache_topology(1, CACHE).expect("B has the join topology");
    assert!(t.pending_ch.is_none());
    assert!(t.members().contains(&c.address(0)));
    assert!(!t.members().contains(&c.address(1)));
    assert!(!t.current_ch.as_ref().unwrap().members().contains(&c.address(1)));
    // no keys should be present on node B yet because state transfer is blocked
    assert!(c.cache(1, CACHE).keys().is_empty());
}

async fn write_during_rebalance(op: Op, expected_old: &str, expected_new: Option<&str>) {
    let latch = Latch::new();
    let l2 = latch.clone();
    let (c, hold) = node_b_not_yet_a_member(move |addr| Hold { node: addr, write: Some((op, l2)), apply: None, get: None }).await;
    join_b(&c, Arc::new(hold)).await;
    assert_b_is_not_a_member(&c);

    // initiate the operation on node B
    let c2 = c.clone();
    let key = s("myKey");
    let fut = tokio::spawn(async move {
        let cache = c2.cache(1, CACHE);
        match op {
            Op::Remove => cache.remove(&key).await,
            Op::Put => cache.put(&key, &s("newValue")).await,
            Op::Replace => cache.replace(&key, &s("newValue")).await,
        }
    });
    // wait for the command on node B to reach the hold point; the value seen so far is null
    latch.wait_started().await;
    // paranoia, yes the value is still missing from the data container
    assert!(c.cache(1, CACHE).keys().is_empty());

    // allow rebalance to start
    c.nodes.topology_node(0).ctm.set_rebalancing_enabled(true).unwrap();
    c.nodes.wait_for_no_rebalance(&[0, 1], CACHE, WAIT).await.unwrap();
    // the state should be already transferred now
    assert_eq!(c.cache(1, CACHE).keys().len(), 1);

    // allow the operation to continue
    latch.proceed();
    let old = tokio::time::timeout(WAIT, fut).await.expect("operation finishes").unwrap().unwrap();
    assert_eq!(old, Some(s(expected_old)));
    for i in 0..2 {
        assert_eq!(c.cache(i, CACHE).get(&s("myKey")).await.unwrap(), expected_new.map(s), "node {i}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove() {
    write_during_rebalance(Op::Remove, "myValue", None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put() {
    write_during_rebalance(Op::Put, "myValue", Some("newValue")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replace() {
    write_during_rebalance(Op::Replace, "myValue", Some("newValue")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get() {
    let apply = Latch::new();
    let get = Latch::new();
    let (a2, g2) = (apply.clone(), get.clone());
    let c = Arc::new(ReplCluster::start(1, &[CACHE]).await);
    c.cache(0, CACHE).put(&s("myKey"), &s("myValue")).await.unwrap();
    let idx = c.add_started_node();
    c.nodes.start_node(idx).await;
    let hold = Arc::new(Hold { node: c.address(idx), write: None, apply: Some(a2), get: Some(g2) });
    // the hook must be on node B's cache before the state arrives, so it is attached while joining
    let c2 = c.clone();
    let hook2 = hold.clone();
    let join = tokio::spawn(async move {
        c2.start_cache_with_engine_hook(1, CACHE, await_no_initial(CacheConfig::new(CACHE)), hook2).await.unwrap();
    });
    // wait for state transfer on node B to progress to the point where data segments are about to be applied
    apply.wait_started().await;
    // state transfer is blocked, no keys should be present on node B yet
    assert_eq!(c.cache(1, CACHE).engine().container.len(), 0);

    // initiate a GET
    let c3 = c.clone();
    let get_future = tokio::spawn(async move { c3.cache(1, CACHE).get(&s("myKey")).await });
    // wait for GET on node B to reach the hold point; the value seen so far came from a remote lookup
    get.wait_started().await;

    // allow state transfer to apply state
    apply.proceed();
    join.await.unwrap();
    c.nodes.wait_for_no_rebalance(&[0, 1], CACHE, WAIT).await.unwrap();
    assert_eq!(c.cache(1, CACHE).engine().container.len(), 1);

    // allow GET to continue
    get.proceed();
    let value = tokio::time::timeout(WAIT, get_future).await.expect("get finishes").unwrap().unwrap();
    assert_eq!(value, Some(s("myValue")));
}
