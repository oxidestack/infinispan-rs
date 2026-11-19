//! Port of `statetransfer/StateTransferFunctionalTest` (infinispan-core 14.0.35.Final), REPL_SYNC,
//! non-transactional: the cache is `REPL_SYNC` with `chunkSize(20)`.
//!
//! Ported: `testStateTransferException`, `testInitialStateTransfer`, `testInitialStateTransferCacheNotPresent`, `testConcurrentStateTransfer`,
//! `testSTWithThirdWritingNonTxCache`, `testSTWithWritingNonTxThread`, `testInitialStateTransferAfterRestart`.
//! Not ported: `testSTWithThirdWritingTxCache` and `testSTWithWritingTxThread` (transactions are out of
//! scope). `testStateTransferException` is ported with `stateTransfer().timeout()` as the join timeout
//! (`CacheConfig::state_transfer_timeout`, carried in whole milliseconds by `CacheJoinInfo` like upstream).
//!
//! `DelayTransfer` is a value whose protostream marshalling sleeps one second; here the same delay is put on
//! the first `StateResponse` that carries it, by delaying its delivery.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::{Intercepted, ReplCluster, RpcInterceptor};
use infinispan::cache::write_pipeline::CacheConfig;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::ch::address::Address;
use infinispan::topology::support::BoxFuture;

const CACHE: &str = "nbst";
const WAIT: Duration = Duration::from_secs(60);

fn chunk20(c: CacheConfig) -> CacheConfig {
    CacheConfig { state_chunk_size: 20, ..c }
}

async fn first_node() -> ReplCluster {
    let c = ReplCluster::new();
    c.add_node_and_join(&[CACHE], chunk20).await;
    c
}

async fn write_initial_data(c: &ReplCluster, idx: usize) {
    let cache = c.cache(idx, CACHE);
    cache.put(&s("a_b_name"), &s("Joe")).await.unwrap();
    cache.put(&s("a_b_age"), &s("20")).await.unwrap();
    cache.put(&s("a_c_name"), &s("Bob")).await.unwrap();
    cache.put(&s("a_c_age"), &s("40")).await.unwrap();
}

async fn verify_initial_data(c: &ReplCluster, idx: usize) {
    let cache = c.cache(idx, CACHE);
    for (k, v) in [("a_b_name", "Joe"), ("a_b_age", "20"), ("a_c_name", "Bob"), ("a_c_age", "40")] {
        assert_eq!(cache.get(&s(k)).await.unwrap(), Some(s(v)), "node {idx}, key {k}");
    }
}

async fn settled(c: &ReplCluster, n: usize) {
    let nodes: Vec<usize> = (0..n).collect();
    c.nodes.wait_for_no_rebalance(&nodes, CACHE, WAIT).await.unwrap();
}

/// Delays the first `StateResponse` delivery by a second: the one entry with `DelayTransfer`'s slow marshalling.
struct DelayStateResponses(AtomicBool);

impl RpcInterceptor for DelayStateResponses {
    fn intercept(&self, _from: &Address, _to: &Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
        let delay = matches!(command.payload, CacheRpcPayload::StateResponse(_)) && self.0.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if delay {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
            Intercepted::Deliver
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_state_transfer() {
    let c = first_node().await;
    write_initial_data(&c, 0).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 2).await;
    verify_initial_data(&c, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_state_transfer_cache_not_present() {
    let c = first_node().await;
    write_initial_data(&c, 0).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 2).await;
    verify_initial_data(&c, 1).await;
    // another cache that only the first node runs
    c.start_cache_with(0, "otherCache", chunk20(CacheConfig::new("otherCache"))).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_state_transfer() {
    let c = first_node().await;
    write_initial_data(&c, 0).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    let delay = Arc::new(DelayStateResponses(AtomicBool::new(true)));
    c.set_interceptor(Some(delay));
    settled(&c, 2).await;
    verify_initial_data(&c, 1).await;

    // two more nodes join at once
    let n3 = c.add_started_node();
    let n4 = c.add_started_node();
    c.nodes.start_node(n3).await;
    c.nodes.start_node(n4).await;
    let (c1, c2) = (&c, &c);
    let (r3, r4) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(30), c1.start_cache_with(n3, CACHE, chunk20(CacheConfig::new(CACHE)))),
        tokio::time::timeout(Duration::from_secs(30), c2.start_cache_with(n4, CACHE, chunk20(CacheConfig::new(CACHE)))),
    );
    r3.expect("join 3 in time").unwrap();
    r4.expect("join 4 in time").unwrap();
    settled(&c, 4).await;
    verify_initial_data(&c, n3).await;
    verify_initial_data(&c, n4).await;
}

struct Writer {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Result<usize, String>>,
}

/// `WritingTask`: puts `test<c> -> c` with a millisecond between puts until stopped.
fn start_writer(c: &Arc<ReplCluster>, idx: usize) -> Writer {
    let stop = Arc::new(AtomicBool::new(false));
    let (stop2, cluster) = (stop.clone(), c.clone());
    let task = tokio::spawn(async move {
        let cache = cluster.cache(idx, CACHE);
        let mut n = 0usize;
        while !stop2.load(Ordering::SeqCst) {
            cache.put(&s(&format!("test{n}")), &s(&n.to_string())).await.map_err(|e| format!("Error writing key test{n}: {e}"))?;
            n += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(n)
    });
    Writer { stop, task }
}

impl Writer {
    async fn finish(self) -> usize {
        self.stop.store(true, Ordering::SeqCst);
        tokio::time::timeout(WAIT, self.task).await.expect("writer stops").unwrap().expect("no write failed")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_st_with_third_writing_non_tx_cache() {
    let c = Arc::new(ReplCluster::new());
    c.add_node_and_join(&[CACHE], chunk20).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 2).await;
    write_initial_data(&c, 0).await;
    // delay the transient copy, so that the writer is busy for the whole transfer
    c.set_interceptor(Some(Arc::new(DelayStateResponses(AtomicBool::new(true)))));
    for i in 0..60 {
        c.cache(0, CACHE).put(&s(&format!("filler{i}")), &s("x")).await.unwrap();
    }
    // node 1 is the writer, node 0 and the joiner (2) hold the data
    let writer = start_writer(&c, 1);
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 3).await;
    let count = writer.finish().await;
    verify_initial_data(&c, 2).await;
    for i in 0..count {
        assert_eq!(c.cache(2, CACHE).get(&s(&format!("test{i}"))).await.unwrap(), Some(s(&i.to_string())), "test{i} of {count}");
    }
    assert!(count > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_st_with_writing_non_tx_thread() {
    let c = Arc::new(ReplCluster::new());
    c.add_node_and_join(&[CACHE], chunk20).await;
    assert_eq!(c.cache(0, CACHE).size(), 0);
    write_initial_data(&c, 0).await;
    c.set_interceptor(Some(Arc::new(DelayStateResponses(AtomicBool::new(true)))));
    for i in 0..60 {
        c.cache(0, CACHE).put(&s(&format!("filler{i}")), &s("x")).await.unwrap();
    }
    let writer = start_writer(&c, 0);
    verify_initial_data(&c, 0).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 2).await;
    let count = writer.finish().await;
    verify_initial_data(&c, 0).await;
    verify_initial_data(&c, 1).await;
    for i in 0..count {
        assert_eq!(c.cache(1, CACHE).get(&s(&format!("test{i}"))).await.unwrap(), Some(s(&i.to_string())), "test{i} of {count}");
    }
    assert!(count > 0);
    // the two nodes hold the same entries
    assert_eq!(c.cache(0, CACHE).size(), c.cache(1, CACHE).size());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_state_transfer_after_restart() {
    let c = first_node().await;
    write_initial_data(&c, 0).await;
    c.add_node_and_join(&[CACHE], chunk20).await;
    settled(&c, 2).await;
    verify_initial_data(&c, 1).await;

    // cache2.stop(); cache2.start()
    c.stop_cache(1, CACHE).await;
    c.nodes.wait_for_no_rebalance(&[0], CACHE, WAIT).await.unwrap();
    c.start_cache_with(1, CACHE, chunk20(CacheConfig::new(CACHE))).await.unwrap();
    settled(&c, 2).await;
    verify_initial_data(&c, 1).await;
}

/// `testStateTransferException`: a joiner whose `remoteTimeout` and `stateTransfer().timeout()` are one
/// nanosecond (0 ms in `CacheJoinInfo`) gets a `TimeoutException` from `getCache`: the join request or, as
/// here when the coordinator answers within the poll, `waitForInitialStateTransferToComplete`
/// (ISPN000658).
#[tokio::test(flavor = "multi_thread")]
async fn test_state_transfer_exception() {
    let c = first_node().await;
    write_initial_data(&c, 0).await;
    assert_eq!(c.nodes.cache_topology(0, CACHE).unwrap().members().len(), 1);

    // The transfer takes a second (upstream's `DelayTransfer` entry), so it cannot be done when the
    // zero-millisecond wait looks at it, however fast the in-memory network answers the join.
    c.set_interceptor(Some(Arc::new(DelayStateResponses(AtomicBool::new(true)))));
    let idx = c.add_started_node();
    c.nodes.start_node(idx).await;
    let cfg = CacheConfig {
        remote_timeout: Duration::from_nanos(1),
        state_transfer_timeout: Duration::from_nanos(1),
        ..chunk20(CacheConfig::new(CACHE))
    };
    let err = c.start_cache_with(idx, CACHE, cfg).await.err().expect("the join must time out");
    assert!(err.0.contains("TimeoutException"), "{err:?}");

    // The coordinator may still process the late join request; the failed cache stops (as upstream's
    // failed `getCache` does) and the cluster is back to the one member.
    c.stop_cache(idx, CACHE).await;
    // (the coordinator's status stays IN_PROGRESS after a rebalance cancelled by a leave, as upstream's
    // `rebalanceInProgress` is only cleared by `endRebalance`, so this does not use wait_for_no_rebalance)
    assert!(
        c.nodes.eventually(WAIT, || c.nodes.cache_topology(0, CACHE).is_some_and(|t| t.members().len() == 1 && t.pending_ch.is_none())).await,
        "the failed joiner never left the topology"
    );
    verify_initial_data(&c, 0).await;
}
