//! Cluster-level ports of the topology tests that need real members, over the in-memory cluster
//! (`topology::testing::TestCluster`, whose transport marshals every command and answer):
//! `statetransfer/ClusterTopologyManagerTest`, `topology/TopologyUnorderedJoinTest`,
//! `partitionhandling/ThreeNodesReplicatedSplitAndMergeTest` and
//! `distribution/ConcurrentStartWithReplTest`.
//!
//! Adaptations, all because this port reaches only REPL_SYNC caches without a data plane:
//! - upstream runs `ClusterTopologyManagerTest` as DIST_SYNC transactional; here the caches are
//!   REPL_SYNC (`SyncReplicatedConsistentHashFactory`). `testLeaveDuringGetTransactions` is not
//!   ported: it blocks `StateProvider.getTransactionsForSegments`, which is state transfer,
//!   not topology.
//! - data assertions (`cache.put`/`get`, `dataContainer.containsKey`) become ownership assertions on
//!   the consistent hash: a replicated CH puts every segment on every member that has state.
//! - `ThreeNodesReplicatedSplitAndMergeTest` runs with the default `whenSplit=ALLOW_READ_WRITES`
//!   (`PreferAvailabilityStrategy`, what MockServer uses); upstream's base class configures
//!   `DENY_READ_WRITES`, where the minority partition goes DEGRADED. That strategy is out of scope
//!   (a port decision: MockServer never uses it), so both partitions stay AVAILABLE here.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use infinispan::ch::address::Address;
use infinispan::topology::cache_topology::CacheTopology;
use infinispan::topology::cluster_topology_manager::TopologyConfig;
use infinispan::topology::commands::TopologyCommand;
use infinispan::topology::local_topology_manager::LocalTopologyInterceptor;
use infinispan::topology::messages::AvailabilityMode;
use infinispan::topology::support::BoxFuture;
use infinispan::topology::testing::{DeliveryInterceptor, TestCluster};

const CACHE: &str = "testCache";
const WAIT: Duration = Duration::from_secs(20);

fn config() -> TopologyConfig {
    TopologyConfig { global_timeout: Duration::from_secs(20) }
}

/// `CheckPoint`-style one-shot signal.
#[derive(Clone)]
struct Signal(Arc<watch::Sender<bool>>);

impl Signal {
    fn new() -> Self {
        Signal(Arc::new(watch::channel(false).0))
    }
    fn trigger(&self) {
        self.0.send_replace(true);
    }
    fn is_triggered(&self) -> bool {
        *self.0.borrow()
    }
    async fn wait(&self) {
        let mut rx = self.0.subscribe();
        tokio::time::timeout(WAIT, rx.wait_for(|v| *v)).await.expect("signal not triggered in time").ok();
    }
}

/// Start `n` nodes one after another, each joining every cache in `caches`, the way
/// `createClusteredCaches` grows a cluster (a new view per node).
async fn start_cluster(n: usize, caches: &[&str]) -> Arc<TestCluster> {
    let c = TestCluster::new(config());
    for i in 0..n {
        add_and_start(&c, caches, &(0..i).collect::<Vec<_>>()).await;
        let members: Vec<usize> = (0..=i).collect();
        for cache in caches {
            c.wait_for_no_rebalance(&members, cache, WAIT).await.unwrap();
        }
    }
    c
}

/// Add a node, put it in a view with `existing`, start it and join `caches` on it. Returns its index.
async fn add_and_start(c: &Arc<TestCluster>, caches: &[&str], existing: &[usize]) -> usize {
    let idx = c.add_node();
    let mut members = existing.to_vec();
    members.push(idx);
    c.install_view(&members, false);
    c.start_node(idx).await;
    for cache in caches {
        c.start_cache(idx, cache).await.unwrap();
    }
    idx
}

fn members_of(c: &TestCluster, idx: usize, cache: &str) -> Vec<Address> {
    c.cache_topology(idx, cache).map(|t| t.members().to_vec()).unwrap_or_default()
}

fn addrs(c: &TestCluster, idxs: &[usize]) -> Vec<Address> {
    idxs.iter().map(|i| c.address(*i)).collect()
}

fn as_set(v: &[Address]) -> HashSet<Address> {
    v.iter().cloned().collect()
}

async fn wait_members(c: &Arc<TestCluster>, idxs: &[usize], cache: &str, expected: &[usize]) {
    let want = as_set(&addrs(c, expected));
    assert!(
        c.eventually(WAIT, || idxs.iter().all(|i| as_set(&members_of(c, *i, cache)) == want)).await,
        "members of {cache} on {idxs:?} never became {expected:?}: {:?}",
        idxs.iter().map(|i| members_of(c, *i, cache).len()).collect::<Vec<_>>()
    );
}

// ------------------------------------------------------------ ClusterTopologyManagerTest

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_node_abrupt_leave() {
    // Create some more caches to trigger ISPN-2572
    let c = start_cluster(3, &[CACHE]).await;
    for (cache, nodes) in [("cache2", [0, 1]), ("cache3", [0, 2]), ("cache4", [1, 2]), ("cache5", [0, 1])] {
        for n in nodes {
            c.start_cache(n, cache).await.unwrap();
        }
    }
    // create the partitions
    c.install_view(&[0, 1], false);
    c.install_view(&[2], false);
    // wait for the partitions to form
    for (nodes, cache) in [(vec![0, 1], CACHE), (vec![2], CACHE), (vec![0, 1], "cache2"), (vec![0], "cache3"), (vec![1], "cache4"), (vec![0, 1], "cache5")] {
        c.wait_for_no_rebalance(&nodes, cache, WAIT).await.unwrap();
    }
    wait_members(&c, &[0, 1], CACHE, &[0, 1]).await;
    wait_members(&c, &[2], CACHE, &[2]).await;
    wait_members(&c, &[0], "cache3", &[0]).await;
    wait_members(&c, &[1], "cache4", &[1]).await;
    // Check that a new node can join
    let n3 = add_and_start(&c, &[CACHE], &[0, 1]).await;
    c.wait_for_no_rebalance(&[0, 1, n3], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, n3], CACHE, &[0, 1, n3]).await;
    for (cache, nodes) in [("cache2", vec![0, 1]), ("cache3", vec![0]), ("cache4", vec![1]), ("cache5", vec![0, 1])] {
        c.start_cache(n3, cache).await.unwrap();
        let mut all = nodes.clone();
        all.push(n3);
        c.wait_for_no_rebalance(&all, cache, WAIT).await.unwrap();
        wait_members(&c, &all, cache, &all).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_recovery_after_coord_leave() {
    let c = start_cluster(3, &[CACHE]).await;
    // create the partitions
    c.install_view(&[0], false);
    c.install_view(&[1, 2], false);
    c.wait_for_no_rebalance(&[0], CACHE, WAIT).await.unwrap();
    c.wait_for_no_rebalance(&[1, 2], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0], CACHE, &[0]).await;
    wait_members(&c, &[1, 2], CACHE, &[1, 2]).await;
    // Check that a new node can join
    let n3 = add_and_start(&c, &[CACHE], &[1, 2]).await;
    c.wait_for_no_rebalance(&[1, 2, n3], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[1, 2, n3], CACHE, &[1, 2, n3]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_recovery_after_three_way_split() {
    let c = start_cluster(3, &[CACHE]).await;
    // create the partitions
    for i in 0..3 {
        c.install_view(&[i], false);
    }
    for i in 0..3 {
        c.wait_for_no_rebalance(&[i], CACHE, WAIT).await.unwrap();
    }
    // merge the remaining partitions
    c.install_view(&[0, 1, 2], true);
    // wait for the merged cluster to form
    c.wait_for_no_rebalance(&[0, 1, 2], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, 2], CACHE, &[0, 1, 2]).await;
    // Check that a new node can join
    let n3 = add_and_start(&c, &[CACHE], &[0, 1, 2]).await;
    c.wait_for_no_rebalance(&[0, 1, 2, n3], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, 2, n3], CACHE, &[0, 1, 2, n3]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_recovery_after_split_and_coord_leave() {
    let c = start_cluster(3, &[CACHE]).await;
    for i in 0..3 {
        c.install_view(&[i], false);
    }
    for i in 0..3 {
        c.wait_for_no_rebalance(&[i], CACHE, WAIT).await.unwrap();
    }
    // kill the coordinator
    c.kill_node(0);
    // merge the two remaining partitions
    c.install_view(&[1, 2], true);
    c.wait_for_no_rebalance(&[1, 2], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[1, 2], CACHE, &[1, 2]).await;
    // Check that a new node can join
    let n3 = add_and_start(&c, &[CACHE], &[1, 2]).await;
    c.wait_for_no_rebalance(&[1, 2, n3], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[1, 2, n3], CACHE, &[1, 2, n3]).await;
}

/// Block `handleRebalance` for rebalances with `num_members` members until `merge` fires
/// (`ClusterTopologyManagerTest.blockRebalanceStart`).
struct BlockRebalanceStart {
    num_members: usize,
    seen: Mutex<Vec<Vec<Address>>>,
    merge: Signal,
    member_sets_signal: Mutex<Vec<(Vec<Address>, Signal)>>,
}

impl BlockRebalanceStart {
    fn new(num_members: usize, merge: Signal) -> Arc<Self> {
        Arc::new(Self { num_members, seen: Mutex::new(vec![]), merge, member_sets_signal: Mutex::new(vec![]) })
    }
}

impl LocalTopologyInterceptor for BlockRebalanceStart {
    fn before_handle_rebalance(&self, cache_name: &str, topology: &CacheTopology, _view_id: i32) -> BoxFuture<'static, ()> {
        // `when(spy).handleRebalance(eq(CACHE_NAME), ...)`: other caches go through untouched
        if cache_name != CACHE {
            return Box::pin(async {});
        }
        let members = topology.members().to_vec();
        self.seen.lock().unwrap().push(members.clone());
        for (m, sig) in self.member_sets_signal.lock().unwrap().iter() {
            if *m == members {
                sig.trigger();
            }
        }
        let block = members.len() == self.num_members;
        let merge = self.merge.clone();
        Box::pin(async move {
            if block {
                merge.wait().await;
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_recovery_with_rebalance() {
    let c = start_cluster(3, &[CACHE, "other_cache"]).await;
    // Compute the merge coordinator by sorting the addresses, the same way MERGE2/3 do
    let mut order: Vec<usize> = (0..3).collect();
    order.sort_by(|a, b| {
        let (x, y) = (c.address(*a), c.address(*b));
        match (x.uuid_bits(), y.uuid_bits()) {
            (Some((m1, l1)), Some((m2, l2))) => (m1, l1).cmp(&(m2, l2)),
            _ => std::cmp::Ordering::Equal,
        }
    });
    let merge_coord = order[0];
    // create the partitions
    for i in 0..3 {
        c.install_view(&[i], false);
    }
    for i in 0..3 {
        c.wait_for_no_rebalance(&[i], CACHE, WAIT).await.unwrap();
    }
    // Disable DISCARD *only* on the merge coordinator: it can reach the new node
    let merge = Signal::new();
    let blocker_mc = BlockRebalanceStart::new(2, merge.clone());
    c.set_local_interceptor(merge_coord, Some(blocker_mc.clone()));
    let cm4 = c.add_node();
    let blocker_4 = BlockRebalanceStart::new(2, merge.clone());
    c.set_local_interceptor(cm4, Some(blocker_4));
    let two_members = vec![c.address(merge_coord), c.address(cm4)];
    let rebalance_started = Signal::new();
    blocker_mc.member_sets_signal.lock().unwrap().push((two_members.clone(), rebalance_started.clone()));
    c.install_view(&[merge_coord, cm4], false);
    c.start_node(cm4).await;
    // cm4.getCache(OTHER_CACHE_NAME) is synchronous upstream; the blockers only hold CACHE_NAME's rebalance
    c.start_cache(cm4, "other_cache").await.unwrap();
    // cm4.getCache(CACHE_NAME) is forked
    let c3 = c.clone();
    let join_main = tokio::spawn(async move { c3.start_cache(cm4, CACHE).await });
    // Waiting for the REBALANCE_START command to reach the merge coordinator
    rebalance_started.wait().await;
    // merge the partitions
    c.install_view(&[0, 1, 2, cm4], true);
    // wait for the merge to settle on the old members
    for cache in [CACHE] {
        let ok = c
            .eventually(WAIT, || {
                (0..3).all(|i| as_set(&members_of(&c, i, cache)).len() >= 3)
            })
            .await;
        assert!(ok, "merged members never reached the old nodes");
    }
    // unblock the REBALANCE_START command
    merge.trigger();
    // wait for the 4th cache to finish joining
    join_main.await.unwrap().unwrap();
    c.wait_for_no_rebalance(&[0, 1, 2, cm4], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, 2, cm4], CACHE, &[0, 1, 2, cm4]).await;
    // Check that another node can join
    let cm5 = add_and_start(&c, &[CACHE], &[0, 1, 2, cm4]).await;
    c.wait_for_no_rebalance(&[0, 1, 2, cm4, cm5], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, 2, cm4, cm5], CACHE, &[0, 1, 2, cm4, cm5]).await;
}

// -------------------------------------------------- testAbruptLeaveAfterGetStatus

/// Block `handleStatusRequest` on one node until `release` fires, announcing each request's view id.
struct BlockStatusRequest {
    seen_view: Mutex<Vec<(i32, Signal)>>,
    announce: Mutex<Option<(i32, Signal)>>,
    release: Signal,
    /// a topology (or rebalance) with this many members must never be installed
    forbidden_members: usize,
    violated: Mutex<bool>,
}

impl LocalTopologyInterceptor for BlockStatusRequest {
    fn before_handle_status_request(&self, view_id: i32) -> BoxFuture<'static, ()> {
        let sig = {
            let mut v = self.seen_view.lock().unwrap();
            let s = Signal::new();
            v.push((view_id, s.clone()));
            s
        };
        sig.trigger();
        let release = self.release.clone();
        Box::pin(async move { release.wait().await })
    }

    fn before_handle_topology_update(&self, _c: &str, topology: &CacheTopology, _v: i32) -> BoxFuture<'static, ()> {
        if topology.members().len() == self.forbidden_members {
            *self.violated.lock().unwrap() = true;
        }
        Box::pin(async {})
    }

    fn before_handle_rebalance(&self, _c: &str, topology: &CacheTopology, _v: i32) -> BoxFuture<'static, ()> {
        if topology.members().len() == self.forbidden_members {
            *self.violated.lock().unwrap() = true;
        }
        Box::pin(async {})
    }
}

/// Test that cluster recovery can finish if one of the members leaves before sending the status response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_abrupt_leave_after_get_status() {
    let c = start_cluster(3, &[CACHE]).await;
    let release = Signal::new();
    let blocker = Arc::new(BlockStatusRequest {
        seen_view: Mutex::new(vec![]),
        announce: Mutex::new(None),
        release: release.clone(),
        forbidden_members: 2,
        violated: Mutex::new(false),
    });
    let _ = blocker.announce.lock().unwrap().take();
    c.set_local_interceptor(1, Some(blocker.clone()));
    let initial = c.cache_topology(1, CACHE).unwrap();
    assert!(initial.topology_id > 0);

    // Node 0 (the coordinator) dies. Node 1 becomes coordinator and tries to call GET_STATUS
    c.kill_node(0);
    let view_after_kill = c.install_view(&[1, 2], false);
    // Wait for the GET_STATUS command and stop node 2 abruptly
    assert!(
        c.eventually(WAIT, || blocker.seen_view.lock().unwrap().iter().any(|(v, _)| *v == view_after_kill)).await,
        "no status request for view {view_after_kill}"
    );
    c.kill_node(2);
    c.install_view(&[1], false);
    release.trigger();
    // Wait for node 1 to install a view with only itself and unblock the GET_STATUS command
    c.wait_for_no_rebalance(&[1], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[1], CACHE, &[1]).await;
    // Check there was no topology update or rebalance with 2 members
    assert!(!*blocker.violated.lock().unwrap(), "a topology with 2 members was installed");
}

// -------------------------------------------------------- TopologyUnorderedJoinTest

/// Hold the first `CacheJoinCommand` for `cache` on the coordinator: announce it, wait to be let in,
/// run it, announce it ran, wait again before sending the answer
/// (`Mocks.blockInboundGlobalCommandExecution` with a `CheckPoint`).
struct HoldJoinCommand {
    cache: String,
    only_once: Mutex<bool>,
    hold_flag: Mutex<Vec<bool>>,
    joins_seen: std::sync::atomic::AtomicUsize,
    before: Signal,
    release_before: Signal,
    after: Signal,
    release_after: Signal,
}

impl DeliveryInterceptor for HoldJoinCommand {
    fn before_invoke(&self, command: &TopologyCommand) -> BoxFuture<'static, ()> {
        let is_join = matches!(command, TopologyCommand::CacheJoin { cache_name, .. } if *cache_name == self.cache);
        if is_join {
            self.joins_seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let matches_first = is_join && std::mem::replace(&mut *self.only_once.lock().unwrap(), false);
        self.hold_flag.lock().unwrap().push(matches_first);
        let (before, release) = (self.before.clone(), self.release_before.clone());
        Box::pin(async move {
            if matches_first {
                before.trigger();
                release.wait().await;
            }
        })
    }

    fn after_invoke(&self, command: &TopologyCommand) -> BoxFuture<'static, ()> {
        // only the held command pauses after running (the first CacheJoin for the cache)
        let held = matches!(command, TopologyCommand::CacheJoin { cache_name, .. } if *cache_name == self.cache)
            && self.hold_flag.lock().unwrap().iter().any(|h| *h)
            && !self.after.is_triggered();
        let (after, release) = (self.after.clone(), self.release_after.clone());
        Box::pin(async move {
            if held {
                after.trigger();
                release.wait().await;
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delay_join_response_after_rebalance_start() {
    // Create the cache on the initial two nodes
    let c = start_cluster(2, &[CACHE]).await;
    let coordinator = 0;
    // Replace to block on the join request. We allow the request to process, and unlock only after
    // another node joins and the view updates.
    let hold = Arc::new(HoldJoinCommand {
        cache: CACHE.to_string(),
        only_once: Mutex::new(true),
        hold_flag: Mutex::new(vec![]),
        joins_seen: std::sync::atomic::AtomicUsize::new(0),
        before: Signal::new(),
        release_before: Signal::new(),
        after: Signal::new(),
        release_after: Signal::new(),
    });
    c.set_delivery_interceptor(coordinator, Some(hold.clone()));

    // Add new node. Request the cache asynchronously, since we block the initial commands.
    let joiner = c.add_node();
    c.install_view(&[0, 1, joiner], false);
    c.start_node(joiner).await;
    let c2 = c.clone();
    let join_future = tokio::spawn(async move { c2.start_cache(joiner, CACHE).await });
    // We allow the join request to process.
    hold.before.wait().await;
    hold.release_before.trigger();
    // Wait the join to process on the coordinator and the rebalance request arrive at the joiner.
    hold.after.wait().await;
    // New node joins, this causes the view to update. No need to request the cache now.
    let second = c.add_node();
    let view = c.install_view(&[0, 1, joiner, second], false);
    c.start_node(second).await;
    // Wait view is installed on all nodes.
    assert!(c.eventually(WAIT, || (0..4).all(|i| c.view_of(i).0 == view)).await);
    // Release the join on the coordinator. The joiner will receive a response and retry.
    hold.release_after.trigger();
    // Wait to the state transfer to finish. This also creates the cache on the second joiner.
    c.start_cache(second, CACHE).await.unwrap();
    c.wait_for_no_rebalance(&[0, 1, joiner, second], CACHE, WAIT).await.unwrap();
    join_future.await.unwrap().unwrap();
    // The first answer was sent in a view that was already stale, so the joiner asked again
    assert!(hold.joins_seen.load(std::sync::atomic::Ordering::SeqCst) >= 3, "joins seen: {}", hold.joins_seen.load(std::sync::atomic::Ordering::SeqCst));
    // All members know the cache: ownership is on every member (REPL), where upstream asserts data
    wait_members(&c, &[0, 1, joiner, second], CACHE, &[0, 1, joiner, second]).await;
    for i in [0, 1, joiner, second] {
        let t = c.cache_topology(i, CACHE).unwrap();
        let ch = t.read_consistent_hash().unwrap().as_replicated();
        for m in t.members() {
            assert_eq!(ch.segments_for_owner(m).len(), 256);
        }
    }
}

// -------------------------------------------------- ThreeNodesReplicatedSplitAndMergeTest

fn assert_all_own_every_segment(c: &TestCluster, nodes: &[usize]) {
    for i in nodes {
        let t = c.cache_topology(*i, CACHE).unwrap();
        let ch = t.read_consistent_hash().unwrap().as_replicated();
        for m in t.members() {
            assert_eq!(ch.segments_for_owner(m).len(), 256, "{m:?} owns every segment");
        }
    }
}

async fn test_split_and_merge(p0: &[usize], p1: &[usize]) {
    let c = start_cluster(3, &[CACHE]).await;
    let all_members = as_set(&addrs(&c, &[0, 1, 2]));
    // assertStableTopologyMembers on every node, and every node AVAILABLE
    for i in 0..3 {
        assert_eq!(as_set(c.stable_topology(i, CACHE).unwrap().members()), all_members);
        assert_eq!(c.topology_node(i).ltm.cache_availability(CACHE), Some(AvailabilityMode::Available));
    }
    assert_all_own_every_segment(&c, &[0, 1, 2]);

    // splitCluster(p0, p1)
    c.install_view(p0, false);
    c.install_view(p1, false);
    c.wait_for_no_rebalance(p0, CACHE, WAIT).await.unwrap();
    c.wait_for_no_rebalance(p1, CACHE, WAIT).await.unwrap();
    // ALLOW_READ_WRITES: both partitions stay available and each only holds its own members
    wait_members(&c, p0, CACHE, p0).await;
    wait_members(&c, p1, CACHE, p1).await;
    for i in 0..3 {
        assert_eq!(c.topology_node(i).ltm.cache_availability(CACHE), Some(AvailabilityMode::Available));
    }
    assert_all_own_every_segment(&c, &[0, 1, 2]);

    // partition(0).merge(partition(1))
    let mut merged: Vec<usize> = p0.to_vec();
    merged.extend_from_slice(p1);
    c.install_view(&merged, true);
    // use set comparison as the merge view will reshuffle the order of nodes
    for i in 0..3 {
        let want = all_members.clone();
        assert!(
            c.eventually(WAIT, || c.stable_topology(i, CACHE).is_some_and(|t| as_set(t.members()) == want)).await,
            "node {i} never saw the merged stable topology"
        );
    }
    c.wait_for_no_rebalance(&[0, 1, 2], CACHE, WAIT).await.unwrap();
    wait_members(&c, &[0, 1, 2], CACHE, &[0, 1, 2]).await;
    for i in 0..3 {
        assert_eq!(c.topology_node(i).ltm.cache_availability(CACHE), Some(AvailabilityMode::Available));
    }
    // recheck key ownership
    assert_all_own_every_segment(&c, &[0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_and_merge0() {
    test_split_and_merge(&[0, 1], &[2]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_and_merge1() {
    test_split_and_merge(&[0, 2], &[1]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_and_merge2() {
    test_split_and_merge(&[1, 2], &[0]).await;
}

// ------------------------------------------------------------- ConcurrentStartWithReplTest

/// Two nodes start two caches ("r" first, then "d", standing in for the DIST cache of upstream, which
/// this port does not reach) in the four orders upstream uses.
async fn concurrent_start(in_order: bool, non_blocking_second: bool) {
    let c = TestCluster::new(config());
    let (cm1, cm2) = (c.add_node(), c.add_node());
    c.install_view(&[cm1, cm2], false);
    // cm1 is the first member: the coordinator
    c.start_node(cm1).await;
    c.start_node(cm2).await;

    // first start the "r" caches
    c.start_cache(cm1, "r").await.unwrap();
    c.start_cache(cm2, "r").await.unwrap();
    c.wait_for_no_rebalance(&[cm1, cm2], "r", WAIT).await.unwrap();
    wait_members(&c, &[cm1, cm2], "r", &[cm1, cm2]).await;

    // now the second ones
    let (first, second) = if in_order { (cm1, cm2) } else { (cm2, cm1) };
    if non_blocking_second {
        let (ca, cb) = (c.clone(), c.clone());
        let fa = tokio::spawn(async move { ca.start_cache(first, "d").await });
        let fb = tokio::spawn(async move { cb.start_cache(second, "d").await });
        fa.await.unwrap().unwrap();
        fb.await.unwrap().unwrap();
    } else {
        c.start_cache(first, "d").await.unwrap();
        c.start_cache(second, "d").await.unwrap();
    }
    c.wait_for_no_rebalance(&[cm1, cm2], "d", WAIT).await.unwrap();
    wait_members(&c, &[cm1, cm2], "d", &[cm1, cm2]).await;
    for cache in ["r", "d"] {
        assert_eq!(c.cache_topology(cm1, cache).unwrap(), c.cache_topology(cm2, cache).unwrap());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence1() {
    // C1 (r) (becomes coord), C2 (d), C1 (r), C2 (d), in the same thread
    concurrent_start(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence2() {
    // C1 (r) (becomes coord), C2 (r), C2 (d), C1 (d), in the same thread
    concurrent_start(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence3() {
    // C1 (r), C2 (r), then C1 (d), C2 (d) in separate tasks
    concurrent_start(true, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence4() {
    // C1 (r), C2 (r), then C2 (d), C1 (d) in separate tasks
    concurrent_start(false, true).await;
}
