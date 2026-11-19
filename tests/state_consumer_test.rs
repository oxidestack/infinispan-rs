//! Port of `statetransfer/StateConsumerTest` (infinispan-core 14.0.35.Final).
//!
//! Upstream builds a DIST_SYNC transactional cache and mocks `RpcManager`, the interceptor chain, the data
//! container and the commands factory. Here the cache is REPL_SYNC (`SyncReplicatedConsistentHashFactory`)
//! and the same mocks are [`MockEnv`]. Two adaptations follow from that:
//!
//! * in a replicated cache every member owns every segment, so a rebalance only moves segments to a *joining*
//!   node. The consumer under test is that joiner (upstream's is an existing member that gains segments).
//! * upstream records the requested segments from `StateTransferGetTransactionsCommand`s, which it sends to
//!   all sources at once; a non-transactional cache only sends `StateTransferStartCommand`, one source at a
//!   time (`stateRequestExecutor` has one thread), so the test answers each request as it arrives.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::st_env::{Answer, MockEnv};
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::statetransfer::chunk::{InternalCacheEntry, StateChunk};
use infinispan::statetransfer::commit_manager::{CommitFlag, CommitManager};
use infinispan::statetransfer::consumer::StateConsumer;
use infinispan::statetransfer::lock::{DistributionManager, StateTransferLock};
use infinispan::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use infinispan::topology::messages::ConsistentHashFactoryKind::SyncReplicated;

const NUM_SEGMENTS: i32 = 40;

fn addr(i: u64) -> Address {
    Address::uuid(0x3000 + i as i64, 0x4000 + i as i64)
}

struct Fixture {
    consumer: Arc<StateConsumer>,
    env: Arc<MockEnv>,
    commit: Arc<CommitManager>,
    /// members, the consumer's node last
    ch2: ConsistentHash,
    ch3: ConsistentHash,
    ch23: ConsistentHash,
    members: Vec<Address>,
}

fn fixture() -> Fixture {
    // three members and the consumer's node, which joins
    let all: Vec<Address> = (0..4).map(addr).collect();
    let current: Vec<Address> = all[..3].to_vec();
    let ch1 = SyncReplicated.create(2, NUM_SEGMENTS, &all, None).unwrap();
    // node 3 (the consumer) is not a member of the current hash
    let ch2 = SyncReplicated.update_members(&ch1, &current, None).unwrap();
    let joined = SyncReplicated.update_members(&ch2, &all, None).unwrap();
    let ch3 = SyncReplicated.rebalance(&joined).unwrap();
    let ch23 = SyncReplicated.union(&ch2, &ch3).unwrap();
    let env = MockEnv::new(all[3].clone(), Answer::Successful);
    let commit = Arc::new(CommitManager::new());
    let consumer = StateConsumer::new(env.clone(), "test-cache", NUM_SEGMENTS, Duration::from_secs(30), commit.clone(), DistributionManager::new(), Arc::new(StateTransferLock::new()));
    Fixture { consumer, env, commit, ch2, ch3, ch23, members: all }
}

fn no_rebalance(f: &Fixture, id: i32, rebalance_id: i32, ch: &ConsistentHash) {
    let t = CacheTopology::new(id, rebalance_id, false, Some(ch.clone()), None, None, Phase::NoRebalance, ch.members().to_vec(), None).unwrap();
    drop(f.consumer.on_topology_update(t, false));
}

fn rebalance_start(f: &Fixture, id: i32, rebalance_id: i32) {
    let t = CacheTopology::new(id, rebalance_id, false, Some(f.ch2.clone()), Some(f.ch3.clone()), Some(f.ch23.clone()), Phase::ReadOldWriteAll, f.ch23.members().to_vec(), None).unwrap();
    drop(f.consumer.on_topology_update(t, true));
}

/// `assertRebalanceStart`: every segment this node gained is in flight.
async fn assert_rebalance_start(f: &Fixture) {
    let me = &f.members[3];
    let old: BTreeSet<usize> = f.ch2.as_replicated().segments_for_owner(me).into_iter().collect();
    let new: BTreeSet<usize> = f.ch23.as_replicated().segments_for_owner(me).into_iter().collect();
    let added: BTreeSet<usize> = new.difference(&old).copied().collect();
    assert_eq!(added.len(), NUM_SEGMENTS as usize, "the joiner gains every segment");
    assert!(f.consumer.has_active_transfers());
    assert_eq!(f.consumer.inflight_request_count(), added.len());
    // the first source was asked for its segments (the others wait their turn)
    eventually(|| !f.env.start_requests().is_empty()).await;
}

async fn eventually(mut c: impl FnMut() -> bool) {
    for _ in 0..400 {
        if c() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

/// `completeAndCheckRebalance`: apply an empty last chunk for every requested segment and watch the in-flight
/// count drop one by one; requests of the next source are answered as they come.
async fn complete_and_check_rebalance(f: &Fixture, topology_id: i32) {
    let mut inflight = f.consumer.inflight_request_count();
    assert_eq!(inflight, NUM_SEGMENTS as usize);
    let mut done = 0;
    while done < f.env.start_requests().len().max(1) || f.consumer.has_active_transfers() {
        eventually(|| f.env.start_requests().len() > done || !f.consumer.has_active_transfers()).await;
        if !f.consumer.has_active_transfers() {
            break;
        }
        let (source, segments) = f.env.start_requests()[done].clone();
        done += 1;
        for s in segments {
            f.consumer.apply_state(&source, topology_id, false, vec![StateChunk { segment: s, entries: vec![], is_last: true }]).await;
            inflight -= 1;
            assert_eq!(f.consumer.inflight_request_count(), inflight);
        }
    }
    assert_eq!(f.consumer.inflight_request_count(), 0);
    eventually(|| !f.consumer.has_active_transfers()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_recover_during_state_transfer() {
    let f = fixture();
    assert!(!f.consumer.has_active_transfers());

    // the joiner starts out with the topology it joined in, without itself
    no_rebalance(&f, 1, 1, &f.ch2);
    assert!(!f.consumer.has_active_transfers());

    // start a rebalance
    rebalance_start(&f, 2, 2);
    assert_rebalance_start(&f).await;

    // simulate a cluster state recovery and return to ch2, delivered twice concurrently
    let c2 = f.consumer.clone();
    let ch2 = f.ch2.clone();
    let other = tokio::task::spawn_blocking(move || {
        let t = CacheTopology::new(3, 2, false, Some(ch2.clone()), None, None, Phase::NoRebalance, ch2.members().to_vec(), None).unwrap();
        drop(c2.on_topology_update(t, false));
    });
    no_rebalance(&f, 3, 2, &f.ch2);
    other.await.unwrap();
    assert!(!f.consumer.has_active_transfers());
    // the transfers were cancelled at their sources
    assert!(!f.env.cancels().is_empty());

    // restart the rebalance
    f.env.invoked.lock().unwrap().clear();
    rebalance_start(&f, 4, 4);
    assert_rebalance_start(&f).await;

    complete_and_check_rebalance(&f, 4).await;
    f.consumer.stop();
}

/// Reproducer for ISPN-14982: a chunk whose application is still in progress when the rebalance restarts.
#[tokio::test(flavor = "multi_thread")]
async fn test_join_during_state_transfer() {
    let f = fixture();
    let gate = f.env.block_apply();
    let _ = gate;

    // initial topology
    no_rebalance(&f, 21, 7, &f.ch2);
    assert!(!f.consumer.has_active_transfers());

    rebalance_start(&f, 22, 8);
    assert_rebalance_start(&f).await;

    // a chunk with an entry arrives; its application blocks
    eventually(|| !f.env.start_requests().is_empty()).await;
    let (source, segments) = f.env.start_requests()[0].clone();
    let consumer = f.consumer.clone();
    let blocked = tokio::spawn(async move {
        consumer
            .apply_state(&source, 22, false, vec![StateChunk { segment: segments[0], entries: vec![InternalCacheEntry::immortal(CacheObject::string("a"), CacheObject::string("b"))], is_last: true }])
            .await;
    });

    // merge view update
    no_rebalance(&f, 23, 9, &f.ch2);

    // restart the rebalance
    f.env.invoked.lock().unwrap().clear();
    rebalance_start(&f, 24, 10);
    assert_rebalance_start(&f).await;

    // let the apply state complete
    f.env.open_apply();
    blocked.await.unwrap();

    complete_and_check_rebalance(&f, 24).await;
    f.consumer.stop();
}

// ---- behaviour of the consumer that the two upstream tests do not exercise

#[tokio::test(flavor = "multi_thread")]
async fn state_of_an_old_rebalance_is_discarded_and_tracking_stops_with_the_transfer() {
    let f = fixture();
    no_rebalance(&f, 1, 1, &f.ch2);
    rebalance_start(&f, 2, 2);
    assert!(f.commit.is_tracking(CommitFlag::PutForStateTransfer));
    eventually(|| !f.env.start_requests().is_empty()).await;
    let (source, segments) = f.env.start_requests()[0].clone();
    // an answer to a request of an older topology is ignored
    f.consumer.apply_state(&source, 1, false, vec![StateChunk { segment: segments[0], entries: vec![InternalCacheEntry::immortal(CacheObject::string("x"), CacheObject::string("y"))], is_last: true }]).await;
    assert!(f.env.applied.lock().unwrap().is_empty());
    assert_eq!(f.consumer.inflight_request_count(), NUM_SEGMENTS as usize);
    complete_and_check_rebalance(&f, 2).await;
    // all segments received: key tracking ended
    assert!(!f.commit.is_tracking(CommitFlag::PutForStateTransfer));
    f.consumer.stop();
}
