//! Port of `statetransfer/StateProviderTest` (infinispan-core 14.0.35.Final), `test1` and `test2`.
//!
//! The upstream test is a DIST cache of six members with `DefaultConsistentHashFactory`; a REPL_SYNC cache
//! reaches `SyncReplicatedConsistentHashFactory`, so the same member changes (A and F leave, G joins) run
//! over that factory. The `getTransactionsForSegments` assertions are transactional state transfer, which
//! a non-transactional REPL cache never does, and are not ported. The mocks of the data container and the
//! publisher are [`MockEnv`].

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::st_env::{Answer, MockEnv};
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::statetransfer::chunk::InternalCacheEntry;
use infinispan::statetransfer::int_set::IntSet;
use infinispan::statetransfer::provider::StateProvider;
use infinispan::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use infinispan::topology::messages::ConsistentHashFactoryKind::SyncReplicated;

const NUM_SEGMENTS: i32 = 60;
const TIMEOUT: Duration = Duration::from_secs(30);

fn addr(i: u64) -> Address {
    Address::uuid(0x1000 + i as i64, 0x2000 + i as i64)
}

fn topology(id: i32, ch: &ConsistentHash, phase: Phase) -> CacheTopology {
    CacheTopology::new(id, 1, false, Some(ch.clone()), Some(ch.clone()), Some(ch.clone()), phase, ch.members().to_vec(), None).unwrap()
}

fn setup(entries: Vec<(i32, &str, &str)>) -> (Arc<StateProvider>, Arc<MockEnv>, Vec<Address>, ConsistentHash, ConsistentHash) {
    let members1: Vec<Address> = (0..6).map(addr).collect(); // A..F
    let mut members2 = members1.clone();
    members2.retain(|a| *a != members1[0] && *a != members1[5]);
    members2.push(addr(6)); // G
    let ch1 = SyncReplicated.create(2, NUM_SEGMENTS, &members1, None).unwrap();
    let ch2 = SyncReplicated.update_members(&ch1, &members2, None).unwrap();
    let env = MockEnv::new(members1[0].clone(), Answer::Never);
    for (seg, k, v) in entries {
        env.entries.lock().unwrap().entry(seg).or_default().push(InternalCacheEntry::immortal(CacheObject::string(k), CacheObject::string(v)));
    }
    let provider = StateProvider::new(env.clone(), "testCache", 512, TIMEOUT);
    (provider, env, members1, ch1, ch2)
}

async fn run(is_rebalance_second: bool) {
    let (provider, env, m, ch1, ch2) = setup(vec![(0, "key1", "value1"), (0, "key2", "value2")]);
    let (a, d, f) = (&m[0], &m[3], &m[5]);
    let _ = a;
    provider.on_topology_update(&topology(1, &ch1, Phase::ReadOldWriteAll));

    provider.start_outbound_transfer(f.clone(), 1, IntSet::of([0]), true);
    assert!(provider.is_state_transfer_in_progress());
    // the transfer pushed a StateResponse for segment 0 to F, with both entries
    tokio::time::sleep(Duration::from_millis(50)).await;
    let invoked = env.invoked.lock().unwrap().clone();
    assert_eq!(invoked.len(), 1);
    assert_eq!(&invoked[0].0, f);

    // F is no longer a member of the write hash: its transfer is cancelled
    let _ = is_rebalance_second;
    provider.on_topology_update(&topology(2, &ch2, Phase::ReadOldWriteAll));
    assert!(!provider.is_state_transfer_in_progress());

    provider.start_outbound_transfer(d.clone(), 1, IntSet::of([0]), true);
    assert!(provider.is_state_transfer_in_progress());

    provider.stop();
    assert!(!provider.is_state_transfer_in_progress());
}

#[tokio::test(flavor = "multi_thread")]
async fn test1() {
    run(true).await;
}

/// `test2` differs only in the keys it serves (segments 0 and 1) and in `onTopologyUpdate(..., false)`.
#[tokio::test(flavor = "multi_thread")]
async fn test2() {
    let (provider, env, m, ch1, ch2) = setup(vec![(0, "key1", "value1"), (0, "key2", "value2"), (1, "key3", "value3"), (1, "key4", "value4")]);
    let (d, e, f) = (&m[3], &m[4], &m[5]);
    let _ = d;
    provider.on_topology_update(&topology(1, &ch1, Phase::ReadOldWriteAll));
    provider.start_outbound_transfer(f.clone(), 1, IntSet::of([0]), true);
    assert!(provider.is_state_transfer_in_progress());
    // only segment 0 was requested, and only its two entries are sent, as one StateResponse with the last chunk
    tokio::time::sleep(Duration::from_millis(50)).await;
    let invoked = env.invoked.lock().unwrap().clone();
    assert_eq!(invoked.len(), 1);
    let infinispan::commands::rpc::CacheRpcPayload::StateResponse(r) = &invoked[0].1.payload else { panic!() };
    assert_eq!(r.chunks.len(), 1);
    assert_eq!((r.chunks[0].segment, r.chunks[0].entries.len(), r.chunks[0].is_last), (0, 2, true));
    assert!(r.apply_state && !r.push_transfer);

    provider.on_topology_update(&topology(2, &ch2, Phase::ReadOldWriteAll));
    assert!(!provider.is_state_transfer_in_progress());
    provider.start_outbound_transfer(e.clone(), 1, IntSet::of([0]), true);
    assert!(provider.is_state_transfer_in_progress());
    provider.stop();
    assert!(!provider.is_state_transfer_in_progress());
}
