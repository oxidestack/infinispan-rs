#![allow(dead_code)]
//! Rust mirror of upstream-tools/topology-vectors/src/main/java/gen/Scenarios.java.

use super::*;
use infinispan::ch::address::CapacityFactors;
use infinispan::ch::replicated_consistent_hash::ReplicatedConsistentHash;
use infinispan::ch::sync_replicated_consistent_hash_factory as repl;
use infinispan::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use infinispan::topology::messages::*;

pub fn pu(i: i64) -> Address {
    Address::Persistent { msb: 0x100 + i, lsb: 0x200 + i }
}

pub fn pus(n: i64) -> Vec<Address> {
    (0..n).map(pu).collect()
}

pub fn ones(ms: &[Address]) -> CapacityFactors {
    let mut cf = CapacityFactors::default();
    for m in ms {
        cf.put(m.clone(), 1.0);
    }
    cf
}

pub fn ch_abc() -> ReplicatedConsistentHash {
    let abc = members_n(3);
    repl::create(2, 256, &abc, Some(&ones(&abc))).unwrap()
}

pub fn pending_abcd() -> ReplicatedConsistentHash {
    let abcd = members_n(4);
    let up = repl::update_members(&ch_abc(), &abcd, Some(&ones(&abcd))).unwrap();
    repl::rebalance(&up).unwrap()
}

pub fn union_ch() -> ReplicatedConsistentHash {
    repl::union(&ch_abc(), &pending_abcd()).unwrap()
}

pub fn stable() -> CacheTopology {
    CacheTopology::new(
        3,
        1,
        false,
        Some(ConsistentHash::Replicated(ch_abc())),
        None,
        None,
        Phase::NoRebalance,
        members_n(3),
        Some(pus(3)),
    )
    .unwrap()
}

pub fn rebalancing(p: Phase, id: i32) -> CacheTopology {
    CacheTopology::new(
        id,
        2,
        false,
        Some(ConsistentHash::Replicated(ch_abc())),
        Some(ConsistentHash::Replicated(pending_abcd())),
        Some(ConsistentHash::Replicated(union_ch())),
        p,
        members_n(4),
        Some(pus(4)),
    )
    .unwrap()
}

pub fn join_info(checksum: Option<i32>) -> CacheJoinInfo {
    CacheJoinInfo {
        consistent_hash_factory: ConsistentHashFactoryKind::SyncReplicated,
        num_segments: 256,
        num_owners: 2,
        timeout: 60000,
        cache_mode: CacheMode::ReplSync,
        capacity_factor: 1.0,
        persistent_uuid: Some(pu(0)),
        persistent_state_checksum: checksum,
    }
}

pub fn full_status() -> CacheStatusResponse {
    CacheStatusResponse {
        cache_join_info: Some(join_info(None)),
        cache_topology: Some(rebalancing(Phase::ReadOldWriteAll, 4)),
        stable_topology: Some(stable()),
        availability_mode: Some(AvailabilityMode::Available),
        current: Some(members_n(3)),
    }
}

pub fn manager() -> ManagerStatusResponse {
    ManagerStatusResponse::with_hash_map_order(
        vec![
            ("org.infinispan.CONFIG".into(), full_status()),
            (
                "mockserver-state".into(),
                CacheStatusResponse {
                    cache_join_info: Some(join_info(Some(42))),
                    cache_topology: Some(stable()),
                    stable_topology: Some(stable()),
                    availability_mode: Some(AvailabilityMode::DegradedMode),
                    current: Some(members_n(3)),
                },
            ),
            ("empty".into(), CacheStatusResponse::empty()),
        ],
        true,
    )
}
