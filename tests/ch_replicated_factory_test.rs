//! Port of `distribution/ch/ReplicatedConsistentHashFactoryTest` (core test tree, 14.0.35.Final):
//! even primary-owner distribution after every membership change, plus the same walk for the
//! `SyncReplicatedConsistentHashFactory` that REPL caches actually use.

use infinispan::ch::address::Address;
use infinispan::ch::replicated_consistent_hash::ReplicatedConsistentHash;
use infinispan::ch::{replicated_consistent_hash_factory as plain, sync_replicated_consistent_hash_factory as sync};

fn addr(i: i64) -> Address {
    // TestAddress(i, name): value equality on the number
    Address::Persistent { msb: 0, lsb: i }
}

fn check_distribution(ch: &ReplicatedConsistentHash, strict: bool) {
    let (mut min, mut max) = (usize::MAX, 0usize);
    for member in ch.members() {
        let primary = ch.primary_segments_for_owner(member).len();
        min = min.min(primary);
        max = max.max(primary);
        // OwnershipStatistics.getOwned: a replicated CH has every segment on every stateful member
        assert_eq!(ch.segments_for_owner(member).len(), ch.num_segments());
    }
    if strict {
        assert!(max - min <= 1, "min={min} max={max}");
    }
    let total: usize = ch.members().iter().map(|m| ch.primary_segments_for_owner(m).len()).sum();
    assert_eq!(total, ch.num_segments());
}

#[test]
fn test1_even_distribution_after_membership_changes() {
    let segments_to_test = [1, 2, 4, 8, 16, 31, 32, 33, 67, 128];
    let (a, b, c, d) = (addr(0), addr(1), addr(2), addr(3));
    let steps: Vec<Vec<Address>> = vec![
        vec![a.clone(), b.clone()],
        vec![a.clone(), b.clone(), c.clone()],
        vec![a.clone(), b.clone(), c.clone(), d.clone()],
        vec![b.clone(), c.clone(), d.clone()],
        vec![c.clone()],
    ];
    for segments in segments_to_test {
        let mut ch = plain::create(0, segments, std::slice::from_ref(&a), None).unwrap();
        check_distribution(&ch, true);
        for members in &steps {
            ch = plain::update_members(&ch, members, None).unwrap();
            ch = plain::rebalance(&ch).unwrap();
            check_distribution(&ch, true);
        }
    }
}

#[test]
fn sync_replicated_factory_keeps_every_segment_owned_through_the_same_walk() {
    let (a, b, c, d) = (addr(0), addr(1), addr(2), addr(3));
    let steps: Vec<Vec<Address>> = vec![
        vec![a.clone(), b.clone()],
        vec![a.clone(), b.clone(), c.clone()],
        vec![a.clone(), b.clone(), c.clone(), d.clone()],
        vec![b.clone(), c.clone(), d.clone()],
        vec![c.clone()],
    ];
    for segments in [1, 2, 4, 8, 16, 31, 32, 33, 67, 128, 256] {
        let mut ch = sync::create(0, segments, std::slice::from_ref(&a), None).unwrap();
        check_distribution(&ch, false);
        for members in &steps {
            ch = sync::update_members(&ch, members, None).unwrap();
            ch = sync::rebalance(&ch).unwrap();
            check_distribution(&ch, false);
        }
    }
}
