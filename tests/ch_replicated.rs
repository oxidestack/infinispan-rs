//! `SyncReplicatedConsistentHashFactory` + `ReplicatedConsistentHash` wire format against
//! tests/vectors/rch.hex: bytes the real Infinispan 14.0.35.Final `GlobalMarshaller` wrote for the
//! unmodified Java classes (upstream-tools/generate-topology-vectors.sh).

mod common;

use common::*;
use infinispan::ch::address::{Address, CapacityFactors};
use infinispan::ch::replicated_consistent_hash::ReplicatedConsistentHash;
use infinispan::ch::sync_replicated_consistent_hash_factory as repl;
use infinispan::topology::wire::{ObjectInput, ObjectOutput};

/// `segs;members;cf`
fn create_from(spec: &str) -> (Vec<Address>, Option<CapacityFactors>, ReplicatedConsistentHash) {
    let p: Vec<&str> = spec.split(';').collect();
    let members = parse_members(p[1]);
    let cf = parse_cf(&members, p[2]);
    let ch = repl::create(2, p[0].parse().unwrap(), &members, cf.as_ref()).unwrap();
    (members, cf, ch)
}

fn serialise(ch: &ReplicatedConsistentHash) -> String {
    let mut o = ObjectOutput::new();
    o.write_replicated_consistent_hash(ch);
    hex(&o.into_bytes())
}

#[test]
fn rch_bytes_match_java_for_create_update_and_union() {
    let mut n = (0, 0, 0);
    for line in vector_lines("rch.hex") {
        let (op, spec, java_hex) = (&line[0], &line[1], &line[2]);
        let ch = match op.as_str() {
            "create" => {
                n.0 += 1;
                create_from(spec).2
            }
            "update" => {
                n.1 += 1;
                let (base, new) = spec.split_once('|').unwrap();
                let (_, _, b) = create_from(base);
                let nm: Vec<&str> = new.split(';').collect();
                let new_members = parse_members(nm[0]);
                let ncf = parse_cf(&new_members, nm[1]);
                repl::update_members(&b, &new_members, ncf.as_ref()).unwrap()
            }
            "union" => {
                n.2 += 1;
                let (a, b) = spec.split_once('&').unwrap();
                let (_, _, ca) = create_from(a);
                let (_, _, cb) = create_from(&format!("256;{b}"));
                repl::union(&ca, &cb).unwrap()
            }
            other => panic!("unknown op {other}"),
        };
        assert_eq!(&serialise(&ch), java_hex, "{op} {spec}");

        // Reading Java's bytes gives back the same CH, and re-writing it is the identity.
        let bytes = unhex(java_hex);
        let mut inp = ObjectInput::new(&bytes);
        let back = inp.read_replicated_consistent_hash().unwrap();
        assert_eq!(back, ch, "{op} {spec}");
        assert_eq!(inp.r.remaining(), 0);
        assert_eq!(&serialise(&back), java_hex, "re-serialise {op} {spec}");
    }
    assert!(n.0 >= 40 && n.1 == 5 && n.2 == 8, "{n:?}");
}

#[test]
fn members_without_state_are_the_zero_capacity_and_new_members() {
    let a = Address::uuid(1, 10);
    let b = Address::uuid(2, 20);
    let mut cf = CapacityFactors::default();
    cf.put(a.clone(), 1.0);
    cf.put(b.clone(), 0.0);
    let ch = repl::create(2, 256, &[a.clone(), b.clone()], Some(&cf)).unwrap();
    assert_eq!(ch.members_without_state(), std::slice::from_ref(&b));
    assert_eq!(ch.num_owners(), 1);
    // every segment is primary-owned by the stateful member, and only it is an owner
    assert!(ch.primary_owners().iter().all(|&p| p == 0));
    assert_eq!(ch.locate_owners_for_segment(7), vec![a.clone()]);
    assert!(ch.segments_for_owner(&b).is_empty());
    assert_eq!(ch.segments_for_owner(&a).len(), 256);

    // update: a joiner has no state until rebalance; b was already a member, so it keeps state
    let c = Address::uuid(3, 30);
    let mut cf2 = CapacityFactors::default();
    for m in [&a, &b, &c] {
        cf2.put(m.clone(), 1.0);
    }
    let up = repl::update_members(&ch, &[a.clone(), b.clone(), c.clone()], Some(&cf2)).unwrap();
    assert_eq!(up.members_without_state(), std::slice::from_ref(&c));
}

#[test]
fn invalid_replicated_capacity_factor_is_rejected() {
    let a = Address::uuid(1, 10);
    let b = Address::uuid(2, 20);
    let mut cf = CapacityFactors::default();
    cf.put(a.clone(), 1.0);
    cf.put(b.clone(), 0.5);
    assert!(repl::create(2, 256, &[a, b], Some(&cf)).unwrap_err().starts_with("Invalid replicated cache capacity factor"));
}
