//! `SyncConsistentHashFactory` against tests/vectors/sync_ch.hex (unmodified Infinispan
//! 14.0.35.Final output, upstream-tools/generate-topology-vectors.sh).

use infinispan::ch::address::{Address, CapacityFactors};
use infinispan::ch::default_consistent_hash::DefaultConsistentHash;
use infinispan::ch::sync_consistent_hash_factory as sync;

fn parse_members(s: &str) -> Vec<Address> {
    s.split(',')
        .map(|p| {
            let (m, l) = p.split_once(':').unwrap();
            Address::uuid(m.parse().unwrap(), l.parse().unwrap())
        })
        .collect()
}

fn parse_cf(members: &[Address], s: &str) -> Option<CapacityFactors> {
    if s == "-" {
        return None;
    }
    let mut cf = CapacityFactors::default();
    for (a, f) in members.iter().zip(s.split(',')) {
        cf.put(a.clone(), f.parse().unwrap());
    }
    Some(cf)
}

fn owners_string(ch: &DefaultConsistentHash, members: &[Address]) -> String {
    (0..ch.num_segments())
        .map(|s| {
            ch.locate_owners_for_segment(s)
                .iter()
                .map(|o| members.iter().position(|m| m == o).unwrap().to_string())
                .collect::<Vec<_>>()
                .join(".")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// `owners;segments;members;cf`
fn parse_spec(spec: &str) -> (i32, i32, Vec<Address>, Option<CapacityFactors>) {
    let p: Vec<&str> = spec.split(';').collect();
    let members = parse_members(p[2]);
    let cf = parse_cf(&members, p[3]);
    (p[0].parse().unwrap(), p[1].parse().unwrap(), members, cf)
}

#[test]
fn create_update_members_and_rebalance_match_java() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/sync_ch.hex")).unwrap();
    let (mut creates, mut updates, mut rebalances) = (0, 0, 0);
    for line in text.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        match f[0] {
            "create" => {
                let (owners, segs, members, cf) = parse_spec(f[1]);
                let ch = sync::create(owners, segs, &members, cf.as_ref()).unwrap();
                assert_eq!(owners_string(&ch, &members), f[2], "{line}");
                creates += 1;
            }
            "update" | "rebalance" => {
                let (base, new) = f[1].split_once('|').unwrap();
                let (owners, segs, bm, bcf) = parse_spec(base);
                let nm_spec = format!("{owners};{segs};{new}");
                let (_, _, nm, ncf) = parse_spec(&nm_spec);
                let b = sync::create(owners, segs, &bm, bcf.as_ref()).unwrap();
                let u = sync::update_members(&b, &nm, ncf.as_ref()).unwrap();
                if f[0] == "update" {
                    assert_eq!(owners_string(&u, &nm), f[2], "{line}");
                    updates += 1;
                } else {
                    let r = sync::rebalance(&u).unwrap();
                    assert_eq!(owners_string(&r, &nm), f[2], "{line}");
                    rebalances += 1;
                }
            }
            other => panic!("unknown kind {other}"),
        }
    }
    assert!(creates > 200 && updates >= 14 && rebalances >= 14, "{creates} {updates} {rebalances}");
}

#[test]
fn invalid_capacity_factors_are_rejected_like_upstream() {
    let ms = vec![Address::uuid(1, 2), Address::uuid(3, 4)];
    let mut cf = CapacityFactors::default();
    cf.put(ms[0].clone(), 0.0);
    cf.put(ms[1].clone(), 0.0);
    assert_eq!(
        sync::create(2, 256, &ms, Some(&cf)).unwrap_err(),
        "There must be at least one node with a non-zero capacity factor"
    );
    let mut cf = CapacityFactors::default();
    cf.put(ms[0].clone(), 1.0);
    assert!(sync::create(2, 256, &ms, Some(&cf)).unwrap_err().starts_with("Invalid capacity factor for node"));
}
