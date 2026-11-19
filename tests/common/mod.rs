#![allow(dead_code)]
//! Shared helpers for the vector-driven integration tests.

use infinispan::ch::address::{Address, CapacityFactors};

pub fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn parse_members(s: &str) -> Vec<Address> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(|p| {
            let (m, l) = p.split_once(':').unwrap();
            Address::uuid(m.parse().unwrap(), l.parse().unwrap())
        })
        .collect()
}

pub fn parse_cf(members: &[Address], s: &str) -> Option<CapacityFactors> {
    if s == "-" {
        return None;
    }
    let mut cf = CapacityFactors::default();
    for (a, f) in members.iter().zip(s.split(',')) {
        cf.put(a.clone(), f.parse().unwrap());
    }
    Some(cf)
}

pub fn vector_lines(name: &str) -> Vec<Vec<String>> {
    let path = format!("{}/tests/vectors/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(path).unwrap().lines().map(|l| l.split('\t').map(String::from).collect()).collect()
}

/// Mirrors `gen.Marshal.member(int)`.
pub fn member(i: i64) -> Address {
    let msb = 0x6a09e667f3bcc908u64.wrapping_mul((i + 1) as u64) ^ (0x243f6a8885a308d3u64 >> i);
    let lsb = 0xbb67ae8584caa73bu64.wrapping_mul((i + 3) as u64).wrapping_add(i as u64);
    Address::uuid(msb as i64, lsb as i64)
}

pub fn members_n(n: i64) -> Vec<Address> {
    (0..n).map(member).collect()
}
pub mod cluster_listener;
pub mod repl;
pub mod st_env;
pub mod scenarios;
