#![allow(dead_code)]
//! Helpers for the REPL_SYNC data-path ports.

use infinispan::cache::testing::ReplCluster;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::commands::object::CacheObject;

pub fn s(v: &str) -> CacheObject {
    CacheObject::string(v)
}

pub fn vw(value: &str, version: i64) -> CacheObject {
    CacheObject::User(VersionedWrapper::encode_object(&s(value), version).unwrap())
}

/// A key whose primary owner is node `idx` in the cluster's current topology. Upstream's tests assume
/// the primary is `cache(0)` (their addresses and CH make that so for "key"); here the primary is a
/// hash of random addresses, so the key is picked instead.
pub fn key_with_primary(c: &ReplCluster, cache: &str, idx: usize) -> CacheObject {
    for i in 0..10_000 {
        let k = s(&format!("key{i}"));
        let (p, _) = c.cache(0, cache).engine().owners_of(&k).unwrap();
        if p == c.address(idx) {
            return k;
        }
    }
    panic!("no key with primary on node {idx}");
}
