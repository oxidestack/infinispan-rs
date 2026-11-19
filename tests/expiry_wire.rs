//! `RemoveExpiredCommand` against tests/vectors/expiry.hex: bytes the real Infinispan 14.0.35.Final
//! `GlobalMarshaller` wrote for commands built by the real `CacheImpl.removeLifespanExpired` /
//! `removeMaxIdleExpired` (with the `ZERO_LOCK_ACQUISITION_TIMEOUT` flag a read-triggered expiration adds),
//! captured by an interceptor (upstream-tools/generate-topology-vectors.sh, `ExpiryVectorGen`).

mod common;

use common::*;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::commands::object::CacheObject;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::commands::write::*;

const CACHE: &str = "mockserver-state";

fn line(name: &str) -> Vec<String> {
    vector_lines("expiry.hex").into_iter().find(|l| l[0] == name).unwrap_or_else(|| panic!("no vector {name}"))
}

fn command(name: &str) -> RemoveExpired {
    match CacheRpcCommand::from_bytes(&unhex(&line(name)[1])).unwrap_or_else(|e| panic!("{name}: {e}")).payload {
        CacheRpcPayload::Single(WriteCommand::RemoveExpired(c)) => c,
        other => panic!("{name}: {other:?}"),
    }
}

fn s(v: &str) -> CacheObject {
    CacheObject::string(v)
}

fn vw(value: &str, version: i64) -> CacheObject {
    CacheObject::User(VersionedWrapper::encode_object(&CacheObject::string(value), version).unwrap())
}

#[test]
fn every_expiry_vector_decodes_and_reencodes_byte_for_byte() {
    let mut n = 0;
    for l in vector_lines("expiry.hex") {
        if l[0].starts_with("flags:") {
            continue;
        }
        let cmd = CacheRpcCommand::from_bytes(&unhex(&l[1])).unwrap_or_else(|e| panic!("{}: {e}", l[0]));
        assert_eq!(cmd.cache_name, CACHE);
        assert_eq!(hex(&cmd.to_bytes()), l[1], "{}", l[0]);
        n += 1;
    }
    assert_eq!(n, 4);
}

#[test]
fn lifespan_expiration_matches_java() {
    let c = command("expire:lifespan-string");
    assert_eq!((c.key.clone(), c.value.clone(), c.lifespan, c.max_idle, c.topology_id), (s("k1"), s("v1"), Some(10_000), false, 7));
    assert_eq!(c.segment, 19);
    // ZERO_LOCK_ACQUISITION_TIMEOUT (the read) | SKIP_CACHE_LOAD | SKIP_XSITE_BACKUP (removeLifespanExpired)
    assert_eq!(c.flags, flags::ZERO_LOCK_ACQUISITION_TIMEOUT | flags::SKIP_CACHE_LOAD | flags::SKIP_XSITE_BACKUP);
    assert_eq!(format!("{:x}", c.flags), line("flags:expire:lifespan-string")[1]);

    // built from scratch, as the engine does
    let mine = WriteCommand::RemoveExpired(RemoveExpired {
        key: s("k1"),
        value: s("v1"),
        lifespan: Some(10_000),
        max_idle: false,
        segment: 19,
        flags: flags::ZERO_LOCK_ACQUISITION_TIMEOUT | flags::SKIP_CACHE_LOAD | flags::SKIP_XSITE_BACKUP,
        invocation_id: c.invocation_id.clone(),
        topology_id: 7,
    });
    assert_eq!(hex(&CacheRpcCommand::single(CACHE, mine).to_bytes()), line("expire:lifespan-string")[1]);
}

#[test]
fn a_versioned_user_value_is_carried_as_is() {
    let c = command("expire:lifespan-versioned");
    assert_eq!(c.key, s("k2"));
    assert_eq!(c.value, vw("v2", 1));
    assert_eq!(c.lifespan, Some(25_000));
}

#[test]
fn flags_of_a_plain_removal_have_no_zero_lock_bit() {
    let c = command("expire:lifespan-plain-flags");
    assert_eq!(c.flags, flags::SKIP_CACHE_LOAD | flags::SKIP_XSITE_BACKUP);
    assert_eq!(format!("{:x}", c.flags), line("flags:expire:lifespan-plain-flags")[1]);
}

#[test]
fn a_max_idle_expiration_has_no_lifespan() {
    let c = command("expire:maxidle");
    assert_eq!((c.lifespan, c.max_idle), (None, true));
    assert_eq!(c.flags, flags::ZERO_LOCK_ACQUISITION_TIMEOUT | flags::SKIP_CACHE_LOAD);
    assert_eq!(format!("{:x}", c.flags), line("flags:expire:maxidle")[1]);
}

#[test]
fn the_matcher_is_fixed_and_not_serialized() {
    let c = WriteCommand::RemoveExpired(command("expire:lifespan-string"));
    assert_eq!(c.value_matcher(), ValueMatcher::MatchExpectedOrNull);
    assert!(c.is_conditional());
    assert_eq!(c.command_id(), 58);
}
