//! `VersionedWrapper.equals` (version and `Objects.equals(value)`) and the identity of a value class
//! without `equals`: the semantics that decide `replace(k, old, new)` / `remove(k, v)`.

mod common;

use common::repl::*;
use infinispan::cache::equality::java_equals;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::commands::object::CacheObject;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::commands::write::WriteCommand;

fn blob() -> CacheObject {
    let line = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/write_commands.hex")).unwrap();
    let hex = line.lines().find_map(|l| l.strip_prefix("put:blob\t")).unwrap();
    let bytes: Vec<u8> = (0..hex.len() / 2).map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap()).collect();
    match CacheRpcCommand::from_bytes(&bytes).unwrap().payload {
        CacheRpcPayload::Single(WriteCommand::Put(p)) => p.value,
        other => panic!("{other:?}"),
    }
}

#[test]
fn versioned_wrapper_equals_compares_version_and_value() {
    // two separately built (deserialised) instances
    assert!(java_equals(&vw("a", 1), &vw("a", 1)));
    assert!(!java_equals(&vw("a", 1), &vw("a", 2)), "version differs");
    assert!(!java_equals(&vw("a", 1), &vw("b", 1)), "value differs");
    assert!(java_equals(&CacheObject::Null, &CacheObject::Null));
    assert!(!java_equals(&CacheObject::Null, &vw("a", 1)));
    assert!(java_equals(&s("x"), &s("x")) && !java_equals(&s("x"), &s("y")));
}

#[test]
fn a_value_class_without_equals_is_only_equal_to_the_same_instance() {
    let b1 = blob();
    let b2 = blob(); // the same bytes, deserialised again: another instance
    assert!(!java_equals(&b1, &b2), "Blob has no equals(): distinct instances are different");
    assert!(java_equals(&b1, &b1.clone()), "a clone of the local handle is the same instance");
    // inside a VersionedWrapper the payload's equals decides, so a wrapper of such a payload only matches itself
    let w1 = {
        let CacheObject::User(u) = &b1 else { panic!() };
        let mut vwv = VersionedWrapper { version: 1, value: infinispan::marshall::java_serial::decode_one(u.bytes()).unwrap() };
        vwv.version = 1;
        CacheObject::User(vwv.encode())
    };
    let w2 = match &w1 {
        CacheObject::User(u) => CacheObject::User(infinispan::commands::object::UserValue::new(u.bytes().to_vec())),
        _ => unreachable!(),
    };
    assert!(java_equals(&w1, &w1.clone()));
    assert!(!java_equals(&w1, &w2), "same bytes, different instance, payload without equals");
}
