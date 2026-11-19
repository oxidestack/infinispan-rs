//! The write commands, `ClusteredGetCommand` and their responses against tests/vectors/write_commands.hex:
//! bytes the real Infinispan 14.0.35.Final `GlobalMarshaller` wrote for commands built by the real
//! `CacheImpl`/`EncoderCache` and MockServer's own `InfinispanKeyValueStore` (unmodified, from the pinned
//! submodule), captured by an interceptor (upstream-tools/generate-topology-vectors.sh).

mod common;

use common::*;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::commands::compute_function::{BiFunctionMapper, CacheLambda, PUT_LAMBDA_NAME};
use infinispan::commands::metadata::{Expiry, Metadata, NumericVersion};
use infinispan::commands::object::{CacheObject, InternalCacheValue};
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload, ClusteredGet};
use infinispan::commands::write::*;
use infinispan::marshall::primitives::PrimitiveValue;
use infinispan::topology::messages::{ExceptionKind, Response, ResponseValue};
use infinispan::topology::wire::{ObjectInput, ObjectOutput};

const CACHE: &str = "mockserver-state";

fn vector(name: &str) -> Vec<u8> {
    let line = vector_lines("write_commands.hex").into_iter().find(|l| l[0] == name).unwrap_or_else(|| panic!("no vector {name}"));
    unhex(&line[1])
}

fn single(name: &str) -> WriteCommand {
    match CacheRpcCommand::from_bytes(&vector(name)).unwrap_or_else(|e| panic!("{name}: {e}")).payload {
        CacheRpcPayload::Single(c) => c,
        other => panic!("{name}: {other:?}"),
    }
}

fn vw(value: &str, version: i64) -> CacheObject {
    CacheObject::User(VersionedWrapper::encode_object(&CacheObject::string(value), version).unwrap())
}

fn s(v: &str) -> CacheObject {
    CacheObject::string(v)
}

fn id_of(c: &WriteCommand) -> CommandInvocationId {
    c.invocation_id().cloned().unwrap()
}

fn encode(c: WriteCommand) -> Vec<u8> {
    CacheRpcCommand::single(CACHE, c).to_bytes()
}

#[test]
fn every_command_vector_decodes_and_reencodes_byte_for_byte() {
    let mut n = 0;
    for line in vector_lines("write_commands.hex") {
        if line[0].starts_with("resp:") {
            continue;
        }
        let bytes = unhex(&line[1]);
        let cmd = CacheRpcCommand::from_bytes(&bytes).unwrap_or_else(|e| panic!("{}: {e}", line[0]));
        assert_eq!(cmd.cache_name, CACHE);
        assert_eq!(hex(&cmd.to_bytes()), line[1], "{}", line[0]);
        n += 1;
    }
    assert_eq!(n, 13);
}

#[test]
fn put_commands_match_java() {
    let java = single("put:plain");
    let WriteCommand::Put(p) = &java else { panic!() };
    assert_eq!(p.key, s("k1"));
    assert_eq!(p.value_matcher, ValueMatcher::MatchAlways);
    assert!(!p.put_if_absent && !p.return_entry);
    assert_eq!(p.topology_id, 7);
    assert_eq!(p.metadata, Some(Metadata::DEFAULT));
    // rebuild it from scratch with the fields the Java call implied
    let mine = WriteCommand::Put(PutKeyValue {
        key: s("k1"),
        value: vw("v1", 1),
        segment: p.segment,
        metadata: p.metadata,
        value_matcher: ValueMatcher::MatchAlways,
        invocation_id: id_of(&java),
        flags: 0,
        put_if_absent: false,
        return_entry: false,
        topology_id: 7,
    });
    assert_eq!(hex(&encode(mine)), hex(&vector("put:plain")));

    let java = single("put:lifespan");
    let WriteCommand::Put(p) = &java else { panic!() };
    assert_eq!(p.metadata.unwrap().expiry, Expiry::Expirable { lifespan: 10_000, max_idle: -1 });

    let java = single("putIfAbsent:new");
    let WriteCommand::Put(p) = &java else { panic!() };
    assert!(p.put_if_absent);
    assert_eq!(p.value_matcher, ValueMatcher::MatchExpected);
    assert!(java.is_conditional() && java.is_return_value_expected());
}

#[test]
fn compute_command_with_the_mockserver_lambda_matches_java() {
    let java = single("compute:store-put-new");
    let WriteCommand::Compute(c) = &java else { panic!() };
    assert_eq!(c.key, s("kc"));
    assert!(!c.compute_if_present);
    assert_eq!(c.function.function.impl_method_name, PUT_LAMBDA_NAME);
    assert_eq!(c.function.function.captured_args, vec![s("first")]);
    assert!(c.function.key_conversion.is_key);
    assert!(!c.function.value_conversion.is_key);
    let mine = WriteCommand::Compute(Compute {
        key: s("kc"),
        compute_if_present: false,
        function: BiFunctionMapper::for_default_cache(CacheLambda::mockserver_put(s("first"))),
        segment: c.segment,
        metadata: Some(Metadata::DEFAULT),
        invocation_id: id_of(&java),
        flags: 0,
        topology_id: 7,
    });
    assert_eq!(hex(&encode(mine)), hex(&vector("compute:store-put-new")));
}

#[test]
fn the_compute_lambda_runs_its_rust_equivalent() {
    let WriteCommand::Compute(c) = single("compute:store-put-existing") else { panic!() };
    let lambda = &c.function.function;
    // no existing entry: version 1
    let first = lambda.apply(None).unwrap().unwrap();
    let CacheObject::User(u) = &first else { panic!() };
    let w = VersionedWrapper::decode(u.bytes()).unwrap();
    assert_eq!(w.version, 1);
    // existing entry at version 2: version 3, same captured value
    let second = lambda.apply(Some(&vw("old", 2))).unwrap().unwrap();
    let CacheObject::User(u) = &second else { panic!() };
    assert_eq!(VersionedWrapper::decode(u.bytes()).unwrap().version, 3);
    // and the stream is exactly what Java's `new VersionedWrapper<>("second", 3)` serialises to
    assert_eq!(second, vw("second", 3));
    // an unknown lambda has no Rust equivalent
    let mut other = lambda.clone();
    other.impl_method_name = "lambda$other$1".into();
    assert!(other.apply(None).is_err());
}

#[test]
fn replace_remove_and_clear_match_java() {
    let java = single("replace:cas");
    let WriteCommand::Replace(r) = &java else { panic!() };
    assert_eq!(r.value_matcher, ValueMatcher::MatchExpected);
    assert_eq!(r.metadata.unwrap().version, Some(NumericVersion(1)), "VersionInterceptor stamps a NumericVersion");
    let mine = WriteCommand::Replace(Replace {
        key: s("kc"),
        old_value: vw("second", 2),
        new_value: vw("third", 3),
        return_entry: false,
        segment: r.segment,
        metadata: r.metadata,
        value_matcher: ValueMatcher::MatchExpected,
        flags: 0,
        invocation_id: id_of(&java),
        topology_id: 7,
    });
    assert_eq!(hex(&encode(mine)), hex(&vector("replace:cas")));

    let java = single("remove:key");
    let WriteCommand::Remove(r) = &java else { panic!() };
    assert_eq!(r.value_matcher, ValueMatcher::MatchAlways);
    assert!(r.value.is_null() && !java.is_conditional());
    let mine = WriteCommand::Remove(Remove {
        key: s("k3"),
        value: CacheObject::Null,
        return_entry: false,
        segment: r.segment,
        metadata: None,
        flags: 0,
        value_matcher: ValueMatcher::MatchAlways,
        invocation_id: id_of(&java),
        topology_id: 7,
    });
    assert_eq!(hex(&encode(mine)), hex(&vector("remove:key")));

    let java = single("remove:cas");
    let WriteCommand::Remove(r) = &java else { panic!() };
    assert_eq!(r.value_matcher, ValueMatcher::MatchExpected);
    assert!(java.is_conditional());

    let mine = WriteCommand::Clear(Clear { flags: 0, topology_id: 7 });
    assert_eq!(hex(&encode(mine)), hex(&vector("clear")));
    assert!(!single("clear").is_return_value_expected());
}

#[test]
fn blob_and_plain_string_values_round_trip() {
    let WriteCommand::Put(p) = single("put:null-key-value-string") else { panic!() };
    assert_eq!(p.value, CacheObject::Primitive(PrimitiveValue::Str("plain string value".into())));
    let WriteCommand::Put(b) = single("put:blob") else { panic!() };
    assert!(matches!(b.value, CacheObject::User(_)));
}

#[test]
fn clustered_get_matches_java() {
    let java = CacheRpcCommand::from_bytes(&vector("clusteredget")).unwrap();
    let mine = CacheRpcCommand {
        cache_name: CACHE.into(),
        payload: CacheRpcPayload::ClusteredGet(ClusteredGet { key: s("k1"), segment: Some(33), flags: 0, is_write: false, topology_id: 7 }),
    };
    assert_eq!(java, mine);
    assert_eq!(hex(&mine.to_bytes()), hex(&vector("clusteredget")));
}

#[test]
fn write_responses_match_java() {
    let cases: Vec<(&str, Response)> = vec![
        ("resp:successful-value", Response::Successful(Some(Box::new(ResponseValue::Object(vw("v", 9)))))),
        ("resp:successful-null", Response::Successful(None)),
        ("resp:successful-true", Response::Successful(Some(Box::new(ResponseValue::Boolean(true))))),
        ("resp:successful-false", Response::Successful(Some(Box::new(ResponseValue::Boolean(false))))),
        ("resp:unsuccessful-value", Response::Unsuccessful(Some(Box::new(ResponseValue::Object(vw("v", 9)))))),
        ("resp:unsuccessful-null", Response::Unsuccessful(None)),
        ("resp:icv-immortal", Response::Successful(Some(Box::new(ResponseValue::InternalCacheValue(InternalCacheValue { value: vw("v", 9), mortal: None }))))),
        ("resp:icv-mortal", Response::Successful(Some(Box::new(ResponseValue::InternalCacheValue(InternalCacheValue { value: vw("v", 9), mortal: Some((1_700_000_000_123, 10_000)) }))))),
        ("resp:exception-timeout", Response::Exception { kind: ExceptionKind::Generic(infinispan::topology::messages::TIMEOUT_EXCEPTION), message: Some("ISPN000299: Unable to acquire lock after 10 seconds for key k and requestor r".into()) }),
        ("resp:exception-outdated-same", Response::Exception { kind: ExceptionKind::OutdatedTopology { retry_same_topology: true }, message: None }),
        ("resp:exception-outdated-next", Response::Exception { kind: ExceptionKind::OutdatedTopology { retry_same_topology: false }, message: None }),
        ("resp:unsuccessful-false", Response::Unsuccessful(Some(Box::new(ResponseValue::Boolean(false))))),
    ];
    for (name, r) in cases {
        let java = vector(name);
        let mut o = ObjectOutput::new();
        o.write_response(&r);
        assert_eq!(hex(&o.into_bytes()), hex(&java), "{name}");
        assert_eq!(ObjectInput::new(&java).read_response().unwrap(), r, "{name}");
    }
}

/// The lambda as it crossed the wire between a real 8.0.0 node and ours: both
/// directions are the bytes the Java node's own marshaller produced for the command vectors, and the bytes our encoder
/// writes for the same lambda.
#[test]
fn the_compute_lambda_captured_from_the_mixed_cluster_wire_matches_both_encoders() {
    let lines = vector_lines("mixed_cluster_compute_lambda.hex");
    let get = |label: &str| unhex(&lines.iter().find(|l| l[0] == label).unwrap_or_else(|| panic!("no {label}"))[1]);
    let (java, rs) = (get("captured-java"), get("captured-rs"));
    assert_eq!(hex(&java), hex(&rs), "the two directions differ");
    // inside the recorded Java command vector
    let recorded = vector("compute:store-put-new");
    assert!(recorded.windows(java.len()).any(|w| w == java.as_slice()), "not inside compute:store-put-new");
    // our encoder writes the same prefix, then the captured arguments
    let mut out = ObjectOutput::new();
    out.write_lambda(&CacheLambda::mockserver_put(s("first")));
    let mine = out.into_bytes();
    assert_eq!(hex(&mine[..java.len()]), hex(&java));
    // and the wire bytes plus the arguments decode to the lambda we run
    let mut full = java.clone();
    full.extend_from_slice(&mine[java.len()..]);
    let lambda = ObjectInput::new(&full).read_lambda().unwrap();
    assert_eq!(lambda.impl_method_name, PUT_LAMBDA_NAME);
    assert_eq!(lambda.captured_args, vec![s("first")]);
}
