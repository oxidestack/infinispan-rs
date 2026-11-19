//! State transfer commands, chunks and entries against tests/vectors/state_transfer.hex: bytes the real
//! Infinispan 14.0.35.Final `GlobalMarshaller` wrote (upstream-tools/generate-topology-vectors.sh).

mod common;

use common::*;
use infinispan::commands::object::CacheObject;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload, StateResponse, StateTransferSegments};
use infinispan::statetransfer::chunk::{InternalCacheEntry, StateChunk};
use infinispan::statetransfer::int_set::{IntSet, IntSetClass};

const CACHE: &str = "mockserver-state";

fn line(name: &str) -> String {
    vector_lines("state_transfer.hex").into_iter().find(|l| l[0] == name).unwrap_or_else(|| panic!("no vector {name}"))[1].clone()
}

fn decode(name: &str) -> CacheRpcCommand {
    CacheRpcCommand::from_bytes(&unhex(&line(name))).unwrap_or_else(|e| panic!("{name}: {e}"))
}

#[test]
fn every_command_vector_decodes_and_reencodes_except_the_two_class_variants() {
    let mut n = 0;
    for l in vector_lines("state_transfer.hex") {
        if l[0].starts_with("probe:") {
            continue;
        }
        let cmd = CacheRpcCommand::from_bytes(&unhex(&l[1])).unwrap_or_else(|e| panic!("{}: {e}", l[0]));
        assert_eq!(cmd.cache_name, CACHE);
        // an empty ArrayList chunk never occurs in upstream (only Collections.emptyList()); it decodes to the same chunk
        if l[0] == "chunk:empty-arraylist-last" {
            assert_eq!(cmd, decode("chunk:empty-last"));
        } else {
            assert_eq!(hex(&cmd.to_bytes()), l[1], "{}", l[0]);
        }
        n += 1;
    }
    assert_eq!(n, 14);
}

#[test]
fn start_and_cancel_segments() {
    let CacheRpcPayload::StateStart(s) = decode("start:all").payload else { panic!() };
    assert_eq!((s.topology_id, s.segments.len()), (7, 256));
    assert_eq!(s.segments, IntSet::range(256));
    let CacheRpcPayload::StateStart(s) = decode("start:few").payload else { panic!() };
    assert_eq!(s.segments.iter().collect::<Vec<_>>(), vec![0, 5, 255]);
    let mine = CacheRpcCommand { cache_name: CACHE.into(), payload: CacheRpcPayload::StateCancel(StateTransferSegments { topology_id: 7, segments: IntSet::of([3, 4]) }) };
    assert_eq!(hex(&mine.to_bytes()), line("cancel:two"));
    let CacheRpcPayload::StateStart(s) = decode("start:singleton-class").payload else { panic!() };
    assert_eq!((s.segments.class(), s.segments.iter().collect::<Vec<_>>()), (IntSetClass::Singleton, vec![42]));
    let CacheRpcPayload::StateStart(s) = decode("start:empty-class").payload else { panic!() };
    assert!(s.segments.is_empty());
    let mine = CacheRpcCommand::from_bytes(&unhex(&line("start:sparse-high"))).unwrap();
    let CacheRpcPayload::StateStart(s) = &mine.payload else { panic!() };
    assert_eq!(s.segments.iter().collect::<Vec<_>>(), vec![200, 201, 255]);
}

#[test]
fn chunk_of_two_immortal_entries_is_built_identically() {
    let CacheRpcPayload::StateResponse(java) = decode("chunk:immortal-two").payload else { panic!() };
    assert!(java.apply_state && !java.push_transfer);
    assert_eq!(java.chunks.len(), 1);
    assert_eq!(java.chunks[0].segment, 11);
    assert!(!java.chunks[0].is_last);
    let keys: Vec<_> = java.chunks[0].entries.iter().map(|e| e.key.clone()).collect();
    assert_eq!(keys, vec![CacheObject::string("k1"), CacheObject::string("k2")]);
    let mine = CacheRpcCommand {
        cache_name: CACHE.into(),
        payload: CacheRpcPayload::StateResponse(StateResponse {
            topology_id: 7,
            chunks: vec![StateChunk { segment: 11, entries: java.chunks[0].entries.clone(), is_last: false }],
            apply_state: true,
            push_transfer: false,
        }),
    };
    assert_eq!(hex(&mine.to_bytes()), line("chunk:immortal-two"));
}

#[test]
fn every_entry_kind_keeps_its_timestamps_and_expiry() {
    let CacheRpcPayload::StateResponse(r) = decode("chunk:every-entry-kind").payload else { panic!() };
    let e: &Vec<InternalCacheEntry> = &r.chunks[0].entries;
    assert_eq!(e.len(), 8);
    assert!(e[0].metadata.is_none() && e[0].lifespan == -1 && e[0].max_idle == -1);
    assert_eq!((e[1].created, e[1].lifespan), (1_700_000_000_123, 10_000));
    assert_eq!((e[2].last_used, e[2].max_idle), (1_700_000_000_456, 5_000));
    assert_eq!((e[3].created, e[3].lifespan, e[3].last_used, e[3].max_idle), (1_700_000_000_123, 10_000, 1_700_000_000_456, 5_000));
    assert_eq!(e[4].metadata.unwrap().version.unwrap().0, 0x0007000100000003);
    assert_eq!((e[5].created, e[5].lifespan), (1_700_000_000_789, 60_000));
    assert_eq!((e[6].last_used, e[6].max_idle), (1_700_000_000_321, 7_000));
    assert_eq!((e[7].created, e[7].lifespan, e[7].last_used, e[7].max_idle), (1_700_000_000_654, 8_000, 1_700_000_000_321, 7_000));
}

#[test]
fn several_segments_keep_the_hash_map_order_java_wrote() {
    let CacheRpcPayload::StateResponse(r) = decode("chunk:segments-order").payload else { panic!() };
    assert_eq!(r.chunks.iter().map(|c| (c.segment, c.entries.len(), c.is_last)).collect::<Vec<_>>(),
        vec![(0, 0, true), (16, 0, true), (17, 1, false), (1, 1, true), (259, 0, true), (255, 0, true)]);
}

#[test]
fn a_real_joiner_stores_the_class_the_provider_has() {
    for key in ["plain", "versioned", "lifespan", "idle", "both"] {
        assert_eq!(line(&format!("probe:A:{key}")), line(&format!("probe:B:{key}")), "{key}");
    }
}
