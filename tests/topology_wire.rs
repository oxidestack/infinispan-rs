//! Topology objects, commands and responses against tests/vectors/topology_wire.hex: bytes the real
//! Infinispan 14.0.35.Final `GlobalMarshaller` wrote for the unmodified Java classes
//! (upstream-tools/generate-topology-vectors.sh).

mod common;

use common::*;
use infinispan::topology::cache_topology::{CacheTopology, Phase};
use infinispan::topology::cluster_cache_status::RebalancingStatus;
use infinispan::topology::commands::{TopologyCommand, TopologyFields};
use infinispan::topology::messages::*;
use infinispan::topology::wire::{ObjectInput, ObjectOutput};
use common::scenarios::*;

fn vector(name: &str) -> Vec<u8> {
    let line = vector_lines("topology_wire.hex").into_iter().find(|l| l[0] == name).unwrap_or_else(|| panic!("no vector {name}"));
    unhex(&line[1])
}

fn topology_bytes(t: &CacheTopology) -> Vec<u8> {
    let mut o = ObjectOutput::new();
    o.write_cache_topology(Some(t));
    o.into_bytes()
}

#[test]
fn cache_topology_matches_java_in_every_phase_and_round_trips() {
    let cases: Vec<(&str, CacheTopology)> = vec![
        ("topology:stable", stable()),
        ("topology:READ_OLD_WRITE_ALL", rebalancing(Phase::ReadOldWriteAll, 4)),
        ("topology:READ_ALL_WRITE_ALL", rebalancing(Phase::ReadAllWriteAll, 5)),
        ("topology:READ_NEW_WRITE_ALL", rebalancing(Phase::ReadNewWriteAll, 6)),
    ];
    for (name, t) in cases {
        let java = vector(name);
        assert_eq!(hex(&topology_bytes(&t)), hex(&java), "{name}");
        let mut i = ObjectInput::new(&java);
        let back = i.read_cache_topology().unwrap().unwrap();
        assert_eq!(back, t, "{name}");
        assert_eq!(back.persistent_uuids, t.persistent_uuids);
        assert_eq!(i.r.remaining(), 0);
        assert_eq!(hex(&topology_bytes(&back)), hex(&java), "re-serialise {name}");
    }
}

#[test]
fn cache_join_info_matches_java() {
    for (name, checksum) in [("joininfo:none", None), ("joininfo:42", Some(42))] {
        let java = vector(name);
        let mut o = ObjectOutput::new();
        o.write_cache_join_info(Some(&join_info(checksum)));
        assert_eq!(hex(&o.into_bytes()), hex(&java), "{name}");
        let mut i = ObjectInput::new(&java);
        assert_eq!(i.read_cache_join_info().unwrap().unwrap(), join_info(checksum));
    }
}

fn fields(t: &CacheTopology, view: i32) -> TopologyFields {
    TopologyFields::of(t, view)
}

#[test]
fn commands_match_java_and_round_trip() {
    let cmds: Vec<(&str, TopologyCommand)> = vec![
        ("cmd:join", TopologyCommand::CacheJoin { cache_name: "mockserver-state".into(), join_info: join_info(None), view_id: 7 }),
        ("cmd:join-checksum", TopologyCommand::CacheJoin { cache_name: "org.infinispan.CONFIG".into(), join_info: join_info(Some(42)), view_id: 12 }),
        ("cmd:leave", TopologyCommand::CacheLeave { cache_name: "mockserver-state".into(), view_id: 9 }),
        ("cmd:confirm", TopologyCommand::RebalancePhaseConfirm { cache_name: "mockserver-state".into(), topology_id: 5, view_id: 9 }),
        ("cmd:rebalance-status", TopologyCommand::RebalanceStatusRequest { cache_name: Some("mockserver-state".into()) }),
        (
            "cmd:rebalance-start",
            TopologyCommand::RebalanceStart {
                cache_name: "mockserver-state".into(),
                fields: fields(&rebalancing(Phase::ReadOldWriteAll, 4), 9),
            },
        ),
        (
            "cmd:topology-update-available",
            TopologyCommand::TopologyUpdate {
                cache_name: "mockserver-state".into(),
                fields: fields(&rebalancing(Phase::ReadAllWriteAll, 5), 9),
                availability_mode: Some(AvailabilityMode::Available),
            },
        ),
        (
            "cmd:topology-update-degraded",
            TopologyCommand::TopologyUpdate {
                cache_name: "mockserver-state".into(),
                fields: fields(&stable(), 9),
                availability_mode: Some(AvailabilityMode::DegradedMode),
            },
        ),
        (
            "cmd:topology-update-null",
            TopologyCommand::TopologyUpdate {
                cache_name: "mockserver-state".into(),
                fields: fields(&rebalancing(Phase::ReadAllWriteAll, 5), 9),
                availability_mode: None,
            },
        ),
        ("cmd:status-request", TopologyCommand::CacheStatusRequest { view_id: 9 }),
        (
            "cmd:stable-update",
            TopologyCommand::TopologyUpdateStable {
                cache_name: "mockserver-state".into(),
                fields: fields(&stable(), 9),
                topology_restored: false,
            },
        ),
        ("cmd:heartbeat", TopologyCommand::HeartBeat),
    ];
    for (name, c) in cmds {
        let java = vector(name);
        assert_eq!(hex(&c.to_bytes()), hex(&java), "{name}");
        assert_eq!(TopologyCommand::from_bytes(&java).unwrap(), c, "{name}");
    }
}

#[test]
fn responses_match_java_and_round_trip() {
    let resps: Vec<(&str, Response)> = vec![
        ("resp:successful-empty", Response::Successful(None)),
        ("resp:cache-not-found", Response::CacheNotFound),
        ("resp:unsuccessful-empty", Response::Unsuccessful(None)),
        ("resp:bool-true", Response::Successful(Some(Box::new(ResponseValue::Boolean(true))))),
        ("resp:status", Response::Successful(Some(Box::new(ResponseValue::CacheStatus(full_status()))))),
        ("resp:status-empty", Response::Successful(Some(Box::new(ResponseValue::CacheStatus(CacheStatusResponse::empty()))))),
        ("resp:rebalancing-SUSPENDED", Response::Successful(Some(Box::new(ResponseValue::RebalancingStatus(RebalancingStatus::Suspended))))),
        ("resp:rebalancing-PENDING", Response::Successful(Some(Box::new(ResponseValue::RebalancingStatus(RebalancingStatus::Pending))))),
        ("resp:rebalancing-IN_PROGRESS", Response::Successful(Some(Box::new(ResponseValue::RebalancingStatus(RebalancingStatus::InProgress))))),
        ("resp:rebalancing-COMPLETE", Response::Successful(Some(Box::new(ResponseValue::RebalancingStatus(RebalancingStatus::Complete))))),
        ("resp:exception-join", Response::Exception { kind: ExceptionKind::CacheJoinException, message: Some("boom: joining".into()) }),
        ("resp:exception-cache", Response::Exception { kind: ExceptionKind::CacheException, message: Some("plain failure".into()) }),
        (
            "resp:exception-lifecycle",
            Response::Exception { kind: ExceptionKind::Generic(ILLEGAL_LIFECYCLE_STATE_EXCEPTION), message: None },
        ),
        ("resp:exception-suspect", Response::Exception { kind: ExceptionKind::SuspectException, message: Some("gone".into()) }),
        ("resp:manager", Response::Successful(Some(Box::new(ResponseValue::ManagerStatus(manager()))))),
    ];
    for (name, r) in resps {
        let java = vector(name);
        let mut o = ObjectOutput::new();
        o.write_response(&r);
        assert_eq!(hex(&o.into_bytes()), hex(&java), "{name}");
        let mut i = ObjectInput::new(&java);
        assert_eq!(i.read_response().unwrap(), r, "{name}");
        assert_eq!(i.r.remaining(), 0, "{name}");
    }
}
