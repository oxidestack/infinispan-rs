//! Port of `topology/ClusterTopologyManagerImplTest` (core test tree, 14.0.35.Final): a coordinator
//! manager on a transport that records every command, with a real `LocalTopologyManager` where
//! upstream uses `MockLocalTopologyManager`. Both tests keep upstream's sequence and assertions; the
//! other cluster members do not exist, the test plays them (view changes, phase confirmations).

use std::sync::Arc;
use std::time::Duration;

use infinispan::ch::address::Address;
use infinispan::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use infinispan::topology::cluster_topology_manager::{ClusterManagerStatus, TopologyConfig};
use infinispan::topology::commands::TopologyCommand;
use infinispan::topology::local_topology_manager::{AllowReadWritesPartitionHandling, PartitionHandling};
use infinispan::topology::messages::*;
use infinispan::topology::node::TopologyNode;
use infinispan::topology::testing::{MockTransport, PhaseConfirmingHandler};

const CACHE: &str = "testCache";

fn addr(n: i64) -> Address {
    // TestAddress(n, name)
    Address::uuid(0x7e57, n)
}

fn join_info(puuid: i64) -> CacheJoinInfo {
    CacheJoinInfo {
        consistent_hash_factory: ConsistentHashFactoryKind::Replicated,
        num_segments: 16,
        num_owners: 1,
        timeout: 1000,
        cache_mode: CacheMode::ReplSync,
        capacity_factor: 1.0,
        persistent_uuid: Some(Address::Persistent { msb: 0xAB, lsb: puuid }),
        persistent_state_checksum: None,
    }
}

fn ch_members(ch: &Option<ConsistentHash>) -> Vec<Address> {
    ch.as_ref().map(|c| c.members().to_vec()).unwrap_or_default()
}

fn config() -> TopologyConfig {
    TopologyConfig { global_timeout: Duration::from_secs(10) }
}

async fn eventually(mut f: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition not reached in time");
}

fn is_status_request(c: &TopologyCommand) -> bool {
    matches!(c, TopologyCommand::CacheStatusRequest { .. })
}

fn is_heartbeat(c: &TopologyCommand) -> bool {
    matches!(c, TopologyCommand::HeartBeat)
}

#[allow(clippy::too_many_arguments)]
async fn verify_rebalance(
    transport: &Arc<MockTransport>,
    node: &Arc<TopologyNode>,
    handler: &Arc<PhaseConfirmingHandler>,
    others: &[Address],
    rebalance_topology_id: i32,
    rebalance_view_id: i32,
    initial_members: &[Address],
    final_members: &[Address],
) {
    // CTMI starts rebalance
    let cmd = transport
        .expect_command("RebalanceStart", |c| matches!(c, TopologyCommand::RebalanceStart { .. }))
        .await;
    let TopologyCommand::RebalanceStart { fields, .. } = cmd else { unreachable!() };
    assert_eq!(fields.topology_id, rebalance_topology_id);
    assert_eq!(fields.phase, Phase::ReadOldWriteAll);
    assert_eq!(ch_members(&fields.current_ch), initial_members);
    assert_eq!(ch_members(&fields.pending_ch), final_members);
    for _ in 0..200 {
        if node.ltm.cache_topology(CACHE).is_some_and(|t| t.topology_id == rebalance_topology_id && t.phase == Phase::ReadOldWriteAll && t.members() == final_members) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    eprintln!("local topology: {:?}", node.ltm.cache_topology(CACHE).map(|t| (t.topology_id, t.phase, t.members().len())));
    let t = node.ltm.cache_topology(CACHE).unwrap();
    assert_eq!(ch_members(&t.current_ch), initial_members);
    assert_eq!(ch_members(&t.pending_ch), final_members);
    let _ = handler;

    for (offset, phase) in [(1, Phase::ReadAllWriteAll), (2, Phase::ReadNewWriteAll)] {
        // Confirm state transfer: the local node confirmed by itself, the others are played here
        for o in others {
            node.ctm.handle_rebalance_phase_confirm(CACHE, o, rebalance_topology_id + offset - 1, rebalance_view_id).await.unwrap();
        }
        // CTMI starts the next phase
        let cmd = transport
            .expect_command("TopologyUpdate", |c| matches!(c, TopologyCommand::TopologyUpdate { fields, .. } if fields.topology_id == rebalance_topology_id + offset))
            .await;
        let TopologyCommand::TopologyUpdate { fields, .. } = cmd else { unreachable!() };
        assert_eq!(fields.phase, phase);
        assert_eq!(ch_members(&fields.current_ch), initial_members);
        assert_eq!(ch_members(&fields.pending_ch), final_members);
    }
    // Confirm phase 3
    for o in others {
        node.ctm.handle_rebalance_phase_confirm(CACHE, o, rebalance_topology_id + 2, rebalance_view_id).await.unwrap();
    }
    // CTMI finishes rebalance
    let cmd = transport
        .expect_command("TopologyUpdate NO_REBALANCE", |c| matches!(c, TopologyCommand::TopologyUpdate { fields, .. } if fields.topology_id == rebalance_topology_id + 3))
        .await;
    let TopologyCommand::TopologyUpdate { fields, .. } = cmd else { unreachable!() };
    assert_eq!(fields.phase, Phase::NoRebalance);
    assert_eq!(ch_members(&fields.current_ch), final_members);
    assert!(fields.pending_ch.is_none());
    let cmd = transport
        .expect_command("TopologyUpdateStable", |c| matches!(c, TopologyCommand::TopologyUpdateStable { fields, .. } if fields.topology_id == rebalance_topology_id + 3))
        .await;
    let TopologyCommand::TopologyUpdateStable { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), final_members);
    assert!(fields.pending_ch.is_none());
}

/// Start two nodes and make both join the cache.
#[tokio::test]
async fn test_cluster_startup_with_2_nodes() {
    let (a, b) = (addr(0), addr(1));
    let (join_a, join_b) = (join_info(1), join_info(2));
    let transport = MockTransport::new(a.clone());
    transport.update_view(1, vec![a.clone()]);
    let node = TopologyNode::new(transport.clone(), config());
    node.start().await;

    // CTMI becomes coordinator and fetches the cluster status
    transport.expect_command("CacheStatusRequest", is_status_request).await;
    eventually(|| node.ctm.status() == ClusterManagerStatus::Coordinator).await;

    // First node joins the cache
    let handler = PhaseConfirmingHandler::new(CACHE, &node.ltm, a.clone());
    let phm: Arc<dyn PartitionHandling> = AllowReadWritesPartitionHandling::new();
    let ct = node.ltm.join(CACHE, join_a.clone(), handler.clone(), phm).await.unwrap().unwrap();
    assert_eq!(ct.topology_id, 1);
    assert_eq!(ch_members(&ct.current_ch), vec![a.clone()]);
    assert!(ct.pending_ch.is_none());
    // LTMI updates the topology when receiving the join response
    let local = node.ltm.cache_topology(CACHE).unwrap();
    assert_eq!((local.topology_id, local.phase), (1, Phase::NoRebalance));
    assert_eq!(local.members(), std::slice::from_ref(&a));
    // CTMI broadcasts the initial stable topology
    let cmd = transport.expect_command("TopologyUpdateStable", |c| matches!(c, TopologyCommand::TopologyUpdateStable { .. })).await;
    let TopologyCommand::TopologyUpdateStable { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), vec![a.clone()]);
    assert!(fields.pending_ch.is_none());

    // Add a second node
    transport.update_view(2, vec![a.clone(), b.clone()]);
    node.on_view_change(false, 2);
    // CTMI confirms availability
    transport.expect_command("HeartBeat", is_heartbeat).await;

    // Second node tries to join with old view and is rejected
    let r = node.ctm.handle_join(CACHE, &b, &join_b, 1).await.unwrap();
    assert!(r.is_none());
    // Second node joins the cache with correct view id, receives the initial topology
    let r = node.ctm.handle_join(CACHE, &b, &join_b, 2).await.unwrap().unwrap();
    let t = r.cache_topology.unwrap();
    assert_eq!(t.topology_id, 1);
    assert_eq!(ch_members(&t.current_ch), vec![a.clone()]);
    assert!(t.pending_ch.is_none());
    verify_rebalance(&transport, &node, &handler, std::slice::from_ref(&b), 2, 1, std::slice::from_ref(&a), &[a.clone(), b.clone()]).await;
}

/// Assume there are already 2 nodes and the coordinator leaves during rebalance.
#[tokio::test]
async fn test_coordinator_lost_during_rebalance() {
    let (a, b) = (addr(0), addr(1));
    let (join_a, join_b) = (join_info(1), join_info(2));
    let transport = MockTransport::new(b.clone());
    transport.update_view(2, vec![a.clone(), b.clone()]);
    // `RebalanceStatusRequestCommand` to the coordinator: SuccessfulResponse.create(true)
    transport.set_responder(|_, cmd| {
        Ok(match cmd {
            TopologyCommand::RebalanceStatusRequest { .. } => Response::Successful(Some(Box::new(ResponseValue::Boolean(true)))),
            TopologyCommand::CacheStatusRequest { .. } => {
                Response::Successful(Some(Box::new(ResponseValue::ManagerStatus(ManagerStatusResponse { caches: vec![], rebalancing_enabled: true }))))
            }
            _ => Response::Successful(None),
        })
    });
    let node = TopologyNode::new(transport.clone(), config());

    // Initial conditions (rebalance in phase 3, READ_NEW_WRITE_ALL)
    let kind = ConsistentHashFactoryKind::Replicated;
    let stable_ch = kind.create(join_a.num_owners, join_a.num_segments, std::slice::from_ref(&a), None).unwrap();
    let pending_ch = kind.create(join_a.num_owners, join_a.num_segments, &[a.clone(), b.clone()], None).unwrap();
    let pu = |j: &CacheJoinInfo| j.persistent_uuid.clone().unwrap();
    let initial_topology = CacheTopology::new(
        4, 2, false,
        Some(stable_ch.clone()), Some(pending_ch), None,
        Phase::ReadNewWriteAll, vec![a.clone(), b.clone()], Some(vec![pu(&join_a), pu(&join_b)]),
    )
    .unwrap();
    let stable_topology =
        CacheTopology::new(1, 1, false, Some(stable_ch), None, None, Phase::NoRebalance, vec![a.clone()], Some(vec![pu(&join_a)])).unwrap();
    let handler = PhaseConfirmingHandler::new(CACHE, &node.ltm, b.clone());
    node.ltm.preload_cache_status(
        CACHE,
        join_a.clone(),
        handler.clone(),
        AllowReadWritesPartitionHandling::new(),
        Some(initial_topology),
        Some(stable_topology),
        vec![],
    );
    // Normally LocalTopologyManagerImpl.start()/doHandleTopologyUpdate() registers the persistent UUIDs
    node.persistent_uuids.add_persistent_address_mapping(&a, &pu(&join_a));
    node.persistent_uuids.add_persistent_address_mapping(&b, &pu(&join_b));

    // When CTMI starts as regular member it requests the rebalancing status from the coordinator
    node.start().await;
    transport.expect_command("RebalanceStatusRequest", |c| matches!(c, TopologyCommand::RebalanceStatusRequest { .. })).await;
    // Wait for the initial view update in CTMI to finish
    eventually(|| node.ctm.status() == ClusterManagerStatus::RegularMember).await;

    // The coordinator (node A) leaves the cluster
    transport.update_view(3, vec![b.clone()]);
    node.on_view_change(false, 3);
    // Node B becomes coordinator and CTMI tries to recover the cluster status
    transport.expect_command("CacheStatusRequest", is_status_request).await;

    // CTMI gets a single cache topology with READ_NEW and broadcasts a new topology with only the read CH
    let cmd = transport
        .expect_command("TopologyUpdate 5", |c| matches!(c, TopologyCommand::TopologyUpdate { fields, .. } if fields.topology_id == 5))
        .await;
    let TopologyCommand::TopologyUpdate { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), vec![a.clone(), b.clone()]);
    assert!(fields.pending_ch.is_none());
    assert_eq!(fields.phase, Phase::NoRebalance);
    let cmd = transport
        .expect_command("TopologyUpdateStable 1", |c| matches!(c, TopologyCommand::TopologyUpdateStable { fields, .. } if fields.topology_id == 1))
        .await;
    let TopologyCommand::TopologyUpdateStable { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), vec![a.clone()]);
    assert!(fields.pending_ch.is_none());

    // CTMI broadcasts a new cache topology with only node B
    let cmd = transport
        .expect_command("TopologyUpdate 6", |c| matches!(c, TopologyCommand::TopologyUpdate { fields, .. } if fields.topology_id == 6))
        .await;
    let TopologyCommand::TopologyUpdate { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), vec![b.clone()]);
    assert!(fields.pending_ch.is_none());
    // The new topology doesn't need rebalancing, so CTMI updates the stable topology
    let cmd = transport
        .expect_command("TopologyUpdateStable 6", |c| matches!(c, TopologyCommand::TopologyUpdateStable { fields, .. } if fields.topology_id == 6))
        .await;
    let TopologyCommand::TopologyUpdateStable { fields, .. } = cmd else { unreachable!() };
    assert_eq!(ch_members(&fields.current_ch), vec![b.clone()]);
    assert!(fields.pending_ch.is_none());
    // the local manager installed 5 then 6
    eventually(|| node.ltm.cache_topology(CACHE).is_some_and(|t| t.topology_id == 6 && t.members() == [b.clone()])).await;

    // Shouldn't send any more commands here
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(transport.sent().is_empty(), "unexpected commands: {:?}", transport.sent());

    // Node A restarts
    transport.update_view(4, vec![b.clone(), a.clone()]);
    node.on_view_change(false, 4);
    // CTMI confirms members are available in case it needs to starts a rebalance
    transport.expect_command("HeartBeat", is_heartbeat).await;
    // Node A rejoins
    node.ctm.handle_join(CACHE, &a, &join_a, 4).await.unwrap();
    verify_rebalance(&transport, &node, &handler, std::slice::from_ref(&a), 7, 4, std::slice::from_ref(&b), &[b.clone(), a.clone()]).await;
}
