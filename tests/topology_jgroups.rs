//! The topology protocol over real `jgroups-rs` channels (TCP + TCPPING + GMS + NAKACK2/UNICAST3):
//! a joining node exchanges `CacheJoinCommand`/`CacheStatusResponse` with the coordinator and goes
//! through all four rebalance phases, each raising the topology id; `CacheLeaveCommand` fires when
//! a cache stops; a new coordinator recovers state when the old one disappears.

use std::sync::Arc;
use std::time::Duration;

use infinispan::topology::cache_topology::Phase;
use infinispan::topology::cluster_cache_status::RebalancingStatus;
use infinispan::topology::cluster_topology_manager::{ClusterManagerStatus, TopologyConfig};
use infinispan::topology::jgroups_transport::JGroupsTopologyTransport;
use infinispan::topology::local_topology_manager::{AllowReadWritesPartitionHandling, PartitionHandling};
use infinispan::topology::node::TopologyNode;
use infinispan::topology::testing::{PhaseConfirmingHandler, repl_join_info};
use infinispan::topology::transport::TopologyTransport;
use jgroups::wire::address::Address as JAddress;

const WAIT: Duration = Duration::from_secs(30);

fn stack_xml(initial_hosts: &str) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="0" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
        </config>"#
    )
}

struct Member {
    transport: Arc<JGroupsTopologyTransport>,
    node: Arc<TopologyNode>,
}

async fn start_member(cluster: &str, initial_hosts: &str) -> Member {
    let transport = JGroupsTopologyTransport::connect(&stack_xml(initial_hosts), cluster).await.unwrap();
    let node = TopologyNode::new(transport.clone(), TopologyConfig { global_timeout: Duration::from_secs(20) });
    node.start().await;
    transport.attach(node.clone());
    Member { transport, node }
}

fn port_of(m: &Member) -> u16 {
    match m.transport.transport().physical_address() {
        JAddress::IpAddress { port, .. } => *port,
        other => panic!("expected an IpAddress, got {other:?}"),
    }
}

async fn join(m: &Member, cache: &str) -> Arc<PhaseConfirmingHandler> {
    let handler = PhaseConfirmingHandler::new(cache, &m.node.ltm, m.node.address());
    let phm: Arc<dyn PartitionHandling> = AllowReadWritesPartitionHandling::new();
    m.node.ltm.join(cache, repl_join_info(m.node.ltm.persistent_uuid()), handler.clone(), phm).await.unwrap();
    assert!(handler.wait_for_initial_transfer(WAIT).await, "initial transfer of {cache}");
    handler
}

async fn eventually(mut f: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_goes_through_the_four_phases_with_the_coordinator() {
    let a = start_member("TopoJoin", "").await;
    assert!(eventually(|| a.node.ctm.status() == ClusterManagerStatus::Coordinator).await);
    let a_handler = join(&a, "org.infinispan.CONFIG").await;
    let a_first = a.node.ltm.cache_topology("org.infinispan.CONFIG").unwrap();
    assert_eq!((a_first.topology_id, a_first.phase, a_first.members().len()), (1, Phase::NoRebalance, 1));

    let b = start_member("TopoJoin", &format!("127.0.0.1[{}]", port_of(&a))).await;
    assert!(eventually(|| a.transport.members().len() == 2 && b.transport.members().len() == 2).await, "views never converged");
    let b_handler = join(&b, "org.infinispan.CONFIG").await;

    // both nodes end on the same NO_REBALANCE topology listing both members
    assert!(
        eventually(|| {
            let (ta, tb) = (a.node.ltm.cache_topology("org.infinispan.CONFIG"), b.node.ltm.cache_topology("org.infinispan.CONFIG"));
            matches!((&ta, &tb), (Some(ta), Some(tb)) if ta.phase == Phase::NoRebalance && ta.topology_id == tb.topology_id && ta.members().len() == 2 && tb.members().len() == 2)
        })
        .await,
        "never settled: a={:?} b={:?}",
        a.node.ltm.cache_topology("org.infinispan.CONFIG").map(|t| (t.topology_id, t.phase)),
        b.node.ltm.cache_topology("org.infinispan.CONFIG").map(|t| (t.topology_id, t.phase)),
    );
    // the coordinator-side history: READ_OLD_WRITE_ALL, READ_ALL_WRITE_ALL, READ_NEW_WRITE_ALL, NO_REBALANCE with ids +1 each
    let phases: Vec<(i32, Phase)> = a_handler.topologies().iter().map(|t| (t.topology_id, t.phase)).collect();
    let expected = [
        (1, Phase::NoRebalance),
        (2, Phase::ReadOldWriteAll),
        (3, Phase::ReadAllWriteAll),
        (4, Phase::ReadNewWriteAll),
        (5, Phase::NoRebalance),
    ];
    assert_eq!(phases, expected, "coordinator history");
    // the joiner saw the rebalance too (it may join mid-way, so only the tail is fixed)
    let b_phases: Vec<Phase> = b_handler.topologies().iter().map(|t| t.phase).collect();
    assert_eq!(b_phases.last(), Some(&Phase::NoRebalance));
    assert!(b_phases.contains(&Phase::ReadOldWriteAll) || b_phases.contains(&Phase::ReadAllWriteAll));
    assert_eq!(a.node.ctm.rebalancing_status(Some("org.infinispan.CONFIG")), RebalancingStatus::Complete);

    // CacheLeaveCommand fires when a cache stops: the coordinator drops B from the cache
    b.node.ltm.leave("org.infinispan.CONFIG", Duration::from_secs(10)).await;
    assert!(eventually(|| a.node.ltm.cache_topology("org.infinispan.CONFIG").is_some_and(|t| t.members().len() == 1)).await);

    b.transport.disconnect().await;
    a.transport.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_coordinator_recovers_the_cache_status_when_the_old_one_leaves() {
    let a = start_member("TopoRecover", "").await;
    assert!(eventually(|| a.node.ctm.status() == ClusterManagerStatus::Coordinator).await);
    join(&a, "mockserver-state").await;
    let b = start_member("TopoRecover", &format!("127.0.0.1[{}]", port_of(&a))).await;
    assert!(eventually(|| b.transport.members().len() == 2 && a.transport.members().len() == 2).await);
    join(&b, "mockserver-state").await;
    assert!(
        eventually(|| {
            a.node.ltm.cache_topology("mockserver-state").is_some_and(|t| t.phase == Phase::NoRebalance && t.members().len() == 2)
                && b.node.ltm.cache_topology("mockserver-state").is_some_and(|t| t.phase == Phase::NoRebalance && t.members().len() == 2)
        })
        .await
    );
    let before = b.node.ltm.cache_topology("mockserver-state").unwrap();
    assert_eq!(b.node.ctm.status(), ClusterManagerStatus::RegularMember);

    // the coordinator goes away: B becomes coordinator, asks for the cluster status (answered by its
    // own LocalTopologyManager) and installs a topology without A
    a.transport.disconnect().await;
    assert!(eventually(|| b.node.ctm.status() == ClusterManagerStatus::Coordinator).await, "B never became coordinator");
    assert!(
        eventually(|| {
            b.node.ltm.cache_topology("mockserver-state").is_some_and(|t| t.topology_id > before.topology_id && t.members() == [b.node.address()] && t.phase == Phase::NoRebalance)
        })
        .await,
        "B's topology: {:?}",
        b.node.ltm.cache_topology("mockserver-state").map(|t| (t.topology_id, t.phase, t.members().len()))
    );
    // and a node can join the new coordinator's cluster
    let c = start_member("TopoRecover", &format!("127.0.0.1[{}]", port_of(&b))).await;
    assert!(eventually(|| c.transport.members().len() == 2 && b.transport.members().len() == 2).await);
    join(&c, "mockserver-state").await;
    assert!(eventually(|| b.node.ltm.cache_topology("mockserver-state").is_some_and(|t| t.phase == Phase::NoRebalance && t.members().len() == 2)).await);
    c.transport.disconnect().await;
    b.transport.disconnect().await;
}
