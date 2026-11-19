//! Port of `topology/ClusterCacheStatusTest` (core test tree, 14.0.35.Final) plus the four-phase
//! progression the acceptance criteria name. Adaptation: upstream builds a DIST_SYNC cache with
//! `DefaultConsistentHashFactory` (8 segments); this port reaches only REPL_SYNC caches
//! (`SyncReplicatedConsistentHashFactory`, 256 segments, `StateTransferManagerImpl:144`), so the same
//! sequence runs on those. The flow and the assertions are upstream's.

use std::sync::{Arc, Mutex};

use infinispan::ch::address::{Address, CapacityFactors};
use infinispan::ch::sync_replicated_consistent_hash_factory as repl;
use infinispan::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use infinispan::topology::cluster_cache_status::*;
use infinispan::topology::messages::*;

#[derive(Debug, Clone, PartialEq)]
enum Event {
    RebalanceStart(CacheTopology),
    TopologyUpdate(CacheTopology, Option<AvailabilityMode>),
    StableUpdate(CacheTopology),
    RemoveCacheStatus,
}

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Event>>,
    rebalancing_enabled: Mutex<bool>,
}

impl Recorder {
    fn new() -> Arc<Self> {
        let r = Arc::new(Self::default());
        *r.rebalancing_enabled.lock().unwrap() = true;
        r
    }

    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

impl ClusterTopologyHooks for Recorder {
    fn broadcast_rebalance_start(&self, _c: &str, t: &CacheTopology) {
        self.events.lock().unwrap().push(Event::RebalanceStart(t.clone()));
    }
    fn broadcast_topology_update(&self, _c: &str, t: &CacheTopology, m: Option<AvailabilityMode>) {
        self.events.lock().unwrap().push(Event::TopologyUpdate(t.clone(), m));
    }
    fn broadcast_stable_topology_update(&self, _c: &str, t: &CacheTopology) {
        self.events.lock().unwrap().push(Event::StableUpdate(t.clone()));
    }
    fn remove_cache_status(&self, _c: &str) {
        self.events.lock().unwrap().push(Event::RemoveCacheStatus);
    }
    fn is_rebalancing_enabled(&self) -> bool {
        *self.rebalancing_enabled.lock().unwrap()
    }
}

struct View(Mutex<(Vec<Address>, i32)>);

impl ViewSource for View {
    fn members(&self) -> Vec<Address> {
        self.0.lock().unwrap().0.clone()
    }
    fn view_id(&self) -> i32 {
        self.0.lock().unwrap().1
    }
}

const CACHE: &str = "test";

fn node(n: i64) -> Address {
    Address::uuid(n, n * 100)
}

fn join_info(a: &Address) -> CacheJoinInfo {
    // `new PersistentUUID(a.hashCode(), a.hashCode())`
    let h = a.hash_code() as i64;
    CacheJoinInfo {
        consistent_hash_factory: ConsistentHashFactoryKind::SyncReplicated,
        num_segments: 256,
        num_owners: 2,
        timeout: 1000,
        cache_mode: CacheMode::ReplSync,
        capacity_factor: 1.0,
        persistent_uuid: Some(Address::Persistent { msb: h, lsb: h }),
        persistent_state_checksum: None,
    }
}

struct Fixture {
    status: ClusterCacheStatus,
    rec: Arc<Recorder>,
    view: Arc<View>,
}

fn fixture() -> Fixture {
    let rec = Recorder::new();
    let view = Arc::new(View(Mutex::new((vec![], 0))));
    let status = ClusterCacheStatus::new(CACHE, RebalanceType::FourPhase, rec.clone(), view.clone(), PersistentUuidManager::new());
    Fixture { status, rec, view }
}

fn confirm_phase(f: &mut Fixture, expected_phase: Phase) {
    let t = f.status.current_topology().unwrap().clone();
    assert_eq!(t.phase, expected_phase);
    let id = t.topology_id;
    for a in t.members().to_vec() {
        f.status.confirm_rebalance_phase(&a, id).unwrap();
    }
    assert_eq!(f.status.current_topology().unwrap().topology_id, id + 1);
}

fn complete_rebalance(f: &mut Fixture) {
    for (phase, next) in [
        (Phase::ReadOldWriteAll, Phase::ReadAllWriteAll),
        (Phase::ReadAllWriteAll, Phase::ReadNewWriteAll),
        (Phase::ReadNewWriteAll, Phase::NoRebalance),
    ] {
        confirm_phase(f, phase);
        assert_eq!(f.status.current_topology().unwrap().phase, next);
        let events = f.rec.take();
        let t = f.status.current_topology().unwrap().clone();
        assert!(events.contains(&Event::TopologyUpdate(t.clone(), Some(AvailabilityMode::Available))), "{events:?}");
        if next == Phase::NoRebalance {
            // endReadNewPhase -> startQueuedRebalance finds nothing queued and promotes the stable topology
            assert!(events.contains(&Event::StableUpdate(t)), "{events:?}");
        }
    }
}

#[test]
fn test_queue_rebalance_single_node() {
    let (a, b, c) = (node(1), node(2), node(3));
    let mut f = fixture();

    f.status.do_join(&a, &join_info(&a)).unwrap();
    let stable = f.status.stable_topology().unwrap().clone();
    assert_eq!(f.rec.take(), vec![Event::StableUpdate(stable.clone())]);
    assert_eq!(stable.topology_id, 1);

    f.status.do_join(&b, &join_info(&b)).unwrap();
    let t = f.status.current_topology().unwrap().clone();
    assert_eq!(f.rec.take(), vec![Event::RebalanceStart(t)]);
    complete_rebalance(&mut f);
    let stable = f.status.stable_topology().unwrap().clone();
    assert_eq!(stable.topology_id, f.status.current_topology().unwrap().topology_id);

    f.status.do_join(&c, &join_info(&c)).unwrap();
    f.rec.take();
    complete_rebalance(&mut f);
    assert_eq!(f.status.current_topology().unwrap().topology_id, 9, "1 + 4 phase changes per join, twice");
    assert_eq!(f.status.current_topology().unwrap().phase, Phase::NoRebalance);

    // the view shrinks to [C]
    *f.view.0.lock().unwrap() = (vec![c.clone()], 1);
    f.rec.take();
    f.status.do_handle_cluster_view(1).unwrap();

    // TestClusterCacheStatus.start(JOIN_INFO, C).incrementIds(9, 2) / incrementStableIds(9, 2)
    let mut cf = CapacityFactors::default();
    cf.put(c.clone(), 1.0);
    let ch = ConsistentHash::Replicated(repl::create(2, 256, std::slice::from_ref(&c), Some(&cf)).unwrap());
    let expected = CacheTopology::new(10, 3, false, Some(ch), None, None, Phase::NoRebalance, vec![c.clone()], None).unwrap();
    assert_eq!(f.status.current_topology().unwrap(), &expected);
    assert_eq!(f.status.stable_topology().unwrap(), &expected);
    let events = f.rec.take();
    assert_eq!(
        events,
        vec![
            Event::TopologyUpdate(expected.clone(), Some(AvailabilityMode::Available)),
            Event::StableUpdate(expected),
        ]
    );
}

#[test]
fn the_four_phases_raise_the_topology_id_each_step() {
    let (a, b) = (node(1), node(2));
    let mut f = fixture();
    f.status.do_join(&a, &join_info(&a)).unwrap();
    f.rec.take();
    f.status.do_join(&b, &join_info(&b)).unwrap();

    let mut seen = vec![];
    for _ in 0..4 {
        let t = f.status.current_topology().unwrap();
        seen.push((t.topology_id, t.phase));
        if t.phase == Phase::NoRebalance {
            break;
        }
        let id = t.topology_id;
        for m in t.members().to_vec() {
            f.status.confirm_rebalance_phase(&m, id).unwrap();
        }
    }
    assert_eq!(
        seen,
        vec![
            (2, Phase::ReadOldWriteAll),
            (3, Phase::ReadAllWriteAll),
            (4, Phase::ReadNewWriteAll),
            (5, Phase::NoRebalance),
        ]
    );
    let t = f.status.current_topology().unwrap();
    assert!(t.pending_ch.is_none());
    assert_eq!(t.members(), &[a, b]);
    assert_eq!(f.status.rebalancing_status(), RebalancingStatus::Complete);
}

#[test]
fn a_confirmation_without_a_rebalance_in_progress_is_an_error() {
    let a = node(1);
    let mut f = fixture();
    f.status.do_join(&a, &join_info(&a)).unwrap();
    let id = f.status.current_topology().unwrap().topology_id;
    let err = f.status.confirm_rebalance_phase(&a, id).unwrap_err();
    assert!(err.0.contains("we don't have a rebalance in progress"), "{err}");
}

#[test]
fn a_joiner_with_persistent_state_is_refused_on_a_cluster_without_state() {
    let a = node(1);
    let mut f = fixture();
    let mut ji = join_info(&a);
    ji.persistent_state_checksum = Some(7);
    let err = f.status.do_join(&a, &ji).unwrap_err();
    assert!(err.0.contains("with persistent state attempting to join cache test on cluster without state"), "{err}");
}

#[test]
fn a_repeated_join_answers_with_the_current_status_and_changes_nothing() {
    let a = node(1);
    let mut f = fixture();
    f.status.do_join(&a, &join_info(&a)).unwrap();
    f.rec.take();
    let r = f.status.do_join(&a, &join_info(&a)).unwrap();
    assert!(r.cache_join_info.is_none());
    assert_eq!(r.current, Some(vec![a]));
    assert!(f.rec.take().is_empty());
}

#[test]
fn the_last_member_leaving_removes_the_cache_status() {
    let a = node(1);
    let mut f = fixture();
    f.status.do_join(&a, &join_info(&a)).unwrap();
    f.rec.take();
    f.status.do_leave(&a).unwrap();
    let events = f.rec.take();
    assert!(events.contains(&Event::RemoveCacheStatus), "{events:?}");
    assert!(f.status.current_topology().is_none());
    assert!(f.status.stable_topology().is_none());
}

/// `PreferAvailabilityStrategy.onPartitionMerge` hands `updateTopologiesAfterMerge` a `null` mode, which
/// `ClusterCacheStatus` stores, so the recovered topology and every phase of the rebalance that follows are broadcast
/// with a `null` availability (a mixed cluster stalled on it when the member could not decode `null`).
#[test]
fn a_merge_leaves_the_availability_mode_null_for_the_rebalance_that_follows() {
    let (a, b) = (node(1), node(2));
    let mut f = fixture();
    *f.view.0.lock().unwrap() = (vec![a.clone(), b.clone()], 1);
    f.status.do_join(&a, &join_info(&a)).unwrap();
    f.status.do_join(&b, &join_info(&b)).unwrap();
    f.rec.take();
    complete_rebalance(&mut f);
    let before_split = f.status.current_topology().unwrap().clone();
    let stable_before_split = f.status.stable_topology().unwrap().clone();

    // B is cut off: A's view shrinks to [A]
    *f.view.0.lock().unwrap() = (vec![a.clone()], 2);
    f.status.do_handle_cluster_view(2).unwrap();
    f.rec.take();
    let a_alone = f.status.current_topology().unwrap().clone();
    assert_eq!(f.status.availability_mode(), Some(AvailabilityMode::Available));

    // the partitions merge: A reports its own topology, B the one from before the split
    *f.view.0.lock().unwrap() = (vec![a.clone(), b.clone()], 3);
    let response = |who: &Address, t: &CacheTopology, members: Vec<Address>| CacheStatusResponse {
        cache_join_info: Some(join_info(who)),
        cache_topology: Some(t.clone()),
        stable_topology: Some(stable_before_split.clone()),
        availability_mode: Some(AvailabilityMode::Available),
        current: Some(members),
    };
    f.status
        .do_merge_partitions(&[(a.clone(), response(&a, &a_alone, vec![a.clone()])), (b.clone(), response(&b, &before_split, vec![a.clone(), b.clone()]))])
        .unwrap();
    assert_eq!(f.status.availability_mode(), None);
    let events = f.rec.take();
    assert!(matches!(events.first(), Some(Event::TopologyUpdate(_, None))), "{events:?}");
    assert!(!events.iter().any(|e| matches!(e, Event::TopologyUpdate(_, Some(_)))), "{events:?}");

    // the rebalance that adds B back goes through its phases with the null mode
    assert_eq!(f.status.current_topology().unwrap().phase, Phase::ReadOldWriteAll);
    for _ in 0..3 {
        let t = f.status.current_topology().unwrap().clone();
        for m in t.members().to_vec() {
            f.status.confirm_rebalance_phase(&m, t.topology_id).unwrap();
        }
        let now = f.status.current_topology().unwrap().clone();
        assert!(f.rec.take().contains(&Event::TopologyUpdate(now, None)));
    }
    assert_eq!(f.status.current_topology().unwrap().phase, Phase::NoRebalance);
    assert_eq!(f.status.current_topology().unwrap().members(), &[a, b]);
}
