//! `StateConsumerImpl` (`statetransfer/StateConsumerImpl.java`) for a non-transactional REPL_SYNC cache.
//!
//! On a topology update the consumer installs the topology (after it started tracking commits), works
//! out which segments the node newly owns in the write hash, and requests them, one source at a time
//! (`stateRequestExecutor` is a `LimitedExecutor` of 1: the next [`InboundTransferTask`] starts when the
//! previous one has received everything). Received chunks are applied with `PUT_FOR_STATE_TRANSFER`
//! semantics: no locks, no replication, no listener notifications, and dropped for a key a normal write
//! already committed (`CommitManager`). The future returned by [`StateConsumer::on_topology_update`]
//! completes when the rebalance phase can be confirmed.
//!
//! Not ported: transactions (`requestTransactions`, `applyTransactions`), cluster listeners, scattered
//! and conflict-resolution state transfer, the cache store, `DataRehashed` notifications.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use super::chunk::StateChunk;
use super::commit_manager::{CommitFlag, CommitManager};
use super::env::StateTransferEnv;
use super::inbound_task::InboundTransferTask;
use super::int_set::IntSet;
use super::lock::{DistributionManager, StateTransferLock};
use crate::ch::address::Address;
use crate::topology::cache_topology::{CacheTopology, ConsistentHash, Phase};
use crate::topology::support::BoxFuture;

/// The topology id meaning no state transfer is in progress.
pub const NO_STATE_TRANSFER_IN_PROGRESS: i32 = -1;

struct Transfers {
    by_source: HashMap<Address, Vec<Arc<InboundTransferTask>>>,
    by_segment: HashMap<i32, Vec<Arc<InboundTransferTask>>>,
}

/// `StateConsumerImpl`: on a topology change, requests the segments the node gained from their owners and applies the state received.
pub struct StateConsumer {
    env: Arc<dyn StateTransferEnv>,
    /// The cache's name.
    pub cache_name: String,
    commit_manager: Arc<CommitManager>,
    distribution: Arc<DistributionManager>,
    lock: Arc<StateTransferLock>,
    timeout: Duration,
    num_segments: i32,
    cache_topology: Mutex<Option<CacheTopology>>,
    state_transfer_topology_id: AtomicI32,
    waiting_for_state: AtomicBool,
    /// `stateTransferFuture`: replaced at every topology update, completed when the transfers are done.
    state_transfer_future: Mutex<Arc<watch::Sender<bool>>>,
    transfers: Mutex<Transfers>,
    /// `stateRequestExecutor`: one request at a time.
    request_executor: Arc<tokio::sync::Mutex<()>>,
    owns_data: AtomicBool,
    running: AtomicBool,
    /// serialises `on_topology_update` (the local topology manager does too; the upstream test calls it twice concurrently)
    update_lock: Mutex<()>,
}

fn owned_segments(ch: &ConsistentHash, address: &Address) -> IntSet {
    IntSet::of(ch.as_replicated().segments_for_owner(address).into_iter().map(|s| s as i32))
}

impl StateConsumer {
    /// The consumer of `cache_name`, with `num_segments` segments.
    pub fn new(
        env: Arc<dyn StateTransferEnv>,
        cache_name: &str,
        num_segments: i32,
        timeout: Duration,
        commit_manager: Arc<CommitManager>,
        distribution: Arc<DistributionManager>,
        lock: Arc<StateTransferLock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            env,
            cache_name: cache_name.to_string(),
            commit_manager,
            distribution,
            lock,
            timeout,
            num_segments,
            cache_topology: Mutex::new(None),
            state_transfer_topology_id: AtomicI32::new(NO_STATE_TRANSFER_IN_PROGRESS),
            waiting_for_state: AtomicBool::new(false),
            state_transfer_future: Mutex::new(Arc::new(watch::channel(true).0)),
            transfers: Mutex::new(Transfers { by_source: HashMap::new(), by_segment: HashMap::new() }),
            request_executor: Arc::new(tokio::sync::Mutex::new(())),
            owns_data: AtomicBool::new(false),
            running: AtomicBool::new(true),
            update_lock: Mutex::new(()),
        })
    }

    /// The local node's address.
    pub fn local_address(&self) -> Address {
        self.env.address()
    }

    /// Whether any inbound transfer is running.
    pub fn has_active_transfers(&self) -> bool {
        !self.transfers.lock().unwrap().by_source.is_empty()
    }

    /// Whether a state transfer is in progress.
    pub fn is_state_transfer_in_progress(&self) -> bool {
        self.state_transfer_topology_id.load(Ordering::SeqCst) != NO_STATE_TRANSFER_IN_PROGRESS
    }

    /// `isStateTransferInProgressForKey`: the node is a write owner but not yet a read owner of the segment.
    pub fn is_state_transfer_in_progress_for_segment(&self, segment: i32) -> bool {
        let Some(t) = self.distribution.cache_topology() else { return false };
        let (Some(w), Some(r)) = (t.write_consistent_hash(), t.read_consistent_hash()) else { return false };
        let me = self.env.address();
        w.as_replicated().locate_owners_for_segment(segment as usize).contains(&me) && !r.as_replicated().locate_owners_for_segment(segment as usize).contains(&me)
    }

    /// `inflightRequestCount`: segments with an outstanding transfer.
    pub fn inflight_request_count(&self) -> usize {
        self.transfers.lock().unwrap().by_segment.len()
    }

    /// Whether the node is a member of the last topology it received, and so holds the cache's data.
    pub fn owns_data(&self) -> bool {
        self.owns_data.load(Ordering::SeqCst)
    }

    /// The topology the consumer last received.
    pub fn cache_topology(&self) -> Option<CacheTopology> {
        self.cache_topology.lock().unwrap().clone()
    }

    /// `stopApplyingState`: no more state transfer entries are accepted until the next rebalance.
    pub fn stop_applying_state(&self, _topology_id: i32) {
        self.commit_manager.stop_track(CommitFlag::PutForStateTransfer);
    }

    /// `onTopologyUpdate`: returns the future that completes when this node holds the state of the
    /// topology (what the manager waits for before it confirms the phase).
    pub fn on_topology_update(self: &Arc<Self>, topology: CacheTopology, is_rebalance: bool) -> BoxFuture<'static, ()> {
        let _serial = self.update_lock.lock().unwrap();
        let new_write_ch = topology.write_consistent_hash().expect("a topology has a write hash").clone();
        let previous = self.cache_topology.lock().unwrap().clone();
        let previous_write_ch = previous.as_ref().and_then(|p| p.write_consistent_hash().cloned());
        let new_write_segments = owned_segments(&new_write_ch, &self.env.address());

        let address = self.env.address();
        let is_member = topology.members().contains(&address);
        let was_member = previous_write_ch.as_ref().is_some_and(|ch| ch.members().contains(&address));

        if !self.owns_data() && is_member {
            self.owns_data.store(true, Ordering::SeqCst);
        } else if self.owns_data() && !is_member {
            self.owns_data.store(false, Ordering::SeqCst);
        }

        // If a member leaves/crashes immediately after a rebalance was started, the new CH_UPDATE command may be
        // executed before the REBALANCE_START command, so it has to start the rebalance.
        let added_pending_ch = topology.pending_ch.is_some() && was_member && previous.as_ref().is_some_and(|p| p.pending_ch.is_none());
        let start_conflict_resolution = !is_rebalance && topology.phase == Phase::ConflictResolution;
        let start_state_transfer = is_rebalance || (added_pending_ch && !start_conflict_resolution);
        if start_state_transfer {
            // Only update the rebalance topology id when starting the rebalance, as we're going to ignore any
            // state response with a smaller topology id
            let _ = self.state_transfer_topology_id.compare_exchange(NO_STATE_TRANSFER_IN_PROGRESS, topology.topology_id, Ordering::SeqCst, Ordering::SeqCst);
        }
        if start_conflict_resolution {
            self.state_transfer_topology_id.store(NO_STATE_TRANSFER_IN_PROGRESS, Ordering::SeqCst);
        }
        // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
        self.waiting_for_state.store(false, Ordering::SeqCst);
        *self.state_transfer_future.lock().unwrap() = Arc::new(watch::channel(false).0);

        // We need to track changes so that user puts are prioritised over state transfer updates. Tracking is
        // stopped once the state transfer completes.
        if (start_state_transfer || start_conflict_resolution) && !self.commit_manager.is_tracking(CommitFlag::PutForStateTransfer) {
            self.commit_manager.start_track(CommitFlag::PutForStateTransfer);
        }

        // Ensures writes to the data container use the right consistent hash
        *self.cache_topology.lock().unwrap() = Some(topology.clone());
        self.distribution.set_cache_topology(topology.clone());
        self.lock.notify_topology_installed(topology.topology_id);

        // fetch data segments from other owners
        if !start_conflict_resolution {
            let mut added = IntSet::new();
            if let Some(prev) = &previous_write_ch {
                let previous_segments = owned_segments(prev, &address);
                let mut removed = if new_write_segments.len() as i32 == self.num_segments {
                    // Optimization for replicated caches
                    IntSet::new()
                } else {
                    let mut r = previous_segments.clone();
                    r.remove_all(&new_write_segments);
                    r
                };
                // This is a rebalance, we need to request the segments we own in the new CH.
                added = new_write_segments.clone();
                added.remove_all(&previous_segments);
                self.cancel_transfers(&removed);
                removed.clear();
                if !start_state_transfer && !added.is_empty() {
                    // If the last owner of a segment leaves the cluster, a new set of owners is assigned, but the
                    // new owners should not try to retrieve the segment from each other.
                    added.clear();
                }
                // check if any of the existing transfers should be restarted from a different source because
                // the initial source is no longer a member
                self.restart_broken_transfers(&topology, &mut added);
            }
            self.handle_segments(&topology, added);
        }

        let st_id = self.state_transfer_topology_id.load(Ordering::SeqCst);
        if st_id != NO_STATE_TRANSFER_IN_PROGRESS
            && !start_state_transfer
            && !topology.phase.is_rebalance()
            && self.state_transfer_topology_id.compare_exchange(st_id, NO_STATE_TRANSFER_IN_PROGRESS, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        {
            // we have received a topology update without a pending CH, signalling the end of the rebalance
            self.stop_applying_state(st_id);
        }

        // Only set the flag here, after all the transfers have been added to the transfersBySource map
        if self.state_transfer_topology_id.load(Ordering::SeqCst) != NO_STATE_TRANSFER_IN_PROGRESS && is_member {
            self.waiting_for_state.store(true, Ordering::SeqCst);
        }
        self.notify_end_of_state_transfer_if_needed();

        // The rebalance (READ_OLD_WRITE_ALL/TRANSITORY) is completed through notifyEndOfStateTransferIfNeeded and
        // STABLE does not have to be confirmed at all
        if matches!(topology.phase, Phase::ReadAllWriteAll | Phase::ReadNewWriteAll) {
            self.state_transfer_future.lock().unwrap().send_replace(true);
        }

        // Any data for segments we do not own should be removed from data container
        if (is_member || was_member) && topology.phase == Phase::NoRebalance {
            let keep = new_write_segments.clone();
            self.env.remove_segments_not(&|s| keep.contains(s));
        }

        let sender = self.state_transfer_future.lock().unwrap().clone();
        Box::pin(async move {
            let mut rx = sender.subscribe();
            // a future that was replaced by a newer topology update never completes (as upstream's)
            if rx.wait_for(|v| *v).await.is_err() {
                std::future::pending::<()>().await;
            }
        })
    }

    fn notify_end_of_state_transfer_if_needed(&self) {
        if self.waiting_for_state.load(Ordering::SeqCst) {
            if self.has_active_transfers() {
                return;
            }
            if self.waiting_for_state.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                let id = self.state_transfer_topology_id.load(Ordering::SeqCst);
                log::debug!("Finished receiving of segments for cache {} for topology {id}.", self.cache_name);
                self.stop_applying_state(id);
                self.state_transfer_future.lock().unwrap().send_replace(true);
            }
        }
    }

    /// `handleSegments` + `requestSegments`.
    fn handle_segments(self: &Arc<Self>, topology: &CacheTopology, added: IntSet) {
        if added.is_empty() {
            return;
        }
        log::debug!("Adding inbound state transfer for segments {:?}", added.iter().collect::<Vec<_>>());
        let sources = self.find_sources(topology, &added, &[]);
        for (source, segments) in sources {
            self.add_transfer(topology.topology_id, source, segments);
        }
    }

    /// `findSources` (`:624-660`): per segment, the first read owner that is not this node.
    fn find_sources(&self, topology: &CacheTopology, segments: &IntSet, excluded: &[Address]) -> Vec<(Address, IntSet)> {
        let me = self.env.address();
        let Some(read) = topology.read_consistent_hash() else { return Vec::new() };
        let mut sources: Vec<(Address, IntSet)> = Vec::new();
        let mut without_source = IntSet::new();
        for s in segments.iter() {
            let owners = read.as_replicated().locate_owners_for_segment(s as usize);
            let source = if owners.contains(&me) { None } else { owners.into_iter().find(|o| *o != me && !excluded.contains(o)) };
            match source {
                Some(src) => match sources.iter_mut().find(|(a, _)| *a == src) {
                    Some((_, set)) => {
                        set.insert(s);
                    }
                    None => sources.push((src, IntSet::of([s]))),
                },
                None => {
                    without_source.insert(s);
                }
            }
        }
        if !without_source.is_empty() {
            log::warn!("No live owners found for segments {:?} of cache {}", without_source.iter().collect::<Vec<_>>(), self.cache_name);
        }
        sources
    }

    /// `addTransfer(source, segmentsFromSource)`.
    fn add_transfer(self: &Arc<Self>, topology_id: i32, source: Address, mut segments: IntSet) {
        let task = {
            let mut t = self.transfers.lock().unwrap();
            // already in progress segments are excluded
            for s in t.by_segment.keys() {
                segments.remove(*s);
            }
            if segments.is_empty() {
                return;
            }
            if !self.running.load(Ordering::SeqCst) {
                return;
            }
            let task = Arc::new(InboundTransferTask::new(&segments, source.clone(), topology_id, self.timeout, &self.cache_name).expect("non-empty segments"));
            for s in segments.iter() {
                t.by_segment.entry(s).or_default().push(task.clone());
            }
            t.by_source.entry(source).or_default().push(task.clone());
            task
        };
        let this = self.clone();
        let executor = self.request_executor.clone();
        tokio::spawn(async move {
            let _one_at_a_time = executor.lock().await;
            let result = task.request_segments(this.env.as_ref()).await;
            if let Err(e) = &result {
                log::debug!("Inbound transfer from {:?} ended: {e:?}", task.source());
            }
            this.on_task_completion(&task);
        });
    }

    fn on_task_completion(&self, task: &Arc<InboundTransferTask>) {
        if task.is_completed_successfully() {
            self.remove_transfer(task);
            self.notify_end_of_state_transfer_if_needed();
        }
    }

    fn remove_transfer(&self, task: &Arc<InboundTransferTask>) {
        let mut t = self.transfers.lock().unwrap();
        if let Some(list) = t.by_source.get_mut(task.source()) {
            list.retain(|x| !Arc::ptr_eq(x, task));
            if list.is_empty() {
                t.by_source.remove(task.source());
            }
        }
        for s in task.segments().iter() {
            if let Some(list) = t.by_segment.get_mut(&s) {
                list.retain(|x| !Arc::ptr_eq(x, task));
                if list.is_empty() {
                    t.by_segment.remove(&s);
                }
            }
        }
    }

    /// `cancelTransfers`: cancel transfers for segments we no longer own.
    fn cancel_transfers(&self, removed: &IntSet) {
        let mut t = self.transfers.lock().unwrap();
        let mut to_cancel: Vec<i32> = removed.iter().collect();
        while !to_cancel.is_empty() {
            let segment = to_cancel.remove(0);
            let Some(tasks) = t.by_segment.get(&segment).cloned() else { continue };
            for task in tasks {
                let mut cancelled = removed.clone();
                cancelled.retain_all(&task.segments());
                to_cancel.retain(|s| !cancelled.contains(*s));
                for s in cancelled.iter() {
                    t.by_segment.remove(&s);
                }
                let _ = task.cancel_segments(&cancelled, self.env.as_ref());
                if task.is_cancelled() {
                    Self::remove_locked(&mut t, &task);
                }
            }
        }
    }

    fn remove_locked(t: &mut Transfers, task: &Arc<InboundTransferTask>) {
        if let Some(list) = t.by_source.get_mut(task.source()) {
            list.retain(|x| !Arc::ptr_eq(x, task));
            if list.is_empty() {
                t.by_source.remove(task.source());
            }
        }
        for s in task.segments().iter() {
            if let Some(list) = t.by_segment.get_mut(&s) {
                list.retain(|x| !Arc::ptr_eq(x, task));
                if list.is_empty() {
                    t.by_segment.remove(&s);
                }
            }
        }
    }

    /// `restartBrokenTransfers`: transfers whose source left are cancelled and their unfinished segments
    /// requested again; segments already in progress from a valid source are not requested twice.
    fn restart_broken_transfers(&self, topology: &CacheTopology, added: &mut IntSet) {
        let members: Vec<Address> = topology.read_consistent_hash().map(|c| c.members().to_vec()).unwrap_or_default();
        let mut t = self.transfers.lock().unwrap();
        let broken: Vec<Address> = t.by_source.keys().filter(|s| !members.contains(s)).cloned().collect();
        for source in broken {
            let tasks = t.by_source.remove(&source).unwrap_or_default();
            for task in tasks {
                // these segments will be restarted if they are still in new write CH
                let unfinished = task.unfinished_segments();
                task.cancel(self.env.as_ref());
                added.add_all(&unfinished);
                for s in unfinished.iter() {
                    t.by_segment.remove(&s);
                }
            }
        }
        // exclude those that are already in progress from a valid source
        let in_progress: Vec<i32> = t.by_segment.keys().copied().collect();
        for s in in_progress {
            added.remove(s);
        }
    }

    /// `applyState` (`:487-523`): a `StateResponseCommand` arrived.
    pub async fn apply_state(&self, sender: &Address, topology_id: i32, push_transfer: bool, chunks: Vec<StateChunk>) {
        let Some(topology) = self.cache_topology() else { return };
        let Some(write_ch) = topology.write_consistent_hash() else { return };
        // Ignore responses received after we are no longer a member
        if !write_ch.members().contains(&self.env.address()) {
            log::trace!("Ignoring received state because we are no longer a member of cache {}", self.cache_name);
            return;
        }
        // Ignore segments that we requested for a previous rebalance (the coordinator left and the new one
        // cancelled the rebalance in progress)
        let rebalance_topology_id = self.state_transfer_topology_id.load(Ordering::SeqCst);
        if rebalance_topology_id == NO_STATE_TRANSFER_IN_PROGRESS && !push_transfer {
            log::debug!("Discarding state response with topology id {topology_id} for cache {}, we don't have a state transfer in progress", self.cache_name);
            return;
        }
        if topology_id < rebalance_topology_id {
            log::debug!("Discarding state response with old topology id {topology_id} for cache {}", self.cache_name);
            return;
        }
        let my_segments = owned_segments(write_ch, &self.env.address());
        for chunk in chunks {
            if push_transfer {
                self.env.apply_entries(chunk.segment, chunk.entries).await;
            } else {
                self.apply_chunk(sender, &my_segments, chunk).await;
            }
        }
    }

    async fn apply_chunk(&self, sender: &Address, my_segments: &IntSet, chunk: StateChunk) {
        if !my_segments.contains(chunk.segment) {
            log::debug!("Discarding received cache entries for segment {} of cache {} because they do not belong to this node.", chunk.segment, self.cache_name);
            return;
        }
        // Notify the inbound task that a chunk of cache entries was received
        let task = {
            let t = self.transfers.lock().unwrap();
            t.by_segment.get(&chunk.segment).and_then(|l| l.iter().find(|task| task.source() == sender).cloned())
        };
        match task {
            Some(task) => {
                let (segment, last) = (chunk.segment, chunk.is_last);
                if !chunk.entries.is_empty() {
                    self.env.apply_entries(segment, chunk.entries).await;
                }
                task.on_state_received(segment, last);
                if last {
                    self.on_completed_segment(segment, &task);
                }
            }
            None => {
                if self.env.is_running() {
                    log::warn!("Ignoring unsolicited state from {sender:?} for segment {} of cache {}", chunk.segment, self.cache_name);
                }
            }
        }
    }

    fn on_completed_segment(&self, segment: i32, task: &Arc<InboundTransferTask>) {
        let mut t = self.transfers.lock().unwrap();
        let mut emptied = false;
        if let Some(list) = t.by_segment.get_mut(&segment) {
            let before = list.len();
            list.retain(|x| !Arc::ptr_eq(x, task));
            emptied = list.len() != before && list.is_empty();
        }
        if emptied {
            self.commit_manager.stop_track_for(CommitFlag::PutForStateTransfer, segment);
            t.by_segment.remove(&segment);
        }
        drop(t);
        // the task completes when its last segment arrived; remove it so the end of the transfer is noticed
        if task.is_completed_successfully() {
            self.remove_transfer(task);
            self.notify_end_of_state_transfer_if_needed();
        }
    }

    /// `stop`: cancel every inbound transfer.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let tasks: Vec<Arc<InboundTransferTask>> = {
            let mut t = self.transfers.lock().unwrap();
            let all = t.by_source.values().flatten().cloned().collect();
            t.by_source.clear();
            t.by_segment.clear();
            all
        };
        for task in tasks {
            task.cancel(self.env.as_ref());
        }
    }
}
