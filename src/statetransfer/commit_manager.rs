//! `CommitManager` (`util/concurrent/CommitManager.java`, in 14.0.35.Final `statetransfer/CommitManager.java`):
//! while state transfer is tracked, an entry committed by a normal write must not be overwritten by a
//! state transfer entry that was read before the write happened. Every normal commit leaves a
//! `DiscardPolicy` on its key; a later `PUT_FOR_STATE_TRANSFER` commit of the same key is dropped.
//! A state transfer commit while nothing is tracked (the transfer ended or a `clear` cancelled it) is
//! dropped too.

use std::collections::HashMap;
use std::sync::Mutex;

/// The `Flag` a commit carries (`null` for a normal operation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFlag {
    /// An ordinary write.
    Normal,
    /// `PUT_FOR_STATE_TRANSFER`: a write state transfer applies.
    PutForStateTransfer,
    /// `PUT_FOR_X_SITE_STATE_TRANSFER`: a write cross-site state transfer applies.
    PutForXSiteStateTransfer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscardPolicy {
    discard_st: bool,
    discard_xsite_st: bool,
}

impl DiscardPolicy {
    fn ignore(&self, op: CommitFlag) -> bool {
        (self.discard_st && op == CommitFlag::PutForStateTransfer) || (self.discard_xsite_st && op == CommitFlag::PutForXSiteStateTransfer)
    }

    /// `update`: returns whether the policy became useless.
    fn update(&mut self, discard_st: bool, discard_xsite_st: bool) -> bool {
        self.discard_st = discard_st;
        self.discard_xsite_st = discard_xsite_st;
        !self.discard_st && !self.discard_xsite_st
    }
}

#[derive(Default)]
struct State {
    track_state_transfer: bool,
    track_xsite_state_transfer: bool,
    /// segment -> key -> policy
    tracker: HashMap<i32, HashMap<Vec<u8>, DiscardPolicy>>,
}

/// `CommitManager`: while a state transfer runs, remembers the keys ordinary writes committed, so the state transfer does not overwrite them with older values.
#[derive(Default)]
pub struct CommitManager {
    state: Mutex<State>,
}

impl CommitManager {
    /// A manager tracking nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts tracking the commits that `flag`'s writes must not overwrite.
    pub fn start_track(&self, flag: CommitFlag) {
        self.set_track(flag, true);
    }

    /// Stops tracking for `flag`, dropping the keys no other tracking still needs.
    pub fn stop_track(&self, flag: CommitFlag) {
        self.set_track(flag, false);
        let mut s = self.state.lock().unwrap();
        if !s.track_state_transfer && !s.track_xsite_state_transfer {
            s.tracker.clear();
        } else {
            let (st, xs) = (s.track_state_transfer, s.track_xsite_state_transfer);
            s.tracker.retain(|_, entries| {
                entries.retain(|_, p| !p.update(st, xs));
                !entries.is_empty()
            });
        }
    }

    /// Stop tracking one segment (called when its transfer completed).
    pub fn stop_track_for(&self, flag: CommitFlag, segment: i32) {
        let mut s = self.state.lock().unwrap();
        if flag == CommitFlag::PutForStateTransfer
            && s.track_state_transfer
            && let Some(entries) = s.tracker.get_mut(&segment)
        {
            // DiscardPolicy.stopForST: update(false, discardXSiteST)
            entries.retain(|_, p| {
                let x = p.discard_xsite_st;
                !p.update(false, x)
            });
            if entries.is_empty() {
                s.tracker.remove(&segment);
            }
        }
    }

    /// Whether commits are tracked for `flag`.
    pub fn is_tracking(&self, flag: CommitFlag) -> bool {
        let s = self.state.lock().unwrap();
        match flag {
            CommitFlag::PutForStateTransfer => s.track_state_transfer,
            CommitFlag::PutForXSiteStateTransfer => s.track_xsite_state_transfer,
            CommitFlag::Normal => false,
        }
    }

    /// `true` if no keys are tracked.
    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().tracker.is_empty()
    }

    /// The segments with tracked keys, ascending.
    pub fn tracked_segments(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self.state.lock().unwrap().tracker.keys().copied().collect();
        v.sort_unstable();
        v
    }

    fn set_track(&self, flag: CommitFlag, value: bool) {
        let mut s = self.state.lock().unwrap();
        match flag {
            CommitFlag::PutForStateTransfer => s.track_state_transfer = value,
            CommitFlag::PutForXSiteStateTransfer => s.track_xsite_state_transfer = value,
            CommitFlag::Normal => {}
        }
    }

    /// `commit`: run `apply` (which makes the entry visible) unless the policy says the entry is stale.
    /// Returns whether the entry was committed.
    pub fn commit(&self, key: &[u8], op: CommitFlag, segment: i32, apply: impl FnOnce()) -> bool {
        let mut s = self.state.lock().unwrap();
        let (st, xs) = (s.track_state_transfer, s.track_xsite_state_transfer);
        if op == CommitFlag::Normal && !st && !xs {
            drop(s);
            apply();
            return true;
        }
        let track_disabled = (op == CommitFlag::PutForStateTransfer && !st) || (op == CommitFlag::PutForXSiteStateTransfer && !xs);
        if track_disabled {
            return false;
        }
        let existing = s.tracker.get(&segment).and_then(|e| e.get(key)).copied();
        if existing.is_some_and(|p| p.ignore(op)) {
            return false;
        }
        apply();
        // calculateDiscardPolicy
        let discard_st = st && op != CommitFlag::PutForStateTransfer;
        let discard_xs = xs && op != CommitFlag::PutForXSiteStateTransfer;
        let entries = s.tracker.entry(segment).or_default();
        if discard_st || discard_xs {
            entries.insert(key.to_vec(), DiscardPolicy { discard_st, discard_xsite_st: discard_xs });
        } else {
            entries.remove(key);
        }
        if entries.is_empty() {
            s.tracker.remove(&segment);
        }
        true
    }
}
