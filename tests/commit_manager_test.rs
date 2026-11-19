//! Port of `statetransfer/CommitManagerTest` (infinispan-core 14.0.35.Final). The mocks of the data
//! container are replaced by a counter of applied commits.

use std::cell::Cell;

use infinispan::statetransfer::commit_manager::{CommitFlag, CommitManager};

#[test]
fn should_track_and_stop_tracking() {
    let m = CommitManager::new();
    assert!(!m.is_tracking(CommitFlag::PutForStateTransfer));
    assert!(!m.is_tracking(CommitFlag::PutForXSiteStateTransfer));
    m.start_track(CommitFlag::PutForStateTransfer);
    assert!(m.is_tracking(CommitFlag::PutForStateTransfer));
    m.stop_track(CommitFlag::PutForStateTransfer);
    assert!(!m.is_tracking(CommitFlag::PutForStateTransfer));
    m.start_track(CommitFlag::PutForXSiteStateTransfer);
    assert!(m.is_tracking(CommitFlag::PutForXSiteStateTransfer));
    m.stop_track(CommitFlag::PutForXSiteStateTransfer);
    assert!(!m.is_tracking(CommitFlag::PutForXSiteStateTransfer));
}

#[test]
fn should_commit_entries_for_segment() {
    let m = CommitManager::new();
    m.start_track(CommitFlag::PutForStateTransfer);
    let applied = Cell::new(0);
    for i in 0..10 {
        for j in 0..10 {
            let key = format!("value-{i}-{j}");
            assert!(m.commit(key.as_bytes(), CommitFlag::PutForStateTransfer, i, || applied.set(applied.get() + 1)));
        }
    }
    assert_eq!(applied.get(), 100);
    // only state transfer entries were fed, and only state transfer is tracked: nothing is remembered
    assert!(m.is_empty());
    for s in 0..3 {
        m.stop_track_for(CommitFlag::PutForStateTransfer, s);
    }
    assert!(m.is_tracking(CommitFlag::PutForStateTransfer));
    assert!(m.is_empty());
}

#[test]
fn only_clear_segment_if_no_xsite_st() {
    let m = CommitManager::new();
    m.start_track(CommitFlag::PutForStateTransfer);
    m.start_track(CommitFlag::PutForXSiteStateTransfer);
    let xsite = |i: i32| i % 2 != 0;
    for i in 0..10 {
        for j in 0..10 {
            let key = format!("value-{i}-{j}");
            let op = if xsite(i) { CommitFlag::PutForXSiteStateTransfer } else { CommitFlag::PutForStateTransfer };
            assert!(m.commit(key.as_bytes(), op, i, || {}));
        }
    }
    assert_eq!(m.tracked_segments().len(), 10);
    for i in 0..10 {
        m.stop_track_for(CommitFlag::PutForStateTransfer, i);
    }
    assert_eq!(m.tracked_segments(), vec![0, 2, 4, 6, 8]);
}

// ---- behaviour the upstream class has but its unit test does not exercise

#[test]
fn a_normal_write_wins_over_a_later_state_transfer_entry_of_the_same_key() {
    let m = CommitManager::new();
    m.start_track(CommitFlag::PutForStateTransfer);
    let order = std::cell::RefCell::new(Vec::new());
    assert!(m.commit(b"k", CommitFlag::Normal, 3, || order.borrow_mut().push("write")));
    assert!(!m.commit(b"k", CommitFlag::PutForStateTransfer, 3, || order.borrow_mut().push("state")));
    // another key of the segment is still fed by the transfer
    assert!(m.commit(b"other", CommitFlag::PutForStateTransfer, 3, || order.borrow_mut().push("other")));
    assert_eq!(*order.borrow(), vec!["write", "other"]);
    // once the segment finished, its tracking is dropped
    m.stop_track_for(CommitFlag::PutForStateTransfer, 3);
    assert!(m.is_empty());
}

#[test]
fn state_transfer_entries_are_dropped_when_nothing_is_tracked() {
    let m = CommitManager::new();
    assert!(!m.commit(b"k", CommitFlag::PutForStateTransfer, 0, || panic!("must not apply")));
    assert!(m.commit(b"k", CommitFlag::Normal, 0, || {}));
    m.start_track(CommitFlag::PutForStateTransfer);
    assert!(m.commit(b"a", CommitFlag::Normal, 0, || {}));
    m.stop_track(CommitFlag::PutForStateTransfer);
    assert!(m.is_empty());
    assert!(!m.commit(b"a", CommitFlag::PutForStateTransfer, 0, || panic!("must not apply")));
}
