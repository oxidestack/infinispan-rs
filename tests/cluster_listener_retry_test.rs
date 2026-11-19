//! The three retry cases of `ClusterListenerReplTest` / `AbstractClusterListenerNonTxTest` (14.0.35.Final):
//! the primary owner of a key goes down while a put from the listener's node is in flight, at three points.
//!
//! - `testPrimaryOwnerGoesDownAfterSendingEvent`: the primary has replicated the put and is stuck raising its
//!   own notification (`waitUntilNotificationRaised` blocks `notifyCacheEntryCreated(.., pre=false)`); here
//!   an [`AfterApplyHook`] holds the primary after it replicated and before it commits. The backups already
//!   raised the (not retried) create; killing the primary makes the originator retry, and each retry is a
//!   modification flagged as retried.
//! - `testPrimaryOwnerGoesDownBeforeBackupRaisesEvent`: `ControlledRpcManager` swallows the primary's put to
//!   the backups (`skipSend`); an [`RpcInterceptor`] never delivers it. No backup saw the put before the
//!   primary died, so the first event is a *retried* create.
//! - `testPrimaryOwnerGoesDownAfterBackupRaisesEvent`: `BlockingInterceptor`s before `EntryWrappingInterceptor`
//!   on both backups hold the put; the interceptors are removed, the primary killed, then the backups
//!   released: a create that is not retried, followed by retried modifications. The in-memory cluster has no
//!   latency between the kill and the retry, so the originator's retry is held (`GateRetries`) until the released
//!   commands have committed, which is the order upstream's timing gives.
//!
//! A request in flight to a member that leaves the view fails with a suspect error at once (the in-memory
//! transport polls reachability, `cache/testing.rs`), the way JGroups' `RequestCorrelator` does.
//! `MagicKey(cache1, cache2)` is [`key_with_primary`] on node 1.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::{Intercepted, ReplCluster, RpcInterceptor};
use infinispan::cache::write_pipeline::{AfterApplyHook, ExecuteHook};
use infinispan::ch::address::Address;
use infinispan::commands::object::CacheObject;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::commands::write::{WriteCommand, flags};
use infinispan::notify::event::{Event, EventKind};
use infinispan::topology::support::BoxFuture;
use tokio::sync::Notify;

const CACHE_NAME: &str = "cluster-listener";
const FIRST_VALUE: &str = "first-value";
const WAIT: Duration = Duration::from_secs(20);

use common::cluster_listener::*;

fn is_put(c: &WriteCommand) -> bool {
    matches!(c, WriteCommand::Put(_)) && !c.has_any_flag(flags::PUT_FOR_STATE_TRANSFER)
}

/// Holds `PutKeyValueCommand`s after they were applied on this node, until released (or forever).
#[derive(Default)]
struct HoldPuts {
    blocked: AtomicUsize,
    release: Notify,
    released: std::sync::atomic::AtomicBool,
    forever: bool,
}

impl HoldPuts {
    fn releasable() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn forever() -> Arc<Self> {
        Arc::new(Self { forever: true, ..Default::default() })
    }
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

impl AfterApplyHook for HoldPuts {
    fn after_apply(self: Arc<Self>, _local: &Address, command: &WriteCommand, _origin_local: bool) -> BoxFuture<'static, ()> {
        let hold = is_put(command);
        Box::pin(async move {
            if !hold {
                return;
            }
            self.blocked.fetch_add(1, Ordering::SeqCst);
            if self.forever {
                std::future::pending::<()>().await;
            }
            loop {
                let notified = self.release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.released.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
    }
}

/// Holds the retry of a command (an attempt flagged `COMMAND_RETRY`) on the originator until opened.
struct GateRetries {
    open: std::sync::atomic::AtomicBool,
    changed: Notify,
}

impl GateRetries {
    fn closed() -> Arc<Self> {
        Arc::new(GateRetries { open: Default::default(), changed: Notify::new() })
    }
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

impl ExecuteHook for GateRetries {
    fn before_write(self: Arc<Self>, _local: &Address, command: &WriteCommand) -> BoxFuture<'static, ()> {
        let retry = command.has_any_flag(flags::COMMAND_RETRY);
        Box::pin(async move {
            while retry && !self.open.load(Ordering::SeqCst) {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.open.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        })
    }
}

/// `ControlledRpcManager.expectCommand(PutKeyValueCommand.class).skipSend()`: a put sent by `from` is never delivered.
struct SwallowPutsFrom {
    from: Address,
    swallowed: AtomicUsize,
}

impl RpcInterceptor for SwallowPutsFrom {
    fn intercept(&self, from: &Address, _to: &Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
        let swallow = *from == self.from && matches!(&command.payload, CacheRpcPayload::Single(c) if is_put(c));
        if swallow {
            self.swallowed.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move {
            if swallow {
                std::future::pending::<()>().await;
            }
            Intercepted::Deliver
        })
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `checkEvent`
fn check_event(event: &Event, key: &CacheObject, is_created: bool, is_retried: bool) {
    if is_created {
        assert_eq!(EventKind::CacheEntryCreated, event.get_type());
        assert_eq!(event.command_retried, is_retried, "{event:?}");
    } else {
        assert_eq!(EventKind::CacheEntryModified, event.get_type());
        assert!(event.command_retried, "{event:?}");
    }
    assert_eq!(*key, event.key);
    assert_eq!(s(FIRST_VALUE), event.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_primary_owner_goes_down_after_sending_event() {
    let c = ReplCluster::start(3, &[CACHE_NAME]).await;
    let (cache0, cache1) = (c.cache(0, CACHE_NAME), c.cache(1, CACHE_NAME));
    let cl = listen(&cache0);

    let hold = HoldPuts::forever();
    cache1.engine().set_after_apply_hook(Some(hold.clone()));

    let key = key_with_primary(&c, CACHE_NAME, 1);
    let put = tokio::spawn({
        let (cache0, key) = (cache0.clone(), key.clone());
        async move { cache0.put(&key, &s(FIRST_VALUE)).await }
    });

    // checkPoint.awaitStrict("post_raise_notification_invoked"): the primary replicated and is held
    eventually("the primary to raise its notification", || hold.blocked.load(Ordering::SeqCst) == 1).await;

    // Kill the cache now - note this will automatically unblock the fork thread
    c.kill_node(1);

    tokio::time::timeout(Duration::from_secs(10), put).await.expect("put returns").unwrap().expect("put succeeds");

    c.nodes.wait_for_no_rebalance(&[0, 2], CACHE_NAME, WAIT).await.unwrap();

    // The command is retried during rebalance, but there are 5 topology updates (thus up to 5 retries)
    let events = cl.events();
    assert!(events.len() >= 2 && events.len() <= 6, "Expected 2 - 6 events, but received {events:?}");
    // Since the first event was generated properly it is a create without retry
    check_event(&events[0], &key, true, false);

    let cache0_primary = cache0.engine().owners_of(&key).unwrap().0;
    let cache2_primary = c.cache(2, CACHE_NAME).engine().owners_of(&key).unwrap().0;
    // we expect that now both nodes have the same topology
    assert_eq!(cache0_primary, cache2_primary);

    // Any extra events would be retries as modifications
    for e in events.iter().skip(1) {
        check_event(e, &key, false, true);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_primary_owner_goes_down_before_backup_raises_event() {
    let c = ReplCluster::start(3, &[CACHE_NAME]).await;
    let cache0 = c.cache(0, CACHE_NAME);
    let cl = listen(&cache0);

    // Now we want to block the outgoing put to the backup owners
    let swallow = Arc::new(SwallowPutsFrom { from: c.address(1), swallowed: AtomicUsize::new(0) });
    c.set_interceptor(Some(swallow.clone()));

    let key = key_with_primary(&c, CACHE_NAME, 1);
    let put = tokio::spawn({
        let (cache0, key) = (cache0.clone(), key.clone());
        async move { cache0.put(&key, &s(FIRST_VALUE)).await }
    });

    // Wait until the primary owner has sent the put command to the backups, and discard the request
    eventually("the primary to send the put to the backups", || swallow.swallowed.load(Ordering::SeqCst) >= 1).await;

    // Kill the cache now
    c.kill_node(1);

    // This should return null normally, but when it was retried it returns its own value
    let rv = tokio::time::timeout(Duration::from_secs(10), put).await.expect("put returns").unwrap().expect("put succeeds");
    assert!(rv.is_none() || rv == Some(s(FIRST_VALUE)), "{rv:?}");

    // We should have received an event that was marked as retried
    let mut events = cl.events();
    assert!(!events.is_empty());
    // Because a rebalance has 4 phases, the command may be retried 4 times
    assert!(events.len() <= 4, "{events:?}");
    let first = events.remove(0);
    // First always has to be CREATE
    check_event(&first, &key, true, true);
    for e in &events {
        check_event(e, &key, e.get_type() == EventKind::CacheEntryCreated, true);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_primary_owner_goes_down_after_backup_raises_event() {
    let c = ReplCluster::start(3, &[CACHE_NAME]).await;
    let (cache0, cache2) = (c.cache(0, CACHE_NAME), c.cache(2, CACHE_NAME));
    let cl = listen(&cache0);

    // BlockingInterceptor before EntryWrappingInterceptor on both backups
    let (hold0, hold2) = (HoldPuts::releasable(), HoldPuts::releasable());
    cache0.engine().set_after_apply_hook(Some(hold0.clone()));
    cache2.engine().set_after_apply_hook(Some(hold2.clone()));

    let gate = GateRetries::closed();
    cache0.engine().set_execute_hook(Some(gate.clone()));

    // this is a replicated cache, all other nodes are backup owners
    let key = key_with_primary(&c, CACHE_NAME, 1);
    let put = tokio::spawn({
        let (cache0, key) = (cache0.clone(), key.clone());
        async move { cache0.put(&key, &s(FIRST_VALUE)).await }
    });

    // Wait until the primary owner has sent the put command successfully to both backups
    eventually("both backups to receive the put", || hold0.blocked.load(Ordering::SeqCst) == 1 && hold2.blocked.load(Ordering::SeqCst) == 1).await;

    // Remove the interceptor so the next command can proceed properly
    cache0.engine().set_after_apply_hook(None);
    cache2.engine().set_after_apply_hook(None);

    // Kill the cache now - note this will automatically unblock the fork thread
    c.kill_node(1);

    // Unblock the command. Upstream's `killCacheManagers` returns only once the manager stopped, long before the
    // retry could start; the in-memory cluster is instantaneous, so the retry is held until the released commands
    // have committed and raised their events.
    hold0.release();
    hold2.release();
    eventually("both backups to raise the create", || cl.len() == 1 && cache2.engine().container.len() == 1).await;
    gate.open();

    // This should return null normally, but since it was retried it returns it's own value :(
    // Maybe some day this can work properly
    let rv = tokio::time::timeout(Duration::from_secs(10), put).await.expect("put returns").unwrap().expect("put succeeds");
    assert_eq!(Some(s(FIRST_VALUE)), rv);

    // We should have received an event that was marked as retried
    let events = cl.events();
    assert!(events.len() >= 2, "{events:?}");
    // Because a rebalance has 4 phases, the command may be retried 4 times
    assert!(events.len() <= 4, "{events:?}");

    // First create should not be retried since it was sent before node failure.
    check_event(&events[0], &key, true, false);

    // Events from retried commands are a MODIFY since CREATE was already done
    for e in events.iter().skip(1) {
        check_event(e, &key, false, true);
    }
    check_event(&events[1], &key, false, true);
}
