//! Port of `notifications/cachelistener/CacheNotifierImplTest` (upstream 14.0.35.Final, 14 tests): a
//! listener that handles every event type is registered on a notifier over a local cache, and every
//! `notify*` entry point is called twice (pre, then post) or once (events without a pre/post form).
//!
//! Adaptations: the mock cache is the notifier's cache name; `getCache()` is `event.cache_name`;
//! `GlobalTransaction` is an id string; `ImmortalCacheEntry`/`TestInternalCacheEntryFactory` entries of the
//! two evicted tests are `(key, value)` pairs (this port has no `InternalCacheEntry` hierarchy in the
//! notifier API, and what the tests assert is the key and value in the event's entries).
//! `CacheNotifierImplWithConverterTest` and the `*InitialTransfer*` tests extend this class but need
//! data conversion / `includeCurrentState` and are not ported.

use std::sync::{Arc, Mutex};

use infinispan::commands::object::CacheObject;
use infinispan::notify::cache_notifier::{CacheNotifier, CacheMode, InvocationContext, LocalLogic, NotifierConfig};
use infinispan::notify::event::{Event, EventKind};
use infinispan::notify::listener_registration::{ListenerAttrs, ListenerDefinition, ListenerMethod};

const MOCK_CACHE: &str = "mockCache";

/// `CacheListener`: `@Listener`, one method with every event annotation.
#[derive(Default)]
struct CacheListener {
    events: Mutex<Vec<Event>>,
    received_pre: Mutex<bool>,
    received_post: Mutex<bool>,
}

impl CacheListener {
    fn definition(self: &Arc<Self>) -> Arc<ListenerDefinition> {
        let me = self.clone();
        ListenerDefinition::new(
            "CacheListener",
            ListenerAttrs::default(),
            vec![ListenerMethod::new(
                "handle",
                &[
                    EventKind::CacheEntryActivated,
                    EventKind::CacheEntryCreated,
                    EventKind::CacheEntriesEvicted,
                    EventKind::CacheEntryInvalidated,
                    EventKind::CacheEntryLoaded,
                    EventKind::CacheEntryModified,
                    EventKind::CacheEntryPassivated,
                    EventKind::CacheEntryRemoved,
                    EventKind::CacheEntryVisited,
                    EventKind::TransactionCompleted,
                    EventKind::TransactionRegistered,
                    EventKind::CacheEntryExpired,
                ],
                move |e| {
                    me.events.lock().unwrap().push(e.clone());
                    if e.pre {
                        *me.received_pre.lock().unwrap() = true;
                    } else {
                        *me.received_post.lock().unwrap() = true;
                    }
                },
            )],
        )
    }

    fn event(&self, i: usize) -> Event {
        self.events.lock().unwrap()[i].clone()
    }
    fn invocation_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
    fn received_pre(&self) -> bool {
        *self.received_pre.lock().unwrap()
    }
    fn received_post(&self) -> bool {
        *self.received_post.lock().unwrap()
    }
}

struct Fixture {
    n: CacheNotifier,
    cl: Arc<CacheListener>,
    ctx: InvocationContext,
}

fn setup() -> Fixture {
    let n = CacheNotifier::new(NotifierConfig::new(MOCK_CACHE, CacheMode::Local), Arc::new(LocalLogic));
    let cl = Arc::new(CacheListener::default());
    n.add_listener(&cl.definition()).expect("registers");
    Fixture { n, cl, ctx: InvocationContext::LOCAL }
}

fn o(s: &str) -> CacheObject {
    CacheObject::string(s)
}

fn null() -> CacheObject {
    CacheObject::Null
}

#[test]
fn test_notify_cache_entry_created() {
    let f = setup();
    f.n.notify_cache_entry_created(&o("k"), &o("v1"), None, true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_created(&o("k"), &o("v1"), None, false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    assert_eq!(MOCK_CACHE, f.cl.event(0).cache_name);
    assert_eq!(EventKind::CacheEntryCreated, f.cl.event(0).get_type());
    assert_eq!(o("k"), f.cl.event(0).key);
    assert_eq!(null(), f.cl.event(0).value);
    assert_eq!(MOCK_CACHE, f.cl.event(1).cache_name);
    assert_eq!(EventKind::CacheEntryCreated, f.cl.event(1).get_type());
    assert_eq!(o("k"), f.cl.event(1).key);
    assert_eq!(o("v1"), f.cl.event(1).value);
}

#[test]
fn test_notify_cache_entry_modified() {
    let f = setup();
    f.n.notify_cache_entry_modified(&o("k"), &o("v2"), None, &o("v1"), None, true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_modified(&o("k"), &o("v2"), None, &o("v1"), None, false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    assert_eq!(MOCK_CACHE, f.cl.event(0).cache_name);
    assert_eq!(EventKind::CacheEntryModified, f.cl.event(0).get_type());
    assert_eq!(o("k"), f.cl.event(0).key);
    assert_eq!(o("v1"), f.cl.event(0).value);
    assert!(!f.cl.event(0).created);
    assert_eq!(EventKind::CacheEntryModified, f.cl.event(1).get_type());
    assert_eq!(o("k"), f.cl.event(1).key);
    assert_eq!(o("v2"), f.cl.event(1).value);
    assert!(!f.cl.event(1).created);
}

#[test]
fn test_notify_cache_entry_removed() {
    let f = setup();
    f.n.notify_cache_entry_removed(&o("k"), &o("v"), None, true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_removed(&o("k"), &o("v"), None, false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    assert_eq!(EventKind::CacheEntryRemoved, f.cl.event(0).get_type());
    assert_eq!(o("k"), f.cl.event(0).key);
    assert_eq!(o("v"), f.cl.event(0).value);
    assert_eq!(o("v"), f.cl.event(0).old_value);
    assert_eq!(EventKind::CacheEntryRemoved, f.cl.event(1).get_type());
    assert_eq!(o("k"), f.cl.event(1).key);
    assert_eq!(null(), f.cl.event(1).value);
    assert_eq!(o("v"), f.cl.event(1).old_value);
}

#[test]
fn test_notify_cache_entry_visited() {
    let f = setup();
    f.n.notify_cache_entry_visited(&o("k"), &o("v"), true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_visited(&o("k"), &o("v"), false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(MOCK_CACHE, f.cl.event(i).cache_name);
        assert_eq!(EventKind::CacheEntryVisited, f.cl.event(i).get_type());
        assert_eq!(o("k"), f.cl.event(i).key);
        assert_eq!(o("v"), f.cl.event(i).value);
    }
}

#[test]
fn test_notify_cache_entry_evicted() {
    let f = setup();
    f.n.notify_cache_entries_evicted(&[(o("k"), o("v"))], None).unwrap();

    assert!(f.cl.received_post());
    assert_eq!(1, f.cl.invocation_count());
    assert_eq!(MOCK_CACHE, f.cl.event(0).cache_name);
    assert_eq!(EventKind::CacheEntriesEvicted, f.cl.event(0).get_type());
    let entries = f.cl.event(0).entries;
    assert_eq!((o("k"), o("v")), entries[0]);
}

/// `testNotifyCacheEntriesEvicted`: upstream builds the entry with `TestInternalCacheEntryFactory` rather
/// than `ImmortalCacheEntry`; the notifier sees a key and a value either way.
#[test]
fn test_notify_cache_entries_evicted() {
    let f = setup();
    f.n.notify_cache_entries_evicted(&[(o("k"), o("v"))], None).unwrap();

    assert!(f.cl.received_post());
    assert_eq!(1, f.cl.invocation_count());
    assert_eq!(EventKind::CacheEntriesEvicted, f.cl.event(0).get_type());
    assert_eq!((o("k"), o("v")), f.cl.event(0).entries[0]);
}

#[test]
fn test_notify_cache_entry_expired() {
    let f = setup();
    f.n.notify_cache_entry_expired(&o("k"), &o("v"), None, None).unwrap();

    assert!(f.cl.received_post());
    assert_eq!(f.cl.invocation_count(), 1);
    assert_eq!(f.cl.event(0).cache_name, MOCK_CACHE);
    assert_eq!(f.cl.event(0).get_type(), EventKind::CacheEntryExpired);
    assert_eq!(f.cl.event(0).key, o("k"));
    assert_eq!(f.cl.event(0).value, o("v"));
}

#[test]
fn test_notify_cache_entry_invalidated() {
    let f = setup();
    f.n.notify_cache_entry_invalidated(&o("k"), &o("v"), None, true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_invalidated(&o("k"), &o("v"), None, false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(MOCK_CACHE, f.cl.event(i).cache_name);
        assert_eq!(EventKind::CacheEntryInvalidated, f.cl.event(i).get_type());
        assert_eq!(o("k"), f.cl.event(i).key);
        assert_eq!(o("v"), f.cl.event(i).value);
    }
}

#[test]
fn test_notify_cache_entry_loaded() {
    let f = setup();
    f.n.notify_cache_entry_loaded(&o("k"), &o("v"), true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_loaded(&o("k"), &o("v"), false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(EventKind::CacheEntryLoaded, f.cl.event(i).get_type());
        assert_eq!(o("k"), f.cl.event(i).key);
        assert_eq!(o("v"), f.cl.event(i).value);
    }
}

#[test]
fn test_notify_cache_entry_activated() {
    let f = setup();
    f.n.notify_cache_entry_activated(&o("k"), &o("v"), true, &f.ctx, None).unwrap();
    f.n.notify_cache_entry_activated(&o("k"), &o("v"), false, &f.ctx, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(EventKind::CacheEntryActivated, f.cl.event(i).get_type());
        assert_eq!(o("k"), f.cl.event(i).key);
        assert_eq!(o("v"), f.cl.event(i).value);
    }
}

#[test]
fn test_notify_cache_entry_passivated() {
    let f = setup();
    f.n.notify_cache_entry_passivated(&o("k"), &o("v"), true, None).unwrap();
    f.n.notify_cache_entry_passivated(&o("k"), &o("v"), false, None).unwrap();

    assert!(f.cl.received_post());
    assert!(f.cl.received_pre());
    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(MOCK_CACHE, f.cl.event(i).cache_name);
        assert_eq!(EventKind::CacheEntryPassivated, f.cl.event(i).get_type());
        assert_eq!(o("k"), f.cl.event(i).key);
        assert_eq!(o("v"), f.cl.event(i).value);
    }
}

#[test]
fn test_notify_transaction_completed() {
    let f = setup();
    let tx = "GlobalTx:node:1";
    f.n.notify_transaction_completed(tx, true, &f.ctx).unwrap();
    f.n.notify_transaction_completed(tx, false, &f.ctx).unwrap();

    assert_eq!(2, f.cl.invocation_count());
    assert_eq!(MOCK_CACHE, f.cl.event(0).cache_name);
    assert_eq!(EventKind::TransactionCompleted, f.cl.event(0).get_type());
    assert!(f.cl.event(0).transaction_successful);
    assert_eq!(f.cl.event(0).transaction_id.as_deref(), Some(tx));
    assert_eq!(EventKind::TransactionCompleted, f.cl.event(1).get_type());
    assert!(!f.cl.event(1).transaction_successful);
    assert_eq!(f.cl.event(1).transaction_id.as_deref(), Some(tx));
}

#[test]
fn test_notify_transaction_registered() {
    let f = setup();
    let tx = "GlobalTx:node:1";
    f.n.notify_transaction_registered(tx, false).unwrap();
    f.n.notify_transaction_registered(tx, false).unwrap();

    assert_eq!(2, f.cl.invocation_count());
    for i in 0..2 {
        assert_eq!(MOCK_CACHE, f.cl.event(i).cache_name);
        assert_eq!(EventKind::TransactionRegistered, f.cl.event(i).get_type());
        assert_eq!(f.cl.event(i).transaction_id.as_deref(), Some(tx));
    }
}
