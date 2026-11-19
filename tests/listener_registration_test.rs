//! Port of `notifications/cachelistener/ListenerRegistrationTest` (14.0.35.Final, 14 tests) and, below
//! them, the cluster-listener registration rules of `CacheNotifierImpl.addListenerInternal` that
//! `ClusterListenerReplTest` relies on (written for this port from the upstream source, not upstream tests).
//!
//! A listener class is a [`ListenerDefinition`]: Rust has no annotations or reflection, so what
//! `Class.getMethods()` and `isAnnotationPresent` return is stated by hand. `newNotifier()` is a notifier over
//! a LOCAL cache.

use std::sync::{Arc, Mutex};

use infinispan::notify::cache_notifier::{CacheMode, CacheNotifier, LocalLogic, NotifierConfig};
use infinispan::notify::event::{Event, EventClass, EventKind};
use infinispan::notify::listener_registration::{ListenerAttrs, ListenerDefinition, ListenerMethod, NotifierError, Observation, ReturnType};

fn new_notifier() -> CacheNotifier {
    CacheNotifier::new(NotifierConfig::new("cache", CacheMode::Local), Arc::new(LocalLogic))
}

fn nop(name: &str, annotations: &[EventKind]) -> ListenerMethod {
    ListenerMethod::new(name, annotations, |_| {})
}

fn assert_incorrect(r: Result<(), NotifierError>) {
    match r {
        Err(NotifierError::IncorrectListener(_)) => {}
        other => panic!("expected IncorrectListenerException, got {other:?}"),
    }
}

use EventKind::{CacheEntryRemoved, CacheEntryVisited};

#[test]
fn test_control() {
    let l = ListenerDefinition::new("TestControlListener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryVisited, CacheEntryRemoved])]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    assert_eq!(1, n.get_listeners().len());
}

#[test]
fn test_cache_listener_no_methods() {
    let l = ListenerDefinition::new("TestCacheListenerNoMethodsListener", ListenerAttrs::default(), vec![]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    assert_eq!("TestCacheListenerNoMethodsListener", l.class_name);
    // since the valid listener has no methods to listen
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_non_annotated_listener() {
    let l = ListenerDefinition::unannotated("TestNonAnnotatedListener", vec![]);
    let n = new_notifier();
    let err = n.add_listener(&l).expect_err("Should not accept an un-annotated cache listener");
    assert_eq!(
        err,
        NotifierError::IncorrectListener("Cache listener class TestNonAnnotatedListener must be annotated with org.infinispan.notifications.Listener".into())
    );
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

/// `TestNonPublicListener` is a protected class whose callback has no parameter: the registration fails
/// on the signature, as upstream's class is never checked for visibility.
#[test]
fn test_non_public_listener() {
    let l = ListenerDefinition::new("TestNonPublicListener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryVisited]).with_params(vec![])]);
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_non_public_listener_method() {
    let l = ListenerDefinition::new("TestNonPublicListenerMethodListener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryVisited]).non_public()]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    // should not fail, should just not register anything
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_non_void_return_type_method() {
    let l = ListenerDefinition::new("TestNonVoidReturnTypeMethodListener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryVisited]).with_returns(ReturnType::Other("String".into()))]);
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_incorrect_method_signature1() {
    let l = ListenerDefinition::new("TestIncorrectMethodSignature1Listener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryVisited]).with_params(vec![])]);
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_incorrect_method_signature2() {
    let l = ListenerDefinition::new(
        "TestIncorrectMethodSignature2Listener",
        ListenerAttrs::default(),
        vec![nop("callback", &[CacheEntryVisited]).with_params(vec![EventClass::Event, EventClass::Other("String".into())])],
    );
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

/// `callback(Event e, String... s)` is two parameters.
#[test]
fn test_incorrect_method_signature3() {
    let l = ListenerDefinition::new(
        "TestIncorrectMethodSignature3Listener",
        ListenerAttrs::default(),
        vec![nop("callback", &[CacheEntryVisited]).with_params(vec![EventClass::Event, EventClass::Other("String[]".into())])],
    );
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_unassignable_method_signature() {
    let l = ListenerDefinition::new(
        "TestUnassignableMethodSignatureListener",
        ListenerAttrs::default(),
        vec![nop("callback", &[CacheEntryVisited]).with_params(vec![EventClass::Concrete(CacheEntryRemoved)])],
    );
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
    assert!(n.get_listeners().is_empty(), "No listeners should be registered.");
}

#[test]
fn test_partly_unassignable_method_signature() {
    let l = ListenerDefinition::new(
        "TestPartlyUnassignableMethodSignatureListener",
        ListenerAttrs::default(),
        vec![nop("callback", &[CacheEntryVisited, CacheEntryRemoved]).with_params(vec![EventClass::Concrete(CacheEntryRemoved)])],
    );
    let n = new_notifier();
    assert_incorrect(n.add_listener(&l));
}

#[test]
fn test_multiple_methods() {
    let l = ListenerDefinition::new("TestMultipleMethodsListener", ListenerAttrs::default(), vec![nop("callback1", &[CacheEntryVisited]), nop("callback2", &[CacheEntryRemoved])]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    assert_eq!(1, n.invocation_count(CacheEntryVisited));
    assert_eq!(1, n.invocation_count(CacheEntryRemoved));
    assert_eq!(1, n.get_listeners().len());
}

#[test]
fn test_multiple_annotations_one_method() {
    let l = ListenerDefinition::new("TestMultipleAnnotationsOneMethodListener", ListenerAttrs::default(), vec![nop("callback", &[CacheEntryRemoved, CacheEntryVisited])]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    assert_eq!(1, n.invocation_count(CacheEntryVisited));
    assert_eq!(1, n.invocation_count(CacheEntryRemoved));
    assert_eq!(1, n.get_listeners().len());
}

#[test]
fn test_multiple_methods_one_annotation() {
    let l = ListenerDefinition::new("TestMultipleMethodsOneAnnotationListener", ListenerAttrs::default(), vec![nop("callback1", &[CacheEntryVisited]), nop("callback2", &[CacheEntryVisited])]);
    let n = new_notifier();
    n.add_listener(&l).unwrap();
    assert_eq!(2, n.invocation_count(CacheEntryVisited));
    assert_eq!(1, n.get_listeners().len());
}

// ---------------------------------------------------------------------------------------------------
// Cluster listeners (CacheNotifierImpl.java:191-194, 303-306, 1096-1119, 1278-1282, 1443-1445;
// AbstractListenerImpl.java:216-222)

fn repl_notifier() -> CacheNotifier {
    CacheNotifier::new(NotifierConfig::new("cache", CacheMode::ReplSync), Arc::new(LocalLogic))
}

/// MockServer's `InfinispanCacheListener`: created, modified, removed and evicted handlers on a clustered listener.
fn mockserver_like_listener(seen: &Arc<Mutex<Vec<Event>>>) -> Arc<ListenerDefinition> {
    let s1 = seen.clone();
    let s2 = seen.clone();
    ListenerDefinition::new(
        "InfinispanCacheListener",
        ListenerAttrs::clustered(),
        vec![
            ListenerMethod::new("onChanged", &[EventKind::CacheEntryCreated, EventKind::CacheEntryModified, EventKind::CacheEntryRemoved], move |e| s1.lock().unwrap().push(e.clone())),
            ListenerMethod::new("onEvicted", &[EventKind::CacheEntriesEvicted], move |e| s2.lock().unwrap().push(e.clone())),
        ],
    )
}

#[test]
fn clustered_listener_never_registers_the_evicted_handler() {
    let n = repl_notifier();
    let seen = Arc::new(Mutex::new(Vec::new()));
    n.add_listener(&mockserver_like_listener(&seen)).unwrap();
    assert_eq!(1, n.invocation_count(EventKind::CacheEntryCreated));
    assert_eq!(1, n.invocation_count(EventKind::CacheEntryModified));
    assert_eq!(1, n.invocation_count(EventKind::CacheEntryRemoved));
    assert_eq!(0, n.invocation_count(EventKind::CacheEntriesEvicted), "the @CacheEntriesEvicted handler is silently dropped");
    assert!(!n.has_listener(EventKind::CacheEntriesEvicted));
    n.notify_cache_entries_evicted(&[(infinispan::commands::object::CacheObject::string("k"), infinispan::commands::object::CacheObject::string("v"))], None).unwrap();
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn clustered_listener_only_evicted_handler_registers_nothing() {
    let n = repl_notifier();
    let l = ListenerDefinition::new("EvictedOnly", ListenerAttrs::clustered(), vec![nop("onEvicted", &[EventKind::CacheEntriesEvicted])]);
    n.add_listener(&l).unwrap();
    assert!(n.get_listeners().is_empty());
}

/// The evicted method is not validated either: a clustered listener whose evicted method has a bad
/// signature registers fine, while a plain listener is refused.
#[test]
fn clustered_listener_skips_validation_of_a_disallowed_annotation() {
    let bad = nop("onEvicted", &[EventKind::CacheEntriesEvicted]).with_params(vec![]);
    let clustered = ListenerDefinition::new("C", ListenerAttrs::clustered(), vec![bad.clone()]);
    let plain = ListenerDefinition::new("P", ListenerAttrs::default(), vec![bad]);
    let n = repl_notifier();
    n.add_listener(&clustered).unwrap();
    assert_incorrect(n.add_listener(&plain));
}

#[test]
fn local_listener_keeps_the_evicted_handler() {
    let n = repl_notifier();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    let l = ListenerDefinition::new("Plain", ListenerAttrs::default(), vec![ListenerMethod::new("onEvicted", &[EventKind::CacheEntriesEvicted], move |e| s.lock().unwrap().push(e.clone()))]);
    n.add_listener(&l).unwrap();
    n.notify_cache_entries_evicted(&[(infinispan::commands::object::CacheObject::string("k"), infinispan::commands::object::CacheObject::string("v"))], None).unwrap();
    assert_eq!(1, seen.lock().unwrap().len());
}

#[test]
fn clustered_listener_is_forced_to_post_observation() {
    use infinispan::commands::object::CacheObject;
    use infinispan::notify::cache_notifier::InvocationContext;
    let n = repl_notifier();
    let seen = Arc::new(Mutex::new(Vec::new()));
    // annotated BOTH (the default): the pre event still never arrives
    n.add_listener(&mockserver_like_listener(&seen)).unwrap();
    let (k, v) = (CacheObject::string("k"), CacheObject::string("v"));
    n.notify_cache_entry_created(&k, &v, None, true, &InvocationContext::LOCAL, None).unwrap();
    assert!(seen.lock().unwrap().is_empty(), "pre events are not observed by a cluster listener");
    n.notify_cache_entry_created(&k, &v, None, false, &InvocationContext::LOCAL, None).unwrap();
    assert_eq!(1, seen.lock().unwrap().len());
    // a non-clustered BOTH listener sees both
    let n2 = repl_notifier();
    let seen2 = Arc::new(Mutex::new(Vec::new()));
    let s = seen2.clone();
    n2.add_listener(&ListenerDefinition::new("Both", ListenerAttrs::default(), vec![ListenerMethod::new("m", &[EventKind::CacheEntryCreated], move |e| s.lock().unwrap().push(e.clone()))])).unwrap();
    n2.notify_cache_entry_created(&k, &v, None, true, &InvocationContext::LOCAL, None).unwrap();
    n2.notify_cache_entry_created(&k, &v, None, false, &InvocationContext::LOCAL, None).unwrap();
    assert_eq!(2, seen2.lock().unwrap().len());
}

#[test]
fn clustered_listener_observing_only_pre_is_refused() {
    let n = repl_notifier();
    let attrs = ListenerAttrs { observation: Observation::Pre, ..ListenerAttrs::clustered() };
    let l = ListenerDefinition::new("PreOnly", attrs, vec![nop("m", &[EventKind::CacheEntryCreated])]);
    match n.add_listener(&l) {
        Err(NotifierError::Cache(m)) => assert_eq!(m, "ISPN000364: Attempted to register cluster listener of class PreOnly, but listener is annotated as only observing pre events!"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn clustered_listener_on_an_invalidation_cache_is_refused() {
    let n = CacheNotifier::new(NotifierConfig::new("cache", CacheMode::InvalSync), Arc::new(LocalLogic));
    let l = ListenerDefinition::new("C", ListenerAttrs::clustered(), vec![nop("m", &[EventKind::CacheEntryCreated])]);
    assert_eq!(n.add_listener(&l), Err(NotifierError::UnsupportedOperation("Cluster listeners cannot be used with Invalidation Caches!".into())));
}

#[test]
fn replicated_cache_with_capacity_has_local_cluster_listeners() {
    let repl = repl_notifier();
    assert!(!repl.cluster_listener_on_primary_only());
    let zero = NotifierConfig { capacity_factor: 0.0, ..NotifierConfig::new("c", CacheMode::ReplSync) };
    assert!(CacheNotifier::new(zero, Arc::new(LocalLogic)).cluster_listener_on_primary_only());
    let zero_node = NotifierConfig { zero_capacity_node: true, ..NotifierConfig::new("c", CacheMode::ReplSync) };
    assert!(CacheNotifier::new(zero_node, Arc::new(LocalLogic)).cluster_listener_on_primary_only());
    assert!(CacheNotifier::new(NotifierConfig::new("c", CacheMode::DistSync), Arc::new(LocalLogic)).cluster_listener_on_primary_only());
    assert!(CacheNotifier::new(NotifierConfig::new("c", CacheMode::ScatteredSync), Arc::new(LocalLogic)).cluster_listener_on_primary_only());
    assert!(!CacheNotifier::new(NotifierConfig::new("c", CacheMode::Local), Arc::new(LocalLogic)).cluster_listener_on_primary_only());
}

#[test]
fn clustered_listener_on_a_primary_only_cache_is_not_ported() {
    let n = CacheNotifier::new(NotifierConfig::new("c", CacheMode::DistSync), Arc::new(LocalLogic));
    let l = ListenerDefinition::new("C", ListenerAttrs::clustered(), vec![nop("m", &[EventKind::CacheEntryCreated])]);
    assert!(matches!(n.add_listener(&l), Err(NotifierError::NotPorted(_))));
    assert!(n.get_listeners().is_empty());
}

#[test]
fn remove_listener_validates_the_class_and_removes_every_invocation() {
    let n = repl_notifier();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let l = mockserver_like_listener(&seen);
    n.add_listener(&l).unwrap();
    n.remove_listener(&l).unwrap();
    assert!(n.get_listeners().is_empty());
    assert_incorrect(n.remove_listener(&ListenerDefinition::unannotated("X", vec![])));
}
