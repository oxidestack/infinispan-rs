//! The five MockServer write operations over a 3-node REPL_SYNC cache (in-memory cluster that marshals
//! every request and answer): originator, primary forwarding, backup broadcast, CAS and the failure
//! answers.

use infinispan::cache::testing::ReplCluster;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::commands::compute_function::CacheLambda;
use infinispan::commands::object::CacheObject;

const CACHE: &str = "mockserver-state";

fn s(v: &str) -> CacheObject {
    CacheObject::string(v)
}

fn vw(value: &str, version: i64) -> CacheObject {
    CacheObject::User(VersionedWrapper::encode_object(&s(value), version).unwrap())
}

fn version_of(o: &CacheObject) -> i64 {
    let CacheObject::User(u) = o else { panic!("{o:?}") };
    VersionedWrapper::decode(u.bytes()).unwrap().version
}

/// every node holds `expected` for `key`
fn assert_everywhere(c: &ReplCluster, n: usize, key: &str, expected: Option<&CacheObject>) {
    for i in 0..n {
        let got = c.cache(i, CACHE).engine().local_get(&s(key));
        assert_eq!(got.as_ref(), expected, "node {i} key {key}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn put_from_every_node_reaches_every_node() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    for origin in 0..3 {
        let key = format!("k{origin}");
        let v = vw(&format!("v{origin}"), 1);
        assert_eq!(c.cache(origin, CACHE).put(&s(&key), &v).await.unwrap(), None);
        assert_everywhere(&c, 3, &key, Some(&v));
    }
    // overwrite from a different node: the previous value comes back
    let v2 = vw("w", 2);
    let prev = c.cache(2, CACHE).put(&s("k0"), &v2).await.unwrap();
    assert_eq!(prev, Some(vw("v0", 1)));
    assert_everywhere(&c, 3, "k0", Some(&v2));
}

#[tokio::test(flavor = "multi_thread")]
async fn put_if_absent_returns_the_existing_value_and_keeps_it() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    for origin in 0..3 {
        let key = format!("pia{origin}");
        assert_eq!(c.cache(origin, CACHE).put_if_absent(&s(&key), &vw("first", 1)).await.unwrap(), None);
        let other = (origin + 1) % 3;
        assert_eq!(c.cache(other, CACHE).put_if_absent(&s(&key), &vw("second", 1)).await.unwrap(), Some(vw("first", 1)));
        assert_everywhere(&c, 3, &key, Some(&vw("first", 1)));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compute_with_the_mockserver_lambda_increments_the_version_everywhere() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    for round in 1..=6i64 {
        let origin = (round % 3) as usize;
        let lambda = CacheLambda::mockserver_put(s(&format!("value-{round}")));
        let result = c.cache(origin, CACHE).compute(&s("kc"), lambda).await.unwrap().unwrap();
        assert_eq!(version_of(&result), round);
        assert_everywhere(&c, 3, "kc", Some(&vw(&format!("value-{round}"), round)));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replace_is_a_compare_and_set_on_version_and_value() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    let cache0 = c.cache(0, CACHE);
    cache0.put(&s("k"), &vw("a", 1)).await.unwrap();
    for origin in 0..3 {
        let cache = c.cache(origin, CACHE);
        let current = cache.get(&s("k")).await.unwrap().unwrap();
        let v = version_of(&current);
        // right version: the swap happens
        assert!(cache.replace_if(&s("k"), &current, &vw("a", v + 1)).await.unwrap(), "origin {origin}");
        assert_everywhere(&c, 3, "k", Some(&vw("a", v + 1)));
        // a stale old value (wrong version) fails and changes nothing
        assert!(!cache.replace_if(&s("k"), &vw("a", v), &vw("zzz", v + 2)).await.unwrap(), "origin {origin}");
        assert_everywhere(&c, 3, "k", Some(&vw("a", v + 1)));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replace_of_a_missing_key_does_nothing() {
    let c = ReplCluster::start(2, &[CACHE]).await;
    assert!(!c.cache(0, CACHE).replace_if(&s("nope"), &vw("a", 1), &vw("b", 2)).await.unwrap());
    assert_eq!(c.cache(1, CACHE).replace(&s("nope"), &vw("b", 2)).await.unwrap(), None);
    assert_everywhere(&c, 2, "nope", None);
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_and_conditional_remove() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    c.cache(1, CACHE).put(&s("r1"), &vw("a", 1)).await.unwrap();
    c.cache(1, CACHE).put(&s("r2"), &vw("b", 5)).await.unwrap();
    for origin in 0..3 {
        // remove(k, v) with a stale version fails
        assert!(!c.cache(origin, CACHE).remove_if(&s("r2"), &vw("b", 4)).await.unwrap());
    }
    assert_everywhere(&c, 3, "r2", Some(&vw("b", 5)));
    assert!(c.cache(2, CACHE).remove_if(&s("r2"), &vw("b", 5)).await.unwrap());
    assert_everywhere(&c, 3, "r2", None);
    // plain remove returns the removed value, and a missing key returns nothing
    assert_eq!(c.cache(0, CACHE).remove(&s("r1")).await.unwrap(), Some(vw("a", 1)));
    assert_everywhere(&c, 3, "r1", None);
    assert_eq!(c.cache(0, CACHE).remove(&s("r1")).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn clear_from_any_node_empties_every_node() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    for i in 0..30 {
        c.cache(i % 3, CACHE).put(&s(&format!("c{i}")), &vw("x", 1)).await.unwrap();
    }
    for i in 0..3 {
        assert_eq!(c.cache(i, CACHE).size(), 30, "node {i}");
    }
    c.cache(1, CACHE).clear().await.unwrap();
    for i in 0..3 {
        assert_eq!(c.cache(i, CACHE).size(), 0, "node {i}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_are_local_and_streams_cover_the_whole_cache() {
    let c = ReplCluster::start(3, &[CACHE]).await;
    for i in 0..20 {
        c.cache(0, CACHE).put(&s(&format!("e{i}")), &vw("x", i)).await.unwrap();
    }
    for n in 0..3 {
        let cache = c.cache(n, CACHE);
        assert_eq!(cache.size(), 20);
        assert_eq!(cache.keys().len(), 20);
        assert_eq!(cache.entries().len(), 20);
        for i in 0..20 {
            assert_eq!(cache.get(&s(&format!("e{i}"))).await.unwrap(), Some(vw("x", i)));
        }
    }
}

fn flow_key(c: &ReplCluster, idx: usize) -> CacheObject {
    for i in 0..1000 {
        let k = s(&format!("flow-{i}"));
        let (p, _) = c.cache(0, CACHE).engine().owners_of(&k).unwrap();
        if p == c.address(idx) {
            return k;
        }
    }
    panic!("no key");
}

mod flow {
    use super::*;
    use infinispan::cache::testing::{Intercepted, RpcInterceptor};
    use infinispan::ch::address::Address;
    use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
    use infinispan::commands::write::{ValueMatcher, WriteCommand, flags};
    use infinispan::topology::support::BoxFuture;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    pub struct Recorder(pub Mutex<Vec<(Address, Address, CacheRpcCommand)>>);

    impl RpcInterceptor for Recorder {
        fn intercept(&self, from: &Address, to: &Address, command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
            self.0.lock().unwrap().push((from.clone(), to.clone(), command.clone()));
            Box::pin(async { Intercepted::Deliver })
        }
    }

    fn write(c: &CacheRpcCommand) -> &WriteCommand {
        match &c.payload {
            CacheRpcPayload::Single(w) => w,
            other => panic!("{other:?}"),
        }
    }

    /// A key whose primary is not node `origin`, and one whose primary is.
    fn key_with_primary(c: &ReplCluster, primary_is: Option<usize>, not: Option<usize>) -> String {
        for i in 0..1000 {
            let k = format!("flow-{i}");
            let (p, _) = c.cache(0, CACHE).engine().owners_of(&s(&k)).unwrap();
            let idx = c.index_of(&p);
            if primary_is.is_none_or(|x| x == idx) && not.is_none_or(|x| x != idx) {
                return k;
            }
        }
        panic!("no such key");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_non_primary_originator_forwards_then_the_primary_broadcasts_to_everyone_else() {
        let c = ReplCluster::start(3, &[CACHE]).await;
        let rec = Arc::new(Recorder::default());
        c.set_interceptor(Some(rec.clone()));
        let key = key_with_primary(&c, None, Some(0));
        let (primary, owners) = c.cache(0, CACHE).engine().owners_of(&s(&key)).unwrap();
        assert_eq!(owners.len(), 3, "a replicated cache has every member as an owner");
        assert_ne!(primary, c.address(0));

        c.cache(0, CACHE).put(&s(&key), &vw("v", 1)).await.unwrap();

        let sent = rec.0.lock().unwrap().clone();
        // 1: node 0 -> primary, the same command, matcher MATCH_ALWAYS, no backup flags
        let (from, to, cmd) = &sent[0];
        assert_eq!((from, to), (&c.address(0), &primary));
        let WriteCommand::Put(forwarded) = write(cmd) else { panic!() };
        assert_eq!(forwarded.flags & (flags::BACKUP_WRITE | flags::IGNORE_RETURN_VALUES), 0);
        assert_eq!(forwarded.segment, c.cache(0, CACHE).engine().segment_of(&s(&key)).unwrap());
        // 2: primary -> each other member (the originator included) with IGNORE_RETURN_VALUES|BACKUP_WRITE
        let backups: Vec<_> = sent[1..].iter().collect();
        assert_eq!(backups.len(), 2);
        let mut targets: Vec<Address> = backups.iter().map(|(from, to, _)| {
            assert_eq!(from, &primary);
            to.clone()
        }).collect();
        targets.sort_by_key(|a| c.index_of(a));
        let mut expected: Vec<Address> = (0..3).map(|i| c.address(i)).filter(|a| *a != primary).collect();
        expected.sort_by_key(|a| c.index_of(a));
        assert_eq!(targets, expected);
        assert!(targets.contains(&c.address(0)), "the originator re-applies the command");
        for (_, _, cmd) in &backups {
            let WriteCommand::Put(b) = write(cmd) else { panic!() };
            assert_eq!(b.flags & (flags::BACKUP_WRITE | flags::IGNORE_RETURN_VALUES), flags::BACKUP_WRITE | flags::IGNORE_RETURN_VALUES);
            assert_eq!(b.value_matcher, ValueMatcher::MatchAlways);
        }
        c.set_interceptor(None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_primary_originator_sends_only_the_backup_broadcast() {
        let c = ReplCluster::start(3, &[CACHE]).await;
        let key = key_with_primary(&c, Some(1), None);
        let rec = Arc::new(Recorder::default());
        c.set_interceptor(Some(rec.clone()));
        c.cache(1, CACHE).put_if_absent(&s(&key), &vw("v", 1)).await.unwrap();
        let sent = rec.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|(from, to, cmd)| *from == c.address(1) && *to != c.address(1) && matches!(write(cmd), WriteCommand::Put(p) if p.flags & flags::BACKUP_WRITE != 0)));
        c.set_interceptor(None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_conditional_write_is_not_broadcast_and_answers_unsuccessful() {
        let c = ReplCluster::start(3, &[CACHE]).await;
        let key = key_with_primary(&c, None, Some(2));
        c.cache(0, CACHE).put(&s(&key), &vw("v", 1)).await.unwrap();
        let rec = Arc::new(Recorder::default());
        c.set_interceptor(Some(rec.clone()));
        // origin 2 is not the primary; the primary rejects the putIfAbsent
        let existing = c.cache(2, CACHE).put_if_absent(&s(&key), &vw("other", 9)).await.unwrap();
        assert_eq!(existing, Some(vw("v", 1)));
        let sent = rec.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 1, "only the forward, no backup broadcast: {sent:?}");
        assert!(!c.cache(1, CACHE).engine().local_get(&s(&key)).is_some_and(|v| v == vw("other", 9)));
        c.set_interceptor(None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clear_is_broadcast_by_the_originator_without_a_primary() {
        let c = ReplCluster::start(3, &[CACHE]).await;
        c.cache(0, CACHE).put(&s("a"), &vw("v", 1)).await.unwrap();
        let rec = Arc::new(Recorder::default());
        c.set_interceptor(Some(rec.clone()));
        c.cache(2, CACHE).clear().await.unwrap();
        let sent = rec.0.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|(from, _, cmd)| *from == c.address(2) && matches!(write(cmd), WriteCommand::Clear(_))));
        c.set_interceptor(None);
    }
}

mod java_peer {
    //! A Rust node receiving commands whose bytes the real Infinispan marshaller wrote
    //! (tests/vectors/write_commands.hex) re-executes them, as a backup does for a Java primary.
    use super::*;
    use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
    use infinispan::commands::write::{WriteCommand, flags};
    use infinispan::topology::wire::ObjectInput;

    fn java(name: &str) -> CacheRpcCommand {
        let line = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/write_commands.hex")).unwrap();
        let hex = line.lines().find_map(|l| l.strip_prefix(&format!("{name}\t"))).unwrap_or_else(|| panic!("no vector {name}"));
        let bytes: Vec<u8> = (0..hex.len() / 2).map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap()).collect();
        CacheRpcCommand::from_bytes(&bytes).unwrap()
    }

    /// What a Java primary sends its backups: the command with `IGNORE_RETURN_VALUES|BACKUP_WRITE`,
    /// stamped with the backup's topology id (the vectors carry topology id 7).
    async fn deliver_as_backup(c: &ReplCluster, node: usize, mut cmd: CacheRpcCommand) -> Option<Vec<u8>> {
        let engine = c.cache(node, CACHE).engine().clone();
        let topology_id = c.nodes.cache_topology(node, CACHE).unwrap().topology_id;
        let CacheRpcPayload::Single(w) = &mut cmd.payload else { panic!() };
        w.set_flags(w.flags() | flags::IGNORE_RETURN_VALUES | flags::BACKUP_WRITE);
        w.set_topology_id(topology_id);
        engine.handle_rpc(&cmd.to_bytes()).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rust_backup_re_executes_a_java_put_compute_replace_and_remove() {
        let c = ReplCluster::start(2, &[CACHE]).await;
        // the cache name in the vectors is "mockserver-state"; the Rust node answers for its own cache
        let mut put = java("put:plain");
        put.cache_name = CACHE.into();
        assert!(deliver_as_backup(&c, 1, put).await.is_none(), "a backup write has no return value");
        let local = |k: &str| c.cache(1, CACHE).engine().local_get(&s(k));
        assert_eq!(local("k1"), Some(vw("v1", 1)));

        // compute: the real descriptor of InfinispanKeyValueStore.put, run by the Rust equivalent
        let mut compute = java("compute:store-put-new");
        compute.cache_name = CACHE.into();
        deliver_as_backup(&c, 1, compute).await;
        assert_eq!(local("kc"), Some(vw("first", 1)));
        let mut compute2 = java("compute:store-put-existing");
        compute2.cache_name = CACHE.into();
        deliver_as_backup(&c, 1, compute2).await;
        assert_eq!(local("kc"), Some(vw("second", 2)));

        // replace (the old value is ignored by a backup: MATCH_ALWAYS is what a primary sends)
        let mut replace = java("replace:cas");
        replace.cache_name = CACHE.into();
        deliver_as_backup(&c, 1, replace).await;
        assert_eq!(local("kc"), Some(vw("third", 3)));

        // remove
        let mut remove = java("remove:cas");
        remove.cache_name = CACHE.into();
        deliver_as_backup(&c, 1, remove).await;
        assert_eq!(local("kc"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_java_primary_command_is_answered_like_java_answers() {
        // A forwarded (non-backup) Java `putIfAbsent` that finds the key present: UnsuccessfulResponse
        // carrying the existing value, byte-identical to the vector `resp:unsuccessful-value` shape.
        let c = ReplCluster::start(2, &[CACHE]).await;
        let key = super::flow_key(&c, 0);
        c.cache(0, CACHE).put(&key, &vw("v", 9)).await.unwrap();
        let engine = c.cache(0, CACHE).engine().clone();
        let topology_id = c.nodes.cache_topology(0, CACHE).unwrap().topology_id;
        let mut cmd = java("putIfAbsent:existing");
        cmd.cache_name = CACHE.into();
        let CacheRpcPayload::Single(WriteCommand::Put(p)) = &mut cmd.payload else { panic!() };
        p.key = key.clone();
        p.segment = engine.segment_of(&key).unwrap();
        p.topology_id = topology_id;
        let answer = engine.handle_rpc(&cmd.to_bytes()).await.unwrap().expect("a putIfAbsent expects a return value");
        let response = ObjectInput::new(&answer).read_response().unwrap();
        use infinispan::topology::messages::{Response, ResponseValue};
        assert_eq!(response, Response::Unsuccessful(Some(Box::new(ResponseValue::Object(vw("v", 9))))));
    }
}
