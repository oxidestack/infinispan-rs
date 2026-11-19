//! A node that joins a REPL_SYNC cluster receives every segment, for the caches MockServer uses:
//! `expectations`, `scenarioStates`, `blobs` and a `crud-<ns>` cache.

mod common;

use common::repl::*;
use infinispan::cache::testing::ReplCluster;
use infinispan::cache::write_pipeline::{EntryEvent, EntryListener};
use infinispan::commands::object::CacheObject;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CACHES: [&str; 4] = ["expectations", "scenarioStates", "blobs", "crud-ns1"];

#[derive(Default)]
struct Events(Mutex<Vec<EntryEvent>>);

impl EntryListener for Events {
    fn on_event(&self, e: &EntryEvent) {
        self.0.lock().unwrap().push(e.clone());
    }
}

fn sorted(mut v: Vec<(CacheObject, CacheObject)>) -> Vec<(String, CacheObject)> {
    let mut out: Vec<(String, CacheObject)> = v.drain(..).map(|(k, v)| (format!("{k:?}"), v)).collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_joiner_holds_what_the_members_hold_and_saw_no_events() {
    let c = ReplCluster::start(2, &CACHES).await;
    for (n, name) in CACHES.iter().enumerate() {
        let cache = c.cache(n % 2, name);
        for i in 0..300 {
            cache.put(&s(&format!("{name}-{i}")), &vw(&format!("v{i}"), i as i64)).await.unwrap();
        }
        cache.put_with_lifespan(&s("mortal"), &vw("m", 1), Duration::from_secs(600)).await.unwrap();
    }
    // a node that is not yet there cannot have a listener: the listener goes on the joiner's cache the moment it exists
    let idx = c.nodes.add_node();
    let members: Vec<usize> = (0..=idx).collect();
    c.nodes.install_view(&members, false);
    c.nodes.start_node(idx).await;
    let mut listeners = Vec::new();
    for name in CACHES {
        let l = Arc::new(Events::default());
        listeners.push(l.clone());
        c.start_cache_with_listener(idx, name, l).await.unwrap();
    }
    for name in CACHES {
        c.nodes.wait_for_no_rebalance(&members, name, Duration::from_secs(30)).await.unwrap();
    }
    for name in CACHES {
        let mine = sorted(c.cache(idx, name).entries());
        assert_eq!(mine.len(), 301, "{name}");
        assert_eq!(mine, sorted(c.cache(0, name).entries()), "{name}");
        assert_eq!(mine, sorted(c.cache(1, name).entries()), "{name}");
    }
    for l in &listeners {
        assert!(l.0.lock().unwrap().is_empty(), "state transfer fires no notification");
    }
    // later live writes are seen
    c.cache(0, CACHES[0]).put(&s("live"), &vw("x", 1)).await.unwrap();
    assert_eq!(listeners[0].0.lock().unwrap().len(), 1);
}

/// Records the state transfer commands that cross the wire.
#[derive(Default)]
struct Wire(Mutex<Vec<(infinispan::ch::address::Address, infinispan::ch::address::Address, infinispan::commands::rpc::CacheRpcCommand)>>);

impl infinispan::cache::testing::RpcInterceptor for Wire {
    fn intercept(
        &self,
        from: &infinispan::ch::address::Address,
        to: &infinispan::ch::address::Address,
        command: &infinispan::commands::rpc::CacheRpcCommand,
    ) -> infinispan::topology::support::BoxFuture<'static, infinispan::cache::testing::Intercepted> {
        self.0.lock().unwrap().push((from.clone(), to.clone(), command.clone()));
        Box::pin(async { infinispan::cache::testing::Intercepted::Deliver })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_joiner_requests_all_256_segments_and_every_segment_ends_with_a_last_chunk() {
    use infinispan::commands::rpc::CacheRpcPayload;
    let c = ReplCluster::start(1, &["blobs"]).await;
    // a single entry: 255 of the 256 segments are empty and still get their last chunk
    c.cache(0, "blobs").put(&s("only"), &vw("v", 1)).await.unwrap();
    let wire = Arc::new(Wire::default());
    c.set_interceptor(Some(wire.clone()));
    let idx = c.add_node_and_join(&["blobs"], |c| c).await;
    c.nodes.wait_for_no_rebalance(&[0, idx], "blobs", Duration::from_secs(30)).await.unwrap();

    let seen = wire.0.lock().unwrap().clone();
    let starts: Vec<_> = seen.iter().filter_map(|(f, t, cmd)| if let CacheRpcPayload::StateStart(s) = &cmd.payload { Some((f.clone(), t.clone(), s.clone())) } else { None }).collect();
    assert_eq!(starts.len(), 1, "one source, one request");
    assert_eq!(starts[0].0, c.address(idx));
    assert_eq!(starts[0].1, c.address(0));
    assert_eq!(starts[0].2.segments.len(), 256);

    let mut last_chunks = std::collections::BTreeSet::new();
    let mut entries = 0;
    for (from, to, cmd) in &seen {
        if let CacheRpcPayload::StateResponse(r) = &cmd.payload {
            assert_eq!((from, to), (&c.address(0), &c.address(idx)));
            assert!(r.apply_state && !r.push_transfer);
            for ch in &r.chunks {
                entries += ch.entries.len();
                if ch.is_last {
                    assert!(last_chunks.insert(ch.segment), "one last chunk per segment");
                }
            }
        }
    }
    assert_eq!(entries, 1);
    assert_eq!(last_chunks.len(), 256);
    assert_eq!(c.cache(idx, "blobs").size(), 1);
}
