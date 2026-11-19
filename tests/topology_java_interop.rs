//! Live interop: a Rust node and a real Infinispan 14.0.35.Final node (a JVM running
//! `gen.InteropNode`, JGroups 5.2.28) in one cluster, in both join orders. Run it with
//! `upstream-tools/run-interop-tests.sh` or a plain `cargo test` (needs JDK 17 and Maven; both sides bind
//! free loopback ports the test picks and hands to the JVM; `upstream-tools/build-interop-node.sh` builds
//! the JVM side on first use).
//! Set `INFINISPAN_RS_INTEROP_LOG_DIR` to keep the JVM's stderr as `java-<port>.log` there.
//!
//! - Java coordinator, Rust joiner: the Rust node sends `CacheJoinCommand`, receives the
//!   `CacheStatusResponse`, takes part in the four rebalance phases the Java coordinator drives, and
//!   ends on the topology the Java side holds; its initial state transfer pulls the JVM's entries
//!   (`StateRequestCommand` answered by a real `StateProviderImpl`, chunks decoded); a write
//!   replicates each way.
//! - Rust coordinator, Java joiner: the Rust node builds the `ReplicatedConsistentHash` with
//!   `SyncReplicatedConsistentHashFactory` and drives the rebalance; the JVM parses it and requests the
//!   state of the Rust node's entries, answered by Rust's provider; a write replicates each way.
//!
//! Values are MockServer's `VersionedWrapper<String>` (Java serialization), keys are Strings.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use infinispan::ch::address::Address;
use infinispan::topology::cache_topology::Phase;
use infinispan::cache::cluster_node::ClusterNode;
use infinispan::cache::encoder_cache::Cache;
use infinispan::cache::equality::java_equals;
use infinispan::cache::versioned::VersionedWrapper;
use infinispan::cache::write_pipeline::CacheConfig;
use infinispan::commands::object::CacheObject;
use infinispan::topology::cluster_cache_status::TopologyError;
use infinispan::topology::cluster_topology_manager::ClusterManagerStatus;
use infinispan::topology::testing::PhaseConfirmingHandler;
use infinispan::topology::transport::TopologyTransport;

const WAIT: Duration = Duration::from_secs(60);

/// A free loopback port for a JGroups `TCP` bind (`port_range="0"`, so the channel needs exactly this one):
/// the OS picks it, and the JVM side gets it as its first argument. Ports handed out in this process are
/// never handed out twice, so the two tests (which run in parallel) cannot collide with each other;
/// another checkout running the same tests gets different ones from the OS.
fn free_port() -> u16 {
    static TAKEN: Mutex<Vec<u16>> = Mutex::new(Vec::new());
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let mut taken = TAKEN.lock().unwrap();
        if !taken.contains(&port) {
            taken.push(port);
            return port;
        }
    }
}
const CACHE: &str = "interop-cache";

/// The JVM side needs MockServer's sources (`VersionedWrapper`, `Blob`): the checkout `MOCKSERVER_SRC` names, or the
/// `../../upstream/mockserver/mockserver` of a consumer that has this crate under `crates/`. Without them (or a
/// prebuilt `INFINISPAN_RS_INTEROP_CP`) the live interop tests skip.
fn interop_available() -> bool {
    if std::env::var("INFINISPAN_RS_INTEROP_CP").is_ok() {
        return true;
    }
    let src = std::env::var("MOCKSERVER_SRC")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/mockserver/mockserver").into());
    let ok = std::path::Path::new(&src).is_dir();
    if !ok {
        eprintln!("skipped: no MockServer sources at {src}; set MOCKSERVER_SRC to run the live interop tests");
    }
    ok
}

/// `(java binary, class path)`: built once per test process by `upstream-tools/build-interop-node.sh`
/// unless `INFINISPAN_RS_INTEROP_CP` (and `INFINISPAN_RS_JAVA`) already say where they are.
fn jvm() -> &'static (String, String) {
    static JVM: OnceLock<(String, String)> = OnceLock::new();
    JVM.get_or_init(|| {
        if let Ok(cp) = std::env::var("INFINISPAN_RS_INTEROP_CP") {
            return (std::env::var("INFINISPAN_RS_JAVA").unwrap_or_else(|_| "java".into()), cp);
        }
        let script = concat!(env!("CARGO_MANIFEST_DIR"), "/upstream-tools/build-interop-node.sh");
        // into cargo's target, not a crate-local `target/` (see the script)
        let dir = concat!(env!("CARGO_TARGET_TMPDIR"), "/interop");
        let out = Command::new(script).env("INFINISPAN_RS_INTEROP_DIR", dir).output().expect("run build-interop-node.sh");
        assert!(out.status.success(), "build-interop-node.sh failed (needs JDK 17 and Maven):\n{}", String::from_utf8_lossy(&out.stderr));
        let stdout = String::from_utf8(out.stdout).unwrap();
        let mut lines = stdout.lines();
        let home = lines.next().expect("JAVA_HOME line");
        (format!("{home}/bin/java"), lines.next().expect("class path line").to_string())
    })
}

struct JavaNode {
    child: Child,
    stdin: ChildStdin,
    lines: Mutex<Receiver<String>>,
}

impl JavaNode {
    fn spawn(bind_port: u16, initial_hosts: &str, cluster: &str, caches: &str) -> JavaNode {
        let (java, cp) = jvm();
        let mut child = Command::new(java)
            .args(["-cp", cp, "gen.InteropNode", &bind_port.to_string(), initial_hosts, cluster, caches])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(match std::env::var("INFINISPAN_RS_INTEROP_LOG_DIR") {
                Ok(dir) => Stdio::from(std::fs::File::create(format!("{dir}/java-{bind_port}.log")).unwrap()),
                Err(_) => Stdio::null(),
            })
            .spawn()
            .expect("spawn java");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
        JavaNode { child, stdin, lines: Mutex::new(rx) }
    }

    fn expect_line(&self, prefix: &str) -> String {
        let rx = self.lines.lock().unwrap();
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match rx.recv_timeout(left) {
                Ok(l) if l.starts_with(prefix) => return l,
                Ok(_) => continue,
                Err(_) => panic!("java node never printed {prefix}"),
            }
        }
    }

    fn send(&mut self, cmd: &str) {
        writeln!(self.stdin, "{cmd}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn quit(mut self) {
        let _ = writeln!(self.stdin, "quit");
        let _ = self.stdin.flush();
        let _ = self.child.wait();
    }
}

fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
    format!(
        r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
            <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
        </config>"#
    )
}

fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

async fn eventually(mut f: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

type RustNode = Arc<ClusterNode>;

async fn start_rust(bind_port: u16, initial_hosts: &str, cluster: &str) -> RustNode {
    ClusterNode::start(&stack_xml(bind_port, initial_hosts), cluster, Duration::from_secs(30)).await.unwrap()
}

async fn join(r: &RustNode, cache: &str) -> (Cache, Arc<PhaseConfirmingHandler>) {
    let mut cfg = CacheConfig::new(cache);
    cfg.state_transfer_timeout = WAIT;
    r.start_cache(cfg).await.map_err(|TopologyError(e)| e).expect("the cache joins and its initial transfer completes")
}

fn key(i: usize) -> CacheObject {
    CacheObject::string(&format!("key{i}"))
}

fn vw(value: &str, version: i64) -> CacheObject {
    CacheObject::User(VersionedWrapper::encode_object(&CacheObject::string(value), version).unwrap())
}

/// Every `key<i>` -> `VersionedWrapper("value<i>", i)` the JVM's `fill` (or `fill_rust`) writes.
fn assert_filled(cache: &Cache, n: usize) {
    assert_eq!(cache.size(), n, "entry count");
    for i in 0..n {
        let held = cache.engine().local_get(&key(i)).unwrap_or_else(|| panic!("key{i} missing"));
        assert!(java_equals(&held, &vw(&format!("value{i}"), i as i64)), "key{i} holds {held:?}");
    }
}

async fn fill_rust(cache: &Cache, n: usize) {
    for i in 0..n {
        cache.put(&key(i), &vw(&format!("value{i}"), i as i64)).await.unwrap();
    }
}

fn routing_hash(r: &RustNode, cache: &str) -> (i32, i32, Vec<Address>) {
    let t = r.node.ltm.cache_topology(cache).unwrap();
    let ch = t.current_ch.as_ref().unwrap().as_replicated();
    let table = ch.routing_table_as_string(|_| String::new());
    (t.topology_id, java_string_hash(&table), ch.members().to_vec())
}

/// Parse `TOPOLOGY <cache> <id> <phase> <nmembers> <routing hash> [members]`.
fn parse_topology(line: &str) -> (i32, String, usize, i32) {
    let p: Vec<&str> = line.split(' ').collect();
    (p[2].parse().unwrap(), p[3].to_string(), p[4].parse().unwrap(), p[5].parse().unwrap())
}

const ENTRIES: usize = 50;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn java_coordinator_rust_joiner() {
    if !interop_available() {
        return;
    }
    let (java_port, rust_port) = (free_port(), free_port());
    let hosts = format!("127.0.0.1[{java_port}],127.0.0.1[{rust_port}]");
    let mut java = JavaNode::spawn(java_port, &hosts, "interop-a", CACHE);
    java.expect_line("JOINED");
    java.send(&format!("fill {CACHE} {ENTRIES}"));
    java.expect_line("FILLED");

    let rust = start_rust(rust_port, &hosts, "interop-a").await;
    assert!(eventually(|| rust.transport.members().len() == 2).await, "views never merged with the JVM");
    // the JVM is the coordinator: it was first in the view
    assert_ne!(rust.node.ctm.status(), ClusterManagerStatus::Coordinator);
    let (cache, handler) = join(&rust, CACHE).await;

    assert!(
        eventually(|| rust.node.ltm.cache_topology(CACHE).is_some_and(|t| t.phase == Phase::NoRebalance && t.members().len() == 2)).await,
        "rust never saw the settled topology"
    );
    let phases: Vec<Phase> = handler.topologies().iter().map(|t| t.phase).collect();
    println!("rust saw phases {phases:?}");
    assert_eq!(phases.last(), Some(&Phase::NoRebalance));

    // what the JVM holds
    let (id, hash, members) = routing_hash(&rust, CACHE);
    assert!(
        eventually(|| {
            java.send(&format!("dump {CACHE}"));
            let (jid, jphase, jmembers, jhash) = parse_topology(&java.expect_line("TOPOLOGY"));
            println!("java topology: id={jid} phase={jphase} members={jmembers} hash={jhash}; rust id={id} hash={hash}");
            jid == id && jphase == "NO_REBALANCE" && jmembers == 2 && jhash == hash
        })
        .await,
        "the JVM and the Rust node disagree on the consistent hash"
    );
    assert_eq!(members.len(), 2);

    // state transfer to the Rust joiner: the JVM's StateProvider answered, Rust decoded every chunk
    assert_filled(&cache, ENTRIES);
    replicate_both_ways(&mut java, &cache).await;

    rust.stop().await;
    java.quit();
}

/// A write on each side shows on the other; so does an overwrite and a read of an entry the other wrote.
async fn replicate_both_ways(java: &mut JavaNode, cache: &Cache) {
    // Rust -> Java
    let old = cache.put(&CacheObject::string("from-rust"), &vw("rust-wrote", 7)).await.unwrap();
    assert!(old.is_none());
    java.send(&format!("get {CACHE} from-rust"));
    assert_eq!(java.expect_line("VALUE"), format!("VALUE {CACHE} from-rust rust-wrote 7"));
    // Java -> Rust
    java.send(&format!("put {CACHE} from-java java-wrote 9"));
    assert_eq!(java.expect_line("PUT"), format!("PUT {CACHE} from-java null"));
    let k = CacheObject::string("from-java");
    assert!(
        eventually(|| cache.engine().local_get(&k).is_some_and(|v| java_equals(&v, &vw("java-wrote", 9)))).await,
        "the JVM's write never reached the Rust node"
    );
    // an overwrite from Rust returns what the JVM wrote
    let old = cache.put(&k, &vw("rust-overwrote", 10)).await.unwrap().expect("the previous value");
    assert!(java_equals(&old, &vw("java-wrote", 9)));
    java.send(&format!("get {CACHE} from-java"));
    assert_eq!(java.expect_line("VALUE"), format!("VALUE {CACHE} from-java rust-overwrote 10"));
    java.send(&format!("size {CACHE}"));
    assert_eq!(java.expect_line("SIZE"), format!("SIZE {CACHE} {}", cache.size()));

    // putIfAbsent: Rust refused by the JVM's entry, then the JVM refused by Rust's
    let existing = cache.put_if_absent(&k, &vw("never", 11)).await.unwrap().expect("the JVM-visible value");
    assert!(java_equals(&existing, &vw("rust-overwrote", 10)));
    java.send(&format!("putifabsent {CACHE} from-java never 11"));
    assert_eq!(java.expect_line("ABSENT"), format!("ABSENT {CACHE} from-java rust-overwrote"));
    // remove each way
    let removed = cache.remove(&k).await.unwrap().expect("removed value");
    assert!(java_equals(&removed, &vw("rust-overwrote", 10)));
    java.send(&format!("get {CACHE} from-java"));
    assert_eq!(java.expect_line("VALUE"), format!("VALUE {CACHE} from-java null 0"));
    java.send(&format!("remove {CACHE} from-rust"));
    assert_eq!(java.expect_line("REMOVED"), format!("REMOVED {CACHE} from-rust rust-wrote"));
    let gone = CacheObject::string("from-rust");
    assert!(eventually(|| cache.engine().local_get(&gone).is_none()).await, "the JVM's remove never reached the Rust node");
    // clear from the JVM empties both
    java.send(&format!("clear {CACHE}"));
    java.expect_line("CLEARED");
    assert!(eventually(|| cache.size() == 0).await, "the JVM's clear never reached the Rust node");
    java.send(&format!("size {CACHE}"));
    assert_eq!(java.expect_line("SIZE"), format!("SIZE {CACHE} 0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rust_coordinator_java_joiner() {
    if !interop_available() {
        return;
    }
    let (rust_port, java_port) = (free_port(), free_port());
    let hosts = format!("127.0.0.1[{rust_port}],127.0.0.1[{java_port}]");
    let rust = start_rust(rust_port, &hosts, "interop-b").await;
    assert!(eventually(|| rust.node.ctm.status() == ClusterManagerStatus::Coordinator).await);
    let (cache, handler) = join(&rust, CACHE).await;
    fill_rust(&cache, ENTRIES).await;

    // the JVM joins the cache (getCache blocks until its initial state transfer is done)
    let mut java = JavaNode::spawn(java_port, &hosts, "interop-b", CACHE);
    java.expect_line("JOINED");

    assert!(
        eventually(|| rust.node.ltm.cache_topology(CACHE).is_some_and(|t| t.phase == Phase::NoRebalance && t.members().len() == 2)).await,
        "rust never settled with the JVM as a member"
    );
    let phases: Vec<(i32, Phase)> = handler.topologies().iter().map(|t| (t.topology_id, t.phase)).collect();
    println!("rust (coordinator) history {phases:?}");
    let expected = [(1, Phase::NoRebalance), (2, Phase::ReadOldWriteAll), (3, Phase::ReadAllWriteAll), (4, Phase::ReadNewWriteAll), (5, Phase::NoRebalance)];
    assert_eq!(phases, expected);

    let (id, hash, _) = routing_hash(&rust, CACHE);
    assert!(
        eventually(|| {
            java.send(&format!("dump {CACHE}"));
            let (jid, jphase, jmembers, jhash) = parse_topology(&java.expect_line("TOPOLOGY"));
            println!("java topology: id={jid} phase={jphase} members={jmembers} hash={jhash}; rust id={id} hash={hash}");
            jid == id && jphase == "NO_REBALANCE" && jmembers == 2 && jhash == hash
        })
        .await,
        "the JVM parsed a different consistent hash than the Rust coordinator built"
    );

    // state transfer to the JVM joiner: Rust's provider chunks were decoded by the real StateConsumer
    java.send(&format!("size {CACHE}"));
    assert_eq!(java.expect_line("SIZE"), format!("SIZE {CACHE} {ENTRIES}"));
    for i in [0, 1, ENTRIES / 2, ENTRIES - 1] {
        java.send(&format!("get {CACHE} key{i}"));
        assert_eq!(java.expect_line("VALUE"), format!("VALUE {CACHE} key{i} value{i} {i}"));
    }
    replicate_both_ways(&mut java, &cache).await;

    java.quit();
    rust.stop().await;
}
