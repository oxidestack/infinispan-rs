//! `JGroupsTransport` over a `jgroups-rs` `JChannel`: channel creation, `connect`, the ISPN000094
//! view-change log line, and fire-and-forget **and now correlated request/response** message
//! delivery.
//!
//! # How received headers reach this module
//!
//! An earlier version found and proved that `JChannel::insert_protocol_below` could never see a
//! correlated header on receipt, for any stack `build_stack` can construct (`NAKACK2`/`UNICAST3`
//! are unconditionally present and their `drain_ready` bypass skips every protocol above the
//! buffering one). That gap is closed: `jgroups-rs` gave `JChannel` real `UpHandler` parity —
//! [`jgroups::channel::JChannel::set_up_handler`]/[`jgroups::channel::JChannel::send_message`] — a
//! channel-level callback that sees every delivered message with its headers and flags intact
//! (`jgroups-rs`'s own `tests/up_handler_test.rs` proves both the direct, in-order `up_message` path
//! and the `drain_ready`-redriven path reach it), exactly the seam upstream's real
//! `RequestCorrelator`/`MessageDispatcher` use by installing themselves as a channel's `UpHandler`
//! (`JGroupsTransport.java:1232-1253,1441-1476`).
//!
//! [`Transport`] now installs itself (via `TransportUpHandler`) as the channel's sole `UpHandler`
//! at construction time — mirroring upstream's own contract (`UpHandler`/`Receiver` are mutually
//! exclusive; a plain `channel.receive()` is never used once a handler is installed,
//! `jgroups::channel::JChannel::receive`'s own doc comment). The mechanical envelope<->header
//! splice pass 1 built (`RequestCorrelatorProtocol`) is no longer needed and has been removed:
//! `jgroups::wire::header::RequestCorrelatorHeader`/`HeaderValue::RequestCorrelator` are attached
//! directly to a real [`jgroups::wire::message::Message`] and sent via `send_message`, no
//! byte-envelope translation required.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use jgroups::channel::{ChannelError, JChannel, UpHandler};
use jgroups::stack::BoxFuture;
use jgroups::wire::address::Address;
use jgroups::wire::header::{HeaderValue, RequestCorrelatorHeader, REQCORR_EXC_RSP, REQCORR_REQ, REQCORR_RSP};
use jgroups::wire::ids::PROT_ID_REQUEST_CORRELATOR;
use jgroups::wire::message::{Message, Payload, TRANSIENT_FLAG_DONT_LOOPBACK};
use jgroups::wire::view::View;

use crate::remoting::dispatch::{self, Action, Kind, ResponseBody};
use crate::remoting::request_correlator::{DeliverOrder, PendingRequests, REPLY_FLAGS};

/// A failure to create, connect or use the channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError(pub String);
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for TransportError {}
impl From<ChannelError> for TransportError {
    fn from(e: ChannelError) -> TransportError {
        TransportError(e.to_string())
    }
}

fn message_bytes(msg: &Message) -> Vec<u8> {
    match &msg.payload {
        Payload::Bytes { array: Some(a), offset, length } => a[*offset..*offset + *length].to_vec(),
        _ => Vec::new(),
    }
}

// ------------------------------------- Transport -------------------------------------

/// One inbound fire-and-forget command, or a correlated request awaiting a reply — `processRequest`'s
/// `Invoke` outcome (`JGroupsTransport.java:1530-1538`). `reply_to` is `Some` exactly when
/// `dispatch::Action::Invoke { reply_expected: true, .. }` (a real `REQUEST`, not a `SINGLE_MESSAGE`)
/// — `None` means `Reply.NO_OP`, matching upstream.
pub struct ReceivedCommand {
    /// The sender.
    pub from: Address,
    /// The marshalled command.
    pub body: Vec<u8>,
    /// Where to send the reply; `None` when none is expected.
    pub reply_to: Option<ReplyToken>,
}

/// An opaque capability to answer one specific correlated request — `req_id`/`corr_id` off its
/// `RequestCorrelatorHeader`, plus the sender to reply to. Deliberately has no public fields: the
/// only thing a caller can do with one is hand it back to [`Transport::send_reply`].
#[derive(Debug, Clone)]
pub struct ReplyToken {
    dest: Address,
    req_id: i64,
    corr_id: i16,
}

struct Inner {
    channel: JChannel,
    running: AtomicBool,
    inbound_tx: tokio::sync::mpsc::UnboundedSender<ReceivedCommand>,
    pending: PendingRequests,
}

impl Inner {
    /// `JGroupsTransport.sendResponse` (`:1503`, always `REPLY_FLAGS`, kind `RSP` — upstream never
    /// emits JGroups' own `EXC_RSP` kind; an application-level failure is encoded in the *body*
    /// as a marshalled `ExceptionResponse`, not in this header's `kind`, see the module doc comment
    /// on [`Transport::send_reply`]). Spawned as a detached task by every caller in this module, the
    /// same reason `jgroups-rs`'s own `Gms` detaches its `VIEW_ACK` wait: never block the single
    /// receive loop (`Shared::run`) that is what delivers the very frames a nested `send_message`
    /// would need to read again.
    async fn send_reply_now(&self, dest: Address, req_id: i64, corr_id: i16, body: Vec<u8>) -> Result<(), ChannelError> {
        let mut msg = Message::bytes(Some(dest), body);
        msg.flags = REPLY_FLAGS;
        msg.put_header(PROT_ID_REQUEST_CORRELATOR, HeaderValue::RequestCorrelator(RequestCorrelatorHeader { kind: REQCORR_RSP, req_id, corr_id }));
        self.channel.send_message(msg).await
    }
}

/// The `UpHandler` [`Transport`] installs on its own channel: every delivered message, headers and
/// flags intact, is decided here via [`dispatch::on_message`] and either answered immediately
/// (`RespondCacheNotFound`), handed to the application (`Invoke`), used to complete a pending
/// request (`Complete`), or dropped — never anything decided ad hoc in this type itself.
struct TransportUpHandler(Arc<Inner>);

impl UpHandler for TransportUpHandler {
    fn up<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let inner = &self.0;
            let local = inner.channel.local_address().clone();
            let running = inner.running.load(Ordering::SeqCst);
            let body = message_bytes(&msg);

            match msg.get_header(PROT_ID_REQUEST_CORRELATOR) {
                Some(HeaderValue::RequestCorrelator(h)) if h.kind == REQCORR_REQ => {
                    let (req_id, corr_id) = (h.req_id, h.corr_id);
                    match dispatch::on_message(&local, &sender, running, Kind::Request, &body) {
                        Action::Drop => {}
                        Action::RespondCacheNotFound => {
                            let inner = inner.clone();
                            let dest = sender.clone();
                            tokio::spawn(async move {
                                let _ = inner.send_reply_now(dest, req_id, corr_id, Vec::new()).await;
                            });
                        }
                        Action::Invoke { body, reply_expected } => {
                            let reply_to = reply_expected.then(|| ReplyToken { dest: sender.clone(), req_id, corr_id });
                            let _ = inner.inbound_tx.send(ReceivedCommand { from: sender, body, reply_to });
                        }
                        Action::Complete { .. } => {
                            unreachable!("dispatch::on_message never returns Complete for Kind::Request")
                        }
                    }
                }
                // Upstream's own transport never sends the JGroups-level EXC_RSP kind (see
                // `Inner::send_reply_now`'s doc comment) but a peer that did would still need to
                // complete the same pending-request table, so both RSP and EXC_RSP resolve here.
                Some(HeaderValue::RequestCorrelator(h)) if h.kind == REQCORR_RSP || h.kind == REQCORR_EXC_RSP => {
                    if let Action::Complete { response } = dispatch::on_message(&local, &sender, running, Kind::Response, &body) {
                        inner.pending.complete(h.req_id, response);
                    }
                }
                // Any other header kind, or none at all: SINGLE_MESSAGE, `header == null`
                // (`JGroupsTransport.java:1448-1456`) — fire-and-forget, no reply path.
                _ => {
                    if let Action::Invoke { body, .. } = dispatch::on_message(&local, &sender, running, Kind::SingleMessage, &body) {
                        let _ = inner.inbound_tx.send(ReceivedCommand { from: sender, body, reply_to: None });
                    }
                }
            }
        })
    }
}

/// `org.infinispan.remoting.transport.jgroups.JGroupsTransport`: channel lifecycle, fire-and-forget
/// dispatch, and now (pass 2) correlated request/response over the real `UpHandler` seam.
pub struct Transport {
    inner: Arc<Inner>,
    inbound_rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<ReceivedCommand>>,
    view_log_task: JoinHandle<()>,
}

impl Transport {
    /// `JGroupsTransport.buildChannel`/`initChannel` (`:601-602,695-714`): parses `stack_xml`
    /// (upstream's `configurationFile`, MockServer's `mockserver.clusterTransportConfig`) into a
    /// channel, not yet connected — matching `new JChannel(url.openStream())`/`connect(clusterName)`
    /// being two separate upstream calls, mirrored by [`Transport::connect`]. Installs this
    /// transport as the channel's `UpHandler` right away (upstream installs its `UpHandler` as part
    /// of construction too, well before `connect`/`running = true`), so no frame the channel
    /// delivers from this point on can ever reach a plain `channel.receive()` instead.
    pub async fn new(stack_xml: &str) -> Result<Transport, TransportError> {
        let channel = JChannel::new(stack_xml).await?;

        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(Inner { channel, running: AtomicBool::new(false), inbound_tx, pending: PendingRequests::new() });

        inner.channel.set_up_handler(Some(Arc::new(TransportUpHandler(inner.clone())) as Arc<dyn UpHandler>)).await;

        let log_inner = inner.clone();
        let view_log_task = tokio::spawn(async move { Transport::view_log_loop(log_inner).await });

        Ok(Transport { inner, inbound_rx: Mutex::new(inbound_rx), view_log_task })
    }

    /// `JGroupsTransport.startJGroupsChannelIfNeeded`/`channel.connect(clusterName)` (`:601-610`).
    pub async fn connect(&self, cluster_name: &str) -> Result<View, TransportError> {
        Ok(self.inner.channel.connect(cluster_name).await?)
    }

    /// `JChannel.setReceiver(..)`'s `viewAccepted(view)` / `MergeView` callback seam
    /// (`JGroupsTransport.viewAccepted`, `:769-830`): `listener` is told every installed view and,
    /// for a merge, the partitions that merged (`subgroups`).
    pub fn add_membership_listener(&self, listener: Arc<dyn jgroups::protocols::gms::MembershipListener>) {
        self.inner.channel.gms().add_membership_listener(listener);
    }

    /// The node's logical address.
    pub fn local_address(&self) -> &Address {
        self.inner.channel.local_address()
    }

    /// The transport's own physical bind address, as opposed to [`local_address`](Self::local_address)'s
    /// logical UUID identity — needed by a test/caller building another node's `initial_hosts` from
    /// an OS-assigned port (`jgroups::channel::JChannel::physical_address`'s own doc comment).
    pub fn physical_address(&self) -> &Address {
        self.inner.channel.physical_address()
    }

    /// The installed view, once connected.
    pub async fn view(&self) -> Option<View> {
        self.inner.channel.view().await
    }

    /// `JGroupsTransport.start()`'s last line, `running = true` (`:482`) — everything before it
    /// (channel connect, `UpHandler` installed) already happened via [`Transport::new`]/
    /// [`Transport::connect`]. A message that arrives before this is dropped, or — for a `REQUEST`
    /// — answered `CacheNotFoundResponse` (`processMessage`'s `!running` branch, `:1457-1464`,
    /// ported in `dispatch::on_message`).
    pub fn mark_started(&self) {
        self.inner.running.store(true, Ordering::SeqCst);
    }

    /// Whether the transport was started.
    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::SeqCst)
    }

    /// A `SINGLE_MESSAGE`: no `RequestCorrelator.Header` at all (`JGroupsTransport.send`, used
    /// without `addRequestHeader`, `:1232-1253` with `requestId == Request.NO_REQUEST_ID`). Plain
    /// `channel.send` — no header, no correlation.
    pub async fn send_fire_and_forget(&self, dest: Option<Address>, payload: Vec<u8>) -> Result<(), TransportError> {
        // `sendCommand`/`sendCommandToAll` (`JGroupsTransport.java:1191-1198,1349`): the deliver
        // mode's flags plus NO_RELAY (a JVM treats a message without it as an x-site request) and
        // DONT_LOOPBACK, sent as a real `Message` rather than a bare payload.
        let mut msg = Message::bytes(dest, payload);
        msg.flags = DeliverOrder::None.message_flags(true);
        msg.transient_flags |= TRANSIENT_FLAG_DONT_LOOPBACK;
        Ok(self.inner.channel.send_message(msg).await?)
    }

    /// `JGroupsTransport.send(..., requestId, ...)` with a real request id
    /// (`addRequestHeader`/`:1247-1253`): attaches a `RequestCorrelatorHeader { kind: REQ, req_id,
    /// corr_id: 0 }` (this crate never shares one channel across multiple correlators/fork channels,
    /// so `corr_id` is always `0` — see `RequestCorrelatorHeader`'s own doc comment for what it
    /// identifies upstream) and `order.request_flags()` (`encodeDeliverMode`, `:1255-1263`), sends it
    /// via [`jgroups::channel::JChannel::send_message`], and awaits the matching reply. Resolves to
    /// the peer's [`ResponseBody`] — `CacheNotFound` if the peer wasn't started yet, `SuccessfulEmpty`
    /// if its handler answered `null`, `Bytes` otherwise (no externalizer registry to decode a real
    /// `Response` type into yet).
    ///
    /// `setMessageFlags` (`:1220-1228`) always also sets `TransientFlag.DONT_LOOPBACK` on a request
    /// — purely an optimisation (a self-addressed or multicast-including-self request would
    /// otherwise also loop back locally, only to be dropped anyway by `processRequest`'s own
    /// `src.equals(local)` check, `:1521-1526` — the JGRP-2205 comment there is explicit that this
    /// flag is *not* what makes self-sent filtering correct). Set here for the same reason.
    pub async fn send_request(&self, dest: Address, payload: Vec<u8>, order: DeliverOrder) -> Result<ResponseBody, TransportError> {
        let (req_id, rx) = self.inner.pending.register();
        let mut msg = Message::bytes(Some(dest), payload);
        msg.flags = order.message_flags(true);
        msg.transient_flags |= TRANSIENT_FLAG_DONT_LOOPBACK;
        msg.put_header(PROT_ID_REQUEST_CORRELATOR, HeaderValue::RequestCorrelator(RequestCorrelatorHeader { kind: REQCORR_REQ, req_id, corr_id: 0 }));

        if let Err(e) = self.inner.channel.send_message(msg).await {
            self.inner.pending.cancel(req_id);
            return Err(e.into());
        }
        rx.await.map_err(|_| TransportError("the pending request was cancelled before a response arrived".to_string()))
    }

    /// Answers a correlated request received via [`recv_command`](Self::recv_command)'s
    /// `ReceivedCommand::reply_to` — `processRequest`'s `Invoke` outcome actually running and
    /// calling back into `Reply.reply(Object)` (`JGroupsTransport.java:1534-1538,1503`). `body` is
    /// whatever the caller has already marshalled (a real `Response` needs the externalizer
    /// registry this crate doesn't have yet — so a genuine success or
    /// `ExceptionResponse` value can't be produced end-to-end by this crate today; this method
    /// itself is wire-exact for whatever bytes it is given, matching upstream's own encoding of
    /// "no answer at all" as a zero-length buffer, `:1558-1560`, and "answered `null`" as
    /// `ResponseBody::classify`'s marshalled-null case).
    pub async fn send_reply(&self, token: ReplyToken, body: Vec<u8>) -> Result<(), TransportError> {
        Ok(self.inner.send_reply_now(token.dest, token.req_id, token.corr_id, body).await?)
    }

    /// Pulls the next inbound command the receive path decided to [`Action::Invoke`] (self-sent and
    /// before-start messages are already handled internally — before-start being answered directly,
    /// not queued here at all).
    pub async fn recv_command(&self) -> Option<ReceivedCommand> {
        self.inbound_rx.lock().await.recv().await
    }

    /// Disconnects and closes the channel.
    pub async fn disconnect(&self) {
        self.inner.channel.disconnect().await;
        self.view_log_task.abort();
    }

    /// ISPN000094 (`Log.java:399-401`, "Received new cluster view for channel %s: %s"): jgroups-rs
    /// exposes no post-construction view-change listener hook (`Gms::new` only takes
    /// `view_listeners` at `stack::build_stack` time, itself not public), so this polls
    /// `channel.view()` and logs on `ViewId` change. A latency divergence from upstream's
    /// synchronous `receiveClusterView` (`JGroupsTransport.java:769-830`, fired straight off the
    /// JGroups `VIEW_CHANGE` event).
    async fn view_log_loop(inner: Arc<Inner>) {
        let mut last_view_id: Option<jgroups::wire::header::ViewId> = None;
        loop {
            if let Some(view) = inner.channel.view().await
                && last_view_id.as_ref() != Some(&view.view_id)
            {
                last_view_id = Some(view.view_id.clone());
                let cluster_name = inner.channel.gms().cluster_name().await.unwrap_or_default();
                log::info!("ISPN000094: Received new cluster view for channel {cluster_name}: {}", format_view(&view));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// `View.toString()`/`ViewId.toString()` (`upstream/src/org/jgroups/View.java:189-198`,
/// `ViewId.java:72-74`): `[<coord>|<id>] (<n>) [<members>]`. `Address` formatting is a best-effort
/// approximation, not upstream's `UUID.toString()` (the logical-name/`hostname-NNNNN` lookup table
/// isn't exposed by jgroups-rs's public API either) — this log line's consumers
/// (MockServer's clustered-convergence and DNS-ping checks) grep for the
/// `ISPN000094` prefix and general shape, not this exact member rendering.
fn format_view(view: &View) -> String {
    fn addr_str(a: &Address) -> String {
        match a {
            Address::Uuid { most_sig_bits, least_sig_bits } => format!("{most_sig_bits:x}{least_sig_bits:x}"),
            Address::ExtendedUuid { most_sig_bits, least_sig_bits, .. } => format!("{most_sig_bits:x}{least_sig_bits:x}"),
            Address::IpAddress { ip, port, .. } => format!("{ip}:{port}"),
        }
    }
    let creator = view.view_id.creator.as_ref().map(addr_str).unwrap_or_default();
    let members: Vec<String> = view.members.iter().map(addr_str).collect();
    format!("[{creator}|{}] ({}) [{}]", view.view_id.id, view.members.len(), members.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marshall::global;
    use crate::marshall::primitives::ByteWriter;

    fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            </config>"#
        )
    }

    fn physical_port(addr: &Address) -> u16 {
        match addr {
            Address::IpAddress { port, .. } => *port,
            other => panic!("expected an IpAddress, got {other:?}"),
        }
    }

    async fn two_joined(cluster: &str) -> (Transport, Transport) {
        let a = Transport::new(&stack_xml(0, "")).await.unwrap();
        a.connect(cluster).await.unwrap();
        a.mark_started();
        let a_port = physical_port(a.physical_address());
        let b = Transport::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        b.connect(cluster).await.unwrap();
        b.mark_started();
        (a, b)
    }

    // Every stack `Transport::new` can build includes NAKACK2/UNICAST3 (`build_stack`'s own doc
    // comment) — plain payload delivery survives them unaffected either way.
    #[tokio::test]
    async fn fire_and_forget_arrives_with_the_real_payload_bytes() {
        let (a, b) = two_joined("FireForget").await;
        let b_addr = b.local_address().clone();
        a.send_fire_and_forget(Some(b_addr), b"do-this".to_vec()).await.unwrap();

        let cmd = tokio::time::timeout(Duration::from_secs(2), b.recv_command()).await.unwrap().unwrap();
        assert_eq!(cmd.from, *a.local_address());
        assert_eq!(cmd.body, b"do-this");
        assert!(cmd.reply_to.is_none(), "a SINGLE_MESSAGE never expects a reply");

        a.disconnect().await;
        b.disconnect().await;
    }

    // `processRequest`'s self-sent check (`dispatch::on_message`, already unit-tested there in
    // isolation) — proved here end to end over a real channel: a node addressing itself must not
    // see its own fire-and-forget message delivered back.
    #[tokio::test]
    async fn a_fire_and_forget_message_sent_to_self_is_not_delivered() {
        let a = Transport::new(&stack_xml(0, "")).await.unwrap();
        a.connect("SelfSend").await.unwrap();
        a.mark_started();
        let self_addr = a.local_address().clone();

        a.send_fire_and_forget(Some(self_addr), b"echo".to_vec()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(300), a.recv_command()).await;
        assert!(result.is_err(), "a message addressed to self must be dropped, not delivered");

        a.disconnect().await;
    }

    // The headline case: a correlated request/response round trip over the real stack,
    // through the now-closed jgroups-rs `UpHandler` seam — A calls `send_request`, B's `recv_command`
    // sees the `reply_to` token and answers via `send_reply`, and A's `send_request` future resolves
    // to the exact body B sent back.
    #[tokio::test]
    async fn a_correlated_request_is_answered_and_completes_the_callers_future() {
        let (a, b) = two_joined("Correlated").await;
        let b_addr = b.local_address().clone();

        let a_task = tokio::spawn({
            let a = std::sync::Arc::new(a);
            let a2 = a.clone();
            async move {
                let result = a2.send_request(b_addr, b"ping".to_vec(), DeliverOrder::None).await.unwrap();
                (a, result)
            }
        });

        let cmd = tokio::time::timeout(Duration::from_secs(2), b.recv_command()).await.unwrap().unwrap();
        assert_eq!(cmd.body, b"ping");
        let token = cmd.reply_to.expect("a REQUEST always expects a reply");

        // A real marshalled Java `null` (`marshall::global::write_nullable(Value::Null)`) — the
        // wire-exact encoding `ResponseBody::classify` reads back as `SuccessfulEmpty`
        // (`SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE`, `:1563-1565`).
        let mut w = ByteWriter::new();
        global::write_nullable(&mut w, &global::Value::Null);
        b.send_reply(token, w.into_bytes()).await.unwrap();

        let (a, result) = tokio::time::timeout(Duration::from_secs(2), a_task).await.unwrap().unwrap();
        assert_eq!(result, ResponseBody::SuccessfulEmpty);

        a.disconnect().await;
        b.disconnect().await;
    }

    // `processMessage`'s `!running` branch for a `REQUEST` (`:1460-1462`): B never calls
    // `mark_started`, so its own receive path answers `CacheNotFoundResponse` (a zero-length reply
    // body) without ever queuing a `ReceivedCommand` — proved here by the requester's future
    // resolving to `ResponseBody::CacheNotFound` and B's `recv_command` seeing nothing at all.
    #[tokio::test]
    async fn a_request_before_start_answers_cache_not_found_over_the_real_channel() {
        let a = Transport::new(&stack_xml(0, "")).await.unwrap();
        a.connect("BeforeStart").await.unwrap();
        a.mark_started();
        let a_port = physical_port(a.physical_address());

        let b = Transport::new(&stack_xml(0, &format!("127.0.0.1[{a_port}]"))).await.unwrap();
        b.connect("BeforeStart").await.unwrap();
        // Deliberately no b.mark_started().

        let b_addr = b.local_address().clone();
        let result = tokio::time::timeout(Duration::from_secs(2), a.send_request(b_addr, b"cmd".to_vec(), DeliverOrder::None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, ResponseBody::CacheNotFound);

        let nothing = tokio::time::timeout(Duration::from_millis(200), b.recv_command()).await;
        assert!(nothing.is_err(), "a REQUEST before start must never reach the application, only be answered directly");

        a.disconnect().await;
        b.disconnect().await;
    }

    // `processRequest`'s self-sent check applies to REQUEST just as much as SINGLE_MESSAGE
    // (`dispatch::on_message`'s own match arm covers both) — a correlated request addressed to
    // oneself must be dropped, not looped back into recv_command, and (unlike a peer) never
    // answered either, since dispatch::on_message returns Drop, not RespondCacheNotFound, for it.
    #[tokio::test]
    async fn a_correlated_request_sent_to_self_is_dropped_not_invoked_or_answered() {
        let a = Transport::new(&stack_xml(0, "")).await.unwrap();
        a.connect("SelfCorrelated").await.unwrap();
        a.mark_started();
        let self_addr = a.local_address().clone();

        // `send_request` would await forever (never answered) — send the same wire shape directly
        // instead, so the test can assert "never invoked" with a timeout rather than hang.
        let mut msg = Message::bytes(Some(self_addr), b"echo".to_vec());
        msg.flags = DeliverOrder::None.request_flags();
        msg.put_header(PROT_ID_REQUEST_CORRELATOR, HeaderValue::RequestCorrelator(RequestCorrelatorHeader { kind: REQCORR_REQ, req_id: 0, corr_id: 0 }));
        a.inner.channel.send_message(msg).await.unwrap();

        let nothing = tokio::time::timeout(Duration::from_millis(300), a.recv_command()).await;
        assert!(nothing.is_err(), "a correlated request addressed to self must be dropped, not delivered");

        a.disconnect().await;
    }

    // `JGroupsTransport.REQUEST_FLAGS_UNORDERED`/`REPLY_FLAGS` (`:152-157`) actually land on the
    // wire, not just in this crate's constants — proved by splicing a recording protocol below
    // `GMS` on both ends (the position both `insert_protocol_below("GMS", ...)` and this crate's
    // own `RequestCorrelatorHeader` traffic already use) and inspecting the real `Message` each
    // side's `UpHandler` seam sees before the application ever gets it.
    #[tokio::test]
    async fn request_and_reply_flags_match_upstreams_constants_on_the_wire() {
        use jgroups::stack::AsyncProtocol;
        use std::sync::atomic::AtomicI16;

        struct FlagRecorder(Arc<AtomicI16>);
        impl AsyncProtocol for FlagRecorder {
            fn name(&self) -> &'static str {
                "FLAG_RECORDER"
            }
            fn up_message<'a>(&'a self, sender: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
                Box::pin(async move {
                    if msg.get_header(PROT_ID_REQUEST_CORRELATOR).is_some() {
                        self.0.store(msg.flags, Ordering::SeqCst);
                    }
                    Some((sender, msg))
                })
            }
        }

        let (a, b) = two_joined("FlagCheck").await;
        let b_addr = b.local_address().clone();

        let seen_request_flags = Arc::new(AtomicI16::new(0));
        b.inner.channel.insert_protocol_below("GMS", Box::new(FlagRecorder(seen_request_flags.clone()))).await.unwrap();
        let seen_reply_flags = Arc::new(AtomicI16::new(0));
        a.inner.channel.insert_protocol_below("GMS", Box::new(FlagRecorder(seen_reply_flags.clone()))).await.unwrap();

        let a = Arc::new(a);
        let a_task = tokio::spawn({
            let a2 = a.clone();
            async move { a2.send_request(b_addr, b"flags".to_vec(), DeliverOrder::None).await.unwrap() }
        });

        let cmd = tokio::time::timeout(Duration::from_secs(2), b.recv_command()).await.unwrap().unwrap();
        assert_eq!(seen_request_flags.load(Ordering::SeqCst), DeliverOrder::None.message_flags(true));

        b.send_reply(cmd.reply_to.unwrap(), Vec::new()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), a_task).await.unwrap().unwrap();
        assert_eq!(result, ResponseBody::CacheNotFound, "an empty reply body classifies as CacheNotFound, same as a real empty answer");
        assert_eq!(seen_reply_flags.load(Ordering::SeqCst), REPLY_FLAGS);

        a.disconnect().await;
        b.disconnect().await;
    }
}
