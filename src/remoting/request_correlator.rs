//! Request id allocation, outgoing message flags, and the pending-request table —
//! `JGroupsTransport`'s request half (`upstream/core/src/main/java/org/infinispan/remoting/
//! transport/jgroups/JGroupsTransport.java:152-166,1222-1253`) plus the correlation bookkeeping
//! `org.infinispan.remoting.transport.impl.RequestRepository`/`AbstractRequest` do (not vendored
//! here — this port keeps only their observable behaviour: one outstanding waiter per request id,
//! completed exactly once). Pure Rust, no channel — `crate::remoting::transport` is the only
//! caller, using `jgroups`'s already-ported `RequestCorrelatorHeader`
//! (`jgroups::wire::header::RequestCorrelatorHeader`, magic 67, protocol id 200) for the wire
//! shape.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use jgroups::wire::message::{FLAG_NO_FC, FLAG_NO_RELAY, FLAG_NO_TOTAL_ORDER, FLAG_OOB};

use crate::remoting::dispatch::ResponseBody;

/// `org.infinispan.remoting.transport.Request.NO_REQUEST_ID`: the sentinel meaning "no
/// correlation, this is a plain fire-and-forget command" — no `RequestCorrelator.Header` is put on
/// the message at all when a call site uses it (`JGroupsTransport.addRequestHeader`, `:1247-1252`).
pub const NO_REQUEST_ID: i64 = -1;

/// `JGroupsTransport.REQUEST_FLAGS_UNORDERED` (`:152-153`): `OOB|NO_TOTAL_ORDER`, used for
/// `DeliverOrder.NONE` (`encodeDeliverMode`, `:1255-1263`).
pub const REQUEST_FLAGS_UNORDERED: i16 = FLAG_OOB | FLAG_NO_TOTAL_ORDER;
/// `JGroupsTransport.REQUEST_FLAGS_PER_SENDER` (`:154`): `NO_TOTAL_ORDER` alone, used for
/// `DeliverOrder.PER_SENDER`.
pub const REQUEST_FLAGS_PER_SENDER: i16 = FLAG_NO_TOTAL_ORDER;
/// `JGroupsTransport.REPLY_FLAGS` (`:155-157`): `NO_FC|OOB|NO_TOTAL_ORDER`, always used for a
/// response (`sendResponse`, `:1503`).
pub const REPLY_FLAGS: i16 = FLAG_NO_FC | FLAG_OOB | FLAG_NO_TOTAL_ORDER;

/// `org.infinispan.remoting.transport.jgroups.DeliverOrder`'s two members this crate's reach
/// needs (`NONE`/`PER_SENDER`; `TOTAL` needs total-order anycast, out of scope — MockServer's
/// REPL_SYNC caches never request it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverOrder {
    /// `NONE`: no ordering.
    None,
    /// `PER_SENDER`: in order per sender.
    PerSender,
}

impl DeliverOrder {
    /// `JGroupsTransport.encodeDeliverMode`, `:1255-1263`.
    pub fn request_flags(self) -> i16 {
        match self {
            DeliverOrder::None => REQUEST_FLAGS_UNORDERED,
            DeliverOrder::PerSender => REQUEST_FLAGS_PER_SENDER,
        }
    }

    /// `JGroupsTransport.setMessageFlags(message, deliverOrder, noRelay)` (`:1222-1230`): the deliver
    /// mode's flags plus `NO_RELAY` when `no_relay`. Every intra-cluster command sets it
    /// (`sendCommand`/`sendCommandToAll`/`invokeCommand`: `noRelay = true`); without it a JVM's
    /// `processRequest` treats the message as a cross-site request (`:1539-1546`, a
    /// `ClassCastException` from `UUID` to `SiteAddress`).
    pub fn message_flags(self, no_relay: bool) -> i16 {
        self.request_flags() | if no_relay { FLAG_NO_RELAY } else { 0 }
    }

    /// `JGroupsTransport.decodeDeliverMode`, `:1576-1579`: `OOB` set means `NONE`, otherwise
    /// `PER_SENDER`. Informational only in this crate's scope (nothing downstream branches on it
    /// yet); kept because it's part of the same faithfully-ported pair.
    pub fn decode(flags: i16) -> DeliverOrder {
        if flags & FLAG_OOB != 0 { DeliverOrder::None } else { DeliverOrder::PerSender }
    }
}

/// The pending-request table: one outstanding waiter per request id, completed exactly once —
/// `RequestRepository`'s observable behaviour (not vendored; upstream's own type isn't in the
/// vendored `core/src/main/java` slice this crate reads from directly, only referenced by
/// `JGroupsTransport`'s `requests` field, `:1570`).
pub struct PendingRequests {
    next_id: AtomicI64,
    pending: Mutex<HashMap<i64, tokio::sync::oneshot::Sender<ResponseBody>>>,
}

impl Default for PendingRequests {
    fn default() -> PendingRequests {
        PendingRequests::new()
    }
}

impl PendingRequests {
    /// An empty table.
    pub fn new() -> PendingRequests {
        // Starting above NO_REQUEST_ID (-1) so the first allocated id is never mistaken for "no
        // correlation" — upstream's own `REQUEST_ID_GENERATOR` starts at 0 too
        // (`JGroupsTransport.java:167`, `AtomicLong requestIdCounter = new AtomicLong(0)`).
        PendingRequests { next_id: AtomicI64::new(0), pending: Mutex::new(HashMap::new()) }
    }

    /// Allocates a fresh request id and registers a waiter for it, returning both. `fetch_add` on
    /// an `AtomicI64` never repeats within this process's lifetime for any realistic request
    /// volume (matches `AtomicLong.getAndIncrement`, `JGroupsTransport.java:1259` area — not a
    /// cited line, upstream's own counter is equally unbounded).
    pub fn register(&self) -> (i64, tokio::sync::oneshot::Receiver<ResponseBody>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    /// `processResponse`'s `requests.addResponse(requestId, address, response)` (`:1570`): complete
    /// the waiter for `req_id`, if one is still pending (a request that already timed out, or was
    /// answered by a different member in a fan-out this crate doesn't build yet, has none — that's
    /// not an error here, matching upstream's map-based `addResponse` which is a no-op for an
    /// unknown/already-completed id).
    pub fn complete(&self, req_id: i64, response: ResponseBody) {
        if let Some(tx) = self.pending.lock().unwrap().remove(&req_id) {
            let _ = tx.send(response);
        }
    }

    /// Drops a waiter without completing it (a request that will never get a response — the
    /// channel disconnected already tore down, or a caller gave up). Not a literal upstream method
    /// (`RequestRepository` isn't vendored); kept for symmetry with `register` so
    /// `remoting::transport` never leaks a map entry for a request whose `Receiver` was dropped
    /// without ever being awaited.
    pub fn cancel(&self, req_id: i64) {
        self.pending.lock().unwrap().remove(&req_id);
    }

    /// The number of requests awaiting a response.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_flags_match_upstreams_three_constants() {
        assert_eq!(REQUEST_FLAGS_UNORDERED, FLAG_OOB | FLAG_NO_TOTAL_ORDER);
        assert_eq!(REQUEST_FLAGS_PER_SENDER, FLAG_NO_TOTAL_ORDER);
        assert_eq!(REPLY_FLAGS, FLAG_NO_FC | FLAG_OOB | FLAG_NO_TOTAL_ORDER);
    }

    #[test]
    fn deliver_order_encodes_and_decodes_symmetrically() {
        assert_eq!(DeliverOrder::None.message_flags(true), REQUEST_FLAGS_UNORDERED | FLAG_NO_RELAY);
        assert_eq!(DeliverOrder::PerSender.message_flags(true), REQUEST_FLAGS_PER_SENDER | FLAG_NO_RELAY);
        assert_eq!(DeliverOrder::None.message_flags(false), REQUEST_FLAGS_UNORDERED);
        assert_eq!(DeliverOrder::None.request_flags(), REQUEST_FLAGS_UNORDERED);
        assert_eq!(DeliverOrder::PerSender.request_flags(), REQUEST_FLAGS_PER_SENDER);
        assert_eq!(DeliverOrder::decode(REQUEST_FLAGS_UNORDERED), DeliverOrder::None);
        assert_eq!(DeliverOrder::decode(REQUEST_FLAGS_PER_SENDER), DeliverOrder::PerSender);
    }

    #[test]
    fn registered_ids_are_distinct_and_monotonic() {
        let table = PendingRequests::new();
        let (id1, _rx1) = table.register();
        let (id2, _rx2) = table.register();
        assert!(id2 > id1);
        assert_eq!(table.pending_count(), 2);
    }

    #[tokio::test]
    async fn completing_a_pending_request_resolves_its_receiver() {
        let table = PendingRequests::new();
        let (id, rx) = table.register();
        table.complete(id, ResponseBody::SuccessfulEmpty);
        assert_eq!(rx.await.unwrap(), ResponseBody::SuccessfulEmpty);
        assert_eq!(table.pending_count(), 0);
    }

    // `addResponse` for an id with no pending waiter is a silent no-op, not an error.
    #[test]
    fn completing_an_unknown_request_id_does_nothing() {
        let table = PendingRequests::new();
        table.complete(999, ResponseBody::CacheNotFound);
        assert_eq!(table.pending_count(), 0);
    }

    #[test]
    fn cancel_removes_a_pending_waiter_without_completing_it() {
        let table = PendingRequests::new();
        let (id, mut rx) = table.register();
        table.cancel(id);
        assert_eq!(table.pending_count(), 0);
        // `cancel` dropped the `Sender`; the `Receiver` sees the channel closed, not a value.
        assert!(rx.try_recv().is_err());
    }
}
