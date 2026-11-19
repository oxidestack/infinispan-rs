//! Pure port of `JGroupsTransport.processMessage`/`processRequest`/`processResponse`'s *decision*
//! logic (`upstream/core/src/main/java/org/infinispan/remoting/transport/jgroups/
//! JGroupsTransport.java:1441-1579`) — everything except actually deserializing a command or
//! invoking the application handler, which need the command/response type registry (the externalizer registry). This module works over opaque `Vec<u8>` bodies and the header's
//! `(kind, req_id)` alone, so every branch upstream's own line numbers document is unit-testable
//! here without a live channel, a marshaller-registry, or any command types at all.
//!
//! `crate::remoting::transport` is the only caller: it decodes a `RequestCorrelatorHeader` off a
//! real message, calls [`on_message`], and carries out whichever [`Action`] comes back (send a
//! reply, drop, hand `body` to a registered handler, complete a pending request).

use crate::marshall::global;
use crate::marshall::primitives::ByteReader;
use jgroups::wire::address::Address;

/// Mirrors `JGroupsTransport`'s three wire header kinds
/// (`REQUEST`/`RESPONSE`/`SINGLE_MESSAGE`, `JGroupsTransport.java:1448-1456,1465-1475`).
/// `SingleMessage` is the "no `RequestCorrelator.Header` at all" case (`header == null`) — a plain
/// fire-and-forget command, matching upstream's handling of a message with no header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A request expecting a response.
    Request,
    /// A response to a request.
    Response,
    /// A message with no correlator header: fire and forget.
    SingleMessage,
}

/// What the receive path decided to do with one inbound frame. Nothing here performs I/O; the
/// caller carries each variant out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// `processRequest`'s self-sent check (`:1521-1526`, "DISCARD ignores the DONT_LOOPBACK flag,
    /// see JGRP-2205" — i.e. a request from ourselves is dropped explicitly at this layer, not
    /// relied on to be filtered by the transport) — or a `RESPONSE`/`SINGLE_MESSAGE` that arrives
    /// while not running (`:1457-1464`, only `REQUEST` gets an answer in that branch).
    Drop,
    /// `processMessage`'s `!running` branch for a `REQUEST` (`:1460-1462`): answer
    /// `CacheNotFoundResponse` without ever handing the body to a handler — as upstream does
    /// for a request that arrives before start.
    RespondCacheNotFound,
    /// Hand `body` to the application handler. `reply_expected` mirrors
    /// `requestId != Request.NO_REQUEST_ID` (`:1530-1538`): a `SingleMessage` is still delivered to
    /// the handler, just with no reply path (`Reply.NO_OP`); if the handler itself fails, the
    /// caller answers with `ExceptionResponse` (`:1548-1552`) — that's the caller's job, since
    /// invoking the handler isn't pure.
    Invoke {
        /// The marshalled command.
        body: Vec<u8>,
        /// Whether the sender waits for a reply.
        reply_expected: bool,
    },
    /// `processResponse` (`:1555-1574`): complete the pending request keyed by this frame's
    /// `req_id` with this outcome.
    Complete {
        /// The response.
        response: ResponseBody,
    },
}

/// `Response` outcomes `processResponse` can produce from a raw buffer
/// (`JGroupsTransport.java:1555-1566`), before any real `Response` type exists to deserialize
/// into — a generic externalizer/command registry is out of this module's scope. `Bytes` is the
/// non-null, non-empty case, carried opaque for the caller to interpret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseBody {
    /// `length == 0` (`:1558-1560`, "Empty buffer signals the ForkChannel with this name is not
    /// running on the remote node").
    CacheNotFound,
    /// A non-empty buffer that unmarshals to a Java `null` (`:1563-1565`,
    /// `SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE`).
    SuccessfulEmpty,
    /// A non-empty, non-null buffer whose real type this crate cannot decode yet (no externalizer
    /// registry — see the module doc comment).
    Bytes(Vec<u8>),
}

impl ResponseBody {
    /// `processResponse`'s classification (`:1555-1566`), reusing the `GlobalMarshaller`
    /// envelope (`crate::marshall::global::read_nullable`) to tell "marshals to null" apart from
    /// "marshals to something else" without needing a real `Response` type registry yet. A tag
    /// `read_nullable` doesn't understand (any real `Response`, which rides `ID_INTERNAL` — out of
    /// scope here) falls back to `Bytes(raw)` rather than erroring: the bytes are real
    /// and worth keeping even though this crate can't interpret them yet.
    pub fn classify(buffer: &[u8]) -> ResponseBody {
        if buffer.is_empty() {
            return ResponseBody::CacheNotFound;
        }
        let mut r = ByteReader::new(buffer);
        match global::read_nullable(&mut r) {
            Ok(global::Value::Null) => ResponseBody::SuccessfulEmpty,
            _ => ResponseBody::Bytes(buffer.to_vec()),
        }
    }
}

/// `processMessage`'s dispatch decision (`:1441-1476`), with `processRequest`'s self-sent check
/// (`:1521-1526`) and `processResponse`'s classification (above) folded in — neither needs I/O, so
/// there is no reason to split them into a separate "second pass" the way upstream's method split
/// does (upstream's split exists because `processRequest`/`processResponse` are also each other's
/// only caller; here `on_message` is the one entry point `remoting::transport` calls per frame).
pub fn on_message(local: &Address, src: &Address, running: bool, kind: Kind, buffer: &[u8]) -> Action {
    if !running {
        return match kind {
            Kind::Request => Action::RespondCacheNotFound,
            Kind::Response | Kind::SingleMessage => Action::Drop,
        };
    }
    match kind {
        Kind::Response => Action::Complete { response: ResponseBody::classify(buffer) },
        Kind::Request | Kind::SingleMessage => {
            if src == local {
                return Action::Drop;
            }
            Action::Invoke { body: buffer.to_vec(), reply_expected: kind == Kind::Request }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    // processMessage's `!running` branch, `:1457-1464`: only REQUEST gets CacheNotFoundResponse.
    #[test]
    fn a_request_before_start_answers_cache_not_found() {
        let a = addr(1);
        let b = addr(2);
        assert_eq!(on_message(&a, &b, false, Kind::Request, b"cmd"), Action::RespondCacheNotFound);
    }

    #[test]
    fn a_response_or_single_message_before_start_is_dropped_silently() {
        let a = addr(1);
        let b = addr(2);
        assert_eq!(on_message(&a, &b, false, Kind::Response, b"rsp"), Action::Drop);
        assert_eq!(on_message(&a, &b, false, Kind::SingleMessage, b"cmd"), Action::Drop);
    }

    // processRequest's self-sent check, `:1521-1526` — applies to REQUEST and SINGLE_MESSAGE
    // alike, since upstream's switch sends both to processRequest.
    #[test]
    fn a_request_from_self_is_dropped() {
        let a = addr(1);
        assert_eq!(on_message(&a, &a, true, Kind::Request, b"cmd"), Action::Drop);
        assert_eq!(on_message(&a, &a, true, Kind::SingleMessage, b"cmd"), Action::Drop);
    }

    #[test]
    fn a_request_from_a_peer_is_invoked_with_a_reply_expected() {
        let a = addr(1);
        let b = addr(2);
        assert_eq!(
            on_message(&a, &b, true, Kind::Request, b"cmd"),
            Action::Invoke { body: b"cmd".to_vec(), reply_expected: true }
        );
    }

    // A SINGLE_MESSAGE (no header, fire-and-forget) is still invoked, but with no reply path —
    // `Reply.NO_OP`, `:1534-1538`.
    #[test]
    fn a_single_message_from_a_peer_is_invoked_with_no_reply_expected() {
        let a = addr(1);
        let b = addr(2);
        assert_eq!(
            on_message(&a, &b, true, Kind::SingleMessage, b"cmd"),
            Action::Invoke { body: b"cmd".to_vec(), reply_expected: false }
        );
    }

    // processResponse, `:1555-1566`.
    #[test]
    fn a_zero_length_response_buffer_is_cache_not_found() {
        assert_eq!(ResponseBody::classify(b""), ResponseBody::CacheNotFound);
    }

    #[test]
    fn a_response_buffer_that_unmarshals_to_null_is_successful_empty() {
        // ID_NULL = 0x00 (marshall::global's envelope tag for a marshalled Java `null`).
        assert_eq!(ResponseBody::classify(&[0x00]), ResponseBody::SuccessfulEmpty);
    }

    #[test]
    fn a_non_null_response_buffer_is_kept_opaque() {
        // ID_PRIMITIVE(0x01) + a byte primitive tag/value this test doesn't need to be a real
        // primitive for — classify() only needs read_nullable to NOT return Value::Null.
        let buf = [0x01u8, 0x02, 0x2a];
        assert_eq!(ResponseBody::classify(&buf), ResponseBody::Bytes(buf.to_vec()));
    }

    #[test]
    fn a_response_buffer_with_an_unsupported_tag_is_kept_opaque_not_an_error() {
        // A real Response (CacheNotFoundResponse, SuccessfulResponse, ...) rides ID_INTERNAL
        // (0x02) once the externalizer registry exists — out of scope here, but
        // classify() must not panic or misreport it as a null.
        let buf = [0x02u8, 7, 9, 9];
        assert_eq!(ResponseBody::classify(&buf), ResponseBody::Bytes(buf.to_vec()));
    }

    #[test]
    fn dispatch_via_on_message_for_a_response_reuses_classify() {
        let a = addr(1);
        let b = addr(2);
        assert_eq!(
            on_message(&a, &b, true, Kind::Response, b""),
            Action::Complete { response: ResponseBody::CacheNotFound }
        );
        assert_eq!(
            on_message(&a, &b, true, Kind::Response, &[0x00]),
            Action::Complete { response: ResponseBody::SuccessfulEmpty }
        );
    }
}
