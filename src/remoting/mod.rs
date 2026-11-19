//! `org.infinispan.remoting.transport.jgroups` — fire-and-forget and correlated request/response
//! dispatch over a real `jgroups-rs` channel. See
//! `transport`'s module doc comment for how this is wired onto `jgroups::channel::JChannel`'s
//! `UpHandler`/`send_message` seam.

pub mod dispatch;
pub mod request_correlator;
pub mod transport;
