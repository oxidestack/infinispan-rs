//! [`TopologyTransport`] over the real [`crate::remoting::transport::Transport`] (a `jgroups-rs` channel): topology
//! commands are marshalled with [`TopologyCommand::to_bytes`] and travel as correlated requests (a
//! `RequestCorrelator.Header` REQ, answered with a marshalled `Response`) or, for
//! `executeOnClusterAsync`/`sendTo`, as plain messages with no header (`JGroupsTransport.sendToAll`).
//!
//! View changes arrive through jgroups-rs's `MembershipListener` (`viewAccepted`); a `MergeView`
//! (`subgroups` present) reaches the topology manager as `merge = true`, which is what triggers
//! `RECOVERING_CLUSTER` when the coordinator is unchanged.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use jgroups::wire::address::Address as JAddress;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::commands::TopologyCommand;
use super::messages::{ExceptionKind, ILLEGAL_LIFECYCLE_STATE_EXCEPTION, Response};
use super::node::TopologyNode;
use super::support::{BoxFuture, eager_spawn};
use super::transport::{RemoteError, TopologyTransport};
use super::wire::{ObjectInput, ObjectOutput};
use crate::ch::address::Address;
use crate::remoting::dispatch::ResponseBody;
use crate::remoting::request_correlator::DeliverOrder;
use crate::remoting::transport::{Transport, TransportError};

fn to_jgroups(a: &Address) -> JAddress {
    match a {
        Address::JGroups(j) => j.clone(),
        Address::Persistent { .. } => panic!("a PersistentUUID is not a cluster member address"),
    }
}

fn encode_response(r: &Response) -> Vec<u8> {
    let mut o = ObjectOutput::new();
    o.write_response(r);
    o.into_bytes()
}

/// The `ExceptionResponse` a failed command is answered with.
fn exception_response(e: &super::cluster_cache_status::TopologyError) -> Response {
    let msg = e.0.clone();
    let kind = if msg.starts_with("ISPN000408") || msg.starts_with("ISPN000409") || msg.starts_with("ISPN000410") {
        ExceptionKind::CacheJoinException
    } else if msg.contains("IllegalLifecycleStateException") {
        return Response::Exception { kind: ExceptionKind::Generic(ILLEGAL_LIFECYCLE_STATE_EXCEPTION), message: None };
    } else {
        ExceptionKind::CacheException
    };
    Response::Exception { kind, message: Some(msg) }
}

fn decode_body(body: ResponseBody) -> Result<Response, RemoteError> {
    match body {
        // `JGroupsTransport.java:1558-1560`: an empty buffer is a `CacheNotFoundResponse`
        ResponseBody::CacheNotFound => Err(RemoteError::Suspect("CacheNotFoundResponse".into())),
        ResponseBody::SuccessfulEmpty => Ok(Response::Successful(None)),
        ResponseBody::Bytes(b) => {
            let r = ObjectInput::new(&b).read_response().map_err(|e| RemoteError::Other(e.to_string()))?;
            match r {
                Response::CacheNotFound => Err(RemoteError::Suspect("CacheNotFoundResponse".into())),
                Response::Exception { kind: ExceptionKind::SuspectException, message } => {
                    Err(RemoteError::Suspect(message.unwrap_or_default()))
                }
                Response::Exception { message, .. } => Err(RemoteError::Other(message.unwrap_or_default())),
                ok => Ok(ok),
            }
        }
    }
}

struct ViewListener {
    transport: Arc<JGroupsTopologyTransport>,
    node: Arc<TopologyNode>,
}

impl jgroups::protocols::gms::MembershipListener for ViewListener {
    fn view_accepted<'a>(&'a self, view: &'a jgroups::wire::view::View, subgroups: Option<&'a [jgroups::wire::view::View]>) -> jgroups::stack::BoxFuture<'a, ()> {
        Box::pin(async move {
            self.transport.apply_view(&self.node, view, subgroups.is_some(), false);
        })
    }
}

/// Where inbound cache-level commands (`CacheRpcCommand` frames: writes, clustered gets, state transfer)
/// go; the topology transport only knows the topology commands. `None` answers nothing (the caller
/// sends a marshalled null).
pub trait CacheRpcSink: Send + Sync {
    /// Handles `request`, a marshalled `CacheRpcCommand` from `origin`; returns the marshalled answer.
    fn handle(&self, origin: Address, request: Vec<u8>) -> BoxFuture<'static, Option<Vec<u8>>>;
}

/// A [`super::transport::TopologyTransport`] over a jgroups channel, carrying both topology and cache commands.
pub struct JGroupsTopologyTransport {
    inner: Arc<Transport>,
    local: Address,
    view: Mutex<(i32, Vec<Address>)>,
    view_tx: watch::Sender<i32>,
    node: OnceLock<Arc<TopologyNode>>,
    cache_sink: OnceLock<Arc<dyn CacheRpcSink>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Fire-and-forget sends, drained in order by one task so that two broadcasts leave in the order
    /// they were requested (NAKACK2 numbers them when they are sent).
    outbound: tokio::sync::mpsc::UnboundedSender<(Option<JAddress>, Vec<u8>)>,
}

impl JGroupsTopologyTransport {
    /// Build the channel from `stack_xml`, join `cluster_name` and mark the transport started.
    pub async fn connect(stack_xml: &str, cluster_name: &str) -> Result<Arc<Self>, TransportError> {
        let inner = Arc::new(Transport::new(stack_xml).await?);
        let view = inner.connect(cluster_name).await?;
        inner.mark_started();
        let local = Address::JGroups(inner.local_address().clone());
        let id = view.view_id.id as i32;
        let members = view.members.iter().cloned().map(Address::JGroups).collect();
        let (outbound, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<(Option<JAddress>, Vec<u8>)>();
        let sender = {
            let inner = inner.clone();
            tokio::spawn(async move {
                while let Some((dest, bytes)) = outbound_rx.recv().await {
                    let _ = inner.send_fire_and_forget(dest, bytes).await;
                }
            })
        };
        Ok(Arc::new(Self {
            inner,
            local,
            view: Mutex::new((id, members)),
            view_tx: watch::channel(id).0,
            node: OnceLock::new(),
            cache_sink: OnceLock::new(),
            tasks: Mutex::new(vec![sender]),
            outbound,
        }))
    }

    /// The underlying `JGroupsTransport`, e.g. for its physical address.
    pub fn transport(&self) -> &Arc<Transport> {
        &self.inner
    }

    /// Wire the node in and start the inbound pump and the view poller. Call after
    /// [`TopologyNode::start`], which handles the initial view itself.
    pub fn attach(self: &Arc<Self>, node: Arc<TopologyNode>) {
        let _ = self.node.set(node.clone());
        let pump = {
            let this = self.clone();
            let node = node.clone();
            tokio::spawn(async move {
                while let Some(cmd) = this.inner.recv_command().await {
                    let origin = Address::JGroups(cmd.from.clone());
                    if crate::commands::rpc::CacheRpcCommand::is_cache_rpc(&cmd.body) {
                        // `ID_INTERNAL` 36: a cache's own command, not a topology one
                        let sink = this.cache_sink.get().cloned();
                        let this = this.clone();
                        tokio::spawn(async move {
                            let answer = match sink {
                                Some(sink) => sink.handle(origin, cmd.body).await,
                                None => Some(encode_response(&Response::CacheNotFound)),
                            };
                            if let Some(token) = cmd.reply_to {
                                let bytes = answer.unwrap_or_else(|| encode_response(&Response::Successful(None)));
                                let _ = this.inner.send_reply(token, bytes).await;
                            }
                        });
                        continue;
                    }
                    let command = match TopologyCommand::from_bytes(&cmd.body) {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("Undecodable topology command from {origin:?}: {e}");
                            if let Some(token) = cmd.reply_to {
                                let resp = exception_response(&super::cluster_cache_status::TopologyError(e.to_string()));
                                let _ = this.inner.send_reply(token, encode_response(&resp)).await;
                            }
                            continue;
                        }
                    };
                    // eager: the command is queued on its sequencer before the next one is read
                    let handle = eager_spawn(node.invoke(origin, command));
                    if let Some(token) = cmd.reply_to {
                        let this = this.clone();
                        tokio::spawn(async move {
                            let resp = match handle.await {
                                Ok(Ok(v)) => Response::Successful(v.map(Box::new)),
                                Ok(Err(e)) => exception_response(&e),
                                Err(e) => exception_response(&super::cluster_cache_status::TopologyError(e.to_string())),
                            };
                            let _ = this.inner.send_reply(token, encode_response(&resp)).await;
                        });
                    }
                }
            })
        };
        // `JGroupsTransport.viewAccepted` (`:769-830`): `MergeView` -> `merge = true`. The listener is
        // registered first, then the channel's current view is reconciled, so no view is lost between
        // `connect` and `attach`; both go through `apply_view`, which ignores a view id seen already.
        self.inner.add_membership_listener(Arc::new(ViewListener { transport: self.clone(), node: node.clone() }));
        let reconcile = {
            let this = self.clone();
            let node = node.clone();
            tokio::spawn(async move {
                if let Some(v) = this.inner.view().await {
                    this.apply_view(&node, &v, false, true);
                }
            })
        };
        let mut t = self.tasks.lock().unwrap();
        t.push(pump);
        t.push(reconcile);
    }

    /// Where cache-level commands received from the cluster are handled.
    pub fn set_cache_sink(&self, sink: Arc<dyn CacheRpcSink>) {
        let _ = self.cache_sink.set(sink);
    }

    /// A correlated cache-level request (`RpcManager.invokeCommand`): the answer is a `Response`
    /// as it came off the wire (an `ExceptionResponse` stays a value, it is the engine's to interpret),
    /// `None` for an answer of `null`.
    pub fn request_cache(&self, target: &Address, bytes: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Result<Option<Response>, RemoteError>> {
        let inner = self.inner.clone();
        let target = to_jgroups(target);
        Box::pin(async move {
            match tokio::time::timeout(timeout, inner.send_request(target, bytes, DeliverOrder::None)).await {
                Err(_) => Err(RemoteError::Timeout),
                Ok(Err(e)) => Err(RemoteError::Other(e.to_string())),
                Ok(Ok(ResponseBody::CacheNotFound)) => Ok(Some(Response::CacheNotFound)),
                Ok(Ok(ResponseBody::SuccessfulEmpty)) => Ok(None),
                Ok(Ok(ResponseBody::Bytes(b))) => ObjectInput::new(&b).read_response().map(Some).map_err(|e| RemoteError::Other(e.to_string())),
            }
        })
    }

    /// Install `v` as the current view and tell the node. `first` is the reconciliation of the view
    /// the transport joined with, which only counts when it differs from what [`TopologyNode::start`]
    /// already handled.
    fn apply_view(&self, node: &Arc<TopologyNode>, v: &jgroups::wire::view::View, merge: bool, first: bool) {
        let id = v.view_id.id as i32;
        let members: Vec<Address> = v.members.iter().cloned().map(Address::JGroups).collect();
        let changed = {
            let mut cur = self.view.lock().unwrap();
            if !first && cur.0 == id && cur.1 == members {
                return;
            }
            let changed = cur.0 != id || cur.1 != members;
            *cur = (id, members);
            changed
        };
        self.view_tx.send_replace(id);
        if changed || !first {
            node.on_view_change(merge, id);
        }
    }

    /// Leave the cluster and stop the background tasks.
    pub async fn disconnect(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        self.inner.disconnect().await;
    }

    fn request(&self, target: &Address, bytes: Vec<u8>, timeout: Duration) -> BoxFuture<'static, Result<Response, RemoteError>> {
        let inner = self.inner.clone();
        let target = to_jgroups(target);
        Box::pin(async move {
            match tokio::time::timeout(timeout, inner.send_request(target, bytes, DeliverOrder::None)).await {
                Err(_) => Err(RemoteError::Timeout),
                Ok(Err(e)) => Err(RemoteError::Other(e.to_string())),
                Ok(Ok(body)) => decode_body(body),
            }
        })
    }
}

impl TopologyTransport for JGroupsTopologyTransport {
    fn address(&self) -> Address {
        self.local.clone()
    }

    fn view_id(&self) -> i32 {
        self.view.lock().unwrap().0
    }

    fn members(&self) -> Vec<Address> {
        self.view.lock().unwrap().1.clone()
    }

    fn with_view(&self, view_id: i32) -> BoxFuture<'static, ()> {
        let mut rx = self.view_tx.subscribe();
        Box::pin(async move {
            let _ = rx.wait_for(|v| *v >= view_id).await;
        })
    }

    fn invoke_command(&self, target: &Address, command: TopologyCommand, timeout: Duration) -> BoxFuture<'static, Result<Response, RemoteError>> {
        self.request(target, command.to_bytes(), timeout)
    }

    fn invoke_command_on_all(
        &self,
        targets: Option<Vec<Address>>,
        command: TopologyCommand,
        timeout: Duration,
    ) -> BoxFuture<'static, Vec<(Address, Result<Response, RemoteError>)>> {
        let members = self.members();
        let local = self.local.clone();
        let targets: Vec<Address> = match targets {
            Some(t) => t.into_iter().filter(|a| *a != local).collect(),
            None => members.iter().filter(|a| **a != local).cloned().collect(),
        };
        let bytes = command.to_bytes();
        let calls: Vec<(Address, BoxFuture<'static, Result<Response, RemoteError>>)> = targets
            .into_iter()
            .map(|t| {
                let fut: BoxFuture<'static, Result<Response, RemoteError>> = if members.contains(&t) {
                    self.request(&t, bytes.clone(), timeout)
                } else {
                    let t2 = t.clone();
                    Box::pin(async move { Err(RemoteError::Suspect(format!("{t2:?} is not in the view"))) })
                };
                (t, fut)
            })
            .collect();
        Box::pin(async move {
            let handles: Vec<_> = calls.into_iter().map(|(t, f)| (t, tokio::spawn(f))).collect();
            let mut out = Vec::new();
            for (t, h) in handles {
                out.push((t, h.await.unwrap_or(Err(RemoteError::Other("task failed".into())))));
            }
            out
        })
    }

    fn send_to_all(&self, command: TopologyCommand) {
        let _ = self.outbound.send((None, command.to_bytes()));
    }

    fn send_to(&self, target: &Address, command: TopologyCommand) {
        let _ = self.outbound.send((Some(to_jgroups(target)), command.to_bytes()));
    }
}
