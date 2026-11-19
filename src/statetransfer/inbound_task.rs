//! `InboundTransferTask` (`statetransfer/InboundTransferTask.java`): fetches a set of segments from one
//! source. It sends `StateTransferStartCommand`, then waits until the last chunk of every segment was
//! applied (`onStateReceived`), the task was cancelled, or the request failed.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::watch;

use super::env::StateTransferEnv;
use super::int_set::IntSet;
use crate::cache::write_pipeline::RpcResult;
use crate::ch::address::Address;
use crate::commands::rpc::{CacheRpcCommand, CacheRpcPayload, StateTransferSegments};
use crate::topology::messages::Response;
use crate::topology::transport::RemoteError;

/// Why an inbound transfer did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferFailure {
    /// `CancellationException("Inbound transfer was cancelled")`
    Cancelled,
    /// `SuspectException` (the source does not run the cache)
    Suspect,
    /// Any other failure, with its message.
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Completion {
    Pending,
    Done(Result<(), TransferFailure>),
}

struct Segments {
    segments: IntSet,
    unfinished: IntSet,
}

/// `InboundTransferTask`: requests segments from one source and tracks which have arrived.
pub struct InboundTransferTask {
    segments: Mutex<Segments>,
    source: Address,
    topology_id: i32,
    timeout: Duration,
    cache_name: String,
    cancelled: std::sync::atomic::AtomicBool,
    completion: watch::Sender<Completion>,
}

impl InboundTransferTask {
    /// A task requesting `segments` from `source`; an error when `segments` is empty.
    pub fn new(segments: &IntSet, source: Address, topology_id: i32, timeout: Duration, cache_name: &str) -> Result<Self, String> {
        if segments.is_empty() {
            return Err("segments must not be null or empty".into());
        }
        Ok(Self {
            segments: Mutex::new(Segments { segments: segments.clone(), unfinished: segments.clone() }),
            source,
            topology_id,
            timeout,
            cache_name: cache_name.to_string(),
            cancelled: false.into(),
            completion: watch::channel(Completion::Pending).0,
        })
    }

    /// The segments requested.
    pub fn segments(&self) -> IntSet {
        self.segments.lock().unwrap().segments.clone()
    }

    /// The segments not received yet.
    pub fn unfinished_segments(&self) -> IntSet {
        self.segments.lock().unwrap().unfinished.clone()
    }

    /// The node the segments come from.
    pub fn source(&self) -> &Address {
        &self.source
    }

    /// Whether the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// `requestSegments`: send `START_STATE_TRANSFER` to the source and wait until the transfer is done.
    pub async fn request_segments(&self, env: &dyn StateTransferEnv) -> Result<(), TransferFailure> {
        if self.is_cancelled() {
            return self.wait_completion().await;
        }
        let segments = self.segments();
        if segments.is_empty() {
            self.complete(Ok(()));
            return self.wait_completion().await;
        }
        let cmd = CacheRpcCommand {
            cache_name: self.cache_name.clone(),
            payload: CacheRpcPayload::StateStart(StateTransferSegments { topology_id: self.topology_id, segments }),
        };
        let answer: RpcResult = env.invoke(&self.source, cmd, self.timeout).await;
        match answer {
            Err(RemoteError::Suspect(_)) => self.complete(Err(TransferFailure::Suspect)),
            Err(e) => {
                if !self.is_cancelled() {
                    self.complete(Err(TransferFailure::Failed(format!("{e:?}"))));
                }
            }
            Ok(Some(Response::Successful(_))) | Ok(None) => {}
            Ok(Some(Response::CacheNotFound)) => self.complete(Err(TransferFailure::Suspect)),
            Ok(Some(other)) => self.complete(Err(TransferFailure::Failed(format!("{other:?}")))),
        }
        self.wait_completion().await
    }

    async fn wait_completion(&self) -> Result<(), TransferFailure> {
        let mut rx = self.completion.subscribe();
        let v = rx.wait_for(|c| matches!(c, Completion::Done(_))).await.expect("sender lives as long as self").clone();
        match v {
            Completion::Done(r) => r,
            Completion::Pending => unreachable!(),
        }
    }

    fn complete(&self, r: Result<(), TransferFailure>) {
        self.completion.send_if_modified(|c| {
            if *c == Completion::Pending {
                *c = Completion::Done(r);
                true
            } else {
                false
            }
        });
    }

    /// `cancelSegments`: the whole task is cancelled when no segment is left.
    pub fn cancel_segments(&self, cancelled: &IntSet, env: &dyn StateTransferEnv) -> Result<(), String> {
        if self.is_cancelled() {
            return Err("The task is already cancelled.".into());
        }
        {
            let mut s = self.segments.lock().unwrap();
            if !s.segments.contains_all(cancelled) {
                return Err("Some of the specified segments cannot be cancelled because they were not previously requested".into());
            }
            s.unfinished.remove_all(cancelled);
            if s.unfinished.is_empty() {
                self.cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.send_cancel(cancelled.clone(), env);
        if self.is_cancelled() {
            self.complete(Err(TransferFailure::Cancelled));
        }
        Ok(())
    }

    /// `cancel`.
    pub fn cancel(&self, env: &dyn StateTransferEnv) {
        if !self.cancelled.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let unfinished = {
                let mut s = self.segments.lock().unwrap();
                std::mem::take(&mut s.unfinished)
            };
            self.send_cancel(unfinished, env);
            self.complete(Err(TransferFailure::Cancelled));
        }
    }

    fn send_cancel(&self, segments: IntSet, env: &dyn StateTransferEnv) {
        env.send(
            &self.source,
            CacheRpcCommand { cache_name: self.cache_name.clone(), payload: CacheRpcPayload::StateCancel(StateTransferSegments { topology_id: self.topology_id, segments }) },
        );
    }

    /// `onStateReceived`.
    pub fn on_state_received(&self, segment: i32, is_last_chunk: bool) {
        if !self.is_cancelled() && is_last_chunk {
            let mut completed = false;
            {
                let mut s = self.segments.lock().unwrap();
                if s.segments.contains(segment) {
                    s.unfinished.remove(segment);
                    completed = s.unfinished.is_empty();
                }
            }
            if completed {
                self.complete(Ok(()));
            }
        }
    }

    /// Whether every segment arrived.
    pub fn is_completed_successfully(&self) -> bool {
        matches!(&*self.completion.borrow(), Completion::Done(Ok(())))
    }

    /// Whether the task finished, successfully or not.
    pub fn is_done(&self) -> bool {
        matches!(&*self.completion.borrow(), Completion::Done(_))
    }

    /// `terminate`: the source is gone.
    pub fn terminate(&self) {
        self.complete(Err(TransferFailure::Cancelled));
    }
}

impl std::fmt::Debug for InboundTransferTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.segments.lock().unwrap();
        write!(f, "InboundTransferTask{{source={:?}, segments={}, unfinished={}, cancelled={}}}", self.source, s.segments.len(), s.unfinished.len(), self.is_cancelled())
    }
}
