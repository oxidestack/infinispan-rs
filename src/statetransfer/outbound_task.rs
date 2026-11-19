//! `OutboundTransferTask` (`statetransfer/OutboundTransferTask.java`): pushes segments to one
//! destination as `StateResponseCommand`s.
//!
//! The entries come as a stream of notifications, per segment its values then a "segment complete"
//! marker. They are buffered by `chunkSize` *notifications* (markers count), each batch becomes a map
//! `segment -> StateChunk` (a `HashMap<Integer, StateChunk>`, whose iteration order decides the chunk
//! order on the wire), and a segment whose marker is in the batch gets `isLastChunk = true`: an empty
//! segment is one `Collections.emptyList()` chunk. Batches are sent one at a time and a failed send
//! cancels the task.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use super::chunk::{InternalCacheEntry, StateChunk};
use super::env::StateTransferEnv;
use super::int_set::IntSet;
use crate::ch::address::Address;
use crate::ch::java_util::{HashMapInit, hash_map_iteration_order};
use crate::commands::rpc::{CacheRpcCommand, CacheRpcPayload, StateResponse};
use crate::topology::messages::Response;
use crate::topology::transport::RemoteError;

enum Notification {
    Value(i32, InternalCacheEntry),
    SegmentComplete(i32),
}

/// `OutboundTransferTask`: sends the entries of some segments to one node, in chunks.
pub struct OutboundTransferTask {
    destination: Address,
    segments: Mutex<IntSet>,
    chunk_size: usize,
    topology_id: i32,
    timeout: Duration,
    cache_name: String,
    apply_state: bool,
    cancelled: AtomicBool,
}

impl OutboundTransferTask {
    /// A task sending `segments` to `destination`; an error when `segments` is empty.
    pub fn new(destination: Address, segments: IntSet, chunk_size: usize, topology_id: i32, timeout: Duration, cache_name: &str, apply_state: bool) -> Result<Self, String> {
        if segments.is_empty() {
            return Err("Segments must not be null or empty".into());
        }
        if chunk_size == 0 {
            return Err("chunkSize must be greater than 0".into());
        }
        Ok(Self {
            destination,
            segments: Mutex::new(segments),
            chunk_size,
            topology_id,
            timeout,
            cache_name: cache_name.to_string(),
            apply_state,
            cancelled: false.into(),
        })
    }

    /// The node the segments go to.
    pub fn destination(&self) -> &Address {
        &self.destination
    }

    /// The segments still to send.
    pub fn segments(&self) -> IntSet {
        self.segments.lock().unwrap().clone()
    }

    /// The topology the transfer was requested in.
    pub fn topology_id(&self) -> i32 {
        self.topology_id
    }

    /// Whether the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Cancels the task.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// `cancelSegments`: the whole task is cancelled when no segment is left.
    pub fn cancel_segments(&self, cancelled: &IntSet) {
        let mut s = self.segments.lock().unwrap();
        let before = s.len();
        s.remove_all(cancelled);
        if s.len() != before && s.is_empty() {
            drop(s);
            self.cancel();
        }
    }

    /// `execute`: read the entries of the segments (in segment order, as the local publisher emits them)
    /// and send them.
    pub async fn execute(&self, env: &dyn StateTransferEnv) {
        let mut notifications = Vec::new();
        let mut batch_start = 0usize;
        let segments: Vec<i32> = self.segments().iter().collect();
        // the publisher is lazy, but a batch only needs the entries of its own segments: read them as we go
        let mut seg_iter = segments.into_iter();
        let mut pending: std::collections::VecDeque<Notification> = std::collections::VecDeque::new();
        loop {
            while pending.len() < self.chunk_size {
                match seg_iter.next() {
                    Some(s) => {
                        for e in env.segment_entries(s) {
                            pending.push_back(Notification::Value(s, e));
                        }
                        pending.push_back(Notification::SegmentComplete(s));
                    }
                    None => break,
                }
            }
            if pending.is_empty() {
                break;
            }
            let take = pending.len().min(self.chunk_size);
            notifications.clear();
            notifications.extend(pending.drain(..take));
            batch_start += take;
            let chunks = Self::chunks_of(&notifications);
            self.send_chunks(env, chunks).await;
            // takeUntil(batch -> cancelled): the batch that saw the cancellation is the last one
            if self.is_cancelled() {
                break;
            }
        }
        let _ = batch_start;
    }

    /// One batch of notifications as the chunks of `OutboundTransferTask.execute:128-143`, in the
    /// iteration order of the `HashMap<Integer, StateChunk>` Java builds them in.
    fn chunks_of(batch: &[Notification]) -> Vec<StateChunk> {
        let mut chunks: Vec<StateChunk> = Vec::new();
        for n in batch {
            match n {
                Notification::Value(seg, e) => match chunks.iter_mut().find(|c| c.segment == *seg) {
                    Some(c) => c.entries.push(e.clone()),
                    None => chunks.push(StateChunk { segment: *seg, entries: vec![e.clone()], is_last: false }),
                },
                Notification::SegmentComplete(seg) => match chunks.iter_mut().find(|c| c.segment == *seg) {
                    Some(c) => c.is_last = true,
                    None => chunks.push(StateChunk { segment: *seg, entries: Vec::new(), is_last: true }),
                },
            }
        }
        let hashes: Vec<i32> = chunks.iter().map(|c| c.segment).collect();
        let order = hash_map_iteration_order(&hashes, HashMapInit::Default);
        order.into_iter().map(|i| chunks[i].clone()).collect()
    }

    async fn send_chunks(&self, env: &dyn StateTransferEnv, chunks: Vec<StateChunk>) {
        if chunks.is_empty() {
            return;
        }
        let cmd = CacheRpcCommand {
            cache_name: self.cache_name.clone(),
            payload: CacheRpcPayload::StateResponse(StateResponse { topology_id: self.topology_id, chunks, apply_state: self.apply_state, push_transfer: false }),
        };
        match env.invoke(&self.destination, cmd, self.timeout).await {
            Ok(Some(Response::CacheNotFound)) | Err(RemoteError::Suspect(_)) => {
                log::debug!("Node {:?} left cache {} while we were sending state to it, cancelling transfer.", self.destination, self.cache_name);
                self.cancel();
            }
            Ok(Some(Response::Exception { message, .. })) => {
                log::error!("Failed to send entries to node {:?}: {message:?}", self.destination);
                self.cancel();
            }
            Err(e) => {
                if !self.is_cancelled() {
                    log::error!("Failed to send entries to node {:?}: {e:?}", self.destination);
                }
                self.cancel();
            }
            Ok(_) => {}
        }
    }
}
