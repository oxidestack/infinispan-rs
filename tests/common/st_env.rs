#![allow(dead_code)]
//! A recording [`StateTransferEnv`], the stand-in for the mocks (`RpcManager`, `CommandsFactory`,
//! `InternalDataContainer`, the interceptor chain) `StateConsumerTest` and `StateProviderTest` build.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use infinispan::cache::write_pipeline::RpcResult;
use infinispan::ch::address::Address;
use infinispan::commands::rpc::{CacheRpcCommand, CacheRpcPayload};
use infinispan::statetransfer::chunk::InternalCacheEntry;
use infinispan::statetransfer::env::StateTransferEnv;
use infinispan::topology::messages::Response;
use infinispan::topology::support::BoxFuture;
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq)]
pub enum Answer {
    /// `SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE`
    Successful,
    /// `new CompletableFuture<>()`: never completes
    Never,
}

pub struct MockEnv {
    pub me: Address,
    pub answer: Mutex<Answer>,
    /// (target, command) of every `invoke`
    pub invoked: Mutex<Vec<(Address, CacheRpcCommand)>>,
    pub sent: Mutex<Vec<(Address, CacheRpcCommand)>>,
    pub entries: Mutex<HashMap<i32, Vec<InternalCacheEntry>>>,
    /// entries applied, by segment
    pub applied: Mutex<Vec<(i32, usize)>>,
    /// when set, `apply_entries` waits for it (the blocked interceptor chain of ISPN-14982)
    pub apply_gate: Mutex<Option<Arc<Notify>>>,
    pub gate_open: Mutex<bool>,
}

impl MockEnv {
    pub fn new(me: Address, answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            me,
            answer: Mutex::new(answer),
            invoked: Mutex::new(Vec::new()),
            sent: Mutex::new(Vec::new()),
            entries: Mutex::new(HashMap::new()),
            applied: Mutex::new(Vec::new()),
            apply_gate: Mutex::new(None),
            gate_open: Mutex::new(true),
        })
    }

    /// `(source, segments)` of every `StateTransferStartCommand` sent, in order.
    pub fn start_requests(&self) -> Vec<(Address, Vec<i32>)> {
        self.invoked
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(a, c)| match &c.payload {
                CacheRpcPayload::StateStart(s) => Some((a.clone(), s.segments.iter().collect())),
                _ => None,
            })
            .collect()
    }

    pub fn cancels(&self) -> Vec<(Address, Vec<i32>)> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(a, c)| match &c.payload {
                CacheRpcPayload::StateCancel(s) => Some((a.clone(), s.segments.iter().collect())),
                _ => None,
            })
            .collect()
    }

    pub fn block_apply(&self) -> Arc<Notify> {
        let n = Arc::new(Notify::new());
        *self.apply_gate.lock().unwrap() = Some(n.clone());
        *self.gate_open.lock().unwrap() = false;
        n
    }

    pub fn open_apply(&self) {
        *self.gate_open.lock().unwrap() = true;
        if let Some(n) = self.apply_gate.lock().unwrap().as_ref() {
            n.notify_waiters();
        }
    }
}

impl StateTransferEnv for MockEnv {
    fn address(&self) -> Address {
        self.me.clone()
    }

    fn invoke(&self, target: &Address, command: CacheRpcCommand, _timeout: Duration) -> BoxFuture<'static, RpcResult> {
        self.invoked.lock().unwrap().push((target.clone(), command));
        let answer = *self.answer.lock().unwrap();
        Box::pin(async move {
            match answer {
                Answer::Successful => Ok(Some(Response::Successful(None))),
                Answer::Never => std::future::pending().await,
            }
        })
    }

    fn send(&self, target: &Address, command: CacheRpcCommand) {
        self.sent.lock().unwrap().push((target.clone(), command));
    }

    fn apply_entries(&self, segment: i32, entries: Vec<InternalCacheEntry>) -> BoxFuture<'static, ()> {
        self.applied.lock().unwrap().push((segment, entries.len()));
        let gate = self.apply_gate.lock().unwrap().clone();
        let open = !*self.gate_open.lock().unwrap();
        Box::pin(async move {
            if let (Some(g), true) = (gate, open) {
                g.notified().await;
            }
        })
    }

    fn segment_entries(&self, segment: i32) -> Vec<InternalCacheEntry> {
        self.entries.lock().unwrap().get(&segment).cloned().unwrap_or_default()
    }

    fn remove_segments_not(&self, _keep: &dyn Fn(i32) -> bool) {}
}
