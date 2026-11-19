//! Concurrency helpers the topology managers lean on: `ActionSequencer.orderOnKey`,
//! `ConditionFuture` (`org.infinispan.util.concurrent`), and a way to start a task whose first
//! segment runs on the caller, the way a `CompletionStage` chain that is already complete does.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

/// A boxed future that can be sent between threads.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type Action = BoxFuture<'static, ()>;

#[derive(Default)]
struct KeyQueue {
    state: Mutex<(VecDeque<Action>, bool)>,
}

/// `ActionSequencer.orderOnKey`: actions for one key run one at a time, in the order they were
/// submitted. Submission is synchronous (the action is queued when `order_on_key` is *called*), so
/// the order is the call order even when the returned future is awaited much later or never.
#[derive(Default, Clone)]
pub struct ActionSequencer {
    queues: Arc<Mutex<HashMap<String, Arc<KeyQueue>>>>,
}

impl ActionSequencer {
    /// A sequencer with no queued action.
    pub fn new() -> Self {
        Self::default()
    }

    /// No key has a queued or running action: everything submitted so far has completed.
    pub fn is_idle(&self) -> bool {
        self.queues.lock().unwrap().values().all(|q| {
            let st = q.state.lock().unwrap();
            st.0.is_empty() && !st.1
        })
    }

    /// Queues `action` behind the actions already queued for `key`; the future returned resolves with its result.
    pub fn order_on_key<T: Send + 'static>(
        &self,
        key: &str,
        action: impl Future<Output = T> + Send + 'static,
    ) -> impl Future<Output = T> + Send + 'static {
        let queue = self.queues.lock().unwrap().entry(key.to_string()).or_default().clone();
        let (tx, rx) = oneshot::channel();
        let wrapped: Action = Box::pin(async move {
            let v = action.await;
            let _ = tx.send(v);
        });
        let start_drain = {
            let mut st = queue.state.lock().unwrap();
            st.0.push_back(wrapped);
            let was_running = st.1;
            st.1 = true;
            !was_running
        };
        if start_drain {
            tokio::spawn(async move {
                loop {
                    let next = {
                        let mut st = queue.state.lock().unwrap();
                        match st.0.pop_front() {
                            Some(a) => a,
                            None => {
                                st.1 = false;
                                return;
                            }
                        }
                    };
                    next.await;
                }
            });
        }
        async move { rx.await.expect("sequenced action dropped without completing") }
    }
}

/// The condition did not hold before the timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConditionTimeout;

/// `ConditionFuture`: wait until a predicate holds, re-evaluated on every [`ConditionFuture::update`].
#[derive(Default)]
pub struct ConditionFuture {
    notify: Notify,
}

impl ConditionFuture {
    /// A condition with no waiter.
    pub fn new() -> Self {
        Self::default()
    }

    /// `updateAsync`: re-evaluate every waiter's predicate.
    pub fn update(&self) {
        self.notify.notify_waiters();
    }

    /// `newConditionStage(predicate, timeoutException, timeout)`: `Err(ConditionTimeout)` on timeout.
    pub async fn wait_for(&self, mut predicate: impl FnMut() -> bool, timeout: Duration) -> Result<(), ConditionTimeout> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if predicate() {
                return Ok(());
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(ConditionTimeout);
            }
            // Re-check periodically as a safety net against a missed update.
            let slice = (deadline - now).min(Duration::from_millis(100));
            let _ = tokio::time::timeout(slice, notified).await;
        }
    }
}

/// Run `fut` until its first suspension on the caller's thread, then continue it on the runtime.
/// A `CompletionStage` that is already complete runs its continuations synchronously; this keeps
/// the same "enqueued before the call returns" behaviour for work that is ready to proceed.
pub fn eager_spawn<T: Send + 'static>(fut: impl Future<Output = T> + Send + 'static) -> JoinHandle<T> {
    let mut fut: BoxFuture<'static, T> = Box::pin(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => tokio::spawn(async move { v }),
        Poll::Pending => tokio::spawn(fut),
    }
}
