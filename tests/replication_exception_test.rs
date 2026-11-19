//! Port of `replication/ReplicationExceptionTest` (REPL_SYNC, two nodes), the cases a non-transactional
//! byte-marshalled cache can have:
//! - `testSyncReplTimeout`: a peer slower than `remoteTimeout` makes the write fail with a
//!   `TimeoutException` (a `CacheException`);
//! - `testLockAcquisitionTimeout`: a key locked on the primary makes a write from the other node fail
//!   with `TimeoutException: ISPN000299`.
//!
//! `testNonMarshallableRepl`/`testNonMarshallableAsyncRepl` (`MarshallingException` for a value that is not
//! serialisable) and `testNonSerializableReplWithTx` (a tx rollback) do not apply: a value here is already
//! the bytes `JavaSerializationMarshaller` produced, and there are no transactions or `REPL_ASYNC`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::repl::*;
use infinispan::cache::testing::{Intercepted, ReplCluster, RpcInterceptor};
use infinispan::cache::write_pipeline::{CacheConfig, CacheError};
use infinispan::ch::address::Address;
use infinispan::commands::rpc::CacheRpcCommand;
use infinispan::topology::support::BoxFuture;

const CACHE: &str = "syncReplCache";

/// `DelayInterceptor` on `cache(1)`: every request it receives takes 100 ms.
struct DelayTo(Address);

impl RpcInterceptor for DelayTo {
    fn intercept(&self, _from: &Address, to: &Address, _command: &CacheRpcCommand) -> BoxFuture<'static, Intercepted> {
        let slow = *to == self.0;
        Box::pin(async move {
            if slow {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Intercepted::Deliver
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sync_repl_timeout() {
    let c = ReplCluster::start_with(2, &[CACHE], |mut cfg: CacheConfig| {
        cfg.remote_timeout = Duration::from_millis(10);
        cfg
    })
    .await;
    c.set_interceptor(Some(Arc::new(DelayTo(c.address(1)))));
    // whichever node is the primary, one hop reaches cache(1) and exceeds the 10 ms timeout
    let r = c.cache(0, CACHE).put(&s("k"), &vw("v", 1)).await;
    assert!(matches!(r, Err(CacheError::Timeout(_))), "{r:?}");
    c.set_interceptor(None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_acquisition_timeout() {
    let c = ReplCluster::start_with(2, &[CACHE], |mut cfg: CacheConfig| {
        cfg.lock_acquisition_timeout = Duration::from_millis(10);
        cfg
    })
    .await;
    // get a lock on cache 2 and hold on to it
    let block = key_with_primary(&c, CACHE, 1);
    let _held = c.cache(1, CACHE).engine().lock_key(&block, Duration::from_secs(1)).await.unwrap();
    let r = c.cache(0, CACHE).put(&block, &vw("v", 1)).await;
    match r {
        Err(CacheError::Timeout(m)) => assert!(m.starts_with("ISPN000299: Unable to acquire lock after 10 milliseconds for key"), "{m}"),
        other => panic!("{other:?}"),
    }
}
