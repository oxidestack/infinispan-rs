//! Per-key write locks of `NonTransactionalLockingInterceptor` / `AbstractLockingInterceptor`
//! (`interceptors/locking/AbstractLockingInterceptor.java:130-139`): only the primary owner locks, for
//! the whole command including the broadcast to the backups, with `LockingConfiguration`'s 10 s default
//! acquisition timeout (`configuration/cache/LockingConfiguration.java:21`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// `LockingConfiguration.lockAcquisitionTimeout`'s default.
pub const DEFAULT_LOCK_ACQUISITION_TIMEOUT: Duration = Duration::from_secs(10);

/// `TimeoutException` ISPN000299.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockTimeout {
    /// The exception's message.
    pub message: String,
}

/// One lock per key, created on first use (`LockManager`).
#[derive(Default)]
pub struct KeyLocks {
    locks: Mutex<HashMap<Vec<u8>, Arc<AsyncMutex<()>>>>,
}

/// Held until dropped.
pub struct KeyLockGuard {
    _guard: OwnedMutexGuard<()>,
}

impl KeyLocks {
    /// Acquire the lock of `key` (its marshalled form), waiting at most `timeout`.
    pub async fn lock(&self, key: &[u8], display_key: &str, requestor: &str, timeout: Duration) -> Result<KeyLockGuard, LockTimeout> {
        let m = self.locks.lock().unwrap().entry(key.to_vec()).or_default().clone();
        let acquired = if timeout.is_zero() {
            m.clone().try_lock_owned().ok()
        } else {
            tokio::time::timeout(timeout, m.clone().lock_owned()).await.ok()
        };
        match acquired {
            Some(g) => {
                self.sweep();
                Ok(KeyLockGuard { _guard: g })
            }
            None => Err(LockTimeout {
                message: format!(
                    "ISPN000299: Unable to acquire lock after {} for key {display_key} and requestor {requestor}. Lock is held by another command",
                    format_timeout(timeout)
                ),
            }),
        }
    }

    /// `LockManager.isLocked`.
    pub fn is_locked(&self, key: &[u8]) -> bool {
        match self.locks.lock().unwrap().get(key) {
            Some(m) => m.try_lock().is_err(),
            None => false,
        }
    }

    /// Drop the map entries nobody holds or waits for.
    fn sweep(&self) {
        let mut map = self.locks.lock().unwrap();
        if map.len() > 256 {
            map.retain(|_, m| Arc::strong_count(m) > 1);
        }
    }
}

/// `Util.prettyPrintTime`, for the whole seconds and milliseconds this port uses.
fn format_timeout(d: Duration) -> String {
    if d.as_millis().is_multiple_of(1000) {
        format!("{} seconds", d.as_secs())
    } else {
        format!("{} milliseconds", d.as_millis())
    }
}
