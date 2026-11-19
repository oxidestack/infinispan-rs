//! What the state consumer and provider need from the node they run on: the part of `RpcManager`,
//! `InternalDataContainer` and the interceptor chain (the put with `PUT_FOR_STATE_TRANSFER`) they use.
//! [`crate::cache::write_pipeline::CacheEngine`] implements it; the ports of `StateConsumerTest` and
//! `StateProviderTest` implement it with recording mocks, as the upstream tests mock these components.

use std::time::Duration;

use super::chunk::InternalCacheEntry;
use crate::cache::write_pipeline::RpcResult;
use crate::ch::address::Address;
use crate::commands::rpc::CacheRpcCommand;
use crate::topology::support::BoxFuture;

/// What state transfer needs from its cache: the local address, remote calls and the data container.
pub trait StateTransferEnv: Send + Sync {
    /// The local node's address.
    fn address(&self) -> Address;
    /// `rpcManager.invokeCommand(target, command, PassthroughSingleResponseCollector, rpcOptions)`.
    fn invoke(&self, target: &Address, command: CacheRpcCommand, timeout: Duration) -> BoxFuture<'static, RpcResult>;
    /// `rpcManager.sendTo(target, command, DeliverOrder.NONE)`: no answer is awaited.
    fn send(&self, target: &Address, command: CacheRpcCommand);
    /// `doApplyState`: a put of each entry with `STATE_TRANSFER_FLAGS`, all of segment `segment`.
    fn apply_entries(&self, segment: i32, entries: Vec<InternalCacheEntry>) -> BoxFuture<'static, ()>;
    /// The live entries of a segment, as `InternalCacheEntry`s (`LocalPublisherManager.entryPublisher`).
    fn segment_entries(&self, segment: i32) -> Vec<InternalCacheEntry>;
    /// `removeSegments` for every segment `keep` rejects (data of segments this node lost).
    fn remove_segments_not(&self, keep: &dyn Fn(i32) -> bool);
    /// `cache.getStatus().allowInvocations()`.
    fn is_running(&self) -> bool {
        true
    }
}
