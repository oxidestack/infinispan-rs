//! State transfer of a REPL_SYNC cache: a joiner requests the segments it will own from the
//! current owners, the owners push them as `StateChunk`s, and the joiner applies them with
//! `PUT_FOR_STATE_TRANSFER` semantics (no lock, no replication, no listener notification) before it
//! confirms the rebalance phase to the coordinator.
//!
//! Upstream: `statetransfer/` (`StateConsumerImpl`, `StateProviderImpl`, `InboundTransferTask`,
//! `OutboundTransferTask`, `StateChunk`, `StateTransferLockImpl`, `StateTransferManagerImpl`) and
//! `CommitManager`. Out of scope: transaction transfer (`StateTransferGetTransactionsCommand`),
//! scattered/DIST/invalidation modes, conflict resolution, cluster listeners, cache stores, L1, cross-site
//! transfer. Wire formats are checked against `tests/vectors/state_transfer.hex`.

pub mod chunk;
pub mod commit_manager;
pub mod consumer;
pub mod env;
pub mod inbound_task;
pub mod int_set;
pub mod lock;
pub mod manager;
pub mod outbound_task;
pub mod provider;
