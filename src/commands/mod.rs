//! The cache RPC commands of the REPL_SYNC non-transactional write path: `SingleRpcCommand`
//! carrying `PutKeyValueCommand`/`ComputeCommand`/`ReplaceCommand`/`RemoveCommand`/`ClearCommand`, the
//! `ClusteredGetCommand` read, and the pieces they are made of (metadata, `CommandInvocationId`,
//! `ValueMatcher`, the `BiFunctionMapper` that wraps a `compute` lambda). Every byte layout is checked
//! against `tests/vectors/write_commands.hex`, produced by the real Infinispan 14.0.35.Final marshaller.

pub mod compute_function;
pub mod metadata;
pub mod object;
pub mod rpc;
pub mod write;
