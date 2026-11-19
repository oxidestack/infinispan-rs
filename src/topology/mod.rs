//! Topology: consistent-hash-backed cache topology, rebalance and the topology
//! manager commands. Wire framing for the topology object graph is in [`wire`].

pub mod availability;
pub mod cache_topology;
pub mod cluster_cache_status;
pub mod cluster_topology_manager;
pub mod commands;
pub mod jgroups_transport;
pub mod local_topology_manager;
pub mod messages;
pub mod node;
pub mod rebalance;
pub mod support;
pub mod testing;
pub mod transport;
pub mod wire;
