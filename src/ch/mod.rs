//! Consistent hashing: MurmurHash3, the key partitioner, `SyncConsistentHashFactory`
//! and `ReplicatedConsistentHash`.

pub mod hash_function_partitioner;
pub mod murmur_hash3;
pub mod address;
pub mod default_consistent_hash;
pub mod java_util;
pub mod sync_consistent_hash_factory;
pub mod replicated_consistent_hash;
pub mod sync_replicated_consistent_hash_factory;
pub mod replicated_consistent_hash_factory;
