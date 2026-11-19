//! `org.infinispan.factories`/`org.infinispan.registry`/`org.infinispan.globalstate` — the slice
//! of Infinispan's startup sequence a clustered node needs: connecting a
//! `remoting::transport::Transport` and registering the implicit `org.infinispan.CONFIG` internal
//! cache every clustered node joins, regardless of which of MockServer's own caches are in use.

pub mod global_component_registry;
pub mod global_configuration_manager;
pub mod internal_cache_registry;
