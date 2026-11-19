//! `GlobalConfigurationManagerImpl.start()`'s one action a clustered node needs
//! (`globalstate/impl/GlobalConfigurationManagerImpl.java:101-106`): registering the implicit
//! `org.infinispan.CONFIG` internal cache with `GLOBAL` (`EnumSet.of(Flag.GLOBAL)`). Everything
//! else that method does — picking a `LocalConfigurationManager` strategy, starting
//! user-defined static caches (`:108-115`) — needs a real cache manager/configuration parser this
//! crate doesn't have yet (not ported yet); `org.infinispan.CONFIG` itself is needed now because
//! **every** clustered node joins it regardless of what MockServer's own caches are (the join test), so its registration can't wait for those later pieces.

use crate::factories::internal_cache_registry::{Flag, InternalCacheRegistry};

/// `GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME` (`globalstate/
/// GlobalConfigurationManager.java:22`): `"org.infinispan.CONFIG"`.
pub const CONFIG_STATE_CACHE_NAME: &str = "org.infinispan.CONFIG";

/// `GlobalConfigurationManagerImpl.start()`, lines `:101-104`:
/// ```java
/// internalCacheRegistry.registerInternalCache(
///       CONFIG_STATE_CACHE_NAME,
///       new ConfigurationBuilder().build(),
///       EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
/// ```
pub fn register_config_cache(registry: &InternalCacheRegistry) -> Result<(), String> {
    registry.register_internal_cache(CONFIG_STATE_CACHE_NAME, &[Flag::Global])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starting_registers_org_infinispan_config_with_cluster_attributes_on_a_clustered_manager() {
        let registry = InternalCacheRegistry::new(true);
        register_config_cache(&registry).unwrap();
        let attrs = registry.attributes(CONFIG_STATE_CACHE_NAME).unwrap();
        assert!(attrs.repl_sync && attrs.fetch_in_memory_state && attrs.await_initial_transfer);
    }

    #[test]
    fn starting_twice_is_idempotent() {
        let registry = InternalCacheRegistry::new(true);
        register_config_cache(&registry).unwrap();
        register_config_cache(&registry).unwrap();
        assert!(registry.is_registered(CONFIG_STATE_CACHE_NAME));
    }
}
