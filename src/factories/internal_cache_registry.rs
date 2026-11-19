//! `InternalCacheRegistry`/`InternalCacheRegistryImpl`
//! (`registry/InternalCacheRegistry.java`, `registry/impl/InternalCacheRegistryImpl.java`,
//! 14.0.35.Final): components that need an internal cache register it here rather than defining
//! one directly. Scoped to what its one caller (`global_configuration_manager`) needs:
//! the `GLOBAL` flag's REPL_SYNC + state-transfer attributes
//! (`InternalCacheRegistryImpl.java:64-69`). `PERSISTENT`/`PROTECTED`/`QUERYABLE` are configuration-
//! builder concerns this crate has no configuration builder, persistence, security or query port
//! for yet (the port leaves them out on purpose) — left out of
//! [`Flag`] entirely rather than modeled-and-ignored, so an attempt to use one is a compile error,
//! not a silently-wrong runtime no-op.

use std::collections::HashMap;
use std::sync::Mutex;

/// `InternalCacheRegistry.Flag` (`InternalCacheRegistry.java:21-46`), narrowed per the module doc
/// comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Flag {
    /// "means that the cache must be declared only once" — a second `register_internal_cache` for
    /// the same name is an error rather than a silent no-op.
    Exclusive,
    /// "means that this cache is visible to users".
    User,
    /// "means that this cache will be global to all nodes when running in clustered mode" — the
    /// flag that earns REPL_SYNC + state-transfer attributes on a clustered manager.
    Global,
}

/// The cache-mode attributes a `GLOBAL`-flagged internal cache gets on a clustered manager
/// (`InternalCacheRegistryImpl.java:64-69`, "TODO: choose a merge policy" kept as a citation of
/// upstream's own unresolved comment, not a gap in this port — no merge policy exists either way
/// without transactions/conflict resolution, both out of scope for this port).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheAttributes {
    /// Whether the cache is `REPL_SYNC`.
    pub repl_sync: bool,
    /// Whether a joiner fetches the in-memory state.
    pub fetch_in_memory_state: bool,
    /// Whether joining waits for the initial state transfer.
    pub await_initial_transfer: bool,
}

impl CacheAttributes {
    fn clustered_global() -> CacheAttributes {
        CacheAttributes { repl_sync: true, fetch_in_memory_state: true, await_initial_transfer: true }
    }
}

struct Entry {
    flags: Vec<Flag>,
    attributes: CacheAttributes,
}

/// `InternalCacheRegistryImpl`, narrowed to `registerInternalCache`'s dedupe/`GLOBAL` path
/// (`:44-90`) — `startInternalCaches`/persistence/security are out of scope (no cache manager to
/// actually start a cache on yet; that comes once `org.infinispan.CONFIG` has anywhere
/// to live).
pub struct InternalCacheRegistry {
    /// `GlobalConfiguration.isClustered()` (`:64`), decided once at construction — matches
    /// MockServer's `mockserver.clusterEnabled`, which doesn't change at runtime.
    clustered: bool,
    entries: Mutex<HashMap<String, Entry>>,
}

impl InternalCacheRegistry {
    /// An empty registry for a clustered or local manager.
    pub fn new(clustered: bool) -> InternalCacheRegistry {
        InternalCacheRegistry { clustered, entries: Mutex::new(HashMap::new()) }
    }

    /// `registerInternalCache(String, Configuration, EnumSet<Flag>)` (`:51-90`), minus the
    /// `Configuration`/`ConfigurationBuilder` merge (no configuration builder port yet — this
    /// registry's own `CacheAttributes` stands in for "the configuration", matching its
    /// one real caller, which only ever needs `GLOBAL`'s attributes) and minus persistence
    /// (`PERSISTENT`, out of scope). `configPresent`'s "a *user* config with this name already
    /// exists" check (`:53`) collapses to "this registry already has an entry with this name" —
    /// this crate has no separate user `ConfigurationManager` yet for the two to diverge against.
    pub fn register_internal_cache(&self, name: &str, flags: &[Flag]) -> Result<(), String> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(existing) = entries.get(name) {
            if flags.contains(&Flag::Exclusive) || existing.flags.contains(&Flag::Exclusive) {
                return Err(format!("an existing configuration already exists for internal cache {name}"));
            }
            // "Don't redefine" (`:58-61`).
            return Ok(());
        }
        let attributes =
            if flags.contains(&Flag::Global) && self.clustered { CacheAttributes::clustered_global() } else { CacheAttributes::default() };
        entries.insert(name.to_string(), Entry { flags: flags.to_vec(), attributes });
        Ok(())
    }

    /// The attributes the cache `name` was registered with.
    pub fn attributes(&self, name: &str) -> Option<CacheAttributes> {
        self.entries.lock().unwrap().get(name).map(|e| e.attributes)
    }

    /// Whether the cache `name` is registered.
    pub fn is_registered(&self, name: &str) -> bool {
        self.entries.lock().unwrap().contains_key(name)
    }

    /// `InternalCacheRegistryImpl.getInternalCacheNames()` (not cited directly; the accessor
    /// pairing `internalCaches`, `:41`).
    pub fn internal_cache_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.lock().unwrap().keys().cloned().collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_global_cache_on_a_clustered_manager_gets_repl_sync_and_state_transfer() {
        let reg = InternalCacheRegistry::new(true);
        reg.register_internal_cache("org.infinispan.CONFIG", &[Flag::Global]).unwrap();
        assert_eq!(
            reg.attributes("org.infinispan.CONFIG").unwrap(),
            CacheAttributes { repl_sync: true, fetch_in_memory_state: true, await_initial_transfer: true }
        );
    }

    // `GlobalConfiguration.isClustered()` gate, `:64` — a LOCAL manager never gets REPL_SYNC even
    // for a GLOBAL-flagged cache.
    #[test]
    fn a_global_cache_on_a_local_manager_gets_no_cluster_attributes() {
        let reg = InternalCacheRegistry::new(false);
        reg.register_internal_cache("org.infinispan.CONFIG", &[Flag::Global]).unwrap();
        assert_eq!(reg.attributes("org.infinispan.CONFIG").unwrap(), CacheAttributes::default());
    }

    #[test]
    fn registering_the_same_name_twice_without_exclusive_is_a_silent_no_op() {
        let reg = InternalCacheRegistry::new(true);
        reg.register_internal_cache("expectations", &[Flag::Global]).unwrap();
        reg.register_internal_cache("expectations", &[Flag::Global, Flag::User]).unwrap();
        assert!(reg.is_registered("expectations"));
    }

    #[test]
    fn registering_the_same_name_twice_with_exclusive_errors() {
        let reg = InternalCacheRegistry::new(true);
        reg.register_internal_cache("scenarioStates", &[Flag::Exclusive]).unwrap();
        assert!(reg.register_internal_cache("scenarioStates", &[Flag::Global]).is_err());
    }

    #[test]
    fn internal_cache_names_lists_every_registered_name_sorted() {
        let reg = InternalCacheRegistry::new(true);
        reg.register_internal_cache("blobs", &[]).unwrap();
        reg.register_internal_cache("org.infinispan.CONFIG", &[Flag::Global]).unwrap();
        assert_eq!(reg.internal_cache_names(), vec!["blobs".to_string(), "org.infinispan.CONFIG".to_string()]);
    }
}
