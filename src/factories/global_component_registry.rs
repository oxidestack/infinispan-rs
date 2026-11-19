//! `GlobalComponentRegistry`'s startup slice a clustered node needs
//! (`factories/GlobalComponentRegistry.java:125-171`): eagerly resolving `Transport`
//! (`:153`, which is `JGroupsTransport.start()` — connects the channel, `running = true` at its
//! last line, `:482`) before `GlobalConfigurationManager` (`:156`, which registers
//! `org.infinispan.CONFIG`, `GlobalConfigurationManagerImpl.java:101-106`) — that order matters:
//! by the time `org.infinispan.CONFIG` is registered, the transport is already marked started.
//! Everything else `GlobalComponentRegistry`'s constructor eagerly resolves (`EventLogManager`,
//! `ClusterContainerStats`, JMX/metrics registration, `LocalTopologyManager`/
//! `ClusterTopologyManager`, `:152-171`) needs components this crate doesn't have yet — out of
//! scope, not modeled.

use crate::factories::global_configuration_manager;
use crate::factories::internal_cache_registry::InternalCacheRegistry;
use crate::remoting::transport::{Transport, TransportError};
use jgroups::wire::view::View;

/// What upstream reads off `GlobalConfiguration.transport()`/`.isClustered()`
/// (`ConfigurationProperties.java:4550-4567`) to build the channel and decide
/// the internal-cache attributes.
pub struct StartConfig<'a> {
    /// The JGroups stack XML (`mockserver.clusterTransportConfig`, defaulting to
    /// `jgroups-loopback.xml`'s content — this crate takes the document
    /// directly rather than a file lookup, since `mockserver-core`'s classpath/filesystem lookup
    /// is the caller's job, not this crate's).
    pub stack_xml: &'a str,
    /// `mockserver.clusterName`, default `mockserver-cluster` — the caller
    /// supplies the already-defaulted value.
    pub cluster_name: &'a str,
    /// `mockserver.clusterEnabled` (`InfinispanStateBackend.java:110-113`): LOCAL vs CLUSTERED
    /// cache manager, gating `InternalCacheRegistry`'s REPL_SYNC attribute (`:64`). The tests only exercise `true` (a Rust node always ends up in a cluster, `org.infinispan.CONFIG`
    /// always needs join + state transfer) — kept as a parameter rather than hardcoded so a LOCAL
    /// manager is representable too, matching upstream's own branch.
    pub clustered: bool,
}

/// `GlobalComponentRegistry`, narrowed to its two eagerly-resolved components:
/// `Transport` and the `InternalCacheRegistry`/`org.infinispan.CONFIG` registration
/// `GlobalConfigurationManager` performs. `LocalTopologyManager`/`ClusterTopologyManager`
/// (`cacheComponents`, `:169-172`) live in `topology` and `ch` (topology, consistent hash, rebalance).
pub struct GlobalComponentRegistry {
    /// The node's transport, connected.
    pub transport: Transport,
    /// The registry of internal caches.
    pub internal_cache_registry: InternalCacheRegistry,
}

impl GlobalComponentRegistry {
    /// The constructor's eager-resolution order (`:125-166`), narrowed: `Transport` first
    /// (connect, then `running = true`), `GlobalConfigurationManager` (register
    /// `org.infinispan.CONFIG`) after.
    pub async fn start(config: StartConfig<'_>) -> Result<(GlobalComponentRegistry, View), TransportError> {
        let transport = Transport::new(config.stack_xml).await?;
        let view = transport.connect(config.cluster_name).await?;
        transport.mark_started();

        let internal_cache_registry = InternalCacheRegistry::new(config.clustered);
        global_configuration_manager::register_config_cache(&internal_cache_registry).map_err(TransportError)?;

        Ok((GlobalComponentRegistry { transport, internal_cache_registry }, view))
    }

    /// Disconnects the transport.
    pub async fn stop(&self) {
        self.transport.disconnect().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stack_xml(bind_port: u16, initial_hosts: &str) -> String {
        format!(
            r#"<config>
                <TCP bind_addr="127.0.0.1" bind_port="{bind_port}" port_range="0"/>
                <TCPPING initial_hosts="{initial_hosts}" port_range="0"/>
            </config>"#
        )
    }

    // A single node: becomes its own singleton coordinator, org.infinispan.CONFIG is registered
    // with cluster attributes (clustered: true), and the transport is already running by the time
    // it returns — matching upstream's GlobalComponentRegistry eager-resolution order.
    #[tokio::test]
    async fn starting_connects_the_channel_and_registers_org_infinispan_config() {
        let xml = stack_xml(0, "");
        let (registry, view) = GlobalComponentRegistry::start(StartConfig { stack_xml: &xml, cluster_name: "GcrTest", clustered: true })
            .await
            .unwrap();

        assert_eq!(view.size(), 1);
        assert!(registry.transport.is_running());
        let attrs = registry.internal_cache_registry.attributes("org.infinispan.CONFIG").unwrap();
        assert!(attrs.repl_sync && attrs.fetch_in_memory_state && attrs.await_initial_transfer);

        registry.stop().await;
    }

    #[tokio::test]
    async fn a_local_non_clustered_manager_registers_config_without_cluster_attributes() {
        let xml = stack_xml(0, "");
        let (registry, _view) = GlobalComponentRegistry::start(StartConfig { stack_xml: &xml, cluster_name: "GcrLocalTest", clustered: false })
            .await
            .unwrap();
        let attrs = registry.internal_cache_registry.attributes("org.infinispan.CONFIG").unwrap();
        assert!(!attrs.repl_sync && !attrs.fetch_in_memory_state && !attrs.await_initial_transfer);
        registry.stop().await;
    }
}
