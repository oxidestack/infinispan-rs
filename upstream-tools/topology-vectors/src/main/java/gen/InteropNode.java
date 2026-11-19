package gen;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.mockserver.state.infinispan.VersionedWrapper;

/**
 * A real Infinispan 14.0.35.Final node (REPL_SYNC cache, SyncReplicatedConsistentHashFactory by default)
 * on a hand-written JGroups TCP/TCPPING stack, for the live Java interop test in
 * tests/topology_java_interop.rs. Protocol on stdout, one line each:
 *   PORT <bind port>              the TCP port the channel listens on
 *   JOINED <cache> <members>      after getCache returned
 *   TOPOLOGY <cache> <id> <phase> <members> <primary owners csv hash>   in answer to "dump <cache>"
 *   VALUE <cache> <key> <value|null> <version>   in answer to "get <cache> <key>"
 *   SIZE <cache> <n>                             in answer to "size <cache>"
 *   PUT <cache> <key> <old value|null>           in answer to "put <cache> <key> <value> <version>"
 * Values are MockServer's VersionedWrapper<String> (the type its caches hold), keys are Strings.
 * Commands on stdin: "dump <cache>", "start <cache>", "put ...", "fill <cache> <n>", "remove/putifabsent/clear ...", "get ...", "size ...", "quit".
 */
public class InteropNode {
   public static void main(String[] args) throws Exception {
      int bindPort = Integer.parseInt(args[0]);
      String initialHosts = args[1];
      String cluster = args[2];
      String[] startCaches = args.length > 3 && !args[3].isEmpty() ? args[3].split(",") : new String[0];

      String xml = "<config xmlns=\"urn:org:jgroups\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + " <TCP bind_addr=\"127.0.0.1\" bind_port=\"" + bindPort + "\" port_range=\"0\" thread_pool.min_threads=\"2\" thread_pool.max_threads=\"20\"/>\n"
            + " <TCPPING initial_hosts=\"" + initialHosts + "\" port_range=\"0\"/>\n"
            + " <MERGE3 min_interval=\"3000\" max_interval=\"6000\"/>\n"
            + " <FD_SOCK/>\n"
            + " <FD_ALL timeout=\"20000\" interval=\"3000\"/>\n"
            + " <VERIFY_SUSPECT timeout=\"1500\"/>\n"
            + " <pbcast.NAKACK2 use_mcast_xmit=\"false\"/>\n"
            + " <UNICAST3/>\n"
            + " <pbcast.STABLE desired_avg_gossip=\"50000\" max_bytes=\"4M\"/>\n"
            + " <pbcast.GMS print_local_addr=\"false\" join_timeout=\"3000\"/>\n"
            + " <UFC max_credits=\"2m\" min_threshold=\"0.40\"/>\n"
            + " <MFC max_credits=\"2m\" min_threshold=\"0.40\"/>\n"
            + " <FRAG2 frag_size=\"60K\"/>\n"
            + "</config>\n";
      Path file = Files.createTempFile("interop-jgroups", ".xml");
      Files.writeString(file, xml);

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().clusterName(cluster).addProperty("configurationFile", file.toString());
      gcb.transport().distributedSyncTimeout(30000);
      // MockServer's clustered manager marshals user values with Java serialization (InfinispanStateBackend:155,201)
      gcb.serialization().marshaller(new JavaSerializationMarshaller());
      gcb.serialization().allowList().addRegexp(".*");
      DefaultCacheManager cm = new DefaultCacheManager(gcb.build());
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb.clustering().stateTransfer().timeout(60000);
      cb.clustering().remoteTimeout(30000);

      System.out.println("PORT " + bindPort);
      System.out.flush();
      for (String c : startCaches) {
         cm.defineConfiguration(c, cb.build());
         Cache<Object, Object> cache = cm.getCache(c);
         System.out.println("JOINED " + c + " " + cache.getAdvancedCache().getRpcManager().getMembers());
         System.out.flush();
      }
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      String line;
      while ((line = in.readLine()) != null) {
         String[] p = line.trim().split(" ");
         if (p[0].equals("quit")) break;
         if (p[0].equals("start")) {
            cm.defineConfiguration(p[1], cb.build());
            Cache<Object, Object> cache = cm.getCache(p[1]);
            System.out.println("JOINED " + p[1] + " " + cache.getAdvancedCache().getRpcManager().getMembers());
         } else if (p[0].equals("put")) {
            Cache<Object, Object> cache = cm.getCache(p[1]);
            Object old = cache.put(p[2], new VersionedWrapper<>(p[3], Long.parseLong(p[4])));
            System.out.println("PUT " + p[1] + " " + p[2] + " " + (old == null ? "null" : ((VersionedWrapper<?>) old).getValue()));
         } else if (p[0].equals("fill")) {
            // fill <cache> <n>: key<i> -> VersionedWrapper("value<i>", i)
            Cache<Object, Object> cache = cm.getCache(p[1]);
            int n = Integer.parseInt(p[2]);
            for (int i = 0; i < n; i++) cache.put("key" + i, new VersionedWrapper<>("value" + i, (long) i));
            System.out.println("FILLED " + p[1] + " " + n);
         } else if (p[0].equals("get")) {
            Cache<Object, Object> cache = cm.getCache(p[1]);
            Object v = cache.get(p[2]);
            System.out.println("VALUE " + p[1] + " " + p[2] + " " + (v == null ? "null 0" : ((VersionedWrapper<?>) v).getValue() + " " + ((VersionedWrapper<?>) v).getVersion()));
         } else if (p[0].equals("remove")) {
            Cache<Object, Object> cache = cm.getCache(p[1]);
            Object old = cache.remove(p[2]);
            System.out.println("REMOVED " + p[1] + " " + p[2] + " " + (old == null ? "null" : ((VersionedWrapper<?>) old).getValue()));
         } else if (p[0].equals("putifabsent")) {
            Cache<Object, Object> cache = cm.getCache(p[1]);
            Object old = cache.putIfAbsent(p[2], new VersionedWrapper<>(p[3], Long.parseLong(p[4])));
            System.out.println("ABSENT " + p[1] + " " + p[2] + " " + (old == null ? "null" : ((VersionedWrapper<?>) old).getValue()));
         } else if (p[0].equals("clear")) {
            cm.getCache(p[1]).clear();
            System.out.println("CLEARED " + p[1]);
         } else if (p[0].equals("size")) {
            Cache<Object, Object> cache = cm.getCache(p[1]);
            System.out.println("SIZE " + p[1] + " " + cache.size());
         } else if (p[0].equals("dump")) {
            CacheTopology ct = cm.getGlobalComponentRegistry().getLocalTopologyManager().getCacheTopology(p[1]);
            System.out.println("TOPOLOGY " + p[1] + " " + (ct == null ? "none" : ct.getTopologyId() + " " + ct.getPhase() + " " + ct.getMembers().size() + " " + ct.getCurrentCH().getRoutingTableAsString().hashCode() + " " + ct.getCurrentCH().getMembers()));
         }
         System.out.flush();
      }
      cm.stop();
      System.exit(0);
   }
}
