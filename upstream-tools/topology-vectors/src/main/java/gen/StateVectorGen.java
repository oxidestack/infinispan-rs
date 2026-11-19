package gen;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.util.ByteString;
import org.mockserver.state.infinispan.VersionedWrapper;

/**
 * tests/vectors/state_transfer.hex: `label TAB hex` (commands and responses of state transfer, as the real
 * GlobalMarshaller writes them) and `probe:` lines (what a real two-node REPL_SYNC cluster holds in a
 * joiner's data container after state transfer, and what the provider reads back).
 */
public class StateVectorGen {
   static GlobalMarshaller marshaller;
   static PrintWriter out;
   static final ByteString CACHE = ByteString.fromString("mockserver-state");

   static String hex(Object o) throws Exception {
      byte[] b = marshaller.objectToByteBuffer(o);
      StringBuilder sb = new StringBuilder();
      for (byte x : b) sb.append(String.format("%02x", x));
      return sb.toString();
   }

   static IntSet small(int... v) {
      IntSet s = IntSets.mutableEmptySet(256);
      for (int i : v) s.set(i);
      return s;
   }

   static IntSet all() {
      IntSet s = IntSets.mutableEmptySet(256);
      for (int i = 0; i < 256; i++) s.set(i);
      return s;
   }

   static Collection<StateChunk> chunks(StateChunk... c) {
      // OutboundTransferTask builds `new HashMap<Integer, StateChunk>().values()`
      Map<Integer, StateChunk> m = new java.util.HashMap<>();
      for (StateChunk x : c) m.put(x.getSegmentId(), x);
      return m.values();
   }

   static VersionedWrapper<String> vw(String v, long ver) {
      return new VersionedWrapper<>(v, ver);
   }

   public static void main(String[] args) throws Throwable {
      GlobalConfigurationBuilder g = new GlobalConfigurationBuilder().nonClusteredDefault();
      g.serialization().marshaller(new JavaSerializationMarshaller()).allowList().addRegexp(".*");
      try (PrintWriter pw = new PrintWriter(args[0], "UTF-8")) {
         out = pw;
         DefaultCacheManager cm = new DefaultCacheManager(g.build());
         marshaller = (GlobalMarshaller) cm.getGlobalComponentRegistry().getComponent(StreamingMarshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);

         StateTransferStartCommand start = new StateTransferStartCommand(CACHE, 7, all());
         out.println("start:all\t" + hex(start));
         out.println("start:few\t" + hex(new StateTransferStartCommand(CACHE, 9, small(0, 5, 255))));
         out.println("start:one\t" + hex(new StateTransferStartCommand(CACHE, 9, small(42))));
         out.println("start:sparse-high\t" + hex(new StateTransferStartCommand(CACHE, 12, small(200, 201, 255))));
         out.println("start:singleton-class\t" + hex(new StateTransferStartCommand(CACHE, 9, IntSets.immutableSet(42))));
         out.println("start:empty-class\t" + hex(new StateTransferStartCommand(CACHE, 9, IntSets.immutableEmptySet())));
         StateTransferCancelCommand cancel = new StateTransferCancelCommand(CACHE, 7, small(3, 4));
         out.println("cancel:two\t" + hex(cancel));

         // entries, as InternalEntryFactoryImpl creates them for a REPL_SYNC non-tx cache
         Metadata versioned = new EmbeddedMetadata.Builder().version(new NumericVersion(0x0007000100000003L)).build();
         Metadata versionedMortal = new EmbeddedMetadata.Builder().lifespan(60_000).version(new NumericVersion(5)).build();
         List<InternalCacheEntry<?, ?>> immortal = new ArrayList<>();
         immortal.add(new ImmortalCacheEntry("k1", vw("v1", 1)));
         immortal.add(new ImmortalCacheEntry("k2", vw("v2", 2)));
         out.println("chunk:immortal-two\t" + hex(new StateResponseCommand(CACHE, 7, chunks(new StateChunk(11, immortal, false)), true, false)));
         out.println("chunk:immortal-last\t" + hex(new StateResponseCommand(CACHE, 7, chunks(new StateChunk(11, immortal, true)), true, false)));
         out.println("chunk:empty-last\t" + hex(new StateResponseCommand(CACHE, 7, chunks(new StateChunk(3, Collections.emptyList(), true)), true, false)));
         out.println("chunk:empty-arraylist-last\t" + hex(new StateResponseCommand(CACHE, 7, chunks(new StateChunk(3, new ArrayList<InternalCacheEntry<?, ?>>(), true)), true, false)));
         List<InternalCacheEntry<?, ?>> mixed = new ArrayList<>();
         mixed.add(new ImmortalCacheEntry("a", "plain"));
         mixed.add(new MortalCacheEntry("m", vw("mortal", 3), 10_000, 1_700_000_000_123L));
         mixed.add(new TransientCacheEntry("t", "idle", 5_000, 1_700_000_000_456L));
         mixed.add(new TransientMortalCacheEntry("tm", "both", 5_000, 10_000, 1_700_000_000_456L, 1_700_000_000_123L));
         mixed.add(new MetadataImmortalCacheEntry("mi", "v", versioned));
         mixed.add(new MetadataMortalCacheEntry("mm", "v", versionedMortal, 1_700_000_000_789L));
         mixed.add(new org.infinispan.container.entries.metadata.MetadataTransientCacheEntry("mt", "v", new EmbeddedMetadata.Builder().maxIdle(7_000).version(new NumericVersion(6)).build(), 1_700_000_000_321L));
         mixed.add(new org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry("mtm", "v", new EmbeddedMetadata.Builder().lifespan(8_000).maxIdle(7_000).version(new NumericVersion(6)).build(), 1_700_000_000_321L, 1_700_000_000_654L));
         out.println("chunk:every-entry-kind\t" + hex(new StateResponseCommand(CACHE, 8, chunks(new StateChunk(200, mixed, true)), true, false)));
         // several segments in one command: HashMap<Integer,..>.values() order
         List<InternalCacheEntry<?, ?>> one = new ArrayList<>();
         one.add(new ImmortalCacheEntry("x", "y"));
         out.println("chunk:segments-order\t" + hex(new StateResponseCommand(CACHE, 7, chunks(
               new StateChunk(255, Collections.emptyList(), true), new StateChunk(0, Collections.emptyList(), true),
               new StateChunk(17, one, false), new StateChunk(16, Collections.emptyList(), true), new StateChunk(256 + 3, Collections.emptyList(), true),
               new StateChunk(1, one, true)), true, false)));
         out.println("chunk:push-no-apply\t" + hex(new StateResponseCommand(CACHE, 7, chunks(new StateChunk(11, immortal, true)), false, true)));
         cm.stop();

         probe();
      }
   }

   /** Two real nodes: A holds data, B joins; show the entry class in B's container and what B marshals when it is the provider. */
   static void probe() throws Throwable {
      DefaultCacheManager a = node(17761, "127.0.0.1[17761],127.0.0.1[17762]");
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb.clustering().stateTransfer().timeout(60000);
      cb.clustering().remoteTimeout(30000);
      a.defineConfiguration("c", cb.build());
      Cache<String, Object> ca = a.getCache("c");
      ca.put("plain", "p");
      ca.put("versioned", vw("v", 1));
      ca.put("lifespan", "l", 600, TimeUnit.SECONDS);
      ca.put("idle", "i", -1, TimeUnit.SECONDS, 600, TimeUnit.SECONDS);
      ca.put("both", "b", 600, TimeUnit.SECONDS, 600, TimeUnit.SECONDS);
      ca.replace("plain", "p", "p2"); // stamps a NumericVersion
      DefaultCacheManager b = node(17762, "127.0.0.1[17761],127.0.0.1[17762]");
      b.defineConfiguration("c", cb.build());
      Cache<String, Object> cbb = b.getCache("c");
      for (String k : new String[]{"plain", "versioned", "lifespan", "idle", "both"}) {
         for (String who : new String[]{"A", "B"}) {
            Cache<String, Object> c = who.equals("A") ? ca : cbb;
            InternalCacheEntry<?, ?> e = c.getAdvancedCache().getDataContainer().peek(k);
            out.println("probe:" + who + ":" + k + "\t" + e.getClass().getSimpleName() + "|metadata=" + (e.getMetadata() == null ? "null" : e.getMetadata().getClass().getSimpleName()) + "|lifespan>0=" + (e.getLifespan() > 0) + "|maxIdle>0=" + (e.getMaxIdle() > 0));
         }
      }
      out.println("probe:size:B\t" + cbb.size());
      b.stop();
      a.stop();
   }

   static DefaultCacheManager node(int port, String hosts) throws Exception {
      String xml = "<config xmlns=\"urn:org:jgroups\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + " <TCP bind_addr=\"127.0.0.1\" bind_port=\"" + port + "\" port_range=\"0\" thread_pool.min_threads=\"2\" thread_pool.max_threads=\"20\"/>\n"
            + " <TCPPING initial_hosts=\"" + hosts + "\" port_range=\"0\"/>\n"
            + " <MERGE3 min_interval=\"3000\" max_interval=\"6000\"/>\n"
            + " <FD_SOCK/>\n"
            + " <VERIFY_SUSPECT timeout=\"1500\"/>\n"
            + " <pbcast.NAKACK2 use_mcast_xmit=\"false\"/>\n"
            + " <UNICAST3/>\n"
            + " <pbcast.STABLE desired_avg_gossip=\"50000\" max_bytes=\"4M\"/>\n"
            + " <pbcast.GMS print_local_addr=\"false\" join_timeout=\"3000\"/>\n"
            + " <UFC max_credits=\"2m\" min_threshold=\"0.40\"/>\n"
            + " <MFC max_credits=\"2m\" min_threshold=\"0.40\"/>\n"
            + " <FRAG2 frag_size=\"60K\"/>\n"
            + "</config>\n";
      Path file = Files.createTempFile("state-probe-jgroups", ".xml");
      Files.writeString(file, xml);
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().clusterName("state-probe").addProperty("configurationFile", file.toString());
      gcb.transport().distributedSyncTimeout(30000);
      gcb.serialization().marshaller(new JavaSerializationMarshaller()).allowList().addRegexp(".*");
      return new DefaultCacheManager(gcb.build());
   }
}
