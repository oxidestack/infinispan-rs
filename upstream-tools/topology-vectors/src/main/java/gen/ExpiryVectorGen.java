package gen;

import java.io.PrintWriter;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.util.ByteString;
import org.mockserver.state.infinispan.VersionedWrapper;

/**
 * tests/vectors/expiry.hex: `label TAB hex`. The `RemoveExpiredCommand` a node sends the primary owner
 * (and the primary its backups) when an in-memory entry is found expired, built by the real `CacheImpl`
 * (`removeLifespanExpired`, `removeMaxIdleExpired`, with the `ZERO_LOCK_ACQUISITION_TIMEOUT` flag
 * `ClusterExpirationManager.cacheToUse(false)` adds for a read), captured by an interceptor and marshalled
 * by the real `GlobalMarshaller` inside the `SingleRpcCommand` `RpcManagerImpl` sends. `flags:` lines give
 * the flag bit set of each captured command, so the Rust constants are checked against the real ones.
 */
public class ExpiryVectorGen {
   static GlobalMarshaller marshaller;
   static PrintWriter out;
   static String label = "?";
   static final ByteString CACHE = ByteString.fromString("mockserver-state");

   static String hex(Object o) throws Exception {
      byte[] b = marshaller.objectToByteBuffer(o);
      StringBuilder sb = new StringBuilder();
      for (byte x : b) sb.append(String.format("%02x", x));
      return sb.toString();
   }

   static void emit(RemoveExpiredCommand c) throws Throwable {
      c.setTopologyId(7);
      // A local cache stamps LocalModeAddress, which cannot be marshalled: use a cluster member's id; the
      // segment of a local cache is 0, a clustered one's is the key's murmur segment.
      java.lang.reflect.Field f = org.infinispan.commands.write.AbstractDataWriteCommand.class.getDeclaredField("commandInvocationId");
      f.setAccessible(true);
      f.set(c, org.infinispan.commands.CommandInvocationId.generateId(Marshal.member(1)));
      java.lang.reflect.Field s = org.infinispan.commands.read.AbstractDataCommand.class.getDeclaredField("segment");
      s.setAccessible(true);
      s.setInt(c, 19);
      out.println(label + "\t" + hex(new SingleRpcCommand(CACHE, c)));
      out.println("flags:" + label + "\t" + Long.toHexString(c.getFlagsBitSet()));
   }

   static class Capture extends DDAsyncInterceptor {
      @Override
      public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand c) throws Throwable {
         emit(c);
         return invokeNext(ctx, c);
      }
   }

   public static void main(String[] args) throws Throwable {
      GlobalConfigurationBuilder g = new GlobalConfigurationBuilder().nonClusteredDefault();
      g.serialization().marshaller(new JavaSerializationMarshaller()).allowList().addRegexp(".*");
      try (PrintWriter pw = new PrintWriter(args[0], "UTF-8")) {
         out = pw;
         DefaultCacheManager cm = new DefaultCacheManager(g.build());
         marshaller = (GlobalMarshaller) cm.getGlobalComponentRegistry().getComponent(StreamingMarshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);
         cm.defineConfiguration("c", new ConfigurationBuilder().build());
         Cache<String, Object> cache = cm.getCache("c");
         cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(new Capture(), org.infinispan.interceptors.impl.CallInterceptor.class);
         AdvancedCache<String, Object> read = cache.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);

         label = "expire:lifespan-string";
         cache.put("k1", "v1", 10, java.util.concurrent.TimeUnit.SECONDS);
         read.removeLifespanExpired("k1", "v1", 10_000L).get();
         label = "expire:lifespan-versioned";
         VersionedWrapper<String> w = new VersionedWrapper<>("v2", 1L);
         cache.put("k2", w, 25, java.util.concurrent.TimeUnit.SECONDS);
         read.removeLifespanExpired("k2", w, 25_000L).get();
         label = "expire:lifespan-plain-flags";
         cache.put("k3", "v3", 1, java.util.concurrent.TimeUnit.SECONDS);
         cache.getAdvancedCache().removeLifespanExpired("k3", "v3", 1_000L).get();
         label = "expire:maxidle";
         cache.put("k4", "v4", -1, java.util.concurrent.TimeUnit.SECONDS, 5, java.util.concurrent.TimeUnit.SECONDS);
         read.removeMaxIdleExpired("k4", "v4").get();
         cm.stop();
      }
   }
}
