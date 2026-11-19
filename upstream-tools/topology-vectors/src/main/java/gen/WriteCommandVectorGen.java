package gen;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.*;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.VersionInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.util.ByteString;
import org.mockserver.state.Blob;
import org.mockserver.state.infinispan.InfinispanKeyValueStore;
import org.mockserver.state.infinispan.VersionedWrapper;

/**
 * tests/vectors/write_commands.hex: `label TAB hex`. Real commands, built by the real
 * `CacheImpl`/`EncoderCache` (and MockServer's own `InfinispanKeyValueStore`, compiled unmodified from
 * the pinned submodule) on a local cache, captured by an interceptor placed after `VersionInterceptor`,
 * and marshalled by the real `GlobalMarshaller` as the `SingleRpcCommand` that `RpcManagerImpl` sends.
 * The user marshaller is `JavaSerializationMarshaller`, as in MockServer's clustered manager.
 */
public class WriteCommandVectorGen {
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

   static void emit(String suffix, VisitableCommand c) throws Throwable {
      ((org.infinispan.commands.TopologyAffectedCommand) c).setTopologyId(7);
      // A local cache stamps LocalModeAddress, which cannot be marshalled: use a cluster member's id.
      if (!(c instanceof ClearCommand)) {
         java.lang.reflect.Field f = org.infinispan.commands.write.AbstractDataWriteCommand.class.getDeclaredField("commandInvocationId");
         f.setAccessible(true);
         f.set(c, org.infinispan.commands.CommandInvocationId.generateId(Marshal.member(1)));
      }
      out.println(label + suffix + "\t" + hex(new SingleRpcCommand(CACHE, c)));
   }

   static class Capture extends DDAsyncInterceptor {
      @Override public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand c) throws Throwable { emit("", c); return invokeNext(ctx, c); }
      @Override public Object visitComputeCommand(InvocationContext ctx, ComputeCommand c) throws Throwable { emit("", c); return invokeNext(ctx, c); }
      @Override public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand c) throws Throwable { emit("", c); return invokeNext(ctx, c); }
      @Override public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand c) throws Throwable { emit("", c); return invokeNext(ctx, c); }
      @Override public Object visitClearCommand(InvocationContext ctx, ClearCommand c) throws Throwable { emit("", c); return invokeNext(ctx, c); }
   }

   public static void main(String[] args) throws Throwable {
      GlobalConfigurationBuilder g = new GlobalConfigurationBuilder().nonClusteredDefault();
      g.serialization().marshaller(new JavaSerializationMarshaller()).allowList().addRegexp(".*");
      try (PrintWriter pw = new PrintWriter(args[0], "UTF-8")) {
         out = pw;
         DefaultCacheManager cm = new DefaultCacheManager(g.build());
         marshaller = (GlobalMarshaller) cm.getGlobalComponentRegistry().getComponent(StreamingMarshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);
         cm.defineConfiguration("c", new ConfigurationBuilder().build());
         Cache<String, VersionedWrapper<String>> cache = cm.getCache("c");
         cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(new Capture(), VersionInterceptor.class);
         InfinispanKeyValueStore<String> store = new InfinispanKeyValueStore<>(cache);

         label = "put:plain";
         cache.put("k1", new VersionedWrapper<>("v1", 1L));
         label = "put:lifespan";
         cache.put("k3", new VersionedWrapper<>("v3", 1L), 10, TimeUnit.SECONDS);
         label = "put:null-key-value-string";
         ((Cache<String, Object>) (Cache) cache).put("k4", "plain string value");
         label = "putIfAbsent:new";
         store.putIfAbsent("k2", "v2");
         label = "putIfAbsent:existing";
         store.putIfAbsent("k2", "other");
         label = "compute:store-put-new";
         store.put("kc", "first");
         label = "compute:store-put-existing";
         store.put("kc", "second");
         label = "replace:cas";
         store.compareAndSet("kc", 2L, "third");
         label = "remove:key";
         store.remove("k3");
         label = "remove:cas";
         store.compareAndRemove("kc", 3L);
         label = "put:blob";
         ((Cache<String, Object>) (Cache) cache).put("b1", new Blob("b1", new byte[]{1, 2, 3}, java.util.Collections.emptyMap()));
         label = "clear";
         store.clear();

         // responses a primary sends back
         VersionedWrapper<String> w = new VersionedWrapper<>("v", 9L);
         out.println("resp:successful-value\t" + hex(SuccessfulResponse.create(w)));
         out.println("resp:successful-null\t" + hex(SuccessfulResponse.create(null)));
         out.println("resp:successful-true\t" + hex(SuccessfulResponse.create(Boolean.TRUE)));
         out.println("resp:successful-false\t" + hex(SuccessfulResponse.create(Boolean.FALSE)));
         out.println("resp:unsuccessful-value\t" + hex(UnsuccessfulResponse.create(w)));
         out.println("resp:unsuccessful-null\t" + hex(UnsuccessfulResponse.create(null)));
         out.println("resp:unsuccessful-false\t" + hex(UnsuccessfulResponse.create(Boolean.FALSE)));

         out.println("resp:exception-timeout\t" + hex(new org.infinispan.remoting.responses.ExceptionResponse(new org.infinispan.util.concurrent.TimeoutException("ISPN000299: Unable to acquire lock after 10 seconds for key k and requestor r"))));
         out.println("resp:exception-outdated-same\t" + hex(new org.infinispan.remoting.responses.ExceptionResponse(org.infinispan.statetransfer.OutdatedTopologyException.RETRY_SAME_TOPOLOGY)));
         out.println("resp:exception-outdated-next\t" + hex(new org.infinispan.remoting.responses.ExceptionResponse(org.infinispan.statetransfer.OutdatedTopologyException.RETRY_NEXT_TOPOLOGY)));
         // what a ClusteredGetCommand answers with (InternalCacheValue inside a SuccessfulResponse)
         out.println("resp:icv-immortal\t" + hex(SuccessfulResponse.create(new org.infinispan.container.entries.ImmortalCacheValue(w))));
         out.println("resp:icv-mortal\t" + hex(SuccessfulResponse.create(new org.infinispan.container.entries.MortalCacheValue(w, 1_700_000_000_123L, 10_000L))));

         // ClusteredGetCommand (what a joiner that is not a read owner sends)
         label = "clusteredget";
         ClusteredGetCommand cg = new ClusteredGetCommand("k1", CACHE, 33, 0L);
         cg.setTopologyId(7);
         out.println(label + "\t" + hex(cg));
         cm.stop();
      }
   }
}
