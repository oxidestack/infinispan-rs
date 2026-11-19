package gen;

import java.util.*;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/** Boots a local (non-clustered) cache manager only to obtain the real internal GlobalMarshaller. */
public class Marshal implements AutoCloseable {
   final DefaultCacheManager cm;
   public final GlobalMarshaller marshaller;

   public Marshal() {
      cm = new DefaultCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault().build());
      GlobalComponentRegistry gcr = cm.getGlobalComponentRegistry();
      marshaller = (GlobalMarshaller) gcr.getComponent(StreamingMarshaller.class, KnownComponentNames.INTERNAL_MARSHALLER);
   }

   public String hex(Object o) throws Exception {
      byte[] b = marshaller.objectToByteBuffer(o);
      StringBuilder sb = new StringBuilder();
      for (byte x : b) sb.append(String.format("%02x", x));
      return sb.toString();
   }

   @Override
   public void close() {
      cm.stop();
   }

   public static Address member(int i) {
      long msb = 0x6a09e667f3bcc908L * (i + 1) ^ (0x243f6a8885a308d3L >>> i);
      long lsb = 0xbb67ae8584caa73bL * (i + 3) + i;
      return new JGroupsAddress(new org.jgroups.util.UUID(msb, lsb));
   }

   public static String desc(List<Address> ms) {
      StringBuilder sb = new StringBuilder();
      for (Address a : ms) {
         org.jgroups.util.UUID u = (org.jgroups.util.UUID) ((JGroupsAddress) a).getJGroupsAddress();
         if (sb.length() > 0) sb.append(',');
         sb.append(u.getMostSignificantBits()).append(':').append(u.getLeastSignificantBits());
      }
      return sb.toString();
   }

   public static String descCf(float[] cf) {
      if (cf == null) return "-";
      StringBuilder sb = new StringBuilder();
      for (float f : cf) { if (sb.length() > 0) sb.append(','); sb.append(f); }
      return sb.toString();
   }

   public static Map<Address, Float> cfMap(List<Address> ms, float[] cf) {
      if (cf == null) return null;
      Map<Address, Float> m = new HashMap<>();
      for (int i = 0; i < ms.size(); i++) m.put(ms.get(i), cf[i]);
      return m;
   }

   public static List<Address> members(int... idx) {
      List<Address> l = new ArrayList<>();
      for (int i : idx) l.add(member(i));
      return l;
   }

   public static float[] fill(int n, float v) {
      float[] a = new float[n];
      Arrays.fill(a, v);
      return a;
   }
}
