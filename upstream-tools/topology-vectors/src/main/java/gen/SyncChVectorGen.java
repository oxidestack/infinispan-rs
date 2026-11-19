package gen;

import java.io.PrintWriter;
import java.util.*;

import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * Emits tests/vectors/sync_ch.hex from the unmodified SyncConsistentHashFactory.
 * Line: op TAB numOwners;numSegments;members;capacity-factors TAB result
 *  members: msb:lsb,...  capacity factors: comma floats aligned with members, or "-"
 *  result: per segment, the owner member indices dot-joined, segments space-joined.
 */
public class SyncChVectorGen {
   static Address member(int i) {
      // spread both halves so the 128-bit hash inputs differ in every lane
      long msb = 0x6a09e667f3bcc908L * (i + 1) ^ (0x243f6a8885a308d3L >>> i);
      long lsb = 0xbb67ae8584caa73bL * (i + 3) + i;
      return new JGroupsAddress(new org.jgroups.util.UUID(msb, lsb));
   }

   static String desc(List<Address> ms) {
      StringBuilder sb = new StringBuilder();
      for (Address a : ms) {
         org.jgroups.util.UUID u = (org.jgroups.util.UUID) ((JGroupsAddress) a).getJGroupsAddress();
         if (sb.length() > 0) sb.append(',');
         sb.append(u.getMostSignificantBits()).append(':').append(u.getLeastSignificantBits());
      }
      return sb.toString();
   }

   static String descCf(List<Address> ms, float[] cf) {
      if (cf == null) return "-";
      StringBuilder sb = new StringBuilder();
      for (float f : cf) { if (sb.length() > 0) sb.append(','); sb.append(f); }
      return sb.toString();
   }

   static Map<Address, Float> cfMap(List<Address> ms, float[] cf) {
      if (cf == null) return null;
      Map<Address, Float> m = new HashMap<>();
      for (int i = 0; i < ms.size(); i++) m.put(ms.get(i), cf[i]);
      return m;
   }

   static String owners(DefaultConsistentHash ch, List<Address> ms) {
      StringBuilder sb = new StringBuilder();
      for (int s = 0; s < ch.getNumSegments(); s++) {
         if (s > 0) sb.append(' ');
         List<Address> o = ch.locateOwnersForSegment(s);
         for (int j = 0; j < o.size(); j++) {
            if (j > 0) sb.append('.');
            sb.append(ms.indexOf(o.get(j)));
         }
      }
      return sb.toString();
   }

   public static void main(String[] args) throws Exception {
      SyncConsistentHashFactory f = new SyncConsistentHashFactory();
      float[][] cfs = {null, null, null};
      try (PrintWriter out = new PrintWriter(args[0], "UTF-8")) {
         int[] segs = {256, 60, 7};
         for (int ns : segs) {
            for (int owners = 1; owners <= 3; owners++) {
               for (int n = 1; n <= 8; n++) {
                  List<Address> ms = new ArrayList<>();
                  for (int i = 0; i < n; i++) ms.add(member(i));
                  List<float[]> variants = new ArrayList<>();
                  variants.add(null);
                  float[] ones = new float[n]; Arrays.fill(ones, 1f); variants.add(ones);
                  if (n >= 2) {
                     float[] mixed = new float[n];
                     for (int i = 0; i < n; i++) mixed[i] = (i % 3 == 0) ? 2f : (i % 3 == 1 ? 1f : 0.5f);
                     variants.add(mixed);
                     float[] zero = new float[n]; Arrays.fill(zero, 1f); zero[n - 1] = 0f;
                     variants.add(zero);
                  }
                  for (float[] cf : variants) {
                     DefaultConsistentHash ch = f.create(owners, ns, ms, cfMap(ms, cf));
                     out.println("create\t" + owners + ";" + ns + ";" + desc(ms) + ";" + descCf(ms, cf) + "\t" + owners(ch, ms));
                  }
               }
            }
         }
         // updateMembers: base CH, then a different member list. Result indices refer to the NEW list.
         for (int owners = 1; owners <= 2; owners++) {
            for (int[][] sc : new int[][][]{
                  {{0, 1, 2, 3}, {0, 1, 2}}, {{0, 1, 2, 3}, {1, 2, 3}}, {{0, 1, 2}, {0, 1, 2, 3}},
                  {{0, 1}, {1}}, {{0, 1, 2, 3, 4}, {0, 4}}, {{0, 1, 2}, {3, 4, 5}}, {{0, 1, 2}, {2, 1, 0}}}) {
               List<Address> base = new ArrayList<>(), nm = new ArrayList<>();
               for (int i : sc[0]) base.add(member(i));
               for (int i : sc[1]) nm.add(member(i));
               float[] cf = new float[base.size()]; Arrays.fill(cf, 1f);
               float[] ncf = new float[nm.size()]; Arrays.fill(ncf, 1f);
               DefaultConsistentHash b = f.create(owners, 256, base, cfMap(base, cf));
               DefaultConsistentHash u = f.updateMembers(b, nm, cfMap(nm, ncf));
               out.println("update\t" + owners + ";256;" + desc(base) + ";" + descCf(base, cf) + "|" + desc(nm) + ";" + descCf(nm, ncf) + "\t" + owners(u, nm));
               DefaultConsistentHash r = f.rebalance(u);
               out.println("rebalance\t" + owners + ";256;" + desc(base) + ";" + descCf(base, cf) + "|" + desc(nm) + ";" + descCf(nm, ncf) + "\t" + owners(r, nm));
            }
         }
      }
   }
}
