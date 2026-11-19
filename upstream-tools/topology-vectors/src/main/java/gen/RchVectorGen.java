package gen;

import java.io.PrintWriter;
import java.util.*;

import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

import static gen.Marshal.*;

/**
 * tests/vectors/rch.hex: SyncReplicatedConsistentHashFactory results serialised by the real
 * GlobalMarshaller. Line: op TAB spec TAB hex.
 *   spec = segs;members;cf            (create)
 *        | segs;members;cf|members;cf (update: base then new)
 *        | segs;members;cf&members;cf (union of two creates)
 */
public class RchVectorGen {
   static String spec(int segs, List<Address> ms, float[] cf) {
      return segs + ";" + desc(ms) + ";" + descCf(cf);
   }

   public static void main(String[] args) throws Exception {
      SyncReplicatedConsistentHashFactory f = new SyncReplicatedConsistentHashFactory();
      try (Marshal m = new Marshal(); PrintWriter out = new PrintWriter(args[0], "UTF-8")) {
         for (int segs : new int[]{256, 60}) {
            for (int n = 1; n <= 8; n++) {
               List<Address> ms = new ArrayList<>();
               for (int i = 0; i < n; i++) ms.add(member(i));
               List<float[]> variants = new ArrayList<>();
               variants.add(null);
               variants.add(fill(n, 1f));
               if (n >= 2) {
                  float[] z = fill(n, 1f);
                  z[n - 1] = 0f;
                  variants.add(z);
               }
               for (float[] cf : variants) {
                  ReplicatedConsistentHash ch = f.create(2, segs, ms, cfMap(ms, cf));
                  out.println("create\t" + spec(segs, ms, cf) + "\t" + m.hex(ch));
               }
            }
         }
         int[][][] updates = {
               {{0, 1, 2}, {0, 1}}, {{0, 1, 2}, {0, 1, 2, 3}}, {{0, 1}, {1}}, {{0, 1, 2, 3}, {2, 3, 4}}, {{0}, {0, 1}}};
         for (int[][] u : updates) {
            List<Address> base = members(u[0]), nm = members(u[1]);
            ReplicatedConsistentHash b = f.create(2, 256, base, cfMap(base, fill(base.size(), 1f)));
            ReplicatedConsistentHash r = f.updateMembers(b, nm, cfMap(nm, fill(nm.size(), 1f)));
            out.println("update\t" + spec(256, base, fill(base.size(), 1f)) + "|" + desc(nm) + ";" + descCf(fill(nm.size(), 1f)) + "\t" + m.hex(r));
         }
         int[][][] unions = {{{0, 1, 2}, {1, 2, 3}}, {{0, 1}, {0, 1}}, {{0}, {1}}, {{2, 1, 0}, {0, 3}}};
         for (int[][] u : unions) {
            for (boolean withCf : new boolean[]{true, false}) {
               List<Address> a = members(u[0]), b = members(u[1]);
               ReplicatedConsistentHash ca = f.create(2, 256, a, cfMap(a, withCf ? fill(a.size(), 1f) : null));
               ReplicatedConsistentHash cb = f.create(2, 256, b, cfMap(b, withCf ? fill(b.size(), 1f) : null));
               ReplicatedConsistentHash r = f.union(ca, cb);
               out.println("union\t" + spec(256, a, withCf ? fill(a.size(), 1f) : null) + "&" + desc(b) + ";" + descCf(withCf ? fill(b.size(), 1f) : null) + "\t" + m.hex(r));
            }
         }
      }
   }
}
