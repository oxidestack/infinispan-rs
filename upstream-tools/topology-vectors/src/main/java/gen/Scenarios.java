package gen;

import java.util.*;

import org.infinispan.commons.util.Immutables;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.partitionhandling.AvailabilityMode;

import static gen.Marshal.*;

/** Deterministic objects shared (by construction) with tests/common/scenarios.rs. */
public class Scenarios {
   static final SyncReplicatedConsistentHashFactory F = new SyncReplicatedConsistentHashFactory();

   static PersistentUUID pu(int i) {
      return new PersistentUUID(0x100L + i, 0x200L + i);
   }

   static List<Address> addrs(int n) {
      return members(java.util.stream.IntStream.range(0, n).toArray());
   }

   static List<PersistentUUID> pus(int n) {
      List<PersistentUUID> l = new ArrayList<>();
      for (int i = 0; i < n; i++) l.add(pu(i));
      return l;
   }

   static Map<Address, Float> ones(List<Address> ms) {
      return cfMap(ms, fill(ms.size(), 1f));
   }

   static ReplicatedConsistentHash chABC() {
      List<Address> abc = addrs(3);
      return F.create(2, 256, abc, ones(abc));
   }

   static ReplicatedConsistentHash pendingABCD() {
      List<Address> abcd = addrs(4);
      return F.rebalance(F.updateMembers(chABC(), abcd, ones(abcd)));
   }

   static ReplicatedConsistentHash unionCH() {
      return F.union(chABC(), pendingABCD());
   }

   static CacheTopology stable() {
      return new CacheTopology(3, 1, chABC(), null, CacheTopology.Phase.NO_REBALANCE,
            Immutables.immutableListCopy(addrs(3)), new ArrayList<>(pus(3)));
   }

   static CacheTopology rebalancing(CacheTopology.Phase p, int id) {
      return new CacheTopology(id, 2, chABC(), pendingABCD(), unionCH(), p,
            Immutables.immutableListCopy(addrs(4)), new ArrayList<>(pus(4)));
   }

   static CacheJoinInfo joinInfo(Integer checksum) {
      return new CacheJoinInfo(F, 256, 2, 60000L, CacheMode.REPL_SYNC, 1.0f, pu(0),
            checksum == null ? Optional.empty() : Optional.of(checksum));
   }

   static CacheStatusResponse fullStatus() {
      return new CacheStatusResponse(joinInfo(null), rebalancing(CacheTopology.Phase.READ_OLD_WRITE_ALL, 4), stable(),
            AvailabilityMode.AVAILABLE, new ArrayList<>(addrs(3)));
   }

   static ManagerStatusResponse manager() {
      Map<String, CacheStatusResponse> caches = new HashMap<>();
      caches.put("org.infinispan.CONFIG", fullStatus());
      caches.put("mockserver-state", new CacheStatusResponse(joinInfo(42), stable(), stable(), AvailabilityMode.DEGRADED_MODE, new ArrayList<>(addrs(3))));
      caches.put("empty", CacheStatusResponse.empty());
      return new ManagerStatusResponse(caches, true);
   }
}
