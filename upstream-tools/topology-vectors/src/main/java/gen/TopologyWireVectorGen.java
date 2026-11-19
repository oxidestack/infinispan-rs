package gen;

import java.io.PrintWriter;

import org.infinispan.commands.topology.*;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.topology.CacheJoinException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.HeartBeatCommand;

/** tests/vectors/topology_wire.hex: `name TAB hex`, bytes from the real GlobalMarshaller. */
public class TopologyWireVectorGen {
   public static void main(String[] args) throws Exception {
      try (Marshal m = new Marshal(); PrintWriter out = new PrintWriter(args[0], "UTF-8")) {
         CacheTopology.Phase[] phases = {CacheTopology.Phase.READ_OLD_WRITE_ALL, CacheTopology.Phase.READ_ALL_WRITE_ALL, CacheTopology.Phase.READ_NEW_WRITE_ALL};
         out.println("topology:stable\t" + m.hex(Scenarios.stable()));
         for (int k = 0; k < phases.length; k++) {
            out.println("topology:" + phases[k] + "\t" + m.hex(Scenarios.rebalancing(phases[k], 4 + k)));
         }
         out.println("joininfo:none\t" + m.hex(Scenarios.joinInfo(null)));
         out.println("joininfo:42\t" + m.hex(Scenarios.joinInfo(42)));
         out.println("cmd:join\t" + m.hex(new CacheJoinCommand("mockserver-state", Marshal.member(0), Scenarios.joinInfo(null), 7)));
         out.println("cmd:join-checksum\t" + m.hex(new CacheJoinCommand("org.infinispan.CONFIG", Marshal.member(1), Scenarios.joinInfo(42), 12)));
         out.println("cmd:leave\t" + m.hex(new CacheLeaveCommand("mockserver-state", Marshal.member(0), 9)));
         out.println("cmd:confirm\t" + m.hex(new RebalancePhaseConfirmCommand("mockserver-state", Marshal.member(0), null, 5, 9)));
         out.println("cmd:rebalance-status\t" + m.hex(new RebalanceStatusRequestCommand("mockserver-state")));
         out.println("cmd:rebalance-start\t" + m.hex(new RebalanceStartCommand("mockserver-state", Marshal.member(0), Scenarios.rebalancing(CacheTopology.Phase.READ_OLD_WRITE_ALL, 4), 9)));
         out.println("cmd:topology-update-available\t" + m.hex(new TopologyUpdateCommand("mockserver-state", Marshal.member(0), Scenarios.rebalancing(CacheTopology.Phase.READ_ALL_WRITE_ALL, 5), AvailabilityMode.AVAILABLE, 9)));
         out.println("cmd:topology-update-degraded\t" + m.hex(new TopologyUpdateCommand("mockserver-state", Marshal.member(0), Scenarios.stable(), AvailabilityMode.DEGRADED_MODE, 9)));
         // PreferAvailabilityStrategy.onPartitionMerge passes a null mode to updateTopologiesAfterMerge, and every later
         // update of that cache carries it
         out.println("cmd:topology-update-null\t" + m.hex(new TopologyUpdateCommand("mockserver-state", Marshal.member(0), Scenarios.rebalancing(CacheTopology.Phase.READ_ALL_WRITE_ALL, 5), null, 9)));
         out.println("cmd:status-request\t" + m.hex(new CacheStatusRequestCommand(9)));
         out.println("cmd:stable-update\t" + m.hex(new TopologyUpdateStableCommand("mockserver-state", Marshal.member(0), Scenarios.stable(), 9)));
         out.println("cmd:heartbeat\t" + m.hex(HeartBeatCommand.INSTANCE));
         out.println("resp:successful-empty\t" + m.hex(SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE));
         out.println("resp:cache-not-found\t" + m.hex(CacheNotFoundResponse.INSTANCE));
         out.println("resp:unsuccessful-empty\t" + m.hex(UnsuccessfulResponse.EMPTY));
         out.println("resp:bool-true\t" + m.hex(SuccessfulResponse.create(Boolean.TRUE)));
         out.println("resp:status\t" + m.hex(SuccessfulResponse.create(Scenarios.fullStatus())));
         out.println("resp:status-empty\t" + m.hex(SuccessfulResponse.create(org.infinispan.topology.CacheStatusResponse.empty())));
         for (org.infinispan.topology.RebalancingStatus rs : org.infinispan.topology.RebalancingStatus.values())
            out.println("resp:rebalancing-" + rs + "\t" + m.hex(SuccessfulResponse.create(rs)));
         out.println("resp:exception-join\t" + m.hex(new org.infinispan.remoting.responses.ExceptionResponse(new CacheJoinException("boom: joining"))));
         out.println("resp:exception-cache\t" + m.hex(new org.infinispan.remoting.responses.ExceptionResponse(new org.infinispan.commons.CacheException("plain failure"))));
         out.println("resp:exception-lifecycle\t" + m.hex(new org.infinispan.remoting.responses.ExceptionResponse(new org.infinispan.IllegalLifecycleStateException())));
         out.println("resp:exception-suspect\t" + m.hex(new org.infinispan.remoting.responses.ExceptionResponse(new org.infinispan.remoting.transport.jgroups.SuspectException("gone"))));
         out.println("resp:manager\t" + m.hex(SuccessfulResponse.create(Scenarios.manager())));
      }
   }
}
