package com.dkvstore.server;

import java.util.HashSet;
import java.util.Set;

import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.server.distribution.DirectRoute;
import com.dkvstore.server.distribution.HashEntity;
import com.dkvstore.server.distribution.ReplicationKVHandler;
import com.dkvstore.server.distribution.VirtualNode;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

public class MembershipService {
    /**
     * When a node joins adds the node to the hash ring + if necessary
     * begin copying keys over, via PUT requests. This method should only ever be
     * called by one thread.
     * @param joinedNode the node that joined.
     * @return true if migration process started, false if migration should be halting due to
     *   resource constraints.
     */
    public static void OnNodeJoin(AddressHolder joinedNode) {  	
    	ByteString hostNameAndPort = Util.concatHostnameAndPort(joinedNode.hostname, joinedNode.port);   

        // add new node to hash ring so that now the requests can be routed correctly.
        int joiningNodeId = HashEntity.getInstance().addNode(hostNameAndPort);
    	DirectRoute.getInstance().addNode(joiningNodeId, joinedNode);
    	
    	System.out.println(String.format("[INFO]: Joining node: %s:%d, joiningNodeId: %d", joinedNode.hostname, joinedNode.port, joiningNodeId));

    	MigrateKVHandler.getInstance().migrate(joiningNodeId);
    	
    	int selfNodeId = DirectRoute.getInstance().getSelfNodeId();
        VirtualNode[] selfVNodes = HashEntity.getInstance().getVNodeMap().get(selfNodeId);

        Set<VirtualNode> affectedVNodes = new HashSet<>();
        for (VirtualNode vnode : selfVNodes) {
            if (HashEntity.getInstance().isSuccessor(vnode, joiningNodeId, Protocol.REPLICATION_FACTOR - 1)) {
                affectedVNodes.add(vnode);
            }
        }
        
        if (!affectedVNodes.isEmpty()) {
            ReplicationKVHandler.getInstance().replicateToSuccessors(affectedVNodes);
        }
    }
    
    /**
     * When a node leaves, remove the node from the hash ring.
     * @param leftNode node that left.
     */
    public static void OnNodeLeft(AddressHolder leftNode) {
        ByteString hostNameAndPort = Util.concatHostnameAndPort(leftNode.hostname, leftNode.port);
        
        int leavingNodeId = HashEntity.getInstance().getNodeId(hostNameAndPort);
        
        System.out.println(String.format("[INFO]: Leaving node: %s:%d, leavingNodeId: %d", leftNode.hostname, leftNode.port, leavingNodeId));
        
        VirtualNode[] selfVNodes = HashEntity.getInstance().getVNodeMap().get(DirectRoute.getInstance().getSelfNodeId());
        
        Set<VirtualNode> affectedVNodes = new HashSet<>();
        for (VirtualNode vnode : selfVNodes) {
            if (HashEntity.getInstance().isSuccessor(vnode, leavingNodeId, Protocol.REPLICATION_FACTOR - 1) ||
                HashEntity.getInstance().isPredecessor(vnode, leavingNodeId, 1)) {
                affectedVNodes.add(vnode);
            }
        }
        
        if (!affectedVNodes.isEmpty()) {
            ReplicationKVHandler.getInstance().replicateToSuccessors(affectedVNodes);
        }
        
        HashEntity.getInstance().removeNode(hostNameAndPort);
        DirectRoute.getInstance().removeNode(leavingNodeId);
    }
}
