package com.g8A.CPEN431.A9.server;

import java.util.Set;

import com.g8A.CPEN431.A9.protocol.Util;
import com.g8A.CPEN431.A9.server.distribution.DirectRoute;
import com.g8A.CPEN431.A9.server.distribution.HashEntity;
import com.g8A.CPEN431.A9.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

public class MembershipService {
    /**
     * When a node joins adds the node to the hash ring + if necessary
     * begin copying keys over, via PUT requests. This method should only ever be
     * called by one thread.
     * @param nodeIdx the node index that joined.
     * @param shouldMigrate true if migration process should start, false otherwise. Useful if
     *  node might be temporarily assumed to be down but is actually up, this way migration
     *  does not have to start if it is due to a network problem.
     * @return true if migration process started, false if migration should be halting due to
     *   resource constraints.
     */
    public static void OnNodeJoin(AddressHolder joinedNode, boolean shouldMigrate) {  	
    	ByteString hostNameAndPort = Util.concatHostnameAndPort(joinedNode.hostname, joinedNode.port);   

        // add new node to hash ring so that now the requests can be routed correctly.
        int nodeId = HashEntity.getInstance().addNode(hostNameAndPort);
    	DirectRoute.getInstance().addNode(nodeId, joinedNode);
    	
    	if (shouldMigrate) {
            AddressHolder localAddress = DirectRoute.getInstance().getLocalAddress();
            ByteString localHostNameAndPort = Util.concatHostnameAndPort(localAddress.hostname, localAddress.port);
            
    	    // obtain set of affected nodes, by the new joining node.
            Set<ByteString> affectedNodes = HashEntity.getInstance().getAffectedNodesOnJoin(hostNameAndPort);
            
        	if(affectedNodes.contains(localHostNameAndPort)) {  
        	    MigrateKVHandler.getInstance().migrate(nodeId);
        	}
    	}
    }
    
    /**
     * When a node leaves, remove the node from the hash ring.
     * @param leftNode node that left.
     */
    public static void OnNodeLeft(AddressHolder leftNode) {
        ByteString hostNameAndPort = Util.concatHostnameAndPort(leftNode.hostname, leftNode.port);
        HashEntity.getInstance().removeNode(hostNameAndPort);
    }
}
