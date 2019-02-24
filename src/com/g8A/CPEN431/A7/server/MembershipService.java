package com.g8A.CPEN431.A7.server;

import java.util.List;
import java.util.Map;

import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.distribution.DirectRoute;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

public class MembershipService {
	/**
	 * Given a key from a KV pair and a list of affected ranges, determine if the key is within
	 * @param key
	 * @param affectedRanges
	 * @return true if key is within affected ranges
	 */
	public static boolean isKeyAffected(ByteString key, List<long[]> affectedRanges) {
		long hashedValue = HashEntity.getInstance().getKVNodeId(key);
		for(long[] range : affectedRanges) {
			if(hashedValue >= range[0] && hashedValue <= range[1]) return true;
		}
		return false;
	}
	
    /**
     * When a node joins adds the node to the hash ring + if necessary
     * begin copying keys over, via PUT requests.
     * @param joinedNode node that joined.
     */
    public static void OnNodeJoin(AddressHolder joinedNode) {
    	
    	AddressHolder localAddress = DirectRoute.getInstance().getLocalAddress();
    	
    	// obtain map of affected nodes, with ranges of hash values that need to be migrated on new node join
    	ByteString localHostNameAndPort = Util.concatHostnameAndPort(localAddress.address.getHostName(), localAddress.port);
    	
    	ByteString hostNameAndPort = Util.concatHostnameAndPort(joinedNode.hostname, joinedNode.port);   
        
        // add new node to hash ring so that now the requests can be routed correctly.
        int nodeId = HashEntity.getInstance().addNode(hostNameAndPort);
    	DirectRoute.getInstance().addNode(nodeId, joinedNode);
        System.out.println(String.format("NodeId: %d, hostname: %s, port: %d",
                nodeId, joinedNode.hostname, joinedNode.port));
    	
    	Map<ByteString, List<long[]>> affectedNodes = HashEntity.getInstance().getAffectedNodesOnJoin(hostNameAndPort);
    	
    	// if local address (this node) is affected, stop taking get requests and start copying keys over to new node
    	if(affectedNodes.containsKey(localHostNameAndPort)) {
    	    List<long[]> affectedRanges = affectedNodes.get(localHostNameAndPort);
    		MessageConsumer.startMigration(affectedRanges, joinedNode); // sets flag in MessageConsumer to bounce off get/put requests
    		
    		// Migration thread performs the following:
            // copy relevant keys over to the new node
            // delete nodes from old node
    	    new MigrateKVThread(affectedRanges, joinedNode).start();
    	}
    }
    
    /**
     * When a node leaves, remove the node from the hash ring.
     * @param leftNode node that left.
     */
    public static void OnNodeLeft(AddressHolder leftNode) {
        ByteString hostNameAndPort = Util.concatHostnameAndPort(leftNode.address.getHostName(), leftNode.port);
        HashEntity.getInstance().removeNode(hostNameAndPort);
    }
}
