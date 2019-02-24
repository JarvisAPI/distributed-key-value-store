package com.g8A.CPEN431.A7.server;

import java.util.List;
import java.util.Set;

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
     * begin copying keys over, via PUT requests. This method should only ever be
     * called by one thread.
     * @param joinedNode node that joined.
     */
    public static void OnNodeJoin(AddressHolder joinedNode) {
    	
    	AddressHolder localAddress = DirectRoute.getInstance().getLocalAddress();
    	
    	// obtain map of affected nodes, with ranges of hash values that need to be migrated on new node join
    	ByteString localHostNameAndPort = Util.concatHostnameAndPort(localAddress.hostname, localAddress.port);
    	
    	ByteString hostNameAndPort = Util.concatHostnameAndPort(joinedNode.hostname, joinedNode.port);   
        
        Set<ByteString> affectedNodes = HashEntity.getInstance().getAffectedNodesOnJoin(hostNameAndPort);
        
        System.out.println(String.format("[INFO]: Affected node size: %d", affectedNodes.size()));
        
        if (affectedNodes.contains(localHostNameAndPort) && MessageConsumer.isMigrating()) {
            System.out.println("[INFO]: Already migrating, waiting for completion before furthur migration");
            // Need to migrate but there is already a migrating thread, so best to wait for it
            // to finish and try again later.
            return;
        }
        
        // add new node to hash ring so that now the requests can be routed correctly.
        int nodeId = HashEntity.getInstance().addNode(hostNameAndPort);
    	DirectRoute.getInstance().addNode(nodeId, joinedNode);
        System.out.println(String.format("NodeId: %d, hostname: %s, port: %d",
                nodeId, joinedNode.hostname, joinedNode.port));
    	
    	// if local address (this node) is affected, stop taking get requests and start copying keys over to new node
        System.out.println(String.format("localHostNameAndPort: %s", Util.getHexString(localHostNameAndPort.toByteArray())));
    	if(affectedNodes.contains(localHostNameAndPort)) {
    	    System.out.println("[DEBUG]: Starting migration thread");
    	    
    		MessageConsumer.startMigration(nodeId); // sets flag in MessageConsumer to bounce off get/put requests
    		
    		// Migration thread performs the following:
            // copy relevant keys over to the new node
            // delete nodes from old node
    	    new MigrateKVThread(joinedNode).start();
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
