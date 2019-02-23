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
     * @param holder
     */
    public void OnNodeJoin(AddressHolder holder) {
    	
    	AddressHolder localAddress = DirectRoute.getInstance().getLocalAddress();
    	
    	// obtain map of affected nodes, with ranges of hash values that need to be migrated on new node join
    	ByteString localHostNameAndPort = Util.concatHostnameAndPort(localAddress.address.getHostName(), localAddress.port);
    	ByteString hostNameAndPort = Util.concatHostnameAndPort(holder.address.getHostName(), holder.port);    	
    	Map<ByteString, List<long[]>> affectedNodes = HashEntity.getInstance().getAffectedNodesOnJoin(hostNameAndPort);
    	
    	// if local address (this node) is affected, stop taking get requests and start copying keys over to new node
    	if(affectedNodes.containsKey(localHostNameAndPort)) {
    		MessageConsumer.startMigration(affectedNodes.get(localHostNameAndPort), holder); // sets flag in MessageConsumer to bounce off get/put requests
    		
    		// copy relevant keys over to the new node (may be triggered by message consumer flag?)
    		
    		// delete nodes from old node
    		
    	}
    	
    	// add new node to hash ring
    }
    
    /**
     * When a node leaves, remove the node from the hash ring.
     * @param holder
     */
    public void OnNodeLeft(AddressHolder holder) {
        ByteString hostNameAndPort = Util.concatHostnameAndPort(holder.address.getHostName(), holder.port);
        HashEntity.getInstance().removeNode(hostNameAndPort);
    }
}
