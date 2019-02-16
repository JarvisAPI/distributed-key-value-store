package com.g8A.CPEN431.A6.server.distribution;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.g8A.CPEN431.A6.protocol.Util;
import com.g8A.CPEN431.A6.server.KeyValueStore;
import com.google.protobuf.ByteString;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 * Updated to Singleton pattern
 */
public class DirectRoute implements RouteStrategy {    
	
	// For now the host name and ports of the nodes are hard coded
	private final int DEFAULT_PORT = 8082;
	private AddressHolder[] ipaddrs;
	private Map<Integer, AddressHolder> nodeIdMap = new HashMap<Integer, AddressHolder>();
    private final AddressHolder node1 = new AddressHolder("pl1.eng.monash.edu.au", DEFAULT_PORT); 
    private final AddressHolder node2 = new AddressHolder("planetlab-4.eecs.cwru.edu", DEFAULT_PORT);
    private final AddressHolder node3 = new AddressHolder("planetlab1.cs.ubc.ca", DEFAULT_PORT);
    private final AddressHolder node4 = new AddressHolder("planetlab2.cs.ubc.ca", DEFAULT_PORT);
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute(HashEntity hashCircle) {
    	ipaddrs = new AddressHolder[] { node1, node2, node3, node4 };
    	for(int i = 0; i < ipaddrs.length; i++) {
    		AddressHolder curAddr = ipaddrs[i];
    		ByteString hostnameAndPort = Util.concatHostnameAndPort(curAddr.hostname, curAddr.port);
    		try {
				int nodeId = hashCircle.getKVNodeId(hostnameAndPort);
				nodeIdMap.put(nodeId, curAddr);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
    	}
    }
    
    @Override
    /**
     * Get the addess and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to, null if value is 
     */
    public AddressHolder getRoute(int nodeId) {
    	switch(nodeId) {
    	case 1:
    		return node1;
    	case 2:
    		return node2;
    	case 3:
    		return node3;
    	case 4: 
    		return node4;
    	default: 
    		return null;
    	}
    }
    
    public static synchronized DirectRoute getInstance(HashEntity hashCircle) {
        if (mDirectRoute == null) {
        	mDirectRoute = new DirectRoute(hashCircle);
        }
        return mDirectRoute;
    }
    
    
}
