package com.g8A.CPEN431.A6.server.distribution;

import com.g8A.CPEN431.A6.server.KeyValueStore;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 * Updated to Singleton pattern
 */
public class DirectRoute implements RouteStrategy {    
	
	// For now the host name and ports of the nodes are hard coded
	private final int DEFAULT_PORT = 5000;
    private final AddressHolder node1; 
    private final AddressHolder node2;
    private final AddressHolder node3; 
    private final AddressHolder node4; 
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
    	node1 = new AddressHolder("pl1.eng.monash.edu.au", DEFAULT_PORT);
    	node2 = new AddressHolder("planetlab-4.eecs.cwru.edu", DEFAULT_PORT);
    	node3 = new AddressHolder("planetlab1.cs.ubc.ca", DEFAULT_PORT);
    	node4 = new AddressHolder("planetlab2.cs.ubc.ca", DEFAULT_PORT);
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
    
    public static synchronized DirectRoute getInstance() {
        if (mDirectRoute == null) {
        	mDirectRoute = new DirectRoute();
        }
        return mDirectRoute;
    }
    
    
}
