package com.g8A.CPEN431.A4.server.distribution;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 */
public class DirectRoute implements RouteStrategy {
	// For now the host name and ports of the nodes are hard coded
	private static final int DEFAULT_PORT = 5000;
    private static final AddressHolder node1 = new AddressHolder("pl1.eng.monash.edu.au", DEFAULT_PORT);
    private static final AddressHolder node2 = new AddressHolder("planetlab-4.eecs.cwru.edu", DEFAULT_PORT);
    private static final AddressHolder node3 = new AddressHolder("planetlab1.cs.ubc.ca", DEFAULT_PORT);
    private static final AddressHolder node4 = new AddressHolder("planetlab2.cs.ubc.ca", DEFAULT_PORT);
    
    @Override
    /**
     * Get the addess and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to, null if value is 
     */
    public AddressHolder getRoute(int nodeId) {
    	if(nodeId >= 0 && nodeId < 64) {
        	return node1;
        }else if(nodeId >= 64 && nodeId < 128) {
        	return node2;
        }else if(nodeId >= 128 && nodeId < 192) {
        	return node3;
        }else if(nodeId >= 192 && nodeId < 256) {
        	return node4;
        }else {
        	return null;
        }
    }
    
    
}
