package com.g8A.CPEN431.A4.server.distribution;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 */
public class DirectRoute implements RouteStrategy {
    // TODO: Hard code node values here...
    
    @Override
    public AddressHolder getRoute(int nodeId) {
        // TODO: implement
        return null;
    }

}
