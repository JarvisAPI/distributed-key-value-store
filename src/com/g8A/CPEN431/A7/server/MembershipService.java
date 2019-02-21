package com.g8A.CPEN431.A7.server;

import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;

public class MembershipService {
    /**
     * When a node joins adds the node to the hash ring + if necessary
     * begin copying keys over, via PUT requests.
     * @param holder
     */
    public void OnNodeJoin(AddressHolder holder) {
        
    }
    
    /**
     * When a node leaves, remove the node from the hash ring.
     * @param holder
     */
    public void OnNodeLeft(AddressHolder holder) {
        
    }
}
