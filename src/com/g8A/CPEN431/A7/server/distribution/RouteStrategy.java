package com.g8A.CPEN431.A7.server.distribution;

import java.net.InetAddress;

public interface RouteStrategy {
    public class AddressHolder {
        public final InetAddress address;
        public final int port;
        
        public AddressHolder(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }
    }
    
    /**
     * Get the addess and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to
     */
    public AddressHolder getRoute(int nodeId);
}
