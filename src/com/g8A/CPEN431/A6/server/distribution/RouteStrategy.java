package com.g8A.CPEN431.A6.server.distribution;

public interface RouteStrategy {
    public class AddressHolder {
        public final String hostname;
        public final int port;
        
        public AddressHolder(String hostname, int port) {
            this.hostname = hostname;
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
