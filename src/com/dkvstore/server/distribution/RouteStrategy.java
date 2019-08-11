package com.dkvstore.server.distribution;

import java.net.InetAddress;

public interface RouteStrategy {
    public class AddressHolder {
        public final InetAddress address;
        public final int port;
        public final int epidemicPort;
        public final String hostname;
        
        public AddressHolder(InetAddress address, int port) {
            this.address = address;
            this.port = port;
            this.hostname = null; // Ignored
            this.epidemicPort = 60000; // Ignored
        }
        
        public AddressHolder(InetAddress address, String hostname, int port) {
            this.address = address;
            this.port = port;
            this.hostname = hostname;
            this.epidemicPort = 60000; // Ignored
        }
        
        public AddressHolder(InetAddress address, String hostname, int port, int epidemicPort) {
            this.address = address;
            this.hostname = hostname;
            this.port = port;
            this.epidemicPort = epidemicPort;
        }
    }
    
    /**
     * Get the address and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to
     */
    public AddressHolder getRoute(int nodeId);
}
