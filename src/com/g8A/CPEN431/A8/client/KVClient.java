package com.g8A.CPEN431.A8.client;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy.AddressHolder;

public interface KVClient {
    /**
     * Send a given key/value request to another host.
     * @param msg the network message to send, contains the address to send to.
     * @param fromAddress the address that request came from, or null if this is the node
     *   sending the request.
     * @throws IllegalStateException if the sending queue is full.
     */
    public void send(NetworkMessage msg, AddressHolder fromAddress) throws Exception;
}
