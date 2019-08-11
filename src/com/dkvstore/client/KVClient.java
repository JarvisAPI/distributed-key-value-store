package com.dkvstore.client;

import com.dkvstore.NetworkMessage;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;

public interface KVClient {
    interface OnResponseReceivedListener {
        /**
         * A callback that gets triggered if a reply is received given a requestId.
         * @param requestId the requestId passed in when the request was sent.
         * @param msg the reply message.
         */
        void onResponseReceived(int requestId, NetworkMessage msg);
    }
    
    /**
     * Send a given key/value request to another host.
     * @param msg the network message to send, contains the address to send to.
     * @param fromAddress the address that request came from, or null if this is the node
     *   sending the request.
     */
    void send(NetworkMessage msg, AddressHolder fromAddress);
    
    /**
     * Send a given key/value request to another host.
     * @param msg the network message to send, contains the address to send to.
     * @param fromAddress the address that request came from, or null if this is the node
     *   sending the request.
     * @param requestId the id used to identify the request, must be >= 0.
     */
    void send(NetworkMessage msg, AddressHolder fromAddress, int requestId);
    
    /**
     * Sets the callback listener for when the response is received, optional.
     * @param listener the listener to be invoked on response received.
     */
    void setResponseListener(OnResponseReceivedListener listener);
}
