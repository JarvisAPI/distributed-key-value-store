package com.s44801165.CPEN431.A3;

import com.s44801165.CPEN431.A3.protocol.NetworkMessage;

public interface MessageObserver {
    public enum MessageType {
        ERROR, CHECKSUM_ERROR, TIMEOUT, MSG_RECEIVED
    };
    
    /**
     * Method that is invoked when network message is received.
     * @param type the type of the update.
     * @param msg the network message.
     */
    public void update(MessageType type, NetworkMessage msg);
}
