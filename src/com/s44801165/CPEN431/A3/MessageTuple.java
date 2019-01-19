package com.s44801165.CPEN431.A3;

import com.s44801165.CPEN431.A3.protocol.NetworkMessage;

public class MessageTuple {
    public enum MessageType {
        ERROR, CHECKSUM_ERROR, TIMEOUT, MSG_RECEIVED
    };
    
    public MessageType type;
    public NetworkMessage message;
    // The timeout amount in milliseconds if message type is TIMEOUT.
    public int timeout;
}
