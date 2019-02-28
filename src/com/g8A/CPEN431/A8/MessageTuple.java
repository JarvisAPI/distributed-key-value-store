package com.g8A.CPEN431.A8;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;

public class MessageTuple {
    public enum MessageType {
        ERROR, CHECKSUM_ERROR, TIMEOUT, MSG_RECEIVED
    };
    
    public MessageType type;
    public NetworkMessage message;
    // The timeout amount in milliseconds if message type is TIMEOUT.
    public int timeout;
}
