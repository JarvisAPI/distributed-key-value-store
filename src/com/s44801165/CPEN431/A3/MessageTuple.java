package com.s44801165.CPEN431.A3;

import com.s44801165.CPEN431.A3.protocol.NetworkMessage;

public class MessageTuple {
    public enum MessageType {
        ERROR, CHECKSUM_ERROR, TIMEOUT, MSG_RECEIVED
    };
    
    public final MessageType type;
    public final NetworkMessage message;
    
    public MessageTuple(MessageType type, NetworkMessage message) {
        this.type = type;
        this.message = message;
    }
}
