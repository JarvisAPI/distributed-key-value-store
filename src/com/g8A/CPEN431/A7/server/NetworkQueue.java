package com.g8A.CPEN431.A7.server;

import com.g8A.CPEN431.A7.protocol.NetworkMessage;

public interface NetworkQueue {
    public NetworkMessage take() throws Exception;
}
