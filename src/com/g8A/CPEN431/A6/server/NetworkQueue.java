package com.g8A.CPEN431.A6.server;

import com.g8A.CPEN431.A6.protocol.NetworkMessage;

public interface NetworkQueue {
    public NetworkMessage take() throws Exception;
}
