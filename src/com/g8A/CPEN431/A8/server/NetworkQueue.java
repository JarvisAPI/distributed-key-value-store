package com.g8A.CPEN431.A8.server;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;

public interface NetworkQueue {
    public NetworkMessage take() throws Exception;
}
