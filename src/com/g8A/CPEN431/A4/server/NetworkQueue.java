package com.g8A.CPEN431.A4.server;

import com.g8A.CPEN431.A4.protocol.NetworkMessage;

public interface NetworkQueue {
    public NetworkMessage take() throws Exception;
}
