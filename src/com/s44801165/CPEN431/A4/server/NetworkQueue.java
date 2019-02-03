package com.s44801165.CPEN431.A4.server;

import com.s44801165.CPEN431.A4.protocol.NetworkMessage;

public interface NetworkQueue {
    public NetworkMessage take() throws Exception;
}
