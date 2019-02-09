package com.g8A.CPEN431.A4.client;

import com.g8A.CPEN431.A4.server.distribution.RouteStrategy.AddressHolder;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public interface KVClient {
    /**
     * Send a given key/value request to another host.
     * @param request the key/value request to send.
     * @param holder the destination host.
     */
    public void send(KeyValueRequest.KVRequest request, AddressHolder holder);
}
