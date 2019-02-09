package com.g8A.CPEN431.A4.client;

import com.g8A.CPEN431.A4.server.distribution.RouteStrategy.AddressHolder;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import java.util.TimerTask;

/**
 * This class aims to provide (more) reliable transimission using
 * UDP while still adhering to at-most-once semantics. Therefore this
 * client retries up to the timeout limit assumed by the server.
 *
 */
public class ConcreteKVClient implements KVClient, Runnable {

    @Override
    public void send(KVRequest request, AddressHolder holder) {
        // TODO: implement, this function should really just place
        // the request onto the message queue for the thread that
        // actually runs this class.
    }

    @Override
    public void run() {
        // TODO: implement
        // Should dequeue from the message queue and send the request.
        // Also should probably listens for replies here as well.
    }
    
    public class RequestCleaner extends TimerTask {

        @Override
        public void run() {
            // TODO: implement
            // This should be run when request times out
            // So we should (1) decrement the timeout value for all other requests
            // (2) If request times out, then either retry or stop.
            // (3) After everything is done it should set the new timeout value
            // for when this task should be run again.
            
        }
        
    }

}
