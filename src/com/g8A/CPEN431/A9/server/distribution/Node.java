package com.g8A.CPEN431.A9.server.distribution;

public interface Node {

    /**
     * Returns a byte array that will be used for hashing on the node ring
     * @return
     */
    byte[] getKey();
}
