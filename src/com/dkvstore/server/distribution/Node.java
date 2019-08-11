package com.dkvstore.server.distribution;

public interface Node {

    /**
     * Returns a byte array that will be used for hashing on the node ring
     * @return
     */
    byte[] getKey();
}
