package com.g8A.CPEN431.A6.server.distribution;

import com.google.protobuf.ByteString;

/**
 * Given the key in the key/value request, this class applies
 * a secure hash function to the key and uses the return value
 * to map to a location on a circle, which is then used to find
 * the destination node.
 * key -> secure hash -> node id from circle
 *
 */
public class HashEntity {
    private static final int HASH_CIRCLE_SIZE = 256;
    /**
     * Gets the node id of the node that should store the given key.
     * @param key the key used in the key/value store.
     * @return the node id.
     */
    public int getKVNodeId(ByteString key) {
        // TODO: implement
        return 0;
    }
}
