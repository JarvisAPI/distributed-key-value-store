package com.g8A.CPEN431.A6.server.distribution;

import com.google.protobuf.ByteString;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Given the key in the key/value request, this class applies
 * a secure hash function to the key and uses the return value
 * to map to a location on a circle, which is then used to find
 * the destination node.
 * key -> secure hash -> node id from circle
 *
 */
public class HashEntity {
    // HASH_CIRCLE_SIZE = 256;
    private final SortedMap<Integer, Integer> ring = new TreeMap<>();
    
    private static HashEntity mHashEntity;
    
    private HashEntity() {
        
    }

    /**
     * Maps a SHA256 hash of the entry byte array to a value on the hash circle (0-255)
     * @param entry the byte array to be hashed
     * @return integer in the range of the hash circle
     */
    private int hash(byte[] entry) throws NoSuchAlgorithmException {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] hash = sha256.digest(entry);
        return (int)hash[0] & 0xff;
    }

    /**
     * Gets the node id of the node that should store the given key.
     * @param key the key used in the key/value store.
     * @return the node id.
     */
    public int getKVNodeId(ByteString key) throws NoSuchAlgorithmException {
        if(ring.isEmpty()) return -1;

        int hash = hash(key.toByteArray());
        if(!ring.containsKey(hash)) {
            // TODO: compare space/performance of using tailMap vs binary search
            SortedMap<Integer, Integer> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ?
                    ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    /**
     * Adds the node and its replicas of virtual nodes to the ring
     * @param node the ByteString representing hostname+port of the node
     * @return the node id
     */
    public synchronized int addNode(ByteString node) throws NoSuchAlgorithmException {
        int hash = hash(node.toByteArray());

        for(int i= 0; ring.containsKey(hash); i++) {
            hash = hash((node.toString() + i).getBytes());
        }

        int nodeId = ring.size();
        ring.put(hash, nodeId);
        return nodeId;
    }
    
    public static synchronized HashEntity getInstance() {
        if (mHashEntity == null) {
        	mHashEntity = new HashEntity();
        }
        return mHashEntity;
    }
}
