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
    private static final int HASH_CIRCLE_SIZE = 256;
    private static final int VIRTUAL_REPLICAS = 1;
    private final SortedMap<Integer, Integer> ring = new TreeMap<>();
    private final DirectRoute route = DirectRoute.getInstance();

    /**
     * Maps a SHA256 hash of the entry byte array to a value on the hash circle (0-255)
     * @param entry the byte array to be hashed
     * @return integer in the range of the hash circle
     */
    private int hash(byte[] entry) throws NoSuchAlgorithmException {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] hash = sha256.digest(entry);
        return hash[0];
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
     * @param nodeId the id of the node to add
     */
    public synchronized void addNode(int nodeId) throws NoSuchAlgorithmException {
        int hash;
        for(int i=0; i<VIRTUAL_REPLICAS; i++) {
            do {
                hash = hash((route.getRoute(nodeId).toString() + i).getBytes());
            } while (ring.containsKey(hash));

            ring.put(hash, nodeId);
        }
    }

    /**
     * Remove the node and its replicas from the ring
     * @param nodeId the id of the node to remove
     */
    public synchronized void removeNode(int nodeId) throws NoSuchAlgorithmException {
        int hash;
        for(int i=0; i<VIRTUAL_REPLICAS; i++) {
            hash = hash((route.getRoute(nodeId).toString() + i).getBytes());

            ring.remove(hash);
        }
    }

    /**
     * Relocate the range of requests corresponding to the added (or removed) node to the correct node.
     * @param KVNodeId the mapped id of the added or removed node
     */
    public void relocate(int KVNodeId) {
        // TODO: implement
    }

}
