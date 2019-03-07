package com.g8A.CPEN431.A9.server.distribution;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Given the key in the key/value request, this class applies
 * a secure hash function to the key and uses the return value
 * to map to a location on a circle, which is then used to find
 * the destination node.
 * key -> secure hash -> node id from circle
 *
 */
public class HashEntity {
    private final ConcurrentSkipListMap<Long, VirtualNode> ring = new ConcurrentSkipListMap<>();
    private static HashEntity mHashEntity;
    private int uniquePNodeId = 0;
    private static int numVNodes = 10;
    
    /**
     * Maps a SHA256 hash of the entry byte array to a value on the hash circle (0...2^64-1)
     * @param entry the byte array to be hashed
     * @return long in the range of the hash circle
     */
    public long hash(byte[] entry) {
        MessageDigest instance;
        try {
            instance = MessageDigest.getInstance("MD5");
            byte[] hash;
            hash = instance.digest(entry);
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private HashEntity() {
    }

    /**
     * Gets the node id of the physical node that should store the given key.
     * @param key the key used in the key/value store.
     * @return the unique physical node id.
     */
    public int getKVNodeId(ByteString key) {
        if(ring.isEmpty()) return -1;

        long hash = hash(key.toByteArray());
        if(!ring.containsKey(hash)) {
            SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ?
                    ring.firstKey() : tailMap.firstKey();
        }

        return ring.get(hash).getPNodeId();
    }
    
    /**
     * Gets the previous virtual node on the ring given a virtual node key
     * @param key: the key of a virtual node
     * @return the predecessor virtual node
     */
    private VirtualNode getPrevVNode(byte[] vNodeKey) {
    	if(ring.isEmpty()) {
    	    return null;
    	}
    	
    	long curHash;
    	curHash = hash(vNodeKey);
        SortedMap<Long, VirtualNode> headMap = ring.headMap(curHash);
        long hash = headMap.isEmpty() ?
                ring.lastKey() : headMap.lastKey();
        
    	return curHash == hash ? null : ring.get(hash);
    }
    
    /**
     * Gets the next virtual node on the ring given a virtual node key
     * @param key: the key of a virtual node
     * @return the successor virtual node
     */
    private VirtualNode getNextVNode(byte[] vNodeKey) {
    	if(ring.isEmpty()) {
    	    return null;
    	}
    	
    	long curHash = hash(vNodeKey);
        SortedMap<Long, VirtualNode> tailMap = ring.tailMap(curHash + 1);
        long hash = tailMap.isEmpty() ?
                    ring.firstKey() : tailMap.firstKey();
                    
    	return curHash == hash ? null : ring.get(hash);
    }
    
    /**
     * Gets a set of physical nodes that will be affected if new node joins.
     * @param pNode: the pNode key string of the node that is joining.
     * @return a set of pNode key strings representing the nodes affected.
     */
    public Set<ByteString> getAffectedNodesOnJoin(ByteString pNode) {
        Set<ByteString> affectedNodes = new HashSet<ByteString>();
        
    	if(ring.isEmpty()) {
    	    return affectedNodes;
    	}
    	
    	byte[] pNodeBytes = pNode.toByteArray();
    	for(int i = 0; i < numVNodes; i++) {
    		byte[] vNodeKey = VirtualNode.getKey(pNodeBytes, i);
    		VirtualNode prevVNode = getPrevVNode(vNodeKey);
    		VirtualNode nextVNode = getNextVNode(vNodeKey);
    		
    		if (prevVNode != null) {
    		    if (nextVNode != null) {
    		        affectedNodes.add(nextVNode.getPNode());
    		    }
    		    else {
    		        System.err.println("[WARNING]: HashEntity: Bug nextVNode should not be null.");
    		    }
    		}
    	}
    	
    	return affectedNodes;
    }
    
    /**
     * Given a key, returns the raw hash value
     * @param key
     * @return hashed value of key
     */
    public long getHashValue(ByteString key) {
    	return hash(key.toByteArray());
    }

    /**
     * Adds the node and its replicas of virtual nodes to the ring
     * @param pNode the ByteString representing hostname+port of the node
     * @return the unique physical node id
     */
    public int addNode(ByteString pNode) {
        int pNodeId = uniquePNodeId;
        for(int i=0; i<numVNodes; i++) {
            VirtualNode vNode = new VirtualNode(pNode, pNodeId, i);
            long hash = hash(vNode.getKey());

            ring.put(hash, vNode);
        }

        uniquePNodeId++;
        return pNodeId;
    }

    /**
     * Removes the node and its replicas of virtual nodes from the ring
     * @param pNode the node key string representing the physical node that should be removed
     */
    public void removeNode(ByteString pNode) {
        byte[] pNodeBytes = pNode.toByteArray();
        for(int i = 0; i < numVNodes; i++) {
            long hash = hash(VirtualNode.getKey(pNodeBytes, i));
            VirtualNode vnode = ring.get(hash);
            if (vnode != null) {
                if(vnode.isVirtualNodeOf(pNode)) {
                    ring.remove(hash);
                }
            }
        }
    }

    public static void setNumVNodes(int numVNodes) {
        HashEntity.numVNodes = numVNodes;
    }
    
    public static synchronized HashEntity getInstance() {
        if (mHashEntity == null) {
        	mHashEntity = new HashEntity();
        }
        return mHashEntity;
    }
}