package com.dkvstore.server.distribution;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
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
    private static int numVNodes = 10;
    
    private final Map<Integer, VirtualNode[]> vNodeMap = new ConcurrentHashMap<>();
    
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
     * Gets the virtual node that should store the given key.
     * @param key the key used in the key/value store.
     * @return the virtual node that should store key.
     */
    public VirtualNode getKVNode(ByteString key) {
        if(ring.isEmpty()) return null;

        long hash = hash(key.toByteArray());
        if(!ring.containsKey(hash)) {
            SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ?
                    ring.firstKey() : tailMap.firstKey();
        }

        return ring.get(hash);
    }
    
    /**
     * Get the nodes within distance that the key should be replicated across, the returned
     * node ids will not be the same as the node id associated with startvnode.
     * @param startvnode the virtual node to get successors of.
     * @param key the key used in the key/value store.
     * @param numSuccessors the number of successor nodes to replicate to.
     * @param buf the buffer to hold the node ids that should be returned, must be at least numSuccessors
     * in length.
     * @return the number of successor nodes placed in the buffer.
     */
    public int getSuccessorNodes(VirtualNode startvnode, int numSuccessors, int[] buf) {
        if(ring.isEmpty()) {
            return 0;
        }

        int numVNodesAdded = 0;
        VirtualNode vnode = startvnode;
        for(int i=0; i<numSuccessors; i++) {
            vnode = getNextVNode(vnode.getKey());
            if(vnode == null || vnode == startvnode) {
                break;
            }
            if (vnode.getPNodeId() == startvnode.getPNodeId()) {
                i--;
            }
            else {
                numVNodesAdded++;
                buf[i] = vnode.getPNodeId();
            }
        }

        return numVNodesAdded;
    }
    
    
    /**
     * Find out if the given physical node holdes a virtual node that is a predecessor within distance
     * away from another virtual node.
     * @param startvnode the virtual node to begin searching at.
     * @param matchNodeId the physical node id to match.
     * @param distance the max distance to check till.
     * @return true if the matchNodeId is a predecessor of distance or less away from startvnode.
     */
    public boolean isPredecessor(VirtualNode startvnode, int matchNodeId, int distance) {
        if(ring.isEmpty()) return false;

        VirtualNode vnode = startvnode;
        for(int i=0; i<distance; i++) {
            vnode = getPrevVNode(vnode.getKey());
            if (vnode == null) {
                break;
            }
            if (vnode.getPNodeId() == matchNodeId) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Find out if the given physical node holds a virtual node that is a successor within distance
     * away from another virtual node.
     * @param startvnode the virtual node the begin searching at.
     * @param matchNodeId the physical node id to match.
     * @param distance the max distance to check till.
     * @return true if the matchNodeId is a predecessor of distance or less away from startvnode.
     */
    public boolean isSuccessor(VirtualNode startvnode, int matchNodeId, int distance) {
        if(ring.isEmpty()) return false;

        VirtualNode vnode = startvnode;
        for(int i=0; i<distance; i++) {
            vnode = getNextVNode(vnode.getKey());
            if (vnode == null) {
                break;
            }
            if (vnode.getPNodeId() == matchNodeId) {
                return true;
            }
        }
        return false;
        
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
     * Get the node id of a node with a given physical node string. The physical
     * node string must be the same string passed in when the node is added.
     * @param pNode the physical node string.
     * @return the physical node id or -1 if no such node exists.
     */
    public int getNodeId(ByteString pNode) {
        return (int) (hash(pNode.toByteArray()) & (int) 0x7FFFFFFF);
    }

    /**
     * Adds the node and its replicas of virtual nodes to the ring
     * @param pNode the ByteString representing hostname+port of the node
     * @return the unique physical node id
     */
    public int addNode(ByteString pNode) {
        int pNodeId = getNodeId(pNode);
        //int pNodeId = getPhysicalNodeId(pNode);
        VirtualNode[] vNodes = new VirtualNode[numVNodes];
        for(int i=0; i<numVNodes; i++) {
            VirtualNode vNode = new VirtualNode(pNode, pNodeId, i);
            long hash = hash(vNode.getKey());

            ring.put(hash, vNode);
            vNodes[i] = vNode;
        }

        vNodeMap.put(pNodeId, vNodes);
        return pNodeId;
    }

    /**
     * Removes the node and its replicas of virtual nodes from the ring
     * @param pNode the node key string representing the physical node that should be removed
     */
    public void removeNode(ByteString pNode) {
        byte[] pNodeBytes = pNode.toByteArray();
        int nodeId = -1;
        int numRemoved = 0;
        for(int i = 0; i < numVNodes; i++) {
            long hash = hash(VirtualNode.getKey(pNodeBytes, i));
            VirtualNode vnode = ring.remove(hash);
            if (vnode != null) {
                nodeId = vnode.getPNodeId();
                numRemoved++;
            }
        }
        if (nodeId != -1) {
            System.out.println(String.format("[DEBUG]: HashEntity#removeNode, removed %d vnodes with nodeId: %d", numRemoved, nodeId));
            vNodeMap.remove(nodeId);
        }
        else {
            System.err.println("[WARNING]: HashEntity#removeNode, cannot find node to remove");
        }
    }
    
    public Map<Integer, VirtualNode[]> getVNodeMap() {
        return vNodeMap;
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
