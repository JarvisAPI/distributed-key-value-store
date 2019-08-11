package com.dkvstore.server;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.dkvstore.Util;
import com.dkvstore.server.distribution.DirectRoute;
import com.dkvstore.server.distribution.HashEntity;
import com.dkvstore.server.distribution.NodeTable;
import com.dkvstore.server.distribution.VectorClock;
import com.dkvstore.server.distribution.VectorClock.CompareResult;
import com.google.protobuf.ByteString;

public class KeyValueStore {
    public static final class ValuePair {
        public static final int SIZE_META_INFO = 8;
        
        public final ByteString value;
        public final int version;
        public int[] vectorClock;
        
        public ValuePair(ByteString value, int version, int[] vectorClock) {
            this.value = value;
            this.version = version;
            this.vectorClock = vectorClock;
        }
    }
    
    // Maximum number of bytes that is allowed in the key value store.
    public static int MAX_SIZE_BYTES = 40 * 1024 * 1024;
    private volatile int mSize;
    private static KeyValueStore mKeyValueStore;
    private Map<ByteString, ValuePair> mKeyValMap;
    
    private KeyValueStore() {
        mKeyValMap = new ConcurrentHashMap<>();
        mSize = 0;
    }
    
    /**
     * Set max key value store size in bytes.
     * @param maxCacheSize
     */
    public static final void setMaxCacheSize(int maxKeyValueStoreSize) {
        MAX_SIZE_BYTES = maxKeyValueStoreSize;
    }
    
    /**
     * 
     * @param key
     * @param value
     * @param version
     * @param vectorClock
     * 
     * @return the current entry mapped to by key.
     * 
     * @throws OutOfMemoryError if there is no more space to put values into
     *  the key value store.
     */
    public synchronized ValuePair put(ByteString key, ByteString value, int version, int[] vectorClock) {
        if (mSize + key.size() + value.size() + ValuePair.SIZE_META_INFO > MAX_SIZE_BYTES) {
            throw new OutOfMemoryError();
        }
        
    	ValuePair entry = mKeyValMap.get(key);
        
        // if entry is in store
        if (entry != null) {
        	int[] curVClock = entry.vectorClock;
        	
            if (vectorClock == null) {
                // Case1: request is received from the client
                curVClock = VectorClock.incrementVectorClock(curVClock, DirectRoute.getInstance().getSelfNodeId());
            	
                entry = new ValuePair(value, version, curVClock);
                mKeyValMap.put(key, entry);
            }
            else {
                CompareResult compResult = VectorClock.compareVectorClock(vectorClock, curVClock);
                
                // Case2: Request received from another node
                switch(compResult) {
                case Larger:
                    entry = new ValuePair(value, version, vectorClock);
                    mKeyValMap.put(key, entry);
                    break;
                case Uncomparable:
                    // If Uncomparable: We can pick either current or received value.
                    // We pick one value randomly so that if this does happen it will eventually
                    // lead to one consistent version throughout the key store, if we always
                    // deterministically pick one then we could end up having two divergent copies
                    // forever.
                    if (Util.rand.nextBoolean()) {
                        entry = new ValuePair(value, version, vectorClock);
                        mKeyValMap.put(key, entry);
                        break;
                    }
                default:
                    // If Smaller: Ignore PUT, since it is old value.
                    // If Equal: The two values should be the same so nothing needs to be done.
                    return entry;
                }
            }
            mSize -= (key.size() + entry.value.size() + ValuePair.SIZE_META_INFO);
        }
        else {
            // if entry is not in store
            int[] newVClock;
            if (vectorClock == null) {
                // Case 1: request is from client.
                newVClock = VectorClock.create(DirectRoute.getInstance().getSelfNodeId(), 1);
            }
            else {
                // Case 2: request is from other node.
                newVClock = vectorClock;
            }
            entry = new ValuePair(value, version, newVClock);
            mKeyValMap.put(key, entry);
        }
        mSize += key.size() + value.size() + ValuePair.SIZE_META_INFO;
        
        return entry;
    }
    
    /**
     * Get value pair from the key store.
     * @param key the key in the key value store.
     * @param pair the value pair to store retrieved data in.
     * @return the key value pair if it exists, otherwise null is returned.
     */
    public ValuePair get(ByteString key) {
        return mKeyValMap.get(key);
    }
    
    /**
     * Removes a value pair associated with a key in the store.
     * @param key the key to the value pair.
     * @return true if key was mapped to a value pair and that value pair
     *  was removed, false if key was not mapped to any value pair.
     */
    public synchronized boolean remove(ByteString key) {
        ValuePair prevVal = mKeyValMap.remove(key);
        if (prevVal != null) {
            mSize -= (key.size() + prevVal.value.size() + ValuePair.SIZE_META_INFO);
            return true;
        }
        return false;
    }
    
    /**
     * Remove all keys currently in the key store.
     */
    public synchronized void removeAll() {
        mSize = 0;
        mKeyValMap.clear();
    }
    
    /**
     * Obtain set of all keys in KVStore
     * @return set of keys
     */
    public Set<ByteString> getKeys() {
    	return mKeyValMap.keySet();
    }
    
    public static synchronized KeyValueStore getInstance() {
        if (mKeyValueStore == null) {
            mKeyValueStore = new KeyValueStore();
        }
        return mKeyValueStore;
    }
}
