package com.g8A.CPEN431.A9.server;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

public class KeyValueStore {
    public static final class ValuePair {
        public static final int SIZE_META_INFO = 4;
        
        public final ByteString value;
        public final int version;
        
        public ValuePair(ByteString value, int version) {
            this.value = value;
            this.version = version;
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
     * @throws OutOfMemoryError if there is no more space to put values into
     *  the key value store.
     */
    public synchronized void put(ByteString key, ByteString value, int version) {
        if (mSize + key.size() + value.size() + ValuePair.SIZE_META_INFO > MAX_SIZE_BYTES) {
            throw new OutOfMemoryError();
        }
        ValuePair prevPair = mKeyValMap.put(key, new ValuePair(value, version));
        if (prevPair != null) {
            mSize -= (key.size() + prevPair.value.size() + ValuePair.SIZE_META_INFO);
        }
        mSize += key.size() + value.size() + ValuePair.SIZE_META_INFO;
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
     * Remove all keys currently in the keystore.
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
