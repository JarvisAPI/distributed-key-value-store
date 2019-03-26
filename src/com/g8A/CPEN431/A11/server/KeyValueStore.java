package com.g8A.CPEN431.A11.server;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

public class KeyValueStore {
    public static final class ValuePair {
        public static final int SIZE_META_INFO = 8;
        
        public ByteString value;
        public int version;
        public int sequenceStamp;
        
        public ValuePair(ByteString value, int version, int sequenceStamp) {
            this.value = value;
            this.version = version;
            this.sequenceStamp = sequenceStamp;
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
     * @param sequenceStamp the sequence stamp must be greater than the current sequence stamp for
     * the key if the value for the key were to be updated. However if the sequenceStamp is less than 0
     * then the entry is always overwritten and the new sequenceStamp will be the old sequenceStamp + 1.
     * 
     * @return the current entry mapped to by key.
     * 
     * @throws OutOfMemoryError if there is no more space to put values into
     *  the key value store.
     */
    public synchronized ValuePair put(ByteString key, ByteString value, int version, int sequenceStamp) {
        if (mSize + key.size() + value.size() + ValuePair.SIZE_META_INFO > MAX_SIZE_BYTES) {
            throw new OutOfMemoryError();
        }
        
        ValuePair entry = mKeyValMap.get(key);
        int stamp = 0;
        if (entry != null) {
            if (sequenceStamp < 0) {
                stamp = entry.sequenceStamp + 1;
            }
            else if (entry.sequenceStamp >= sequenceStamp) {
                return entry;
            }
            
            mSize -= (key.size() + entry.value.size() + ValuePair.SIZE_META_INFO);
            
            entry.value = value;
            entry.version = version;
            entry.sequenceStamp = stamp;
        }
        else {
            entry = new ValuePair(value, version, stamp);
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
