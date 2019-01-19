package com.s44801165.CPEN431.A3.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

public class KeyValueStore {
    public static final class ValuePair {
        public final ByteString value;
        public final int version;
        
        public ValuePair(ByteString value, int version) {
            this.value = value;
            this.version = version;
        }
    }
    
    private static KeyValueStore mKeyValueStore;
    private Map<ByteString, ValuePair> mKeyValMap;
    
    private KeyValueStore() {
        mKeyValMap = new ConcurrentHashMap<>();
    }
    
    public void put(ByteString key, ByteString value, int version) {
        mKeyValMap.put(key, new ValuePair(value, version));
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
    public boolean remove(ByteString key) {
        return mKeyValMap.remove(key) != null;
    }
    
    /**
     * Remove all keys currently in the keystore.
     */
    public void removeAll() {
        mKeyValMap.clear();
    }
    
    public static synchronized KeyValueStore getInstance() {
        if (mKeyValueStore == null) {
            mKeyValueStore = new KeyValueStore();
        }
        return mKeyValueStore;
    }
}
