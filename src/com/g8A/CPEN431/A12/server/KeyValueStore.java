package com.g8A.CPEN431.A12.server;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.g8A.CPEN431.A12.server.distribution.NodeTable;
import com.google.protobuf.ByteString;

public class KeyValueStore {
    public static final class ValuePair {
        public static final int SIZE_META_INFO = 8;
        
        public final ByteString value;
        public final int version;
        public final int sequenceStamp;
        public final int[] vectorClock;
        
        public ValuePair(ByteString value, int version, int sequenceStamp, int[] vectorClock) {
            this.value = value;
            this.version = version;
            this.sequenceStamp = sequenceStamp;
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
     * @param sequenceStamp the sequence stamp must be greater than the current sequence stamp for
     * the key if the value for the key were to be updated. However if the sequenceStamp is less than 0
     * then the entry is always overwritten and the new sequenceStamp will be the old sequenceStamp + 1.
     * 
     * @return the current entry mapped to by key.
     * 
     * @throws OutOfMemoryError if there is no more space to put values into
     *  the key value store.
     */
    public synchronized ValuePair put(ByteString key, ByteString value, int version, int sequenceStamp, List<Integer> vectorClock, int vectorClockLength) {
        if (mSize + key.size() + value.size() + ValuePair.SIZE_META_INFO > MAX_SIZE_BYTES) {
            throw new OutOfMemoryError();
        }
        
        // vector clock update logic
    	ValuePair entry = mKeyValMap.get(key);
        int stamp = 0;
        int numOfNodes = NodeTable.getInstance().getNumberOfNodes();
        int selfNodeId = NodeTable.getInstance().getSelfNodeIdx();
        int[] incomingVClock = vectorClock.stream().mapToInt(Integer::intValue).toArray();
        
        // Case1: request is received from the client
        if( vectorClockLength == 0) {
            if (entry != null) {
            	int[] curVClock = entry.vectorClock;
            	
                if (sequenceStamp < 0) {
                    stamp = entry.sequenceStamp + 1;
                    entry.vectorClock[selfNodeId] = stamp;
                }
                else if (entry.sequenceStamp >= sequenceStamp) {
                    return entry;
                }
                
                mSize -= (key.size() + entry.value.size() + ValuePair.SIZE_META_INFO);
                
                entry = new ValuePair(entry.value, entry.version, stamp, entry.vectorClock);
            }
            else {
            	int[] newVClock = new int[numOfNodes];
            	newVClock[selfNodeId] = 1;
                entry = new ValuePair(value, version, stamp, newVClock);
                mKeyValMap.put(key, entry);
            }
            mSize += key.size() + value.size() + ValuePair.SIZE_META_INFO;
            
            return entry;
        }
        
        // Case2: Request received from another node
        else {
        	if (entry != null) {
            	int[] curVClock = entry.vectorClock;
            	int compareVClockRes = compareVectorClock(incomingVClock, curVClock);
            	
                if (sequenceStamp < 0) {
                    stamp = entry.sequenceStamp + 1;
                }
                else if (entry.sequenceStamp >= sequenceStamp) {
                    return entry;
                }
		        // Case A: vector is smaller - ignore
                
		        if(compareVClockRes == -1) {
		        	return entry;
		        }else if(compareVClockRes == 0) {
		        	return entry; // drop the request for now
		        }else if(compareVClockRes == 1) {
		        	entry = new ValuePair(value, version, stamp, incomingVClock);
	                mKeyValMap.put(key, entry);
		        }
		       	// Case B: Vector is neither smaller nor bigger, then try picking one arbitrarily
                
                mSize -= (key.size() + entry.value.size() + ValuePair.SIZE_META_INFO);
                
                entry = new ValuePair(entry.value, entry.version, stamp, incomingVClock);
            }
            else { // put entry into table if it doesn't exist
            	int[] newVClock = new int[numOfNodes];
            	newVClock[selfNodeId] = 1;
                entry = new ValuePair(value, version, stamp, newVClock);
                mKeyValMap.put(key, entry);
            }
            mSize += key.size() + value.size() + ValuePair.SIZE_META_INFO;
            
            return entry;
        	
            
           
        	
        }
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
    
    /**
     * 
     * @param vClockA - clock
     * @param vClockB - clock you with to compare to
     * @return -1 if vClockA is smaller than vClockB, 0 if equal, 1 if larger
     */
    public int compareVectorClock(int[] vClockA, int[] vClockB) {
    	int i=0;
    	int compareResult = 0;
    	while(i < vClockA.length && i < vClockB.length) {
    		if(vClockA[i] > vClockB[i]) {
    			if(compareResult == -1) {
    				return 0;
    			}else {
    				compareResult = 1;
    			}
    		}else if(vClockA[i] < vClockB[i]) {
    			if(compareResult == 1) {
    				return 0;
    			}else {
    				compareResult = -1;
    			}
    		}
    		// continue otherwise
    		i++;
    	}
    	return compareResult;
    }
    
    public static synchronized KeyValueStore getInstance() {
        if (mKeyValueStore == null) {
            mKeyValueStore = new KeyValueStore();
        }
        return mKeyValueStore;
    }
}
