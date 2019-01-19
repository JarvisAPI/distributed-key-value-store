package com.s44801165.CPEN431.A3.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

/**
 * Caches the messages to ensure at-most-once semantics.
 * @author william
 *
 */
public class MessageCache {
    private static MessageCache mMessageCache = null;
    private static final int SIZE_MAX_CACHE = 256;
    private static final int TIMEOUT = 5000; // Timeout of cache entries in milliseconds
    
    public static final ByteString ENTRY_BEING_PROCESSED = ByteString.copyFrom(new byte[0]);
    
    private static final class CacheEntry {
        ByteString value;
        int timeout; // In milliseconds
        
        CacheEntry(ByteString value, int timeout) {
            this.value = value;
            this.timeout = timeout;
        }
    }
    
    private Map<ByteString, CacheEntry> mCache;
    
    
    private MessageCache() {
        mCache = new ConcurrentHashMap<>(SIZE_MAX_CACHE);
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new CacheCleaner(), 0, TIMEOUT / 2);
    }
    
    /**
     * 
     * @param key
     * @param message
     * @throws OutOfMemoryError if cache limit is reached.
     */
    public void put(ByteString key, ByteString message) {
        if (mCache.size() < SIZE_MAX_CACHE || mCache.containsKey(key)) {
            mCache.put(key,  new CacheEntry(message, TIMEOUT));
            return;
        }
        throw new OutOfMemoryError();
    }
    
    /**
     * put only if the key does not exist in the map,
     *  this method is thread safe. That is if two threads call this method
     *  then only on of the puts will succeed.
     * @param key
     * @param message
     * @return true if put was successul, false otherwise.
     * @throws OutOfMemoryError if cache limit is reached.
     */
    public synchronized boolean putIfNotExist(ByteString key, ByteString message) {
        CacheEntry entry = mCache.get(key);
        if (entry == null) {
            put(key, message);
            return true;
        }
        return false;
    }
    
    public ByteString get(ByteString key) {
        CacheEntry entry = mCache.get(key);
        return entry != null ? entry.value : null;
    }
    
    public static synchronized MessageCache getInstance() {
        if (mMessageCache == null) {
            mMessageCache = new MessageCache();
        }
        return mMessageCache;
    }
    
    
    private class CacheCleaner extends TimerTask {
        private long elapsedTime = System.nanoTime();
        
        @Override
        public void run() {
            Iterator<Entry<ByteString, CacheEntry>> it = 
                    mCache.entrySet().iterator();
            CacheEntry entry;
            long time = System.nanoTime();
            long elapsedTimeNano = time - elapsedTime;
            int elapsedTimeMillis = (int) TimeUnit.NANOSECONDS.toMillis(elapsedTimeNano);

            while (it.hasNext()) {
                entry = it.next().getValue();
                entry.timeout -= elapsedTimeMillis;
                if (entry.timeout <= 0) {
                    it.remove();
                }
            }
            elapsedTime = time;
        }
    }
}
