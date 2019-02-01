package com.s44801165.CPEN431.A4.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A4.protocol.Protocol;

/**
 * Caches the messages to ensure at-most-once semantics.
 * @author william
 *
 */
public class MessageCache {
    private static MessageCache mMessageCache = null;
    private static final int SIZE_MAX_CACHE = 10 * 1024 * 1024; // Bytes
    private static final int TIMEOUT = 5000; // Timeout of cache entries in milliseconds
    
    public static final ByteString ENTRY_BEING_PROCESSED = ByteString.copyFrom(new byte[Protocol.SIZE_MAX_VAL_LENGTH]);
    
    private static final class CacheEntry {
        public static final int SIZE_META_INFO = 4;
        ByteString value;
        int timeout; // In milliseconds
        
        CacheEntry(ByteString value, int timeout) {
            this.value = value;
            this.timeout = timeout;
        }
    }
    
    private Map<ByteString, CacheEntry> mCache;
    private volatile int mSize;
    
    
    private MessageCache() {
        mCache = new ConcurrentHashMap<>();
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new CacheCleaner(), 0, TIMEOUT / 2);
    }
    
    /**
     * 
     * @param key
     * @param message
     * @throws OutOfMemoryError if cache limit is reached.
     */
    public synchronized void put(ByteString key, ByteString message) {
        CacheEntry entry = mCache.get(key);
        int entrySize = 0;
        if (entry != null) {
            entrySize = key.size() + entry.value.size() + CacheEntry.SIZE_META_INFO;
        }
        int update = -entrySize + key.size() + message.size() + CacheEntry.SIZE_META_INFO;
        if (mSize + update < SIZE_MAX_CACHE) {
            updateSize(update);
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
        if (!mCache.containsKey(key)) {
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
    
    private synchronized void updateSize(int update) {
        mSize += update;
    }
    
    private class CacheCleaner extends TimerTask {
        private long elapsedTime = System.nanoTime();
        
        @Override
        public void run() {
            Iterator<Entry<ByteString, CacheEntry>> it = 
                    mCache.entrySet().iterator();
            Entry<ByteString, CacheEntry> tuple;
            CacheEntry entry;
            long time = System.nanoTime();
            long elapsedTimeNano = time - elapsedTime;
            int elapsedTimeMillis = (int) TimeUnit.NANOSECONDS.toMillis(elapsedTimeNano);

            int update = 0;
            while (it.hasNext()) {
                tuple = it.next();
                entry = tuple.getValue();
                entry.timeout -= elapsedTimeMillis;
                if (entry.timeout <= 0) {
                    update += -(tuple.getKey().size() + entry.value.size() + CacheEntry.SIZE_META_INFO);
                    it.remove();
                }
            }
            if (update != 0) {
                updateSize(update);
            }
            elapsedTime = time;
        }
    }
}
