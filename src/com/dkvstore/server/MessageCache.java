package com.dkvstore.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.google.protobuf.ByteString;

/**
 * Caches the messages to ensure at-most-once semantics.
 * @author william
 *
 */
public class MessageCache {
    private static MessageCache mMessageCache = null;
    public static int SIZE_MAX_CACHE = 8 * 1024 * 1024; // Bytes
    private static final long TIMEOUT = 5000; // Timeout of cache entries in milliseconds.
    
    public static final ByteString ENTRY_BEING_PROCESSED = ByteString.copyFrom(new byte[Protocol.SIZE_MAX_VAL_LENGTH]);
    
    public static final class CacheEntry {
        public static final int SIZE_META_INFO = 12;
        public static final int SIZE_REFERENCE = 8;
        public final int metaInfo;
        public final ByteString value;
        public final int intField0;
        public final long timestamp; // In nano seconds
        
        CacheEntry(ByteString value, int metaInfo, int intField0) {
            this.value = value;
            this.timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            this.metaInfo = metaInfo;
            this.intField0 = intField0;
        }
    }
    
    private Map<ByteString, CacheEntry> mCache;
    private volatile int mSize;
    
    public static final int META_MASK_CACHE_REFERENCE = (1 << 31);
    
    /**
     * Set max cache size in bytes.
     * @param maxCacheSize
     */
    public static final void setMaxCacheSize(int maxCacheSize) {
        SIZE_MAX_CACHE = maxCacheSize;
    }
    
    private MessageCache() {
        mCache = new ConcurrentHashMap<>();
        Util.scheduler.scheduleAtFixedRate(new CacheCleaner(), 0, TIMEOUT / 2, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 
     * @param key
     * @param message
     * @param metaInfo
     * @throws OutOfMemoryError if cache limit is reached.
     */
    public synchronized void put(ByteString key, ByteString message, int metaInfo, int intField0) {
        CacheEntry entry = mCache.get(key);
        int entrySize = 0;
        if (entry != null) {
            if ((entry.metaInfo & META_MASK_CACHE_REFERENCE) != 0) {
                entrySize = key.size() + CacheEntry.SIZE_REFERENCE + CacheEntry.SIZE_META_INFO;
            }
            else {
                entrySize = key.size() + entry.value.size() + CacheEntry.SIZE_META_INFO;
            }
        }
        int update = -entrySize + key.size() + CacheEntry.SIZE_META_INFO;
        if ((metaInfo & META_MASK_CACHE_REFERENCE) != 0) {
            update += CacheEntry.SIZE_REFERENCE;
        }
        else {
            update += message.size();
        }
        if (mSize + update < SIZE_MAX_CACHE) {
            updateSize(update);
            mCache.put(key,  new CacheEntry(message, metaInfo, intField0));
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
     * @param metaInfo meta data used for cross layer optimization, least-significant 16 bits can be used by application
     *  to store additional meta info, while the other 16 bits are used to tweak caching policy for entry.
     * @return true if put was successul, false otherwise.
     * @throws OutOfMemoryError if cache limit is reached.
     */
    public synchronized boolean putIfNotExist(ByteString key, ByteString message, int metaInfo, int intField0) {
        if (!mCache.containsKey(key)) {
            put(key, message, metaInfo, intField0);
            return true;
        }
        return false;
    }
    
    public CacheEntry get(ByteString key) {
        return mCache.get(key);
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
    
    private class CacheCleaner implements Runnable {
        
        @Override
        public void run() {
            Iterator<Entry<ByteString, CacheEntry>> it = 
                    mCache.entrySet().iterator();
            Entry<ByteString, CacheEntry> tuple;
            CacheEntry entry;
            long time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

            int update = 0;
            while (it.hasNext()) {
                tuple = it.next();
                entry = tuple.getValue();
                if (entry.value == ENTRY_BEING_PROCESSED) {
                    continue;
                }
                if (time - entry.timestamp >= TIMEOUT) {
                    update += -(tuple.getKey().size() + CacheEntry.SIZE_META_INFO);
                    if ((entry.metaInfo & META_MASK_CACHE_REFERENCE) != 0) {
                        update -= CacheEntry.SIZE_REFERENCE;
                    }
                    else {
                        update -= entry.value.size();
                    }
                    it.remove();
                }
            }
            if (update != 0) {
                updateSize(update);
            }
        }
    }
}
