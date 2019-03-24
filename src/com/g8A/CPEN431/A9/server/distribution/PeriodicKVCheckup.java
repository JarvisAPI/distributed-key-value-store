package com.g8A.CPEN431.A9.server.distribution;

import com.g8A.CPEN431.A9.protocol.Protocol;
import com.g8A.CPEN431.A9.protocol.Util;
import com.g8A.CPEN431.A9.server.KeyValueStore;
import com.g8A.CPEN431.A9.server.distribution.DirectRoute;
import com.g8A.CPEN431.A9.server.distribution.HashEntity;
import com.g8A.CPEN431.A9.server.distribution.VirtualNode;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

/**
 * Periodically check the keys in the KeyValue store, if a key K is mapped to a predecessor node that is
 * greater than (N-1) distance away on the hash ring then K is removed from the KeyValue store
 */
public class PeriodicKVCheckup {
    public static int CHECKUP_INTERVAL = 300000; // 5 minutes
    private static PeriodicKVCheckup mPeriodicKVCheckup;
    private KeyValueStore mKVStore;
    private HashEntity mHashEntity;
    private DirectRoute mDirectRoute;
    private int mSelfNodeId;

    private PeriodicKVCheckup() {
        mKVStore = KeyValueStore.getInstance();
        mHashEntity = HashEntity.getInstance();
        mDirectRoute = DirectRoute.getInstance();
        mSelfNodeId = mDirectRoute.getSelfNodeId();
    }

    public static synchronized PeriodicKVCheckup getInstance() {
        if (mPeriodicKVCheckup == null) {
            mPeriodicKVCheckup = new PeriodicKVCheckup();
        }
        return mPeriodicKVCheckup;
    }

    public void start() {
        Util.timer.schedule(new PeriodicKVCheckupTask(), CHECKUP_INTERVAL);
    }

    public class PeriodicKVCheckupTask extends TimerTask {
        @Override
        public void run() {
            try {
                System.out.println("[INFO]: Starting periodic kv checkup task");
                
                Set<ByteString> keys = mKVStore.getKeys();
                System.out.println(String.format("[INFO]: periodic kv checkup, checking %d keys", keys.size()));
                
                int numKeysRemoved = 0;
                
                VirtualNode[] selfVNodes = mHashEntity.getVNodeMap().get(mSelfNodeId);
                Map<Integer, Boolean> isPredecessorMap = new HashMap<>();
                for(ByteString key : keys) {
                    int nodeId = mHashEntity.getKVNodeId(key);
                    if(nodeId != mSelfNodeId) {
                        if (!isPredecessorMap.containsKey(nodeId)) {
                            boolean isPredecessor = false;
                            for (int i = 0; i < selfVNodes.length; i++) {
                                if (mHashEntity.isPredecessor(selfVNodes[i], nodeId, Protocol.REPLICATION_FACTOR - 1)) {
                                    isPredecessor = true;
                                    break;
                                }
                            }
                            isPredecessorMap.put(nodeId, isPredecessor);
                        }
                        if(!isPredecessorMap.get(nodeId)) {
                            numKeysRemoved++;
                            mKVStore.remove(key);
                        }
                    }
                }
                
                System.out.println(String.format("[INFO]: periodic kv checkup, removed %d keys", numKeysRemoved));
                
            } catch(Exception e) {
                e.printStackTrace();
            } finally {
                start();
            }
        }
    }


}